"""
GOLD LAYER — RepoSea Business Logic
========================================
Consumes the Silver merged DataFrame and produces the final Gold output table.

Key computations:
  • Buffer (days) = ETA_Destination − Contract_Last_Free_Day
  • Action Status:  Critical (buffer < 0) | Warning (0–2 days) | Safe (> 2 days)
  • Burn Rate:      penalty accrued (critical) or daily per-diem exposure (others)
  • Priority Score: weighted rank for operations triage queue

Output:
  • data/gold/containers.json   — consumed by GitHub Pages dashboard
  • data/gold/containers.parquet — connects to Tableau / Power BI
  • data/gold/summary.json      — KPI aggregates for API consumption
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
try:
    from pipeline import history_tracker
except ImportError:
    import history_tracker

import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s [GOLD] %(message)s")
log = logging.getLogger(__name__)

SILVER_DIR = Path(__file__).parent.parent / "data" / "silver"
GOLD_DIR   = Path(__file__).parent.parent / "data" / "gold"
GOLD_DIR.mkdir(parents=True, exist_ok=True)

# ── Business Logic Config ─────────────────────────────────────────────────────

WARNING_THRESHOLD_DAYS  = 2   # buffer ≤ 2 → Warning
CRITICAL_THRESHOLD_DAYS = 0   # buffer ≤ 0 → Critical

TRADE_LANE_LABELS = {
    "TRANS_PAC":  "Trans-Pacific",
    "TRANS_ATL":  "Trans-Atlantic",
    "ASIA_EUR":   "Asia-Europe",
    "INTRA_ASIA": "Intra-Asia",
}

LOCATION_LABELS = {
    "CNSHA": "Shanghai, CN",  "NLRTM": "Rotterdam, NL",  "DEHAM": "Hamburg, DE",
    "USLAX": "Los Angeles, US","USNYC": "New York, US",   "SGSIN": "Singapore, SG",
    "KRPUS": "Busan, KR",     "TWKHH": "Kaohsiung, TW",  "GBFXT": "Felixstowe, GB",
    "BEANR": "Antwerp, BE",   "USLONG": "Long Beach, US", "USSEA": "Seattle, US",
    "USOAK": "Oakland, US",   "USSAV": "Savannah, US",   "USBAL": "Baltimore, US",
    "USBOS": "Boston, US",    "CNYTN": "Yantian, CN",    "CNQIN": "Qingdao, CN",
    "JPNGO": "Nagoya, JP",    "JPYOK": "Yokohama, JP",   "THLCH": "Laem Chabang, TH",
    "PHMNL": "Manila, PH",    "VNSGN": "Ho Chi Minh, VN","INPAV": "Nhava Sheva, IN",
    "HKHKG": "Hong Kong, HK",
}


# ── Core Calculations ─────────────────────────────────────────────────────────

def calculate_buffer(df: pd.DataFrame, sim_ts_series: "pd.Series | None" = None) -> pd.DataFrame:
    """
    Buffer (days) = LFD − sim_ts  (per-container simulated production time)
    Using real NOW would make dtl flat across rapid local test runs.
    sim_ts is the per-container simulated time: spawned_at + run_offset * 1h
    """
    now_real = pd.Timestamp.now(tz="UTC")

    df = df.copy()
    df["eta_parsed"] = pd.to_datetime(df["eta_parsed"], utc=True, errors="coerce")
    df["lfd_parsed"] = pd.to_datetime(df["lfd_parsed"], utc=True, errors="coerce")

    TRANSIT_DAYS = {"TRANS_PAC": 14, "TRANS_ATL": 10, "ASIA_EUR": 22, "INTRA_ASIA": 5}
    mask = df["eta_parsed"].isna()
    df.loc[mask, "eta_parsed"] = df.loc[mask, "trade_lane"].map(
        lambda l: now_real + pd.Timedelta(days=TRANSIT_DAYS.get(l, 10))
    )

    # Use per-container simulated time if provided, else real now
    if sim_ts_series is not None:
        sim_ts_utc = pd.to_datetime(sim_ts_series, utc=True, errors="coerce")
        df["buffer_days"] = (df["lfd_parsed"] - sim_ts_utc).dt.total_seconds() / 86400
    else:
        df["buffer_days"] = (df["lfd_parsed"] - now_real).dt.total_seconds() / 86400

    df["days_to_lfd"] = df["buffer_days"].round(2)
    return df


def classify_status(buffer: float) -> str:
    """Business rule: flag status from buffer days."""
    if buffer <= CRITICAL_THRESHOLD_DAYS:
        return "Critical"
    elif buffer <= WARNING_THRESHOLD_DAYS:
        return "Warning"
    return "Safe"


def calculate_burn_rate(row: pd.Series) -> float:
    """
    Burn rate — single source of truth:

      OVERDUE  → state_manager.penalty_accrued_usd (time-based incremental accumulation)
                 Formula: prev_penalty + (hours_elapsed/24 × per_diem) each run
                 This is the ONLY value used — never recalculated from days_to_lfd.
      Warning  → per_diem rate (daily exposure if container tips over)
      Safe     → per_diem rate (monitoring)
    """
    per_diem  = float(row.get("per_diem_rate", 0))
    days_over = float(row.get("days_to_lfd", 0))
    stage     = str(row.get("lifecycle_stage", ""))
    state_pen = row.get("penalty_accrued_usd_state")

    if days_over <= 0 or stage == "OVERDUE":
        # Always use incrementally accumulated state penalty — never recalculate
        if state_pen is not None:
            val = float(state_pen)
            if val > 0:
                return round(val, 2)
        # Fallback only if state penalty is missing (shouldn't happen after run 1)
        return round(per_diem * abs(min(days_over, 0)), 2)

    # Not overdue: show daily rate as exposure indicator
    return round(per_diem, 2)


def priority_score(row: pd.Series) -> int:
    """
    Urgency priority score 1–10 (10 = act today, 1 = monitor only).

    Bands:
      10 → OVERDUE > 7 days           Critical financial exposure
       9 → OVERDUE 3–7 days           Penalty mounting fast
       8 → OVERDUE 0–3 days           Just crossed LFD
       7 → IN_FREE_DAYS ≤ 1 day left  Final warning window
       6 → IN_FREE_DAYS 1–3 days      Schedule return now
       5 → IN_FREE_DAYS 3–7 days      Monitor closely
       4 → IN_FREE_DAYS > 7 days      Low urgency
       3 → ARRIVED / DISCHARGING      Container just landed
       2 → AT_SEA, ETA ≤ 3 days       Approach alert
       1 → GATE_IN / AT_SEA early     Background monitoring
    """
    stage  = str(row.get("lifecycle_stage", "GATE_IN"))
    dtl    = float(row.get("days_to_lfd", 99))
    status = str(row.get("action_status", "Safe"))

    if status == "Critical" or stage == "OVERDUE":
        overdue_days = abs(min(0, dtl))
        if overdue_days > 7:  return 10
        if overdue_days > 3:  return 9
        return 8

    if stage == "IN_FREE_DAYS":
        if dtl <= 1:   return 7
        if dtl <= 3:   return 6
        if dtl <= 7:   return 5
        return 4

    if stage in ("ARRIVED", "DISCHARGING"):
        return 3

    if stage == "AT_SEA":
        eta_days = float(row.get("eta_days_remaining", 99))
        if eta_days <= 3:  return 2
        return 1

    return 1  # GATE_IN and everything else


# ── Surplus ports for repositioning strategy ──────────────────────────────────
_SURPLUS_PORTS = {
    "USLAX","USLONG","USSEA","USOAK","USSAV","USBAL","USBOS","USNYC",
    "NLRTM","DEHAM","BEANR","GBFXT",
}


def projected_exposure_7d(row: pd.Series) -> float:
    """
    Predictive 7-day penalty forecast per container.

    Logic per lifecycle stage:
      GATE_IN / AT_SEA  → check if ETA + free_days falls within 7-day horizon
      IN_FREE_DAYS      → (7 - days_until_lfd) * per_diem, min 0
      OVERDUE           → current_penalty + 7 * per_diem
      ARRIVED           → (7 - free_days) * per_diem if that < 0
    """
    stage    = str(row.get("lifecycle_stage", "GATE_IN"))
    dtl      = float(row.get("days_to_lfd", 99))
    per_diem = float(row.get("per_diem_rate", 0))
    current  = float(row.get("burn_rate_usd", 0))
    horizon  = 7.0

    if stage in ("GATE_IN", "AT_SEA"):
        # Will the container become overdue within 7 days?
        # days_to_lfd already accounts for eta + free window
        if dtl < horizon:
            overdue_days_at_horizon = max(0.0, horizon - max(0.0, dtl))
            return round(overdue_days_at_horizon * per_diem, 2)
        return 0.0

    elif stage in ("ARRIVED", "DISCHARGING"):
        if dtl < horizon:
            overdue_days_at_horizon = max(0.0, horizon - max(0.0, dtl))
            return round(overdue_days_at_horizon * per_diem, 2)
        return 0.0

    elif stage == "IN_FREE_DAYS":
        if dtl < horizon:
            overdue_days_at_horizon = max(0.0, horizon - max(0.0, dtl))
            return round(overdue_days_at_horizon * per_diem, 2)
        return 0.0

    elif stage == "OVERDUE":
        return round(current + horizon * per_diem, 2)

    return 0.0


def repo_strategy(row: pd.Series) -> str:
    """
    Repositioning strategy recommendation based on destination port and stage.

    Industrial logic:
      • Still in transit (GATE_IN / AT_SEA)    → N/A — not yet available
      • OVERDUE                                 → URGENT — Clear Terminal
      • Surplus port + One-Way lease            → One-Way Lease Match
        (find a shipper who needs a box at this port — zero reposition cost)
      • Surplus port + other lease              → Reposition Empty (costly)
      • Non-surplus port                        → Await Local Demand
    """
    stage     = str(row.get("lifecycle_stage", "GATE_IN"))
    dest      = str(row.get("dest_locode", ""))
    lease     = str(row.get("lease_type", ""))
    status    = str(row.get("action_status", "Safe"))

    if stage in ("GATE_IN", "AT_SEA"):
        return "N/A — In Transit"

    if stage == "OVERDUE" or status == "Critical":
        return "URGENT — Clear Terminal"

    is_surplus = dest in _SURPLUS_PORTS

    if is_surplus and lease == "One-Way":
        return "One-Way Lease Match"
    elif is_surplus:
        return "Reposition Empty"
    else:
        return "Await Local Demand"


def eta_days_remaining(row: pd.Series) -> float:
    """Days until vessel ETA (for AT_SEA approach alert in priority_score)."""
    try:
        eta = pd.to_datetime(row.get("eta_parsed"), utc=True)
        if pd.isna(eta):
            return 99.0
        now_ts = pd.to_datetime(row.get("pipeline_run_ts"), utc=True, errors="coerce")
        if pd.isna(now_ts):
            now_ts = pd.Timestamp.now(tz="UTC")
        diff = (eta - now_ts).total_seconds() / 86400
        return round(max(0.0, diff), 1)
    except Exception:
        return 99.0


# ── Current Location Label ────────────────────────────────────────────────────

def location_label(row: pd.Series) -> str:
    """
    Derive a human-readable current location.
    If vessel has a valid IMO match, it's 'At Sea' on that vessel.
    Otherwise it's at the event port.
    """
    vessel = row.get("vessel_name")
    if pd.notna(vessel) and str(vessel).strip():
        return f"At Sea ({vessel})"
    code = str(row.get("port_locode", "UNKNOWN")).strip()
    return LOCATION_LABELS.get(code, code)


# ── Build Gold Table ──────────────────────────────────────────────────────────

def build_gold(silver: pd.DataFrame) -> pd.DataFrame:
    log.info("Computing Gold layer business logic...")

    df = calculate_buffer(silver)  # first pass with real now (for eta fill)

    df["action_status"] = df["days_to_lfd"].apply(classify_status)
    df["burn_rate_usd"] = df.apply(calculate_burn_rate, axis=1)
    df["eta_days_remaining_col"] = df.apply(eta_days_remaining, axis=1)
    # Inject eta_days_remaining into each row for priority_score lookup
    df["eta_days_remaining"] = df["eta_days_remaining_col"]

    df["current_loc"] = df.apply(location_label, axis=1)
    df["trade_lane_label"] = df["trade_lane"].map(TRADE_LANE_LABELS).fillna(df["trade_lane"])

    df["origin_label"] = df["origin_locode"].map(LOCATION_LABELS).fillna(df["origin_locode"])
    df["dest_label"]   = df["dest_locode"].map(LOCATION_LABELS).fillna(df["dest_locode"])

    now_iso = datetime.now(timezone.utc).isoformat()

    # Compute a simulated production timestamp per container
    # = spawned_at + (run_count * 1 hour)
    # This ensures history shows realistic hourly cadence even when the
    # pipeline is run locally multiple times within seconds.
    # In GitHub Actions (truly hourly), this equals the real wall-clock time.
    _run_count_for_ts = 0
    try:
        import json as _j
        from pathlib import Path as _P2
        _sf2 = _P2(__file__).parent.parent / "data" / "state" / "fleet_state.json"
        if _sf2.exists():
            _run_count_for_ts = _j.loads(_sf2.read_text()).get("run_count", 0)
    except Exception:
        pass

    from datetime import timedelta as _td

    # Load spawned_at AND spawn_run per container directly from fleet state
    # spawn_run = the run number when this container was first created
    # This lets us compute ts = spawned_at + (current_run - spawn_run) * 1h
    # → exactly 1-hour gap between each snapshot, matching production cadence
    _state_map = {}
    try:
        import json as _j2
        _sf3 = _P(__file__).parent.parent / "data" / "state" / "fleet_state.json"
        if _sf3.exists():
            for _sc in _j2.loads(_sf3.read_text()).get("containers", []):
                cid = _sc.get("container_id")
                if cid and _sc.get("spawned_at"):
                    _state_map[cid] = {
                        "spawned_at": _sc["spawned_at"],
                        "spawn_run":  int(_sc.get("spawn_run") or 1),
                    }
    except Exception:
        pass

    def _sim_ts(row):
        try:
            cid    = row.get("container_id", "")
            meta   = _state_map.get(cid, {})
            base   = datetime.fromisoformat(str(meta.get("spawned_at", now_iso)))
            s_run  = int(meta.get("spawn_run") or 1)
            offset = max(0, _run_count_for_ts - s_run)  # hours since this container was born
            return (base + _td(hours=offset)).isoformat()[:19]
        except Exception:
            return now_iso[:19]

    df["pipeline_run_ts"] = df.apply(_sim_ts, axis=1)
    df["sim_run_count"]   = _run_count_for_ts

    # Recompute days_to_lfd using simulated per-container time
    # burn_rate_usd is NOT recalculated here — it must wait until after
    # state enrichment sets penalty_accrued_usd_state (see below).
    sim_ts_series = pd.to_datetime(df["pipeline_run_ts"], utc=True, errors="coerce")
    df = calculate_buffer(df, sim_ts_series=sim_ts_series)
    df["action_status"] = df["days_to_lfd"].apply(classify_status)

    # (burn_rate_usd and analytical columns computed after state enrichment below)

    # Enrich from fleet state: lifecycle_stage, lat/lon, voyage progress, penalty
    try:
        import json as _json
        from pathlib import Path as _P
        state_file = _P(__file__).parent.parent / "data" / "state" / "fleet_state.json"
        if state_file.exists():
            state_map = {c["container_id"]: c for c in _json.loads(state_file.read_text()).get("containers",[])}
            df["lifecycle_stage"]         = df["container_id"].map(lambda x: state_map.get(x,{}).get("lifecycle_stage","AT_SEA"))
            df["current_lat"]             = df["container_id"].map(lambda x: state_map.get(x,{}).get("current_lat",0.0))
            df["current_lon"]             = df["container_id"].map(lambda x: state_map.get(x,{}).get("current_lon",0.0))
            # Pull time-accumulated penalty from state (not recalculated)
            df["penalty_accrued_usd_state"] = df["container_id"].map(
                lambda x: state_map.get(x,{}).get("penalty_accrued_usd", None))
            _stage_pct = {"GATE_IN":5,"ARRIVED":82,"IN_FREE_DAYS":91,"OVERDUE":96,"COMPLETE":100}
            df["voyage_pct"] = df["container_id"].map(
                lambda x: round(min(100, state_map.get(x,{}).get("stage_hours_elapsed",0)
                    / max(state_map.get(x,{}).get("transit_hours",1),1)*100
                    if state_map.get(x,{}).get("lifecycle_stage")=="AT_SEA"
                    else _stage_pct.get(state_map.get(x,{}).get("lifecycle_stage","AT_SEA"),50)), 1))
    except Exception as _e:
        log.warning(f"State enrichment skipped: {_e}")
        df["lifecycle_stage"] = "AT_SEA"
        df["current_lat"]     = 0.0
        df["current_lon"]     = 0.0
        df["voyage_pct"]      = 50.0

    # ── Final burn_rate + analytical columns ─────────────────────────────────
    # burn_rate_usd MUST be here: penalty_accrued_usd_state is now populated
    # from state enrichment above, so calculate_burn_rate can use it correctly.
    df["burn_rate_usd"]             = df.apply(calculate_burn_rate, axis=1)
    df["projected_exposure_7d_usd"] = df.apply(projected_exposure_7d, axis=1)
    df["repo_strategy"]             = df.apply(repo_strategy, axis=1)
    df["priority_score"]            = df.apply(priority_score, axis=1)

    # Gold schema columns — exactly as specced in blueprint
    gold_cols = [
        "container_id",
        "contract_id",
        "lease_type",
        "trade_lane",
        "trade_lane_label",
        "vessel_name",
        "imo_number",
        "current_loc",
        "origin_locode",
        "origin_label",
        "dest_locode",
        "dest_label",
        "eta_parsed",
        "lfd_parsed",
        "days_to_lfd",
        "agreed_free_days",
        "per_diem_rate",
        "burn_rate_usd",
        "action_status",
        "priority_score",
        "lifecycle_stage",
        "sim_run_count",
        "current_lat",
        "current_lon",
        "voyage_pct",
        "projected_exposure_7d_usd",
        "repo_strategy",
        "pipeline_run_ts",
    ]
    gold = df[[c for c in gold_cols if c in df.columns]].copy()
    gold = gold.sort_values("priority_score", ascending=False).reset_index(drop=True)

    log.info(f"Gold table: {len(gold)} rows | "
             f"Critical={sum(gold.action_status=='Critical')} | "
             f"Warning={sum(gold.action_status=='Warning')} | "
             f"Safe={sum(gold.action_status=='Safe')}")

    return gold


# ── Summary KPIs ──────────────────────────────────────────────────────────────

def build_summary(gold: pd.DataFrame) -> dict:
    """Aggregated KPIs consumed by the dashboard header cards."""
    now = datetime.now(timezone.utc).isoformat()

    crit  = gold[gold.action_status == "Critical"]
    warn  = gold[gold.action_status == "Warning"]
    safe  = gold[gold.action_status == "Safe"]

    total_penalty = float(crit["burn_rate_usd"].sum())
    avg_buffer    = float(gold["days_to_lfd"].mean())

    by_lane = (
        gold.groupby("trade_lane")
        .agg(
            count=("container_id", "count"),
            critical=("action_status", lambda x: (x == "Critical").sum()),
            total_burn=("burn_rate_usd", lambda x: x[gold.loc[x.index, "action_status"] == "Critical"].sum()),
        )
        .reset_index()
        .to_dict(orient="records")
    )

    by_lease = (
        gold.groupby("lease_type")
        .agg(
            count=("container_id", "count"),
            critical=("action_status", lambda x: (x == "Critical").sum()),
        )
        .reset_index()
        .to_dict(orient="records")
    )

    # Pull run_count from persistent fleet state
    _run_count = 0
    try:
        import json as _json
        from pathlib import Path as _P
        _sf = _P(__file__).parent.parent / "data" / "state" / "fleet_state.json"
        if _sf.exists():
            _run_count = _json.loads(_sf.read_text()).get("run_count", 0)
    except Exception:
        pass

    return {
        "generated_at":     now,
        "run_count":        _run_count,
        "total_containers": len(gold),
        "critical_count":   len(crit),
        "warning_count":    len(warn),
        "safe_count":       len(safe),
        "total_penalty_usd": round(total_penalty, 2),
        "avg_buffer_days":   round(avg_buffer, 1),
        "by_trade_lane":     by_lane,
        "by_lease_type":     by_lease,
        "top_priority": gold.head(5)[[
            "container_id", "lease_type", "trade_lane_label",
            "days_to_lfd", "burn_rate_usd", "action_status"
        ]].to_dict(orient="records"),
    }


# ── Entry Point ───────────────────────────────────────────────────────────────

def run() -> pd.DataFrame:
    log.info("Starting Gold aggregation...")

    # Read Silver — prefer Parquet, fall back to JSON (works without pyarrow locally)
    parquet_path = SILVER_DIR / "merged.parquet"
    json_path    = SILVER_DIR / "merged.json"
    if parquet_path.exists():
        try:
            silver = pd.read_parquet(parquet_path)
        except Exception:
            silver = pd.read_json(json_path)
    else:
        silver = pd.read_json(json_path)
    gold   = build_gold(silver)

    # Write Parquet (Power BI / Tableau connector) — optional, needs pyarrow
    try:
        gold.to_parquet(GOLD_DIR / "containers.parquet", index=False)
        log.info("Written → data/gold/containers.parquet")
    except ImportError:
        log.info("pyarrow not installed — skipping Parquet output (GitHub Actions will have it)")

    # Write JSON (GitHub Pages dashboard)
    def serialize(obj):
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        raise TypeError(type(obj))

    gold_json = gold.to_dict(orient="records")
    (GOLD_DIR / "containers.json").write_text(
        json.dumps(gold_json, indent=2, default=serialize)
    )
    log.info("Written → data/gold/containers.json")

    summary = build_summary(gold)
    (GOLD_DIR / "summary.json").write_text(
        json.dumps(summary, indent=2, default=serialize)
    )
    log.info("Written → data/gold/summary.json")
    log.info(f"Pipeline complete. Total daily exposure: ${summary['total_penalty_usd']:,.2f}")

    # Append to rolling 30-day history and prune old entries
    run_num = summary.get("run_count") or 0
    try:
        hist_stats = history_tracker.append_and_clean(gold.to_dict(orient="records"), run_num)
        log.info(f"History: +{hist_stats['appended']} snapshots, tracking {hist_stats['tracking']} containers")
    except Exception as e:
        log.warning(f"History tracker skipped: {e}")

    return gold


if __name__ == "__main__":
    run()
