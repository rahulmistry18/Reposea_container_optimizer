"""
REPOSEA HISTORY TRACKER — history_tracker.py
================================================
Maintains a rolling 30-day history of every container's journey.

On every pipeline run:
  1. Appends a lightweight snapshot for each container to history store
  2. Prunes entries older than RETENTION_DAYS (default 30)
  3. Writes data/gold/history.json for dashboard consumption

History entry per container per run (compact schema):
  {
    "ts":    "2026-03-26T09:00:00+00:00",  # pipeline run timestamp
    "run":   47,                             # run number
    "stage": "AT_SEA",
    "lat":   38.5,
    "lon":   -162.0,
    "dtl":   4.2,        # days_to_lfd
    "status":"Safe",
    "burn":  0,
    "pct":   45,         # voyage_pct
    "vessel":"Maersk Antares"
  }

Storage: data/state/container_history.json
  Dict keyed by container_id → list of snapshots (newest last)
  Capped at 720 entries per container (30 days × 24 hourly runs)

Dashboard output: data/gold/history.json
  Same structure but only containers that exist in the current fleet.
  This keeps the served file small even after months of operation.

Auto-clean: entries older than RETENTION_DAYS are pruned each run.
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

RETENTION_DAYS   = 30
MAX_PER_CONTAINER = RETENTION_DAYS * 24   # 720 entries at hourly cadence
HISTORY_FILE     = Path(__file__).parent.parent / "data" / "state" / "container_history.json"
GOLD_HISTORY     = Path(__file__).parent.parent / "data" / "gold"  / "history.json"


def _load_history() -> dict:
    if HISTORY_FILE.exists():
        try:
            return json.loads(HISTORY_FILE.read_text())
        except Exception as e:
            log.warning(f"History file corrupt, resetting: {e}")
    return {}


def _save_history(history: dict) -> None:
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    HISTORY_FILE.write_text(json.dumps(history, separators=(',', ':'), default=str))


def _compact_snapshot(row: dict, run_number: int) -> dict:
    """Compact snapshot — keeps history file small."""
    # Use sim_run_count from gold row — this is the actual pipeline run index
    # which gives each snapshot a unique hourly-spaced production timestamp
    actual_run = int(row.get("sim_run_count", run_number) or run_number)
    return {
        "ts":     row.get("pipeline_run_ts", datetime.now(timezone.utc).isoformat())[:19],
        "run":    actual_run,
        "stage":  row.get("lifecycle_stage", ""),
        "lat":    round(float(row.get("current_lat",  0) or 0), 3),
        "lon":    round(float(row.get("current_lon",  0) or 0), 3),
        "dtl":    round(float(row.get("days_to_lfd",  0) or 0), 2),
        "status": row.get("action_status", "Safe"),
        "burn":   round(float(row.get("burn_rate_usd", 0) or 0), 1),
        "pct":    round(float(row.get("voyage_pct",    0) or 0), 1),
        "vessel": str(row.get("vessel_name", ""))[:30],
        "loc":    str(row.get("current_loc", ""))[:40],
    }


def append_and_clean(gold_rows: list, run_number: int) -> dict:
    """
    Main entry point — called from gold_aggregate.run() after building the Gold table.

    1. Load existing history
    2. Append snapshot for every current container
    3. Prune entries older than RETENTION_DAYS
    4. Prune excess entries per container (MAX_PER_CONTAINER)
    5. Save state history
    6. Write Gold history for dashboard
    7. Return pruning stats
    """
    history  = _load_history()
    cutoff   = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    cutoff_s = cutoff.isoformat()[:19]

    appended = 0
    pruned   = 0

    for row in gold_rows:
        cid      = row.get("container_id")
        if not cid:
            continue

        # Append new snapshot
        snap = _compact_snapshot(row, run_number)
        if cid not in history:
            history[cid] = []
        history[cid].append(snap)
        appended += 1

    # Prune old entries across all containers (not just current fleet)
    container_ids_to_drop = []
    for cid, snaps in history.items():
        before = len(snaps)
        # Remove entries older than cutoff
        snaps = [s for s in snaps if s.get("ts", "") >= cutoff_s]
        # Keep max MAX_PER_CONTAINER newest entries
        if len(snaps) > MAX_PER_CONTAINER:
            snaps = snaps[-MAX_PER_CONTAINER:]
        pruned += before - len(snaps)
        if snaps:
            history[cid] = snaps
        else:
            container_ids_to_drop.append(cid)

    for cid in container_ids_to_drop:
        del history[cid]

    _save_history(history)

    # Write Gold history (current fleet only, for dashboard)
    current_ids = {row.get("container_id") for row in gold_rows}
    gold_history = {
        cid: snaps
        for cid, snaps in history.items()
        if cid in current_ids
    }

    # Also write container metadata for journey view header
    meta = {}
    for row in gold_rows:
        cid = row.get("container_id")
        if cid:
            meta[cid] = {
                "container_id":   cid,
                "lease_type":     row.get("lease_type", ""),
                "trade_lane":     row.get("trade_lane_label", row.get("trade_lane", "")),
                "origin":         row.get("origin_locode", ""),
                "origin_label":   row.get("origin_label", ""),
                "dest":           row.get("dest_locode", ""),
                "dest_label":     row.get("dest_label", ""),
                "free_days":      row.get("agreed_free_days", 0),
                "per_diem":       row.get("per_diem_rate", 0),
                "current_status": row.get("action_status", ""),
                "current_stage":  row.get("lifecycle_stage", ""),
                "days_to_lfd":    round(float(row.get("days_to_lfd", 0) or 0), 2),
                "total_runs":     len(gold_history.get(cid, [])),
                "first_seen":     (gold_history.get(cid) or [{}])[0].get("ts", ""),
            }

    GOLD_HISTORY.parent.mkdir(parents=True, exist_ok=True)
    GOLD_HISTORY.write_text(json.dumps({
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "retention_days": RETENTION_DAYS,
        "total_containers_tracked": len(history),
        "current_fleet_size": len(current_ids),
        "container_meta": meta,
        "snapshots": gold_history,
    }, separators=(',', ':'), default=str))

    stats = {
        "appended": appended,
        "pruned":   pruned,
        "tracking": len(history),
        "gold_containers": len(gold_history),
    }
    log.info(
        f"History: +{appended} snapshots, -{pruned} pruned, "
        f"{len(history)} containers tracked, "
        f"{HISTORY_FILE.stat().st_size // 1024}KB on disk"
    )
    return stats


def get_journey(container_id: str) -> dict:
    """
    Return the full journey history for one container.
    Used by API endpoint or test scripts.
    """
    history = _load_history()
    snaps   = history.get(container_id, [])
    return {
        "container_id": container_id,
        "snapshot_count": len(snaps),
        "first_seen": snaps[0]["ts"]  if snaps else None,
        "last_seen":  snaps[-1]["ts"] if snaps else None,
        "snapshots":  snaps,
    }


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    history = _load_history()
    if history:
        print(f"Tracking {len(history)} containers")
        for cid, snaps in list(history.items())[:3]:
            print(f"  {cid}: {len(snaps)} snapshots, "
                  f"first={snaps[0]['ts'] if snaps else '?'}, "
                  f"last={snaps[-1]['ts'] if snaps else '?'}")
    else:
        print("No history yet — run the pipeline first.")
