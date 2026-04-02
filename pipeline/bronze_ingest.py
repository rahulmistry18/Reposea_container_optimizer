"""
REPOSEA BRONZE — Live-Data Stateful Fleet Ingestion
=====================================================
Orchestrates three real data sources + stateful fleet simulation:

  SOURCE 1 — AIS Live (Digitraffic Finland, free, no key)
    Real vessel names, IMO numbers, current positions.
    Falls back to high-quality known-fleet estimates.

  SOURCE 2 — Market Rates (Freightos FBX + Frankfurter FX, free, no key)
    Real freight rate indices → realistic per-diem rates.
    Lane multipliers adjust contract economics to market.

  SOURCE 3 — Port Intelligence (seasonal model + OpenWeatherMap optional)
    Port congestion scores, seasonal disruptions, weather delays.
    Affects transit times and free-day consumption.

  STATE ENGINE — state_manager.py
    Persistent container lifecycle state (GATE_IN → OVERDUE → replaced).
    Vessels from Source 1 are assigned to containers in Source 4.

Output: Three Bronze JSON files matching real EDI/AIS/ERP schema formats.
  data/bronze/ais_feed.json
  data/bronze/container_events.json
  data/bronze/contracts.json
"""

import json
import logging
import datetime as dt
import random
from pathlib import Path

from pipeline.state_manager import tick_fleet
from pipeline.datasources.ais_live    import get_live_vessels
from pipeline.datasources.market_rates import get_market_snapshot, get_per_diem
from pipeline.datasources.port_intel   import get_port_intel, get_congestion_delay_hours

# ── Human-readable port labels (used for terminal location display) ────────────
LOCATION_LABELS = {
    "CNSHA":"Shanghai, CN",     "NLRTM":"Rotterdam, NL",   "DEHAM":"Hamburg, DE",
    "USLAX":"Los Angeles, US",  "USNYC":"New York, US",    "SGSIN":"Singapore, SG",
    "KRPUS":"Busan, KR",        "TWKHH":"Kaohsiung, TW",   "GBFXT":"Felixstowe, GB",
    "BEANR":"Antwerp, BE",      "USLONG":"Long Beach, US", "USSEA":"Seattle, US",
    "USOAK":"Oakland, US",      "USSAV":"Savannah, US",    "USBAL":"Baltimore, US",
    "USBOS":"Boston, US",       "CNYTN":"Yantian, CN",     "CNQIN":"Qingdao, CN",
    "JPYOK":"Yokohama, JP",     "THLCH":"Laem Chabang, TH","PHMNL":"Manila, PH",
    "VNSGN":"Ho Chi Minh, VN",  "INPAV":"Mumbai, IN",      "HKHKG":"Hong Kong, HK",
    "FRFOS":"Marseille, FR",    "ESBCN":"Barcelona, ES",   "MYBTU":"Kota Kinabalu, MY",
    "IDPNK":"Banjarmasin, ID",
}

log = logging.getLogger(__name__)

BRONZE_DIR = Path(__file__).parent.parent / "data" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

# ── Port name variants (Silver will clean these) ───────────────────────────────
DIRTY_VARIANTS = {
    "CNSHA": ["Shanghai","CN SHA","Shanghi","CNSHA"],
    "NLRTM": ["Rotterdam","Rotter-dam","RTM","NLRTM"],
    "DEHAM": ["Hamburg","Hambg","DE-HAM","DEHAM"],
    "USLAX": ["Los Angeles","LA","L.A.","USLAX"],
    "USNYC": ["New York","NYC","New-York","USNYC"],
    "SGSIN": ["Singapore","SIN","Singapor","SGSIN"],
    "KRPUS": ["Busan","Pusan","KR PUS","KRPUS"],
    "TWKHH": ["Kaohsiung","KHH","Kaoshiung","TWKHH"],
    "GBFXT": ["Felixstowe","Felix","GB FXT","GBFXT"],
    "BEANR": ["Antwerp","ANR","Antwrp","BEANR"],
    "USLONG":["Long Beach","LB","LongBeach","USLONG"],
    "USSEA": ["Seattle","SEA","USSEA"],
    "USOAK": ["Oakland","OAK","USOAK"],
    "USSAV": ["Savannah","SAV","USSAV"],
    "USBAL": ["Baltimore","BAL","USBAL"],
    "USBOS": ["Boston","BOS","USBOS"],
    "CNYTN": ["Yantian","YTN","CNYTN"],
    "CNQIN": ["Qingdao","QDO","CNQIN"],
    "JPYOK": ["Yokohama","YOK","JPYOK"],
    "HKHKG": ["Hong Kong","HKG","HKHKG"],
    "THLCH": ["Laem Chabang","Bangkok","THLCH"],
    "PHMNL": ["Manila","MNL","PHMNL"],
    "VNSGN": ["Ho Chi Minh","Saigon","VNSGN"],
    "FRFOS": ["Marseille","FOS","FRFOS"],
    "ESBCN": ["Barcelona","BCN","ESBCN"],
    "MYBTU": ["Kota Kinabalu","BTU","MYBTU"],
    "IDPNK": ["Banjarmasin","PNK","IDPNK"],
}

def _dirty(locode: str) -> str:
    variants = DIRTY_VARIANTS.get(locode, [locode])
    return random.choice(variants)


# ── Assign real vessels to fleet containers ────────────────────────────────────

def _assign_vessels(fleet: list, live_vessels: list) -> list:
    """
    Vessel assignment — LOCKED AT BIRTH, never changed mid-voyage.

    Rule:
      - If a container already has a vessel_name stored in state → keep it.
        Only update its lat/lon/speed from live AIS if the same IMO is visible.
      - If a container has NO vessel yet (first run / just spawned) → pick one
        deterministically from the live fleet using a stable hash seed, and
        lock it into state permanently for this voyage.

    A container loaded on 'Maersk Antares' stays on 'Maersk Antares' until
    it completes its voyage and the slot is replaced by a new container.
    """
    if not live_vessels:
        return fleet

    # Build IMO → position lookup from live AIS for position-only updates
    live_by_imo = {str(v.get("imo_number", "")): v for v in live_vessels if v.get("imo_number")}

    # Group vessels by trade lane for first-time assignment only
    vessel_pool = {
        "TRANS_PAC":  [v for v in live_vessels if v.get("dwt", 0) > 60000] or live_vessels,
        "TRANS_ATL":  [v for v in live_vessels if v.get("dwt", 0) > 40000] or live_vessels,
        "ASIA_EUR":   [v for v in live_vessels if v.get("dwt", 0) > 80000] or live_vessels,
        "INTRA_ASIA": live_vessels,
    }

    enriched = []
    for c in fleet:
        existing_vessel = c.get("vessel_name", "").strip()
        existing_imo    = str(c.get("imo_number", ""))

        # ── CASE 0: Container at terminal (post-berthing) ─────────────────────
        stage = c.get("lifecycle_stage", "GATE_IN")
        if stage in ("ARRIVED", "IN_FREE_DAYS", "OVERDUE", "COMPLETE"):
            # Vessel has delivered the container and departed.
            # Retain vessel_name for audit trail (which vessel delivered this container).
            # Use berthed_vessel if vessel_name was cleared previously.
            retained_vessel = c.get("vessel_name") or c.get("berthed_vessel", "")
            dest_code = c.get("dest_locode", "")
            c = {**c,
                 "vessel_name":    retained_vessel,
                 "imo_number":     c.get("imo_number") or 0,
                 "current_loc":    LOCATION_LABELS.get(dest_code, dest_code),
                 "_vessel_source": "retained_delivering_vessel",
            }
            enriched.append(c)
            continue

        # ── CASE 1: Container already has a vessel locked in state ────────────
        if existing_vessel and existing_vessel != "Unknown":
            # Only update position if the SAME vessel has a live AIS fix
            live = live_by_imo.get(existing_imo)
            if live and live.get("ais_source") == "live_ais" and c.get("lifecycle_stage") == "AT_SEA":
                c = {**c,
                     "current_lat":    live["current_lat"],
                     "current_lon":    live["current_lon"],
                     "speed_knots":    live.get("speed_knots", c.get("speed_knots", 14.0)),
                     "_vessel_source": "live_ais",
                }
            else:
                c = {**c, "_vessel_source": "locked_state"}
            enriched.append(c)
            continue

        # ── CASE 2: New container — assign vessel for the first time ──────────
        lane = c.get("trade_lane", "TRANS_PAC")
        pool = vessel_pool.get(lane, live_vessels)

        # Stable hash: container_id + origin + dest → always same vessel
        # even if pool order changes between runs
        pool_sorted = sorted(pool, key=lambda v: v.get("imo_number", 0))
        seed_str    = c["container_id"] + c.get("origin_locode","") + c.get("dest_locode","")
        idx         = hash(seed_str) % len(pool_sorted)
        vessel      = pool_sorted[idx]

        c = {**c,
             "vessel_name":    vessel["vessel_name"],
             "imo_number":     vessel.get("imo_number", 0),
             "_vessel_source": "assigned_at_birth",
        }

        # Also pull live position if available right now
        live = live_by_imo.get(str(vessel.get("imo_number", "")))
        if live and live.get("ais_source") == "live_ais" and c.get("lifecycle_stage") == "AT_SEA":
            c = {**c,
                 "current_lat":    live["current_lat"],
                 "current_lon":    live["current_lon"],
                 "speed_knots":    live.get("speed_knots", 14.0),
                 "_vessel_source": "live_ais",
            }
        enriched.append(c)

    return enriched

# ── Apply port congestion delays to fleet ─────────────────────────────────────

def _apply_congestion(fleet: list, port_intel) -> list:
    """
    Increase effective wait times for containers at congested ports.
    This affects the discharge window (ARRIVED stage) and free day consumption.
    """
    enriched = []
    for c in fleet:
        stage = c.get("lifecycle_stage", "AT_SEA")
        if stage in ("ARRIVED", "IN_FREE_DAYS"):
            dest = c.get("dest_locode", "")
            cong_delay = get_congestion_delay_hours(dest, port_intel)
            c = {**c, "_port_congestion_h": cong_delay}
        enriched.append(c)
    return enriched


# ── Build Bronze data streams ─────────────────────────────────────────────────

def build_ais_stream(fleet: list, live_vessels: list) -> list:
    """
    AIS Feed — real vessel positions merged with fleet state.
    Format mirrors MarineTraffic / Spire API response schema.
    """
    now = dt.datetime.now(dt.timezone.utc)
    records = []
    seen_imo = set()

    for c in fleet:
        imo = c.get("imo_number", 0)
        if imo in seen_imo:
            continue
        seen_imo.add(imo)

        eta_hours = max(0, c["transit_hours"] - c.get("stage_hours_elapsed", 0)) \
                    if c.get("lifecycle_stage") == "AT_SEA" else 0
        eta = (now + dt.timedelta(hours=eta_hours)).isoformat()

        record = {
            "vessel_name":   c.get("vessel_name", "Unknown"),
            "imo_number":    imo,
            "mmsi":          c.get("mmsi", ""),
            "operator":      c.get("operator", "Unknown"),
            "current_lat":   c.get("current_lat", 0.0),
            "current_lon":   c.get("current_lon", 0.0),
            "next_port":     c.get("dest_locode", ""),
            "next_port_eta": eta,
            "speed_knots":   c.get("speed_knots", 0.0),
            "course":        c.get("course", 0.0),
            "nav_status":    "underway" if c.get("lifecycle_stage") == "AT_SEA" else "moored",
            "ingested_at":   now.isoformat(),
            "_ais_source":   c.get("_vessel_source", "estimated"),
        }
        records.append(record)
        # Simulate duplicate AIS ping (10% rate — real AIS is noisy)
        if hash(str(imo) + str(now.minute)) % 10 == 0:
            dup = dict(record)
            dup["current_lat"] = round(dup["current_lat"] + random.uniform(-0.005, 0.005), 4)
            dup["_is_dup"] = True
            records.append(dup)

    return records


def build_event_stream(fleet: list, port_intel) -> list:
    """
    Container Event Feed — EDI 322 / terminal message format.
    Events are enriched with port congestion context.
    """
    now = dt.datetime.now(dt.timezone.utc)
    stage_event = {
        "GATE_IN":      "Gate-In",
        "AT_SEA":       "Loaded",
        "ARRIVED":      "Discharged",
        "IN_FREE_DAYS": "Discharged",
        "OVERDUE":      "Discharged",
        "COMPLETE":     "Gate-Out",
    }
    records = []
    for c in fleet:
        event_type = stage_event.get(c.get("lifecycle_stage", "GATE_IN"), "Gate-In")
        is_origin  = c.get("lifecycle_stage") == "GATE_IN"
        port       = c.get("origin_locode" if is_origin else "dest_locode", "CNSHA")
        cong_score = port_intel.congestion.get(port, 0.3)

        record = {
            "container_id":    c["container_id"],
            "event_type":      event_type,
            "event_time":      now.isoformat(),
            "port_id":         _dirty(port),
            "terminal_code":   f"T{abs(hash(c['container_id'])) % 9 + 1}",
            "imo_number":      c.get("imo_number", 0),
            "trade_lane":      c.get("trade_lane", "TRANS_PAC"),
            "port_congestion": round(cong_score, 3),
            "_raw_source":     "edi_322",
        }
        records.append(record)
        # Simulate duplicate terminal message (~8% of events)
        if hash(c["container_id"] + event_type) % 12 == 0:
            dup = dict(record)
            dup["event_time"] = (now + dt.timedelta(seconds=random.randint(30, 180))).isoformat()
            dup["_is_dup"]    = True
            records.append(dup)

    return records


def build_contract_stream(fleet: list, market) -> list:
    """
    Contract Master — ERP export format.
    Per-diem rates are driven by current FBX market conditions.
    """
    records = []
    for c in fleet:
        # Re-derive per-diem from current market conditions
        # This means contracts dynamically reflect market reality each run
        market_rate = get_per_diem(
            c.get("lease_type", "One-Way"),
            c.get("trade_lane", "TRANS_PAC"),
            market
        )

        records.append({
            "contract_id":      c.get("contract_id", "CON000000"),
            "container_id":     c["container_id"],
            "lease_type":       c.get("lease_type", "One-Way"),
            "origin_port":      _dirty(c.get("origin_locode", "CNSHA")),
            "destination_port": _dirty(c.get("dest_locode", "USLAX")),
            "trade_lane":       c.get("trade_lane", "TRANS_PAC"),
            "agreed_free_days": c.get("free_days", 10),
            "per_diem_rate":    market_rate,
            "contract_lfd":     c.get("lfd_iso", "")[:10],
            "market_sentiment": market.sentiment,
            "fx_eur":           market.fx_rates.get("EUR", 0.92),
            "fx_gbp":           market.fx_rates.get("GBP", 0.79),
            "fx_cny":           market.fx_rates.get("CNY", 7.24),
            "_raw_source":      "erp_contract_master",
        })
    return records


# ── Entry Point ───────────────────────────────────────────────────────────────

def run() -> dict:
    log.info("RepoSea Bronze: fetching live data sources...")
    now = dt.datetime.now(dt.timezone.utc)

    # ── Fetch live data (all gracefully fallback) ──────────────────────────
    log.info("  [1/3] AIS live vessels (Digitraffic Finland)...")
    live_vessels = get_live_vessels()
    live_count   = sum(1 for v in live_vessels if v.get("ais_source") == "live_ais")
    log.info(f"        {len(live_vessels)} vessels ({live_count} live AIS, {len(live_vessels)-live_count} estimated)")

    log.info("  [2/3] Market rates (Freightos FBX + Frankfurter FX)...")
    market = get_market_snapshot()
    log.info(f"        sentiment={market.sentiment} source={market.source}")

    log.info("  [3/3] Port intelligence (seasonal + weather)...")
    port_intel = get_port_intel()
    log.info(f"        disruptions={len(port_intel.active_disruptions)} source={port_intel.source}")

    # ── Advance persistent fleet state ────────────────────────────────────
    log.info("  [4/3] Advancing fleet state...")
    fleet = tick_fleet()

    # ── Enrich fleet with live data ───────────────────────────────────────
    fleet = _assign_vessels(fleet, live_vessels)
    fleet = _apply_congestion(fleet, port_intel)

    # ── Build Bronze streams ──────────────────────────────────────────────
    ais       = build_ais_stream(fleet, live_vessels)
    events    = build_event_stream(fleet, port_intel)
    contracts = build_contract_stream(fleet, market)

    # ── Write Bronze JSON ─────────────────────────────────────────────────
    paths = {}
    for name, data in [("ais_feed", ais), ("container_events", events), ("contracts", contracts)]:
        path = BRONZE_DIR / f"{name}.json"
        path.write_text(json.dumps(data, indent=2, default=str))
        paths[name] = path

    # ── Write market snapshot to gold/market.json for dashboard ──────────
    market_path = Path(__file__).parent.parent / "data" / "gold" / "market.json"
    market_path.write_text(json.dumps({
        "fetched_at":       market.fetched_at,
        "source":           market.source,
        "sentiment":        market.sentiment,
        "lane_multipliers": market.lane_multipliers,
        "fbx_rates":        market.fbx_rates,
        "fx_rates":         market.fx_rates,
        "disruptions":      port_intel.active_disruptions,
        "top_congestion":   dict(sorted(
            port_intel.congestion.items(), key=lambda x: -x[1])[:5]),
    }, indent=2))
    log.info(f"  Bronze complete: {len(ais)} AIS | {len(events)} events | {len(contracts)} contracts")
    return paths


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [BRONZE] %(message)s")
    run()
