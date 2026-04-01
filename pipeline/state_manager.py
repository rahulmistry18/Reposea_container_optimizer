"""
REPOSEA STATE MANAGER
===================================
Persistent fleet state across every hourly pipeline run.

Each container lives through a real lifecycle:
  GATE_IN → AT_SEA → ARRIVED → IN_FREE_DAYS → OVERDUE → COMPLETE → replaced

On every pipeline run:
  1. Load fleet_state.json from disk
  2. Advance every container by elapsed real time
  3. Auto-transition stages (AT_SEA→ARRIVED when transit hours elapsed, etc.)
  4. Replace COMPLETE containers with fresh spawns
  5. Save state back to disk

Initial seed gives a realistic mix:
  15% GATE_IN · 40% AT_SEA · 5% ARRIVED · 25% IN_FREE_DAYS · 15% OVERDUE
"""

import json
import random
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger(__name__)

STATE_DIR  = Path(__file__).parent.parent / "data" / "state"
STATE_FILE = STATE_DIR / "fleet_state.json"
STATE_DIR.mkdir(parents=True, exist_ok=True)

FLEET_SIZE = 35

# ── Reference tables ──────────────────────────────────────────────────────────

TRANSIT_HOURS = {
    "TRANS_PAC":  (264, 408),
    "TRANS_ATL":  (192, 312),
    "ASIA_EUR":   (480, 600),
    "INTRA_ASIA": (48,  120),
}

FREE_DAYS = {
    "One-Way":      (7,  14),
    "Master Lease": (14, 21),
    "Long-Term":    (21, 35),
}

PER_DIEM = {
    "One-Way":      (125, 175),
    "Master Lease": (70,  100),
    "Long-Term":    (30,   55),
}

ROUTES = {
    "TRANS_PAC": [
        ("CNSHA","USLAX"),("CNSHA","USLONG"),("CNSHA","USSEA"),
        ("CNYTN","USLAX"),("CNQIN","USOAK"),("TWKHH","USSEA"),
        ("HKHKG","USLAX"),("JPYOK","USSEA"),("KRPUS","USLONG"),
    ],
    "TRANS_ATL": [
        ("NLRTM","USNYC"),("DEHAM","USNYC"),("BEANR","USNYC"),
        ("DEHAM","USBAL"),("GBFXT","USBOS"),("NLRTM","USSAV"),
        ("FRFOS","USNYC"),("ESBCN","USNYC"),
    ],
    "ASIA_EUR": [
        ("CNSHA","NLRTM"),("CNSHA","DEHAM"),("CNYTN","DEHAM"),
        ("CNSHA","BEANR"),("KRPUS","NLRTM"),("JPYOK","NLRTM"),
        ("SGSIN","GBFXT"),("CNSHA","GBFXT"),
    ],
    "INTRA_ASIA": [
        ("CNSHA","SGSIN"),("CNSHA","THLCH"),("TWKHH","SGSIN"),
        ("HKHKG","PHMNL"),("CNSHA","VNSGN"),("KRPUS","JPYOK"),
        ("SGSIN","MYBTU"),("CNSHA","IDPNK"),
    ],
}

PORT_COORDS = {
    "CNSHA":(31.23,121.47),"NLRTM":(51.92,4.47),"DEHAM":(53.55,9.99),
    "USLAX":(33.74,-118.25),"USNYC":(40.65,-74.01),"SGSIN":(1.26,103.82),
    "KRPUS":(35.10,129.04),"TWKHH":(22.62,120.30),"GBFXT":(51.96,1.35),
    "BEANR":(51.23,4.40),"USLONG":(33.77,-118.22),"USSEA":(47.57,-122.34),
    "USOAK":(37.80,-122.27),"USSAV":(32.09,-81.10),"USBAL":(39.27,-76.58),
    "USBOS":(42.36,-71.05),"CNYTN":(22.65,114.26),"CNQIN":(36.07,120.33),
    "JPYOK":(35.44,139.64),"THLCH":(13.08,100.90),"PHMNL":(14.59,120.98),
    "VNSGN":(10.78,106.70),"INPAV":(18.96,72.94),"HKHKG":(22.32,114.19),
    "FRFOS":(43.29,5.38),"ESBCN":(41.35,2.17),"MYBTU":(5.84,118.11),
    "IDPNK":(-3.80,114.74),"GBFXT":(51.96,1.35),
}

VESSELS = [
    {"name":"Maersk Antares",        "imo":9302428,"op":"Maersk"},
    {"name":"MSC Gülsün",            "imo":9811000,"op":"MSC"},
    {"name":"OOCL Hong Kong",        "imo":9776171,"op":"OOCL"},
    {"name":"CMA CGM Jacques Saadé", "imo":9839430,"op":"CMA CGM"},
    {"name":"HMM Algeciras",         "imo":9863297,"op":"HMM"},
    {"name":"Ever Ace",              "imo":9831213,"op":"Evergreen"},
    {"name":"Hapag Hamburg",         "imo":9617027,"op":"Hapag-Lloyd"},
    {"name":"Yang Ming Witness",     "imo":9619250,"op":"Yang Ming"},
    {"name":"COSCO Universe",        "imo":9875431,"op":"COSCO"},
    {"name":"MSC Eloane",            "imo":9702183,"op":"MSC"},
    {"name":"ZIM Integrated",        "imo":9703291,"op":"ZIM"},
    {"name":"Maersk Kure",           "imo":9549727,"op":"Maersk"},
    {"name":"PIL Majestic",          "imo":9481033,"op":"PIL"},
    {"name":"APL Vanda",             "imo":9461814,"op":"APL"},
    {"name":"Wan Hai 307",           "imo":9381052,"op":"Wan Hai"},
    {"name":"COSCO Taurus",          "imo":9795611,"op":"COSCO"},
    {"name":"MSC Irina",             "imo":9930614,"op":"MSC"},
    {"name":"Ever Alot",             "imo":9943061,"op":"Evergreen"},
]

PREFIXES = [
    "MSKU","TCKU","CMAU","HLXU","GESU","PCIU","MSCU","APZU",
    "SUDU","NYKU","TRLU","UACU","FCIU","BMOU","OOLU","EITU",
    "TGHU","ZIMU","YMLU","COSU","MAEU","PONU","CLHU","WSKU",
    "SZLU","KKTU","HJSC","MEDU","FSCU","TCNU",
]

LEASE_TYPES   = ["One-Way","Master Lease","Long-Term"]
LEASE_WEIGHTS = [0.50, 0.30, 0.20]

# Initial fleet distribution by lifecycle stage
SEED_DIST = [
    ("GATE_IN",      0.12),
    ("AT_SEA",       0.38),
    ("ARRIVED",      0.05),
    ("IN_FREE_DAYS", 0.30),
    ("OVERDUE",      0.15),
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _cid(seed: str) -> str:
    prefix = PREFIXES[int(hashlib.md5(seed.encode()).hexdigest()[:2], 16) % len(PREFIXES)]
    num    = int(hashlib.md5(seed.encode()).hexdigest()[2:9], 16) % 9000000 + 1000000
    return f"{prefix}{num}"


def _lerp_pos(origin: str, dest: str, t: float) -> tuple:
    o = PORT_COORDS.get(origin, (0.0, 0.0))
    d = PORT_COORDS.get(dest,   (0.0, 0.0))
    lat = o[0] + (d[0] - o[0]) * t + random.uniform(-0.4, 0.4)
    lon = o[1] + (d[1] - o[1]) * t + random.uniform(-0.4, 0.4)
    return round(lat, 4), round(lon, 4)


# ── Spawn a new container seeded into a specific lifecycle stage ───────────────

def _spawn(seed: str, force_stage: str = None) -> dict:
    now  = datetime.now(timezone.utc)
    lane = random.choice(list(ROUTES.keys()))
    origin, dest = random.choice(ROUTES[lane])
    lease = random.choices(LEASE_TYPES, weights=LEASE_WEIGHTS)[0]
    free  = random.randint(*FREE_DAYS[lease])
    perd  = random.randint(*PER_DIEM[lease])
    v     = random.choice(VESSELS)
    trans = random.randint(*TRANSIT_HOURS[lane])
    gate  = random.randint(6, 36)

    if force_stage is None:
        stages  = [s for s, _ in SEED_DIST]
        weights = [w for _, w in SEED_DIST]
        force_stage = random.choices(stages, weights=weights)[0]

    # Set elapsed hours + LFD so container starts exactly at the right stage point
    if force_stage == "GATE_IN":
        # Brand new — just gated in, full voyage ahead
        elapsed_h      = random.uniform(0, gate * 0.7)
        stage_h        = elapsed_h
        lfd            = now + timedelta(hours=gate - elapsed_h + trans, days=free)
        lat, lon       = PORT_COORDS.get(origin, (0.0, 0.0))
        loc            = origin
        spd            = 0.0

    elif force_stage == "AT_SEA":
        # Mid-ocean: stage_h is how far through the transit leg
        stage_h  = random.uniform(trans * 0.05, trans * 0.90)
        elapsed_h = gate + stage_h
        progress  = stage_h / trans
        lfd       = now + timedelta(hours=trans - stage_h, days=free)
        lat, lon  = _lerp_pos(origin, dest, progress)
        loc       = f"At Sea ({v['name']})"
        spd       = round(random.uniform(14, 22), 1)

    elif force_stage == "ARRIVED":
        # Just discharged — still in port window
        stage_h   = random.uniform(0, 10)
        elapsed_h = gate + trans + stage_h
        lfd        = now + timedelta(days=free, hours=-stage_h * 0.5)
        lat, lon   = PORT_COORDS.get(dest, (0.0, 0.0))
        loc        = dest
        spd        = 0.0

    elif force_stage == "IN_FREE_DAYS":
        # Ashore, free days ticking down — buffer is positive (1 day → free_days-1 days)
        days_remaining = random.uniform(0.5, free - 0.5)
        stage_h   = 0
        elapsed_h = gate + trans + 12
        lfd       = now + timedelta(days=days_remaining)
        lat, lon  = PORT_COORDS.get(dest, (0.0, 0.0))
        loc       = dest
        spd       = 0.0

    elif force_stage == "OVERDUE":
        # Past LFD — penalty accumulating
        days_over = random.uniform(0.5, 14)
        stage_h   = 0
        elapsed_h = gate + trans + 12 + free * 24
        lfd       = now - timedelta(days=days_over)
        lat, lon  = PORT_COORDS.get(dest, (0.0, 0.0))
        loc       = dest
        spd       = 0.0
        perd      = random.randint(*PER_DIEM[lease])  # re-roll for variety

    else:
        elapsed_h = 0; stage_h = 0
        lfd = now + timedelta(days=free)
        lat, lon = PORT_COORDS.get(origin, (0.0, 0.0))
        loc = origin; spd = 0.0

    penalty = 0.0
    if force_stage == "OVERDUE":
        overdue_days = (now - lfd).total_seconds() / 86400
        penalty      = round(overdue_days * perd, 2)

    return {
        "container_id":       _cid(seed + origin + dest + lease),
        "contract_id":        f"CON{random.randint(100000,999999)}",
        "lease_type":         lease,
        "trade_lane":         lane,
        "vessel_name":        v["name"],
        "imo_number":         v["imo"],
        "operator":           v["op"],
        "origin_locode":      origin,
        "dest_locode":        dest,
        "free_days":          free,
        "per_diem_rate":      perd,
        "transit_hours":      trans,
        "gate_in_hours":      gate,
        "lfd_iso":            lfd.isoformat(),
        "lifecycle_stage":    force_stage,
        "stage_hours_elapsed":stage_h,
        "total_hours_elapsed":elapsed_h,
        "penalty_accrued_usd":penalty,
        "spawned_at":         now.isoformat(),
        "completed_at":       None,
        "current_lat":        lat,
        "current_lon":        lon,
        "current_loc":        loc,
        "speed_knots":        spd,
        "vessel_locked":      True,   # vessel_name/imo NEVER reassigned after birth
        "spawn_run":          None,   # set by tick_fleet() to the current run_count
    }


# ── Advance a container by elapsed hours ─────────────────────────────────────

def _advance(c: dict, hours: float) -> dict:
    c     = dict(c)
    now   = datetime.now(timezone.utc)
    lfd   = datetime.fromisoformat(c["lfd_iso"])
    stage = c["lifecycle_stage"]
    trans = c["transit_hours"]
    gate  = c["gate_in_hours"]
    origin= c["origin_locode"]
    dest  = c["dest_locode"]
    vname = c["vessel_name"]

    c["stage_hours_elapsed"] = c.get("stage_hours_elapsed", 0) + hours
    c["total_hours_elapsed"] = c.get("total_hours_elapsed", 0) + hours

    # ── GATE_IN ──────────────────────────────────────────────────────────────
    if stage == "GATE_IN":
        if c["stage_hours_elapsed"] >= gate:
            overshoot             = c["stage_hours_elapsed"] - gate
            c["lifecycle_stage"]  = "AT_SEA"
            c["stage_hours_elapsed"] = overshoot
            c["speed_knots"]      = round(random.uniform(14, 22), 1)
            stage = "AT_SEA"
        else:
            c["current_lat"], c["current_lon"] = PORT_COORDS.get(origin, (0.0, 0.0))
            c["current_loc"] = origin
            c["speed_knots"] = 0.0

    # ── AT_SEA ───────────────────────────────────────────────────────────────
    if stage == "AT_SEA":
        progress = min(1.0, c["stage_hours_elapsed"] / max(trans, 1))
        lat, lon = _lerp_pos(origin, dest, progress)
        c["current_lat"] = lat
        c["current_lon"] = lon
        c["current_loc"] = f"At Sea ({vname})"
        c["speed_knots"] = round(random.uniform(14, 22), 1)

        if c["stage_hours_elapsed"] >= trans:
            overshoot             = c["stage_hours_elapsed"] - trans
            c["lifecycle_stage"]  = "ARRIVED"
            c["stage_hours_elapsed"] = overshoot
            c["current_lat"], c["current_lon"] = PORT_COORDS.get(dest, (0.0, 0.0))
            c["current_loc"] = dest
            c["speed_knots"] = 0.0
            # Vessel has berthed and discharged — release the vessel reference
            # Container is now at the terminal; vessel goes to its next voyage
            c["berthed_vessel"]   = c.get("vessel_name", "")   # remember for records
            c["vessel_name"]      = ""                          # no vessel at terminal
            c["imo_number"]       = 0
            stage = "ARRIVED"

    # ── ARRIVED ──────────────────────────────────────────────────────────────
    if stage == "ARRIVED":
        c["current_loc"] = dest
        c["speed_knots"] = 0.0
        c["vessel_name"] = ""   # vessel has berthed, discharged, and departed
        c["imo_number"]  = 0
        if c["stage_hours_elapsed"] >= 12:
            c["lifecycle_stage"]     = "IN_FREE_DAYS"
            c["stage_hours_elapsed"] = 0
            stage = "IN_FREE_DAYS"

    # ── IN_FREE_DAYS / OVERDUE ───────────────────────────────────────────────
    if stage in ("IN_FREE_DAYS", "OVERDUE"):
        c["current_loc"] = dest   # at terminal — no vessel
        c["speed_knots"] = 0.0
        c["vessel_name"] = ""     # ensure vessel is cleared at terminal
        c["imo_number"]  = 0
        buffer_days = (lfd - now).total_seconds() / 86400

        if buffer_days <= 0:
            c["lifecycle_stage"]     = "OVERDUE"
            overdue_days             = abs(buffer_days)
            c["penalty_accrued_usd"] = round(overdue_days * c["per_diem_rate"], 2)
            # Randomly resolve very overdue containers (simulate commercial resolution)
            if overdue_days > 10 and random.random() < 0.12:
                c["lifecycle_stage"] = "COMPLETE"
                c["completed_at"]    = now.isoformat()
        else:
            c["lifecycle_stage"]     = "IN_FREE_DAYS"
            c["penalty_accrued_usd"] = 0.0

    return c


# ── Load / Save ───────────────────────────────────────────────────────────────

def _load() -> dict:
    empty = {"containers": [], "last_run_iso": None, "run_count": 0}
    if STATE_FILE.exists():
        try:
            raw = STATE_FILE.read_text().strip()
            if not raw:
                return empty
            parsed = json.loads(raw)
            # Must be a dict — reject [], null, etc
            if not isinstance(parsed, dict):
                log.warning("State file is not a dict, resetting")
                return empty
            return parsed
        except Exception as e:
            log.warning(f"State corrupt, resetting: {e}")
    return empty


def _save(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str))


# ── Public API ────────────────────────────────────────────────────────────────

def days_to_lfd(c: dict) -> float:
    now = datetime.now(timezone.utc)
    lfd = datetime.fromisoformat(c["lfd_iso"])
    return round((lfd - now).total_seconds() / 86400, 2)


def tick_fleet() -> list:
    """
    Advance the full fleet by elapsed real time.
    Returns the current live fleet (COMPLETE containers excluded).
    """
    now   = datetime.now(timezone.utc)
    state = _load()

    # Elapsed since last run (hours) — use .get() to handle empty/partial state
    last_run = state.get("last_run_iso")
    if last_run:
        try:
            last    = datetime.fromisoformat(last_run)
            elapsed = max(0.25, (now - last).total_seconds() / 3600)
        except Exception:
            elapsed = 1.0
    else:
        elapsed = 1.0

    log.info(f"Fleet tick: {elapsed:.2f}h elapsed since last run")

    containers = state.get("containers") or []

    # Seed fresh fleet if empty
    if not containers:
        log.info(f"Seeding fresh fleet of {FLEET_SIZE} containers...")
        stages  = [s for s, _ in SEED_DIST]
        weights = [w for _, w in SEED_DIST]
        for i in range(FLEET_SIZE):
            fs = random.choices(stages, weights=weights)[0]
            c_new = _spawn(seed=f"seed_{i}_{now.timestamp()}", force_stage=fs)
            c_new["spawn_run"] = 1   # first run
            containers.append(c_new)

    # Advance all non-COMPLETE containers
    active    = []
    completed = 0
    for c in containers:
        if c.get("lifecycle_stage") == "COMPLETE":
            completed += 1
            continue
        advanced = _advance(c, elapsed)
        if advanced["lifecycle_stage"] == "COMPLETE":
            completed += 1
        else:
            active.append(advanced)

    log.info(f"Advanced {len(active)} containers | {completed} completed this tick")

    # Replace completed + fill fleet back to target
    shortage = FLEET_SIZE - len(active)
    for i in range(shortage):
        new_c = _spawn(seed=f"new_{i}_{now.timestamp()}_{random.random()}")
        new_c["spawn_run"] = state.get("run_count", 0) + 1  # +1 because run_count increments after
        active.append(new_c)
        log.info(f"  + Spawned {new_c['container_id']} ({new_c['trade_lane']} | {new_c['lease_type']} | stage={new_c['lifecycle_stage']})")

    state["containers"]   = active
    state["last_run_iso"] = now.isoformat()
    state["run_count"]    = int(state.get("run_count") or 0) + 1
    _save(state)

    from collections import Counter
    stages = Counter(c["lifecycle_stage"] for c in active)
    log.info("Fleet: " + " | ".join(f"{s}={n}" for s, n in sorted(stages.items())))
    return active
