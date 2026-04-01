"""
SILVER LAYER — RepoSea Cleaning & Merge
==================================================
Takes raw Bronze JSON files and produces clean, merged Silver data.

Operations performed:
  1. LOCODE Standardization  — "Rotterdam" / "Rotter-dam" / "RTM" → "NLRTM"
  2. Deduplication           — Drop duplicate EDI 322 Gate-In messages
  3. Merge                   — Container Events ⋈ AIS feed (by IMO) → vessel location
  4. Schema validation       — Drop records missing critical fields
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SILVER] %(message)s")
log = logging.getLogger(__name__)

BRONZE_DIR = Path(__file__).parent.parent / "data" / "bronze"
SILVER_DIR = Path(__file__).parent.parent / "data" / "silver"
SILVER_DIR.mkdir(parents=True, exist_ok=True)

# ── UN/LOCODE Lookup Table ────────────────────────────────────────────────────
# Real implementation: load from UNECE UN/LOCODE CSV
# https://unece.org/trade/uncefact/unlocode

LOCODE_MAP: dict[str, str] = {
    # Shanghai
    "shanghai": "CNSHA", "cn sha": "CNSHA", "shanghi": "CNSHA",
    # Rotterdam
    "rotterdam": "NLRTM", "rotter-dam": "NLRTM", "rtm": "NLRTM",
    # Hamburg
    "hamburg": "DEHAM", "hambg": "DEHAM", "de-ham": "DEHAM",
    # Los Angeles
    "los angeles": "USLAX", "la": "USLAX", "l.a.": "USLAX",
    # New York
    "new york": "USNYC", "nyc": "USNYC", "new-york": "USNYC",
    # Singapore
    "singapore": "SGSIN", "sin": "SGSIN", "singapor": "SGSIN",
    # Busan
    "busan": "KRPUS", "pusan": "KRPUS", "kr pus": "KRPUS",
    # Kaohsiung
    "kaohsiung": "TWKHH", "khh": "TWKHH", "kaoshiung": "TWKHH",
    # Felixstowe
    "felixstowe": "GBFXT", "felix": "GBFXT", "gb fxt": "GBFXT",
    # Antwerp
    "antwerp": "BEANR", "anr": "BEANR", "antwrp": "BEANR",
    # Long Beach
    "long beach": "USLONG", "longbeach": "USLONG",
    # Seattle
    "seattle": "USSEA",
    # Oakland
    "oakland": "USOAK",
    # Savannah
    "savannah": "USSAV",
    # Baltimore
    "baltimore": "USBAL",
    # Boston
    "boston": "USBOS",
    # Yantian
    "yantian": "CNYTN",
    # Qingdao
    "qingdao": "CNQIN",
    # Nagoya
    "nagoya": "JPNGO",
    # Yokohama
    "yokohama": "JPYOK",
    # Bangkok / Laem Chabang
    "bangkok": "THLCH", "laem chabang": "THLCH",
    # Manila
    "manila": "PHMNL",
    # Ho Chi Minh
    "ho chi minh": "VNSGN", "saigon": "VNSGN",
    # Nhava Sheva / Mumbai
    "nhava sheva": "INPAV", "mumbai": "INPAV",
    # Hong Kong
    "hong kong": "HKHKG", "hkg": "HKHKG",
}

# Already-valid 5-char LOCODEs — pass through unchanged
VALID_LOCODES = {
    "CNSHA","NLRTM","DEHAM","USLAX","USNYC","SGSIN","KRPUS","TWKHH",
    "GBFXT","BEANR","USLONG","USSEA","USOAK","USSAV","USBAL","USBOS",
    "CNYTN","CNQIN","JPNGO","JPYOK","THLCH","PHMNL","VNSGN","INPAV","HKHKG",
}


def standardize_locode(raw: str) -> str:
    """Convert any port name variant to UN/LOCODE."""
    if not raw:
        return "UNKNOWN"
    stripped = raw.strip()
    if stripped.upper() in VALID_LOCODES:
        return stripped.upper()
    return LOCODE_MAP.get(stripped.lower(), stripped.upper())


# ── 1. Clean AIS Feed ─────────────────────────────────────────────────────────

def clean_ais(path: Path) -> pd.DataFrame:
    raw = json.loads(path.read_text())
    df  = pd.DataFrame(raw)

    before = len(df)
    # Dedup: same IMO + same ETA → keep first
    df = df.drop_duplicates(subset=["imo_number", "next_port_eta"], keep="first")
    log.info(f"AIS dedup: {before} → {len(df)} records (dropped {before - len(df)} duplicates)")

    # Drop records with missing position
    df = df.dropna(subset=["current_lat", "current_lon", "imo_number"])

    # Standardize next_port LOCODE
    df["next_port_locode"] = df["next_port"].apply(standardize_locode)
    df["eta_parsed"] = pd.to_datetime(df["next_port_eta"], utc=True, errors="coerce")

    log.info(f"AIS clean: {len(df)} valid vessel records")
    return df[["vessel_name", "imo_number", "current_lat", "current_lon",
               "next_port_locode", "eta_parsed", "speed_knots"]]


# ── 2. Clean Container Events ─────────────────────────────────────────────────

def clean_events(path: Path) -> pd.DataFrame:
    raw = json.loads(path.read_text())
    df  = pd.DataFrame(raw)

    before = len(df)
    df["event_time_parsed"] = pd.to_datetime(df["event_time"], utc=True, errors="coerce")
    df = df.dropna(subset=["container_id", "event_type", "event_time_parsed"])

    # Dedup: identical container_id + event_type within a 5-min window = duplicate terminal msg
    df = df.sort_values("event_time_parsed")
    df["event_bucket"] = (df["event_time_parsed"].astype("int64") // (5 * 60 * 1_000_000_000))
    df = df.drop_duplicates(subset=["container_id", "event_type", "event_bucket"], keep="first")
    df = df.drop(columns=["event_bucket"])

    log.info(f"Events dedup: {before} → {len(df)} (dropped {before - len(df)} duplicates)")

    # Standardize port LOCODE
    df["port_locode"] = df["port_id"].apply(standardize_locode)

    # Keep only latest event per container
    df = df.sort_values("event_time_parsed").groupby("container_id").last().reset_index()
    log.info(f"Events: {len(df)} unique containers (latest event per box)")

    return df[["container_id", "event_type", "event_time_parsed", "port_locode",
               "imo_number", "trade_lane"]]


# ── 3. Clean Contracts ────────────────────────────────────────────────────────

def clean_contracts(path: Path) -> pd.DataFrame:
    raw = json.loads(path.read_text())
    df  = pd.DataFrame(raw)

    df = df.dropna(subset=["container_id", "lease_type", "contract_lfd"])
    df["lfd_parsed"] = pd.to_datetime(df["contract_lfd"], utc=True, errors="coerce")

    # Standardize ports
    df["origin_locode"] = df["origin_port"].apply(standardize_locode)
    df["dest_locode"]   = df["destination_port"].apply(standardize_locode)

    # Dedup: one contract per container
    df = df.drop_duplicates(subset=["container_id"], keep="last")

    log.info(f"Contracts: {len(df)} unique container contracts after clean")
    return df[["contract_id", "container_id", "lease_type", "origin_locode",
               "dest_locode", "trade_lane", "agreed_free_days", "per_diem_rate", "lfd_parsed"]]


# ── 4. Merge: Events ⋈ AIS ⋈ Contracts ───────────────────────────────────────

def merge_silver(ais: pd.DataFrame, events: pd.DataFrame, contracts: pd.DataFrame) -> pd.DataFrame:
    """
    Join order:
      events LEFT JOIN ais      ON imo_number   → adds vessel location/ETA
      result LEFT JOIN contracts ON container_id → adds lease/LFD data
    """
    # Events → AIS (many containers per vessel → creates duplicates, dedup after)
    merged = events.merge(
        ais[["imo_number", "vessel_name", "current_lat", "current_lon",
             "next_port_locode", "eta_parsed", "speed_knots"]].drop_duplicates("imo_number"),
        on="imo_number",
        how="left"
    )

    # → Contracts
    merged = merged.merge(contracts, on="container_id", how="inner")

    # Resolve trade_lane: prefer contracts version
    merged["trade_lane"] = merged["trade_lane_y"].fillna(merged["trade_lane_x"])
    merged = merged.drop(columns=["trade_lane_x", "trade_lane_y"], errors="ignore")

    # Deduplicate: one row per container_id (the join can fan-out due to multi-vessel matches)
    merged = merged.drop_duplicates(subset=["container_id"], keep="first")

    log.info(f"Silver merged: {len(merged)} container records ready for Gold")
    return merged


# ── Entry Point ───────────────────────────────────────────────────────────────

def run() -> pd.DataFrame:
    log.info("Starting Silver cleaning & merge...")

    ais       = clean_ais(BRONZE_DIR / "ais_feed.json")
    events    = clean_events(BRONZE_DIR / "container_events.json")
    contracts = clean_contracts(BRONZE_DIR / "contracts.json")

    silver = merge_silver(ais, events, contracts)

    # Always write JSON (works without pyarrow)
    silver_json = SILVER_DIR / "merged.json"
    silver.to_json(silver_json, orient="records", date_format="iso", indent=2)
    log.info(f"Silver JSON written → {silver_json}")

    # Write Parquet if pyarrow is available (optional — used by Tableau)
    try:
        out = SILVER_DIR / "merged.parquet"
        silver.to_parquet(out, index=False)
        log.info(f"Silver Parquet written → {out}")
    except ImportError:
        log.info("pyarrow not installed — skipping Parquet (JSON fallback used)")

    return silver


if __name__ == "__main__":
    run()
