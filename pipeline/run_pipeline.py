"""
REPOSEA PIPELINE ORCHESTRATOR
=========================================
Wakes up Bronze → Silver → Gold in sequence.
Called by GitHub Actions every hour, or locally via:
    python pipeline/run_pipeline.py [--env prod|dev]

Exit codes:
    0 = success
    1 = pipeline failure (GitHub Actions will alert on this)
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ORCHESTRATOR] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent


def banner(text: str) -> None:
    line = "─" * 60
    log.info(f"\n{line}\n  {text}\n{line}")


def run_pipeline(env: str = "dev") -> bool:
    start = time.time()
    run_ts = datetime.now(timezone.utc).isoformat()

    banner(f"REPOSEA — Pipeline Run @ {run_ts}")
    log.info(f"Environment: {env.upper()}")
    log.info(f"Root: {ROOT}")

    try:
        # ── BRONZE ──────────────────────────────────────────────────────────
        banner("STAGE 1 / 3 — Bronze Ingestion")
        from pipeline.bronze_ingest import run as bronze_run
        bronze_paths = bronze_run()
        log.info(f"Bronze: ingested {len(bronze_paths)} data streams")

        # ── SILVER ──────────────────────────────────────────────────────────
        banner("STAGE 2 / 3 — Silver Cleaning & Merge")
        from pipeline.silver_clean import run as silver_run
        silver_df = silver_run()
        log.info(f"Silver: {len(silver_df)} merged container records")

        # ── GOLD ────────────────────────────────────────────────────────────
        banner("STAGE 3 / 3 — Gold Business Logic")
        from pipeline.gold_aggregate import run as gold_run
        gold_df = gold_run()

        # ── DONE ────────────────────────────────────────────────────────────
        elapsed = round(time.time() - start, 2)
        crit = sum(gold_df.action_status == "Critical")
        warn = sum(gold_df.action_status == "Warning")
        safe = sum(gold_df.action_status == "Safe")

        banner(f"Pipeline SUCCESS in {elapsed}s")
        log.info(f"Output: {len(gold_df)} containers | "
                 f"Critical={crit} | Warning={warn} | Safe={safe}")
        log.info(f"Gold files written to: {ROOT / 'data' / 'gold'}")

        # Write a run manifest for DVC / audit trail
        manifest = {
            "run_ts":        run_ts,
            "env":           env,
            "elapsed_sec":   elapsed,
            "total_rows":    len(gold_df),
            "critical":      crit,
            "warning":       warn,
            "safe":          safe,
            "bronze_files":  [str(p.name) for p in bronze_paths.values()],
        }
        import json
        manifest_path = ROOT / "data" / "gold" / "run_manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))
        log.info(f"Manifest → {manifest_path}")

        return True

    except Exception as exc:
        log.error(f"Pipeline FAILED: {exc}", exc_info=True)
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="RepoSea Pipeline")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"],
                        help="Runtime environment")
    args = parser.parse_args()

    success = run_pipeline(env=args.env)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
