"""
Test Suite — RepoSea Pipeline
===================================================
Run with:  pytest tests/ -v
"""

import json
import pandas as pd
import pytest
import sys
from pathlib import Path

# Allow importing pipeline modules from project root
sys.path.insert(0, str(Path(__file__).parent.parent))


# ── Silver Layer Tests ────────────────────────────────────────────────────────

from pipeline.silver_clean import standardize_locode

class TestLOCODEStandardization:
    """All the dirty port name variants that can come from raw EDI feeds."""

    def test_valid_locode_passthrough(self):
        assert standardize_locode("CNSHA") == "CNSHA"
        assert standardize_locode("NLRTM") == "NLRTM"
        assert standardize_locode("DEHAM") == "DEHAM"

    def test_lowercase_locode(self):
        assert standardize_locode("cnsha") == "CNSHA"

    def test_rotterdam_variants(self):
        assert standardize_locode("Rotterdam") == "NLRTM"
        assert standardize_locode("Rotter-dam") == "NLRTM"
        assert standardize_locode("RTM")        == "NLRTM"
        assert standardize_locode("rotterdam")  == "NLRTM"

    def test_hamburg_variants(self):
        assert standardize_locode("Hamburg")  == "DEHAM"
        assert standardize_locode("Hambg")    == "DEHAM"
        assert standardize_locode("DE-HAM")   == "DEHAM"

    def test_shanghai_variants(self):
        assert standardize_locode("Shanghai") == "CNSHA"
        assert standardize_locode("Shanghi")  == "CNSHA"   # Intentional typo from EDI

    def test_empty_input(self):
        assert standardize_locode("") == "UNKNOWN"

    def test_unknown_port(self):
        # Should return the input uppercased, not crash
        result = standardize_locode("XXXXXX")
        assert result == "XXXXXX"


# ── Gold Layer Tests ──────────────────────────────────────────────────────────

from pipeline.gold_aggregate import classify_status, calculate_burn_rate, priority_score

class TestStatusClassification:
    def test_critical_at_zero(self):
        assert classify_status(0.0)  == "Critical"

    def test_critical_negative(self):
        assert classify_status(-1.0) == "Critical"
        assert classify_status(-10)  == "Critical"

    def test_warning_boundary(self):
        assert classify_status(0.1)  == "Warning"
        assert classify_status(1.0)  == "Warning"
        assert classify_status(2.0)  == "Warning"

    def test_safe_above_threshold(self):
        assert classify_status(2.1)  == "Safe"
        assert classify_status(10.0) == "Safe"
        assert classify_status(30.0) == "Safe"


class TestBurnRate:
    def _row(self, days_to_lfd: float, per_diem: float, status: str) -> pd.Series:
        return pd.Series({
            "days_to_lfd":   days_to_lfd,
            "per_diem_rate": per_diem,
            "action_status": status,
        })

    def test_critical_accrues_penalty(self):
        row = self._row(-5, 150.0, "Critical")
        # penalty = per_diem * |days_over|
        assert calculate_burn_rate(row) == 750.0

    def test_critical_zero_days(self):
        row = self._row(0, 130.0, "Critical")
        assert calculate_burn_rate(row) == 0.0   # 130 * 0

    def test_warning_returns_per_diem(self):
        row = self._row(1.5, 85.0, "Warning")
        assert calculate_burn_rate(row) == 85.0

    def test_safe_returns_per_diem(self):
        row = self._row(10.0, 45.0, "Safe")
        assert calculate_burn_rate(row) == 45.0


class TestPriorityScore:
    def _row(self, status: str, lease: str, days: float) -> pd.Series:
        return pd.Series({"action_status": status, "lease_type": lease, "days_to_lfd": days})

    def test_critical_one_way_overdue_is_highest(self):
        high = priority_score(self._row("Critical", "One-Way", -10))
        low  = priority_score(self._row("Safe",     "Long-Term", 15))
        assert high > low

    def test_critical_outranks_warning(self):
        crit = priority_score(self._row("Critical", "One-Way", -1))
        warn = priority_score(self._row("Warning",  "One-Way", 1))
        assert crit > warn

    def test_warning_outranks_safe(self):
        warn = priority_score(self._row("Warning", "Master Lease", 1))
        safe = priority_score(self._row("Safe",    "Long-Term",   20))
        assert warn > safe

    def test_overdue_bonus_increases_score(self):
        """More days overdue → higher priority score."""
        minus3  = priority_score(self._row("Critical", "One-Way", -3))
        minus10 = priority_score(self._row("Critical", "One-Way", -10))
        assert minus10 > minus3


# ── Bronze Layer Tests ────────────────────────────────────────────────────────

from pipeline.bronze_ingest import generate_ais_feed, generate_container_events

class TestBronzeGeneration:
    def test_ais_returns_records(self):
        records = generate_ais_feed(n=10)
        assert len(records) >= 10   # may have synthetic duplicates

    def test_ais_has_required_fields(self):
        records = generate_ais_feed(n=5)
        required = ["vessel_name", "imo_number", "current_lat", "current_lon",
                    "next_port_eta", "next_port", "speed_knots"]
        for r in records:
            for field in required:
                assert field in r, f"Missing field: {field}"

    def test_events_returns_records(self):
        records = generate_container_events(n=10)
        assert len(records) >= 10

    def test_events_container_id_format(self):
        records = generate_container_events(n=20)
        for r in records:
            cid = r["container_id"]
            assert len(cid) == 11, f"Invalid container ID length: {cid}"
            assert cid[:4].isalpha(), f"Container prefix not alpha: {cid}"

    def test_speed_in_realistic_range(self):
        records = generate_ais_feed(n=20)
        for r in records:
            assert 10 <= r["speed_knots"] <= 25, f"Unrealistic speed: {r['speed_knots']}"


# ── Integration Test ──────────────────────────────────────────────────────────

class TestGoldSchema:
    """Verify Gold output contains all required columns from the blueprint."""

    REQUIRED_COLS = [
        "container_id", "lease_type", "trade_lane", "vessel_name",
        "current_loc", "origin_locode", "dest_locode", "days_to_lfd",
        "burn_rate_usd", "action_status", "priority_score", "pipeline_run_ts",
    ]

    def test_gold_output_has_all_schema_columns(self, tmp_path, monkeypatch):
        """End-to-end mini pipeline: bronze → silver → gold, check schema."""
        import pipeline.bronze_ingest as b
        import pipeline.silver_clean as s
        import pipeline.gold_aggregate as g

        # Redirect data directories to temp
        monkeypatch.setattr(b, "BRONZE_DIR", tmp_path / "bronze")
        monkeypatch.setattr(s, "BRONZE_DIR", tmp_path / "bronze")
        monkeypatch.setattr(s, "SILVER_DIR", tmp_path / "silver")
        monkeypatch.setattr(g, "SILVER_DIR", tmp_path / "silver")
        monkeypatch.setattr(g, "GOLD_DIR",   tmp_path / "gold")
        (tmp_path / "bronze").mkdir()
        (tmp_path / "silver").mkdir()
        (tmp_path / "gold").mkdir()

        b.run()
        silver_df = s.run()
        assert len(silver_df) > 0, "Silver output is empty"

        gold_df = g.run()
        assert len(gold_df) > 0, "Gold output is empty"

        for col in self.REQUIRED_COLS:
            assert col in gold_df.columns, f"Gold missing column: {col}"

    def test_status_values_are_valid(self, tmp_path, monkeypatch):
        """All action_status values must be in the allowed ENUM."""
        import pipeline.bronze_ingest as b
        import pipeline.silver_clean as s
        import pipeline.gold_aggregate as g

        monkeypatch.setattr(b, "BRONZE_DIR", tmp_path / "bronze")
        monkeypatch.setattr(s, "BRONZE_DIR", tmp_path / "bronze")
        monkeypatch.setattr(s, "SILVER_DIR", tmp_path / "silver")
        monkeypatch.setattr(g, "SILVER_DIR", tmp_path / "silver")
        monkeypatch.setattr(g, "GOLD_DIR",   tmp_path / "gold")
        for d in ["bronze","silver","gold"]:
            (tmp_path / d).mkdir()

        b.run(); s.run()
        gold_df = g.run()

        valid = {"Critical", "Warning", "Safe"}
        invalid = set(gold_df.action_status.unique()) - valid
        assert not invalid, f"Invalid status values found: {invalid}"
