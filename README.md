<div align="center">

# 🌊 RepoSea

**Real-time container fleet intelligence. Every voyage tracked, every penalty prevented.**

[![Fork this repo](https://img.shields.io/badge/🍴_Fork-Start_in_30s-071E2E?style=for-the-badge)](../../fork)
[![Live Dashboard](https://img.shields.io/badge/🌊_Live_Dashboard-GitHub_Pages-0A9396?style=for-the-badge)](https://YOUR_USERNAME.github.io/reposea/)
[![Pipeline](https://github.com/YOUR_USERNAME/reposea/actions/workflows/pipeline.yml/badge.svg)](../../actions)

```
AIS Feed + EDI 322 + ERP  ──▶  🥉 Bronze  (stateful fleet tick)
                                     │
         Dedup · LOCODE fix · Merge ──▶  🥈 Silver  (clean & merged)
                                              │
              Buffer · Status · Burn Rate ──▶  🥇 Gold
                                                  │
                    ┌─────────────────────────────┼────────────────┐
                    ▼                             ▼                ▼
             🌊 Dashboard                  📗 reposea_report    📦 Parquet
            (GitHub Pages)                  (Power BI)          (Tableau)
```

</div>

---

## 🍴 Fork & Run in 30 Seconds

1. **Fork** this repo
2. **Settings → Actions → General → Workflow permissions → Read and write → Save**
3. **Settings → Pages → Source → GitHub Actions → Save**
4. **Actions → "🍴 Fork Setup" → Run workflow**

Your live dashboard is at `https://YOUR_USERNAME.github.io/reposea/` within 2 minutes.

> Open **`docs/SETUP_GUIDE.html`** in your browser for the full interactive 10-step walkthrough.

---

## 📁 Project Structure

```
reposea/
│
├── 📂 .github/workflows/
│   ├── fork_setup.yml          ← Runs once on fork, auto-configures everything
│   └── pipeline.yml            ← Hourly cron: Bronze → Silver → Gold → Excel → Pages
│
├── 📂 pipeline/
│   ├── state_manager.py        ← Persistent fleet state (lifecycle engine)
│   ├── bronze_ingest.py        ← Stateful AIS + EDI 322 + ERP ingestion
│   ├── silver_clean.py         ← LOCODE standardization, dedup, merge
│   ├── gold_aggregate.py       ← Buffer calc, status flags, burn rate, priority
│   ├── export_excel.py         ← 7-sheet formatted reposea_report.xlsx
│   └── run_pipeline.py         ← Orchestrator entry point
│
├── 📂 dashboard/
│   └── index.html              ← RepoSea live dashboard (GitHub Pages)
│
├── 📂 data/
│   ├── state/fleet_state.json  ← Persistent fleet state (survives hourly runs)
│   ├── gold/containers.json    ← Dashboard + Power BI Web Connector
│   ├── gold/summary.json       ← KPI aggregates
│   └── exports/reposea_report.xlsx  ← 7-sheet Power BI workbook
│
├── 📂 docs/
│   ├── SETUP_GUIDE.html        ← Interactive 10-step setup walkthrough
│   └── POWER_BI_GUIDE.md       ← Power BI & Tableau connection guide
│
└── 📂 tests/
    └── test_pipeline.py        ← pytest suite
```

---

## 🌊 The Stateful Fleet Engine

Containers live through a real lifecycle — they don't reset each run:

```
GATE_IN → AT_SEA → ARRIVED → IN_FREE_DAYS → OVERDUE → COMPLETE → replaced
```

Each hourly pipeline run advances every container by elapsed real time. ETAs count down, buffers shrink, penalties grow, completed voyages get replaced by fresh containers.

Initial fleet seeds with a realistic distribution:
`12% Gate-In · 38% At Sea · 5% Arrived · 30% Free Days · 15% Overdue`

---

## 📊 Connecting Power BI / Tableau

| Method | URL | Best For |
|--------|-----|----------|
| **Excel** | `data/exports/reposea_report.xlsx` (raw GitHub URL) | Power BI Desktop |
| **JSON** | `https://YOUR_USERNAME.github.io/reposea/data/gold/containers.json` | Web connectors |
| **Parquet** | `data/gold/containers.parquet` (raw GitHub URL) | Tableau |

Full guide: `docs/POWER_BI_GUIDE.md`

---

## 🧮 Core Formula

```
Buffer (days) = Contract_Last_Free_Day − NOW

Buffer < 0  →  🔴 Critical  (penalty: per_diem × |days_over|)
Buffer ≤ 2  →  🟡 Warning   (< 48h to act)
Buffer > 2  →  ✅ Safe      (monitor only)
```

---

## 🔬 Tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

---

## 🏭 Plug In Real Data

Replace these three functions in `pipeline/bronze_ingest.py`:

| Function | Replace With |
|----------|-------------|
| `generate_ais_feed()` | Real MarineTraffic / Spire API call |
| `generate_container_events()` | EDI 322 parser from S3 / SFTP |
| `generate_contracts()` | ERP SQL query |

Silver, Gold, Excel, Dashboard — **zero changes needed.**

---

## 📄 License

MIT
