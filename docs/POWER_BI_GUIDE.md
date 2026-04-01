# 📊 Power BI & Tableau Connection Guide

This guide shows how to connect Power BI or Tableau directly to the live Gold data
from this pipeline — no manual downloads, no copy-paste. The data refreshes every hour.

---

## Option A — Excel File (Recommended for Power BI Desktop)

The pipeline auto-generates a formatted Excel workbook at every run:

```
data/exports/container_report.xlsx
```

It contains 7 sheets:
| Sheet | Contents |
|-------|----------|
| 📊 Dashboard | KPI summary, trade lane breakdown, priority segmentation |
| 📦 Containers | Full Gold table — **connect Power BI here** |
| 🔴 Critical | Pre-filtered critical containers |
| 🟡 Warning | Pre-filtered warning containers |
| 🛣️ By Trade Lane | Pivot: penalty & container count by lane |
| 📋 By Lease Type | Pivot: count & penalty by lease |
| ℹ️ Schema | Column definitions for all fields |

### Connect Power BI Desktop to Excel

1. Open **Power BI Desktop**
2. **Home → Get Data → Excel Workbook**
3. Enter the direct download URL:
   ```
   https://raw.githubusercontent.com/YOUR_USERNAME/reposea/main/data/exports/container_report.xlsx
   ```
   Or point to the local file path if running locally.
4. Select the **Containers** table (it's already an Excel Table — Power BI detects it automatically)
5. Click **Load** or **Transform Data** if you want to add custom columns
6. Build your visuals — suggested:
   - Card visuals: Critical / Warning / Safe counts
   - Bar chart: Burn Rate by Trade Lane
   - Table: Full container list with conditional formatting
   - Slicer: Lease Type, Trade Lane, Status

### Set Up Scheduled Refresh (Power BI Service)

1. Publish your `.pbix` report to **Power BI Service**
2. Go to your dataset → **Settings → Scheduled Refresh**
3. Set frequency: **Hourly**
4. This pulls fresh data from the GitHub raw URL every hour — stays in sync with the pipeline

---

## Option B — JSON API (Web Data Connector)

The dashboard's Gold JSON is publicly accessible:

```
https://YOUR_USERNAME.github.io/reposea/data/gold/containers.json
https://YOUR_USERNAME.github.io/reposea/data/gold/summary.json
```

### Power BI — Web Connector

1. **Get Data → Web**
2. URL: `https://YOUR_USERNAME.github.io/reposea/data/gold/containers.json`
3. Power BI will parse the JSON array automatically
4. Expand the nested columns as needed

### Tableau — Web Data Connector

1. **Connect → To a Server → Web Data Connector**
2. Use a WDC that reads JSON from the GitHub Pages URL above
3. Or use **Tableau's JSON connector** directly:
   - Connect → JSON File → paste the URL

---

## Option C — Parquet File (Most Efficient for Large Data)

The pipeline also writes a Parquet file:
```
data/gold/containers.parquet
```

Parquet is 3-5× smaller than Excel and 10× faster to load.

### Tableau — Parquet
1. **Connect → To a File → More... → Parquet**
2. Point to `data/gold/containers.parquet`

### Power BI — Parquet
1. **Get Data → Parquet**
2. URL: `https://raw.githubusercontent.com/YOUR_USERNAME/reposea/main/data/gold/containers.parquet`

---

## 📤 Exporting from Power BI

| Export Type | How |
|-------------|-----|
| **PDF** | File → Export → Export to PDF |
| **PowerPoint** | File → Export → Export to PowerPoint |
| **Excel** | In Power BI Service: Export Data → .xlsx (from any visual) |
| **CSV** | Click any table visual → More options (…) → Export data → CSV |

## 📤 Exporting from Tableau

| Export Type | How |
|-------------|-----|
| **PDF** | File → Print to PDF → Save as PDF |
| **Excel / CSV** | Worksheet → Export → Crosstab → Excel or CSV |
| **Image** | Dashboard → Export → Image |
| **Tableau Packaged Workbook** | File → Export Packaged Workbook (.twbx) |

---

## 🔄 Column Reference

| Column | Type | Use in Power BI / Tableau |
|--------|------|---------------------------|
| `container_id` | Text | Row identifier |
| `lease_type` | Text (Category) | Slicer / Filter |
| `trade_lane_label` | Text (Category) | Slicer / Axis |
| `vessel_name` | Text | Detail dimension |
| `days_to_lfd` | Number | KPI card, conditional color |
| `burn_rate_usd` | Number | Sum/Agg — financial exposure |
| `per_diem_rate` | Number | Daily rate reference |
| `action_status` | Text (Category) | Traffic light conditional format |
| `priority_score` | Integer | Sort order for triage |
| `pipeline_run_ts` | DateTime | "As of" timestamp on report |

---

## 🆘 Troubleshooting

**"File not found" on raw GitHub URL**
→ Check that `data/exports/container_report.xlsx` was committed by the pipeline.
→ The first run happens on fork setup. Check the Actions tab for errors.

**Power BI shows old data**
→ Manually trigger the pipeline: GitHub → Actions → "Hourly Pipeline" → Run workflow
→ Then refresh your Power BI dataset.

**JSON connector returns empty**
→ GitHub Pages takes ~2 minutes to deploy after a pipeline run. Wait and retry.
