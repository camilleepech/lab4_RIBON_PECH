# Team / DAG id: team_pechribon

**Spark module:** `include/team_spark_pechribon.py`  
**Course:** Big Data Processing - Lab 4 Capstone

Our github :

---

## 1. Business problem

A retail partner sends one transaction CSV per day. Operations needs a daily dashboard showing revenue and transaction KPIs by category and country. If the pipeline fails, the dashboard is late or wrong, so the DAG validates input quality before computing KPIs.

---

## 2. Architecture

| Layer | Path | Tool |
|---|---|---|
| Bronze | `data/incoming/transactions_<ds>.csv` | `scripts/vendor_drop.py` |
| Silver | `data/raw/dt=<ds>/transactions.parquet` | DuckDB `ingest_day` |
| Gold | `data/curated/dt=<ds>/kpis_by_category_country.parquet` | PySpark `team_spark_pechribon.py` |
| Serve | `data/reports/dashboard_<ds>.json` | PySpark + Airflow publish task |
| Backup | `demo_backup/dashboard_<ds>_team_pechribon.json` | Airflow export task |

### Airflow tasks

| task_id | Role |
|---|---|
| `wait_for_vendor_csv` | Waits for the vendor file for Airflow `ds`. |
| `ingest` | Converts the CSV into idempotent silver Parquet. |
| `validate` | Fails fast if the silver file is missing, too small, or corrupt. |
| `compute_kpis` | Calls `run_daily(ds)` in the team Spark module. |
| `publish` | Verifies that the dashboard JSON exists and has `status = success`. |
| `export_demo_backup` | Copies the JSON report to `demo_backup/` for the defense. |

**Dependency graph:**

```text
wait_for_vendor_csv -> ingest -> validate -> compute_kpis -> publish -> export_demo_backup
```

---

## 3. Spark transformations

File: `include/team_spark_pechribon.py`

| # | Function | What it does |
|---|---|---|
| 1 | `transform_1` | Reads silver Parquet, casts fields, parses timestamps, filters invalid rows, and keeps the requested logical date. |
| 2 | `transform_2` | Adds hour, amount bucket, country region, card-payment flag, and joins category revenue targets when the reference file exists. |
| 3 | `transform_3` | Aggregates KPI rows by logical date, category, country, and region. It computes revenue, transaction counts, average ticket, min/max ticket, card count, payment-method count, and target achievement. |

---

## 4. Idempotence

The pipeline is idempotent for one `ds`:

- `ingest_day(ds)` removes and rewrites `data/raw/dt=<ds>/transactions.parquet`.
- `run_daily(ds)` removes and rewrites `data/curated/dt=<ds>/kpis_by_category_country.parquet`.
- `run_daily(ds)` removes and rewrites `data/reports/dashboard_<ds>.json`.
- `export_demo_backup(ds)` overwrites the same backup JSON path for the same date.

Re-running the same date does not append duplicate KPI rows.

---

## 5. Backfill

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill team_pechribon -s 2026-06-01 -e 2026-06-07 --reset-dagruns
```

---

## 6. Failure demo

Generate a corrupt day:

```bash
python scripts/vendor_drop.py --date 2026-06-03 --corrupt
```

Trigger `team_pechribon` for `2026-06-03`. The `validate` task fails because the total revenue is zero. This makes the failure visible in the Airflow UI before Spark writes any curated KPI output.

---

## 7. Exploration tracks

| Track | Done? | Describe your implementation |
|---|---|---|
| R Reliability | Yes | Retries are configured; the file sensor uses reschedule mode and a timeout. |
| S Spark depth | Yes | The Spark module casts fields, adds enrichments, and uses a broadcast join for the reference targets. |
| O Orchestration | Basic | The DAG separates wait, ingest, validate, compute, publish, and backup tasks. |
| Q Data quality | Yes | `validate` detects zero-revenue corrupt files from `vendor_drop --corrupt`. |
| P Custom | Yes | The DAG exports a JSON backup for the demo. |
| X SparkSubmit | No | The laptop version keeps an in-process SparkSession with `local[*]`. |

---

## 8. Demo script & backup

```bash
python scripts/vendor_drop.py --seed-pack --volume small
python scripts/vendor_drop.py --reference

# In Airflow UI: unpause team_pechribon and trigger 2026-06-01.
# Check:
cat data/reports/dashboard_2026-06-01.json
ls data/curated/dt=2026-06-01/
ls demo_backup/
```

---

## 9. Production next steps

- Move Spark execution from `local[*]` to a real Spark cluster.
- Store reports in object storage instead of local Docker volumes.
- Add stronger schema checks and row-count anomaly thresholds.
- Add alerting to Slack/email when validation or Spark fails.

