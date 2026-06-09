"""
Lab 4 capstone DAG - Team Pechribon.

Pipeline: wait for vendor CSV -> ingest to silver -> validate silver -> run Spark KPIs
-> publish dashboard -> export a demo backup copy.
"""
from __future__ import annotations

import json
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor

from include.ingest import ingest_day, validate_silver
from include.paths import report_json
from include.team_spark_pechribon import run_daily

DEFAULT_ARGS = {
    "owner": "team_pechribon",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


with DAG(
    dag_id="team_pechribon",
    description="Team Pechribon capstone: vendor CSV to Spark KPI dashboard",
    start_date=datetime(2026, 6, 1),
    end_date=datetime(2026, 6, 14),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab4", "capstone", "pechribon"],
) as dag:

    wait_csv = FileSensor(
        task_id="wait_for_vendor_csv",
        filepath="/opt/airflow/data/incoming/transactions_{{ ds }}.csv",
        poke_interval=30,
        timeout=60 * 20,
        mode="reschedule",
    )

    @task
    def ingest(ds: str) -> dict:
        """Bronze CSV -> silver Parquet for the logical date."""
        return ingest_day(ds)

    @task
    def validate(ds: str) -> dict:
        """Fail fast on missing, too small, or corrupt silver data."""
        return validate_silver(ds, min_rows=10, min_revenue=0.01)

    @task
    def compute_kpis(ds: str) -> dict:
        """Run the team PySpark job and write curated KPIs + dashboard JSON."""
        return run_daily(ds)

    @task
    def publish(ds: str) -> dict:
        """Check that the dashboard JSON exists and reports a successful Spark run."""
        path = report_json(ds)
        if not path.exists():
            raise FileNotFoundError(f"Dashboard JSON not found: {path}")

        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("status") != "success":
            raise RuntimeError(f"Dashboard status is not success: {payload}")

        return {
            "logical_date": ds,
            "report_path": str(path),
            "total_revenue_eur": payload.get("total_revenue_eur"),
            "total_transactions": payload.get("total_transactions"),
            "status": "ready",
        }

    @task
    def export_demo_backup(ds: str) -> dict:
        """Copy the JSON report to demo_backup so the defense has a backup artifact."""
        source = report_json(ds)
        target_dir = Path("/opt/airflow/demo_backup")
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / f"dashboard_{ds}_team_pechribon.json"
        shutil.copy2(source, target)
        return {"backup_report_path": str(target), "status": "backup_ready"}

    ingested = ingest()
    validated = validate()
    computed = compute_kpis()
    published = publish()
    backup = export_demo_backup()

    wait_csv >> ingested >> validated >> computed >> published >> backup