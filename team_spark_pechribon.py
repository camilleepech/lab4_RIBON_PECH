

"""
PySpark KPI module for Lab 4 - Team Pechribon.

This module implements the required three Spark transformations:
1. read and clean the silver Parquet file,
2. enrich transactions with business attributes and optional category targets,
3. aggregate daily KPIs by category and country.

The Airflow DAG calls run_daily(ds). Outputs are idempotent: the curated
partition and JSON dashboard for the same logical date are overwritten.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from include.paths import curated_kpis, raw_parquet, reference_targets, report_json

TEAM_NAME = "pechribon"


def _remove_existing_output(path: Path) -> None:
    """Delete a previous file or Spark output directory before writing a new one."""
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def transform_1(spark: SparkSession, logical_date: str) -> DataFrame:
    """Read the silver layer, cast important fields, and keep valid rows for ds."""
    source_path = raw_parquet(logical_date)

    df = spark.read.parquet(str(source_path))

    cleaned = (
        df.select(
            F.col("tx_id").cast(T.StringType()).alias("tx_id"),
            F.lower(F.trim(F.col("category").cast(T.StringType()))).alias("category"),
            F.lower(F.trim(F.col("payment_method").cast(T.StringType()))).alias("payment_method"),
            F.upper(F.trim(F.col("country").cast(T.StringType()))).alias("country"),
            F.col("amount_eur").cast(T.DoubleType()).alias("amount_eur"),
            F.to_timestamp(F.col("ts")).alias("tx_ts"),
        )
        .withColumn("tx_date", F.to_date("tx_ts"))
        .filter(F.col("tx_id").isNotNull())
        .filter(F.col("category").isNotNull())
        .filter(F.col("country").isNotNull())
        .filter(F.col("amount_eur").isNotNull() & (F.col("amount_eur") > 0))
        .filter(F.col("tx_date") == F.to_date(F.lit(logical_date)))
        .withColumn("logical_date", F.lit(logical_date))
    )

    return cleaned


def transform_2(spark: SparkSession, df: DataFrame, logical_date: str) -> DataFrame:
    """Enrich transactions with time, amount bucket, region, and target data."""
    enriched = (
        df.withColumn("tx_hour", F.hour("tx_ts"))
        .withColumn(
            "amount_bucket",
            F.when(F.col("amount_eur") < 25, F.lit("small"))
            .when(F.col("amount_eur") < 100, F.lit("medium"))
            .otherwise(F.lit("large")),
        )
        .withColumn(
            "country_region",
            F.when(F.col("country").isin("FR", "BE", "NL"), F.lit("western_europe"))
            .when(F.col("country") == "DE", F.lit("central_europe"))
            .when(F.col("country").isin("ES", "IT"), F.lit("southern_europe"))
            .otherwise(F.lit("other")),
        )
        .withColumn("is_card_payment", F.col("payment_method") == F.lit("card"))
    )

    targets_path = reference_targets()
    if targets_path.exists():
        target_schema = T.StructType(
            [
                T.StructField("category", T.StringType(), nullable=False),
                T.StructField("target_revenue_eur", T.DoubleType(), nullable=True),
            ]
        )
        targets = (
            spark.read.option("header", True)
            .schema(target_schema)
            .csv(str(targets_path))
            .withColumn("category", F.lower(F.trim(F.col("category"))))
        )
        enriched = enriched.join(F.broadcast(targets), on="category", how="left")
    else:
        enriched = enriched.withColumn("target_revenue_eur", F.lit(None).cast(T.DoubleType()))

    return enriched.withColumn("processed_for_ds", F.lit(logical_date))


def transform_3(df: DataFrame) -> DataFrame:
    """Aggregate business KPIs by category and country."""
    kpis = (
        df.groupBy("logical_date", "category", "country", "country_region")
        .agg(
            F.count("tx_id").alias("transaction_count"),
            F.round(F.sum("amount_eur"), 2).alias("revenue_eur"),
            F.round(F.avg("amount_eur"), 2).alias("avg_ticket_eur"),
            F.round(F.min("amount_eur"), 2).alias("min_ticket_eur"),
            F.round(F.max("amount_eur"), 2).alias("max_ticket_eur"),
            F.sum(F.col("is_card_payment").cast("int")).alias("card_transaction_count"),
            F.countDistinct("payment_method").alias("payment_method_count"),
            F.first("target_revenue_eur", ignorenulls=True).alias("target_revenue_eur"),
        )
        .withColumn(
            "target_gap_eur",
            F.when(
                F.col("target_revenue_eur").isNotNull(),
                F.round(F.col("revenue_eur") - F.col("target_revenue_eur"), 2),
            ),
        )
        .withColumn(
            "target_achievement_pct",
            F.when(
                F.col("target_revenue_eur").isNotNull() & (F.col("target_revenue_eur") > 0),
                F.round((F.col("revenue_eur") / F.col("target_revenue_eur")) * 100, 2),
            ),
        )
        .orderBy("category", "country")
    )

    return kpis


def _dashboard_payload(logical_date: str, kpis: DataFrame, curated_path: Path, report_path: Path) -> dict[str, Any]:
    """Build a compact JSON payload for the dashboard from the KPI dataframe."""
    totals = kpis.agg(
        F.sum("transaction_count").cast("long").alias("total_transactions"),
        F.round(F.sum("revenue_eur"), 2).alias("total_revenue_eur"),
        F.count("*").cast("long").alias("kpi_group_count"),
    ).first()

    top_category_row = (
        kpis.groupBy("category")
        .agg(F.round(F.sum("revenue_eur"), 2).alias("revenue_eur"))
        .orderBy(F.desc("revenue_eur"), F.asc("category"))
        .limit(1)
        .first()
    )

    return {
        "status": "success",
        "team": TEAM_NAME,
        "logical_date": logical_date,
        "total_transactions": int(totals["total_transactions"] or 0),
        "total_revenue_eur": float(totals["total_revenue_eur"] or 0.0),
        "kpi_group_count": int(totals["kpi_group_count"] or 0),
        "top_category_by_revenue": None if top_category_row is None else top_category_row["category"],
        "top_category_revenue_eur": None if top_category_row is None else float(top_category_row["revenue_eur"]),
        "curated_path": str(curated_path),
        "report_path": str(report_path),
        "spark_transforms": ["transform_1", "transform_2", "transform_3"],
    }


def run_daily(logical_date: str, *, with_reference: bool = True) -> dict[str, Any]:
    """
    Run the daily Spark pipeline for one Airflow logical date.

    Parameters
    ----------
    logical_date:
        Airflow ds value, for example "2026-06-03".
    with_reference:
        Kept for readability and extension; transform_2 uses the reference file
        automatically when it exists.
    """
    spark: SparkSession | None = None
    curated_path = curated_kpis(logical_date)
    dashboard_path = report_json(logical_date)

    try:
        spark = (
            SparkSession.builder.appName(f"team_{TEAM_NAME}_daily_kpis_{logical_date}")
            .master("local[*]")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        cleaned = transform_1(spark, logical_date)
        cleaned_count = cleaned.count()
        if cleaned_count == 0:
            raise RuntimeError(f"No valid transactions left after cleaning for {logical_date}")

        enriched = transform_2(spark, cleaned, logical_date)
        kpis = transform_3(enriched).cache()
        kpi_group_count = kpis.count()
        if kpi_group_count == 0:
            raise RuntimeError(f"No KPI rows produced for {logical_date}")

        curated_path.parent.mkdir(parents=True, exist_ok=True)
        dashboard_path.parent.mkdir(parents=True, exist_ok=True)

        _remove_existing_output(curated_path)
        kpis.write.mode("overwrite").parquet(str(curated_path))

        payload = _dashboard_payload(logical_date, kpis, curated_path, dashboard_path)
        payload["input_valid_transaction_count"] = int(cleaned_count)
        payload["kpi_group_count"] = int(kpi_group_count)

        _remove_existing_output(dashboard_path)
        dashboard_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

        kpis.unpersist()
        return payload
    finally:
        if spark is not None:
            spark.stop()