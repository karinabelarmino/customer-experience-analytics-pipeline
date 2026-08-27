# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 1: full load versus incremental API load
# MAGIC
# MAGIC This notebook runs the complete exercise in Databricks. The synthetic source and local API are prepared automatically by the companion setup notebook. All names, records and credentials are fictional.

# COMMAND ----------

# MAGIC %run ./setup_synthetic_api

# COMMAND ----------

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone

import requests
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window


API_BASE_URL = SYNTHETIC_API_BASE_URL
API_TOKEN = SYNTHETIC_API_TOKEN
ENTITY = "responses"
OVERLAP_MINUTES = 5

# Unqualified names keep the exercise compatible with the user's current catalog and schema.
BRONZE_TABLE = "cx_ex01_bronze_responses"
QUARANTINE_TABLE = "cx_ex01_quarantine_responses"
SILVER_TABLE = "cx_ex01_silver_responses"
CONTROL_TABLE = "cx_ex01_control_watermarks"
GOLD_TABLE = "cx_ex01_gold_responses"
COMPARISON_TABLE = "cx_ex01_load_comparison"

EXERCISE_TABLES = (
    BRONZE_TABLE,
    QUARANTINE_TABLE,
    SILVER_TABLE,
    CONTROL_TABLE,
    GOLD_TABLE,
    COMPARISON_TABLE,
)


# COMMAND ----------

def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-formatted timestamp and normalize it to UTC."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def subtract_overlap(watermark: str | None) -> str | None:
    """Move the watermark back so boundary events can be replayed safely."""
    if watermark is None:
        return None
    adjusted = parse_timestamp(watermark) - timedelta(minutes=OVERLAP_MINUTES)
    return adjusted.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def request_page(parameters: dict, attempts: int = 3) -> dict:
    """Request one API page with a small exponential-backoff retry policy."""
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Accept": "application/json",
    }
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(
                f"{API_BASE_URL}/v1/{ENTITY}",
                params=parameters,
                headers=headers,
                timeout=20,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as error:
            if attempt == attempts:
                raise RuntimeError(
                    f"API request failed after {attempts} attempts."
                ) from error
            time.sleep(0.25 * (2 ** (attempt - 1)))
    raise RuntimeError("Unreachable retry state.")


def count_available_records(as_of: str) -> int:
    """Read API metadata for the counterfactual full scan at a cutoff."""
    payload = request_page({"page": 1, "page_size": 1, "as_of": as_of})
    return int(payload["pagination"]["total_records"])


def extract_all_pages(
    as_of: str,
    previous_watermark: str | None,
    page_size: int = 250,
) -> dict:
    """Extract every page after the overlap-adjusted watermark."""
    query_watermark = subtract_overlap(previous_watermark)
    page = 1
    pages_requested = 0
    records: list[dict] = []
    maximum_updated_at = previous_watermark

    while True:
        parameters = {
            "page": page,
            "page_size": page_size,
            "as_of": as_of,
        }
        if query_watermark is not None:
            parameters["updated_since"] = query_watermark

        payload = request_page(parameters)
        current_page = payload.get("data", [])
        records.extend(current_page)
        pages_requested += 1

        for record in current_page:
            if maximum_updated_at is None or record["updated_at"] > maximum_updated_at:
                maximum_updated_at = record["updated_at"]

        next_page = payload.get("pagination", {}).get("next_page")
        if next_page is None:
            break
        page = int(next_page)

    return {
        "records": records,
        "pages_requested": pages_requested,
        "previous_watermark": previous_watermark,
        "query_watermark": query_watermark,
        "maximum_updated_at": maximum_updated_at,
    }


def read_watermark() -> str | None:
    """Read the last successfully committed watermark."""
    if not spark.catalog.tableExists(CONTROL_TABLE):
        return None
    row = (
        spark.table(CONTROL_TABLE)
        .filter(F.col("entity") == ENTITY)
        .select("watermark")
        .first()
    )
    return row["watermark"] if row else None


def commit_watermark(watermark: str, run_id: str, as_of: str) -> None:
    """Commit the watermark only after every preceding pipeline step succeeds."""
    source = spark.createDataFrame(
        [(ENTITY, watermark, run_id, as_of)],
        ["entity", "watermark", "committed_run_id", "as_of"],
    )
    if spark.catalog.tableExists(CONTROL_TABLE):
        target = DeltaTable.forName(spark, CONTROL_TABLE)
        (
            target.alias("target")
            .merge(source.alias("source"), "target.entity = source.entity")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        source.write.format("delta").mode("overwrite").saveAsTable(CONTROL_TABLE)


def reset_exercise() -> None:
    """Drop only the managed tables created by Exercise 1."""
    for table_name in EXERCISE_TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


# COMMAND ----------

def validate_and_resolve(bronze_df):
    """Separate invalid events and retain the newest valid version in the batch."""
    required_fields = (
        "response_id",
        "customer_id",
        "journey_stage",
        "responded_at",
        "updated_at",
        "event_version",
        "nps_score",
        "csat_score",
        "category",
    )
    missing_condition = None
    for field in required_fields:
        field_is_missing = F.col(field).isNull() | (
            F.trim(F.col(field).cast("string")) == ""
        )
        missing_condition = (
            field_is_missing
            if missing_condition is None
            else missing_condition | field_is_missing
        )

    assessed = bronze_df.withColumn(
        "validation_reason",
        F.when(missing_condition, F.lit("missing_required_field"))
        .when(~F.col("nps_score").between(0, 10), F.lit("invalid_nps_score"))
        .when(~F.col("csat_score").between(1, 5), F.lit("invalid_csat_score")),
    )
    quarantine_df = assessed.filter(F.col("validation_reason").isNotNull())
    valid_df = assessed.filter(F.col("validation_reason").isNull()).drop(
        "validation_reason"
    )

    newest_first = Window.partitionBy("response_id").orderBy(
        F.col("updated_at").desc(),
        F.col("event_version").desc(),
    )
    valid_latest_df = (
        valid_df.withColumn("version_order", F.row_number().over(newest_first))
        .filter(F.col("version_order") == 1)
        .drop("version_order")
    )
    return valid_latest_df, quarantine_df


def calculate_upsert_statistics(valid_latest_df) -> dict:
    """Count inserts, updates and harmless replays before the Delta merge."""
    if not spark.catalog.tableExists(SILVER_TABLE):
        inserted = valid_latest_df.count()
        return {"inserted": inserted, "updated": 0, "replayed_or_unchanged": 0}

    target_keys = spark.table(SILVER_TABLE).select(
        "response_id",
        F.col("updated_at").alias("target_updated_at"),
    )
    comparison = valid_latest_df.select("response_id", "updated_at").join(
        target_keys,
        on="response_id",
        how="left",
    )
    counts = comparison.agg(
        F.sum(F.when(F.col("target_updated_at").isNull(), 1).otherwise(0)).alias(
            "inserted"
        ),
        F.sum(
            F.when(
                F.col("target_updated_at").isNotNull()
                & (F.col("updated_at") > F.col("target_updated_at")),
                1,
            ).otherwise(0)
        ).alias("updated"),
        F.sum(
            F.when(
                F.col("target_updated_at").isNotNull()
                & (F.col("updated_at") <= F.col("target_updated_at")),
                1,
            ).otherwise(0)
        ).alias("replayed_or_unchanged"),
    ).first()
    return {key: int(counts[key] or 0) for key in counts.asDict()}


def merge_silver(valid_latest_df) -> None:
    """Apply an idempotent Delta upsert using response_id and updated_at."""
    if spark.catalog.tableExists(SILVER_TABLE):
        target = DeltaTable.forName(spark, SILVER_TABLE)
        (
            target.alias("target")
            .merge(
                valid_latest_df.alias("source"),
                "target.response_id = source.response_id",
            )
            .whenMatchedUpdateAll(
                condition="source.updated_at > target.updated_at"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        valid_latest_df.write.format("delta").mode("overwrite").saveAsTable(
            SILVER_TABLE
        )


def publish_gold() -> int:
    """Publish the analytical fields as a managed Gold Delta table."""
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {GOLD_TABLE} AS
        SELECT response_id,
               customer_id,
               journey_stage,
               responded_at,
               updated_at,
               event_version,
               nps_score,
               csat_score,
               category,
               comment
        FROM {SILVER_TABLE}
        """
    )
    return spark.table(GOLD_TABLE).count()


# COMMAND ----------

def run_pipeline(as_of: str, reset: bool = False) -> dict:
    """Execute extraction, validation, Delta load, Gold export and state commit."""
    if reset:
        reset_exercise()

    started_at = time.perf_counter()
    previous_watermark = read_watermark()
    mode = "full" if previous_watermark is None else "incremental"
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    extraction = extract_all_pages(as_of, previous_watermark)
    records = extraction["records"]

    if not records:
        return {
            "run_id": run_id,
            "mode": mode,
            "as_of": as_of,
            "extracted_rows": 0,
            "message": "No events were available after the current watermark.",
        }

    bronze_df = (
        spark.createDataFrame(records)
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
        .withColumn("source_entity", F.lit(ENTITY))
    )
    bronze_df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)

    valid_latest_df, quarantine_df = validate_and_resolve(bronze_df)
    quarantine_df.write.format("delta").mode("append").saveAsTable(
        QUARANTINE_TABLE
    )

    extracted_rows = bronze_df.count()
    quarantined_rows = quarantine_df.count()
    valid_latest_rows = valid_latest_df.count()
    superseded_rows = extracted_rows - quarantined_rows - valid_latest_rows

    upsert_statistics = calculate_upsert_statistics(valid_latest_df)
    merge_silver(valid_latest_df)
    analytical_rows = publish_gold()

    committed_watermark = extraction["maximum_updated_at"] or previous_watermark
    commit_watermark(committed_watermark, run_id, as_of)

    full_scan_rows = count_available_records(as_of)
    avoided_rows = max(0, full_scan_rows - extracted_rows)
    reduction_pct = (
        round(100 * avoided_rows / full_scan_rows, 1) if full_scan_rows else 0.0
    )
    return {
        "run_id": run_id,
        "exercise": "01_full_vs_incremental_load",
        "mode": mode,
        "as_of": as_of,
        "elapsed_seconds": round(time.perf_counter() - started_at, 3),
        "full_scan_rows_at_cutoff": full_scan_rows,
        "extracted_rows": extracted_rows,
        "rows_avoided_vs_full_scan": avoided_rows,
        "read_reduction_pct": reduction_pct,
        "pages_requested": extraction["pages_requested"],
        "previous_watermark": previous_watermark,
        "query_watermark": extraction["query_watermark"],
        "committed_watermark": committed_watermark,
        "valid_latest_rows_in_batch": valid_latest_rows,
        "superseded_or_replayed_rows_in_batch": superseded_rows,
        "quarantined_rows": quarantined_rows,
        "inserted": upsert_statistics["inserted"],
        "updated": upsert_statistics["updated"],
        "replayed_or_unchanged": upsert_statistics["replayed_or_unchanged"],
        "analytical_rows": analytical_rows,
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## Demonstration
# MAGIC The first call reproduces a full historical load. The second advances the source cutoff and uses the committed watermark with a five-minute overlap.

# COMMAND ----------

full_run = run_pipeline("2025-09-30T23:59:59Z", reset=True)

print("This output summarizes the initial full historical load.")
print(json.dumps(full_run, indent=2, sort_keys=True))

# COMMAND ----------

incremental_run = run_pipeline("2026-01-15T23:59:59Z")

print("This output summarizes the subsequent incremental load.")
print(json.dumps(incremental_run, indent=2, sort_keys=True))

# COMMAND ----------

comparison_rows = [
    (
        full_run["mode"],
        full_run["as_of"],
        full_run["full_scan_rows_at_cutoff"],
        full_run["extracted_rows"],
        full_run["rows_avoided_vs_full_scan"],
        full_run["read_reduction_pct"],
        full_run["analytical_rows"],
    ),
    (
        incremental_run["mode"],
        incremental_run["as_of"],
        incremental_run["full_scan_rows_at_cutoff"],
        incremental_run["extracted_rows"],
        incremental_run["rows_avoided_vs_full_scan"],
        incremental_run["read_reduction_pct"],
        incremental_run["analytical_rows"],
    ),
]
comparison_df = spark.createDataFrame(
    comparison_rows,
    [
        "mode",
        "as_of",
        "full_scan_rows",
        "rows_read",
        "rows_avoided",
        "read_reduction_pct",
        "analytical_rows",
    ],
)
comparison_df.write.format("delta").mode("overwrite").saveAsTable(
    COMPARISON_TABLE
)

# COMMAND ----------

# MAGIC %md
# MAGIC The table below compares the records that a full scan would read with the records retrieved by the incremental run.

# COMMAND ----------

display(spark.table(COMPARISON_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC The final output is a managed Gold table with one current record per response. Later exercises will reuse this analytical layer.

# COMMAND ----------

display(spark.table(GOLD_TABLE).orderBy("response_id"))
