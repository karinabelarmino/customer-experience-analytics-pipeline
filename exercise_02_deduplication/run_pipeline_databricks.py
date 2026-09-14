# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 02 · Response grain, repeated events and deduplication
# MAGIC Import this notebook and upload the four companion Python modules as Workspace **files**
# MAGIC in the same folder: core.py, setup_synthetic_data.py, run_pipeline_local.py, spark_pipeline.py.
# MAGIC Select a writable catalog/schema. All data and names are synthetic.

# COMMAND ----------
from pathlib import Path
import sys
import json
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Workspace-file imports use the notebook's current directory on supported runtimes.
sys.path.insert(0, str(Path.cwd()))
from core import canonical, model, resolve
from setup_synthetic_data import build_source
from spark_pipeline import build_spark
from run_pipeline_local import KEYS

dbutils.widgets.dropdown("batch", "initial", ["initial", "incremental", "all"])
dbutils.widgets.dropdown("reset_exercise", "no", ["no", "yes"])
batch = dbutils.widgets.get("batch")
reset = dbutils.widgets.get("reset_exercise") == "yes"
BRONZE = "cx_ex02_bronze_receipts"
BUNDLE = "cx_ex02_snapshot_bundle"

# COMMAND ----------
# MAGIC %md
# MAGIC Run initial, then incremental, then incremental again. Keep reset_exercise=no after
# MAGIC the first run. The second and third runs must produce identical business tables.
# MAGIC The Bronze MERGE only accepts new receipt IDs; identical job replays do not append.

# COMMAND ----------
if reset:
    for name in ["responses", "response_categories", "question_answers", "responses_wide", "audit", "quarantine"]:
        spark.sql(f"DROP VIEW IF EXISTS cx_ex02_{name}")
    spark.sql(f"DROP TABLE IF EXISTS {BUNDLE}")
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE}")

receipts, _ = build_source()
incoming = receipts if batch == "all" else [r for r in receipts if r["batch_id"] == batch]
incoming_df = spark.createDataFrame([(r["receipt_id"], canonical(r)) for r in incoming],
                                    "receipt_id string, raw_json string")
if spark.catalog.tableExists(BRONZE):
    conflict = incoming_df.alias("s").join(spark.table(BRONZE).alias("t"), "receipt_id").filter(
        F.col("s.raw_json") != F.col("t.raw_json"))
    if conflict.limit(1).count():
        raise ValueError("receipt_id_collision: immutable delivery changed")
    DeltaTable.forName(spark, BRONZE).alias("t").merge(incoming_df.alias("s"),
        "t.receipt_id = s.receipt_id").whenNotMatchedInsertAll().execute()
else:
    incoming_df.write.format("delta").saveAsTable(BRONZE)

# Driver collection is explicit: the fixture has only 1,525 receipts.
# For production, use distributed parsing and bounded affected-response processing.
history = [json.loads(r.raw_json) for r in spark.table(BRONZE).select("raw_json").collect()]
tables = build_spark(spark, history)

# COMMAND ----------
# Validate Spark against the portable reference on the complete retained fixture.
reference = model(resolve(history)["current"])
for name, expected in reference.items():
    actual = [r.asDict(recursive=True) for r in tables[name].collect()]
    assert sorted(map(canonical, actual)) == sorted(map(canonical, expected)), name
    key = KEYS[name]
    assert tables[name].groupBy(*key).count().filter("count > 1").limit(1).count() == 0, name
assert tables["audit"].count() == len(history)

# Publish all six datasets in one Delta snapshot commit. Derived persistent views
# keep categories/questions consistent with their response snapshot in each query.
bundles = []
for name, df in tables.items():
    bundles.append(df.select(F.lit(name).alias("dataset"),
        F.to_json(F.struct(*[F.col(c) for c in df.columns]), {"ignoreNullFields": "false"}).alias("row_json")))
bundle = bundles[0]
for frame in bundles[1:]:
    bundle = bundle.unionByName(frame)
bundle.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(BUNDLE)
for name, df in tables.items():
    schema = df.schema.simpleString().replace("'", "''")
    spark.sql(f"""CREATE OR REPLACE VIEW cx_ex02_{name} AS
        SELECT parsed.* FROM (
            SELECT from_json(row_json, '{schema}') AS parsed
            FROM {BUNDLE} WHERE dataset = '{name}'
        )""")

# COMMAND ----------
display(spark.table("cx_ex02_responses_wide").orderBy("survey_id", "response_id"))
display(spark.table("cx_ex02_audit").groupBy("status").count())
display(spark.table("cx_ex02_response_categories").filter("response_id = 'R001' AND survey_id = 'survey-demo'"))
print("Validation passed. Current active responses:", len(reference["responses"]))
