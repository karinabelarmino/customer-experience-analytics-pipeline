"""Native Spark version resolution, child tables and fixed-schema pivot.

The tiny synthetic fixture is validated on the driver with the same explicit
JSON adapter as the portable version. Relational transformations run in Spark.
This adapter is not a distributed ingestion design for large source histories.
"""
from core import QUESTION_COLUMNS, canonical, normalize
from run_pipeline_local import RESPONSE_FIELDS


def build_spark(spark, receipts):
    from pyspark.sql import functions as F, types as T, Window

    response_schema = T.StructType([
        T.StructField(c, T.IntegerType() if c in ("source_version", "score") else T.StringType(), True)
        for c in RESPONSE_FIELDS if c != "quality_status"])
    category_schema = T.ArrayType(T.StructType([
        T.StructField("category", T.StringType()), T.StructField("subcategory", T.StringType())]))
    question_schema = T.ArrayType(T.StructType([
        T.StructField("question_id", T.StringType()), T.StructField("question_text", T.StringType()),
        T.StructField("value_json", T.StringType())]))
    schema = T.StructType(response_schema.fields + [
        T.StructField("receipt_id", T.StringType()), T.StructField("deleted", T.BooleanType()),
        T.StructField("categories", category_schema), T.StructField("questions", question_schema)])
    valid, invalid = [], []
    for receipt in receipts:
        p = receipt["payload"]
        try:
            row = normalize(p)
            row["questions"] = [{"question_id": q["question_id"], "question_text": q["question_text"],
                                 "value_json": canonical(q["value"]) if q["value"] is not None else None}
                                for q in row["questions"]]
            valid.append({**row, "receipt_id": receipt["receipt_id"]})
        except (ValueError, TypeError, KeyError) as exc:
            invalid.append((receipt["receipt_id"], str(exc), p.get("actionId") if isinstance(p, dict) else None,
                            p.get("_id") if isinstance(p, dict) else None))
    df = spark.createDataFrame(valid, schema)
    invalid_df = spark.createDataFrame(invalid, "receipt_id string, reason string, survey_id string, response_id string")
    keys = ["survey_id", "response_id"]
    version_keys = keys + ["source_version"]
    event_conflicts = df.groupBy("event_id").agg(F.countDistinct("payload_hash").alias("event_variants"))
    version_conflicts = df.groupBy(*version_keys).agg(F.countDistinct("payload_hash").alias("version_variants"))
    flagged = df.join(event_conflicts, "event_id").join(version_conflicts, version_keys)
    bad = flagged.filter((F.col("event_variants") > 1) | (F.col("version_variants") > 1)).withColumn(
        "reason", F.when(F.col("event_variants") > 1, F.lit("event_id_payload_conflict")).otherwise("response_version_conflict"))
    quarantine = invalid_df.unionByName(bad.select("receipt_id", "reason", *keys))
    eligible = flagged.filter((F.col("event_variants") == 1) & (F.col("version_variants") == 1))
    same_version = Window.partitionBy(*version_keys).orderBy("receipt_id")
    ranked = eligible.withColumn("copy_rank", F.row_number().over(same_version))
    repeated = ranked.filter(F.col("copy_rank") > 1).select("receipt_id").withColumn("status", F.lit("repeated_snapshot")).withColumn("reason", F.lit("same_business_content"))
    version_order = Window.partitionBy(*keys).orderBy(F.col("source_version").desc())
    representatives = ranked.filter("copy_rank = 1").withColumn("version_rank", F.row_number().over(version_order))
    audit_selected = representatives.select("receipt_id", F.when(F.col("version_rank") > 1, "superseded_version").when(
        F.col("deleted"), "deleted_current").otherwise("selected_current").alias("status")).withColumn("reason", F.lit("source_version_order"))
    audit = audit_selected.unionByName(repeated).unionByName(quarantine.select("receipt_id", "reason").withColumn("status", F.lit("quarantined")))
    rejected_keys = quarantine.select(*keys).distinct().withColumn("rejected", F.lit(True))
    current = representatives.filter("version_rank = 1").join(rejected_keys, keys, "left").withColumn(
        "quality_status", F.when(F.col("rejected").isNotNull(), "has_rejected_history").otherwise("ok"))
    active = current.filter(~F.col("deleted"))
    responses = active.select(*RESPONSE_FIELDS)
    categories = active.select(*keys, "source_version", F.explode("categories").alias("category_record")).select(
        *keys, "category_record.category", "category_record.subcategory", "source_version")
    questions = active.select(*keys, "source_version", F.explode("questions").alias("question_record")).select(
        *keys, "question_record.question_id", "question_record.question_text", "question_record.value_json", "source_version")
    # Pivot is safe now: at most one value per response/question key is present.
    pivoted = questions.groupBy(*keys).pivot("question_id", list(QUESTION_COLUMNS)).agg(F.max("value_json"))
    for qid, column in QUESTION_COLUMNS.items():
        pivoted = pivoted.withColumnRenamed(qid, column)
    category_counts = categories.groupBy(*keys).agg(F.count(F.lit(1)).cast("int").alias("category_count"))
    wide = responses.join(pivoted, keys, "left").join(category_counts, keys, "left").fillna(0, subset=["category_count"])
    return {"responses": responses, "response_categories": categories, "question_answers": questions,
            "responses_wide": wide, "audit": audit, "quarantine": quarantine}
