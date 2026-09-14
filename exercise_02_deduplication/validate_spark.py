"""Optional local Spark parity gate; requires PySpark and Java, no Delta package."""
from pathlib import Path
import json
from pyspark.sql import SparkSession
from core import canonical, model, resolve
from setup_synthetic_data import build_source
from spark_pipeline import build_spark


def main():
    spark = SparkSession.builder.master("local[2]").appName("cx-ex02-parity").config(
        "spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        rows, _ = build_source()
        actual = build_spark(spark, rows)
        resolved = resolve(rows)
        expected = model(resolved["current"])
        results = {}
        for name in expected:
            values = [r.asDict(recursive=True) for r in actual[name].collect()]
            assert sorted(map(canonical, values)) == sorted(map(canonical, expected[name])), name
            results[name] = {"rows": len(values), "matches_reference": True}
        assert sorted(map(canonical, [r.asDict() for r in actual["audit"].collect()])) == sorted(map(canonical, resolved["audit"]))
        actual_q = sorted((r.receipt_id, r.reason) for r in actual["quarantine"].collect())
        expected_q = sorted((r["receipt_id"], r["reason"]) for r in resolved["quarantine"])
        assert actual_q == expected_q
        report = {"spark_version": spark.version, "tables": results,
                  "audit_matches_reference": True, "quarantine_matches_reference": True,
                  "managed_delta_execution": "Not executed; validate notebook in Databricks."}
        (Path(__file__).parent / "reports/spark_validation.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
