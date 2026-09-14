"""Portable, persistent reference pipeline. Run: python run_pipeline_local.py --demo."""
from collections import Counter
from pathlib import Path
import argparse
import csv
import json
import sqlite3

from core import QUESTION_COLUMNS, canonical, digest, explode_like_legacy, metrics, model, resolve
from setup_synthetic_data import write_source

RESPONSE_FIELDS = ["survey_id", "response_id", "customer_id", "source_version", "updated_at",
                   "answered_at", "created_at", "metric", "score", "journey_stage", "feedback",
                   "event_id", "payload_hash", "quality_status"]
FIELDS = {
    "responses": RESPONSE_FIELDS,
    "response_categories": ["survey_id", "response_id", "category", "subcategory", "source_version"],
    "question_answers": ["survey_id", "response_id", "question_id", "question_text", "value_json", "source_version"],
    "responses_wide": RESPONSE_FIELDS + list(QUESTION_COLUMNS.values()) + ["category_count"],
}
KEYS = {"responses": ["survey_id", "response_id"],
        "response_categories": ["survey_id", "response_id", "category", "subcategory"],
        "question_answers": ["survey_id", "response_id", "question_id"],
        "responses_wide": ["survey_id", "response_id"]}


def connect(path):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.execute("PRAGMA foreign_keys = ON")
    con.execute("CREATE TABLE IF NOT EXISTS bronze_receipts(receipt_id TEXT PRIMARY KEY, raw_json TEXT NOT NULL)")
    return con


def run_batch(database, receipts):
    """Retain Bronze, then atomically recompute current tables from retained history.

    This intentionally favors an auditable small exercise over scalable CDC.
    The persisted Bronze history makes incremental delivery, old arrivals and
    conflicts arriving in later jobs equivalent to a full-history rebuild.
    """
    con = connect(database)
    try:
        with con:
            for row in receipts:
                encoded = canonical(row)
                previous = con.execute("SELECT raw_json FROM bronze_receipts WHERE receipt_id = ?", (row["receipt_id"],)).fetchone()
                if previous and previous[0] != encoded:
                    raise ValueError("receipt_id_collision: immutable delivery changed")
                con.execute("INSERT OR IGNORE INTO bronze_receipts VALUES (?, ?)", (row["receipt_id"], encoded))
            history = [json.loads(row[0]) for row in con.execute("SELECT raw_json FROM bronze_receipts ORDER BY receipt_id")]
            result = resolve(history)
            tables = model(result["current"])
            # Create parent first, delete children first; one transaction.
            for name in ("responses", "response_categories", "question_answers", "responses_wide"):
                cols = FIELDS[name]
                definitions = [f'"{col}" {"INTEGER" if col in ("score", "source_version", "category_count") else "TEXT"}' for col in cols]
                definitions.append("PRIMARY KEY (" + ",".join(KEYS[name]) + ")")
                if name != "responses":
                    definitions.append("FOREIGN KEY (survey_id,response_id) REFERENCES responses(survey_id,response_id)")
                con.execute(f'CREATE TABLE IF NOT EXISTS "{name}" ({",".join(definitions)})')
            for name in ("response_categories", "question_answers", "responses_wide", "responses"):
                con.execute(f'DELETE FROM "{name}"')
            for name in ("responses", "response_categories", "question_answers", "responses_wide"):
                fields = FIELDS[name]
                values = [tuple(row.get(col) for col in fields) for row in tables[name]]
                con.executemany(f'INSERT INTO "{name}" ({",".join(fields)}) VALUES ({",".join("?" for _ in fields)})', values)
            con.execute("CREATE TABLE IF NOT EXISTS current_snapshots(survey_id TEXT, response_id TEXT, payload_json TEXT, PRIMARY KEY(survey_id,response_id))")
            con.execute("DELETE FROM current_snapshots")
            con.executemany("INSERT INTO current_snapshots VALUES (?,?,?)",
                            [(r["survey_id"], r["response_id"], canonical(r)) for r in result["current"]])
            assert not con.execute("PRAGMA foreign_key_check").fetchall()
        return {**result, "tables": tables, "bronze_receipts": len(history)}
    finally:
        con.close()


def write_csv(path, rows, fields):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader(); writer.writerows(rows)


def write_json(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def export(root, result):
    root = Path(root)
    for name, rows in result["tables"].items():
        layer = "gold" if name == "responses_wide" else "silver"
        write_csv(root / f"data/{layer}/{name}.csv", rows, FIELDS[name])
    exploded = explode_like_legacy(result["current"])
    fields = ["survey_id", "response_id", "metric", "score", "category", "subcategory", "question_id"]
    write_csv(root / "data/diagnostics/legacy_join_latest.csv", exploded, fields)
    micro_current = [r for r in result["current"] if r["survey_id"] == "survey-demo" and r["response_id"] in ("R001", "R002", "R003")]
    micro_flat = explode_like_legacy(micro_current)
    write_csv(root / "data/diagnostics/micro_before.csv", micro_flat, fields)
    micro_after = model(micro_current)["responses_wide"]
    write_csv(root / "data/diagnostics/micro_after.csv", micro_after, FIELDS["responses_wide"])
    write_csv(root / "reports/receipt_audit.csv", result["audit"], ["receipt_id", "status", "reason"])
    quarantine = root / "reports/quarantine.jsonl"
    quarantine.write_text("".join(canonical(r) + "\n" for r in result["quarantine"]), encoding="utf-8")
    statuses = dict(sorted(Counter(r["status"] for r in result["audit"]).items()))
    summary = {"bronze_receipts": result["bronze_receipts"], "receipt_statuses": statuses,
               "quarantine_reasons": dict(sorted(Counter(r["reason"] for r in result["quarantine"]).items())),
               "table_rows": {k: len(v) for k, v in result["tables"].items()},
               "correct_metrics": metrics(result["tables"]["responses"]),
               "wrong_metrics_latest_join_only": metrics(exploded),
               "micro_correct": metrics(micro_after), "micro_wrong": metrics(micro_flat),
               "business_output_sha256": digest(result["tables"]),
               "invariants": {"audit_accounts_for_all_receipts": sum(statuses.values()) == result["bronze_receipts"],
                              "wide_equals_response_count": len(result["tables"]["responses_wide"]) == len(result["tables"]["responses"]),
                              "keys_and_foreign_keys": "enforced by SQLite"}}
    write_json(root / "reports/execution_summary.json", summary)
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--demo", action="store_true", help="Recreate initial, incremental and replay demonstration")
    parser.add_argument("--batch", choices=["initial", "incremental", "all"], default="all")
    parser.add_argument("--reset", action="store_true", help="Remove only this exercise's local runtime database")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    source = root / "data/source/api_deliveries.jsonl"
    if not source.exists():
        write_source(source.parent)
    rows = [json.loads(line) for line in source.read_text(encoding="utf-8").splitlines() if line.strip()]
    db = root / "runtime/cx_ex02.sqlite"
    if args.demo or args.reset:
        db.unlink(missing_ok=True)
    stages = []
    batches = ["initial", "incremental", "incremental"] if args.demo else [args.batch]
    for index, batch in enumerate(batches):
        selected = rows if batch == "all" else [r for r in rows if r["batch_id"] == batch]
        result = run_batch(db, selected)
        stages.append({"step": "replay_job" if args.demo and index == 2 else batch,
                       "delivered_receipts": len(selected), "retained_bronze_receipts": result["bronze_receipts"],
                       "active_responses": len(result["tables"]["responses"]),
                       "business_output_sha256": digest(result["tables"])})
    summary = export(root, result)
    write_json(root / "reports/job_runs.json", stages)
    if args.demo:
        assert stages[1]["business_output_sha256"] == stages[2]["business_output_sha256"]
        assert result["tables"] == model(resolve(rows)["current"])
    print(json.dumps({"table_rows": summary["table_rows"], "micro_correct": summary["micro_correct"],
                      "micro_wrong": summary["micro_wrong"], "stages": stages}, indent=2))


if __name__ == "__main__":
    main()
