"""Exercise 01: run the CX pipeline locally.

Start setup_synthetic_api.py first. This script then extracts the API pages,
validates the batch, updates the current response table and exports Gold.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parent
API_URL = "http://127.0.0.1:8765"
API_TOKEN = "synthetic-demo-token"
OVERLAP_MINUTES = 5
FULL_CUTOFF = "2025-09-30T23:59:59Z"
INCREMENTAL_CUTOFF = "2026-01-15T23:59:59Z"

REQUIRED_FIELDS = (
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

TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS fact_response (
    response_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    journey_stage TEXT NOT NULL,
    responded_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    event_version INTEGER NOT NULL,
    nps_score INTEGER NOT NULL CHECK (nps_score BETWEEN 0 AND 10),
    csat_score INTEGER NOT NULL CHECK (csat_score BETWEEN 1 AND 5),
    category TEXT NOT NULL,
    comment TEXT
);
"""


# 1. EXTRACT -----------------------------------------------------------------

def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
        timezone.utc
    )


def overlap_watermark(
    watermark: str | None,
    minutes: int = OVERLAP_MINUTES,
) -> str | None:
    if watermark is None:
        return None
    value = parse_timestamp(watermark) - timedelta(minutes=minutes)
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def request_json(url: str, token: str, attempts: int = 3) -> dict:
    request = Request(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    for attempt in range(attempts):
        try:
            with urlopen(request, timeout=20) as response:
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError) as error:
            if attempt == attempts - 1:
                raise RuntimeError(
                    "API request failed. Is setup_synthetic_api.py still running?"
                ) from error
            time.sleep(0.25 * (2**attempt))
    raise RuntimeError("API request failed.")


def count_available_records(base_url: str, token: str, as_of: str) -> int:
    query = urlencode({"page": 1, "page_size": 1, "as_of": as_of})
    payload = request_json(f"{base_url}/v1/responses?{query}", token)
    return int(payload["pagination"]["total_records"])


def extract_responses(
    base_url: str,
    token: str,
    bronze_path: Path,
    as_of: str,
    watermark: str | None,
    page_size: int = 250,
) -> dict:
    """Follow next_page until the complete batch has landed in Bronze."""
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    query_watermark = overlap_watermark(watermark)
    maximum_updated_at = watermark
    page = 1
    pages_requested = 0
    rows_read = 0

    with bronze_path.open("w", encoding="utf-8", newline="\n") as bronze:
        while True:
            params = {"page": page, "page_size": page_size, "as_of": as_of}
            if query_watermark:
                params["updated_since"] = query_watermark

            payload = request_json(
                f"{base_url}/v1/responses?{urlencode(params)}",
                token,
            )
            for event in payload.get("data", []):
                bronze.write(json.dumps(event, ensure_ascii=False) + "\n")
                rows_read += 1
                if maximum_updated_at is None or event["updated_at"] > maximum_updated_at:
                    maximum_updated_at = event["updated_at"]

            pages_requested += 1
            next_page = payload.get("pagination", {}).get("next_page")
            if next_page is None:
                break
            page = int(next_page)

    return {
        "pages_requested": pages_requested,
        "extracted_rows": rows_read,
        "previous_watermark": watermark,
        "query_watermark": query_watermark,
        "maximum_updated_at": maximum_updated_at,
    }


# 2. VALIDATE AND RESOLVE VERSIONS -------------------------------------------

def validate_response(event: dict) -> tuple[bool, str | None]:
    missing = [field for field in REQUIRED_FIELDS if event.get(field) in (None, "")]
    if missing:
        return False, f"missing:{','.join(missing)}"
    if not isinstance(event["nps_score"], int) or not 0 <= event["nps_score"] <= 10:
        return False, "invalid_nps_score"
    if not isinstance(event["csat_score"], int) or not 1 <= event["csat_score"] <= 5:
        return False, "invalid_csat_score"
    return True, None


def validate_and_resolve(events: list[dict]) -> dict:
    latest: dict[str, dict] = {}
    quarantine = []

    for event in events:
        valid, reason = validate_response(event)
        if not valid:
            quarantine.append({"reason": reason, "record": event})
            continue

        response_id = event["response_id"]
        order = (event["updated_at"], int(event["event_version"]))
        current = latest.get(response_id)
        if current is None or order > (
            current["updated_at"],
            int(current["event_version"]),
        ):
            latest[response_id] = event

    valid_latest = sorted(latest.values(), key=lambda row: row["response_id"])
    return {
        "valid_latest": valid_latest,
        "quarantine": quarantine,
        "statistics": {
            "valid_latest_rows_in_batch": len(valid_latest),
            "superseded_or_replayed_rows_in_batch": (
                len(events) - len(valid_latest) - len(quarantine)
            ),
            "quarantined_rows": len(quarantine),
        },
    }


def read_jsonl(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as source:
        return [json.loads(line) for line in source if line.strip()]


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as output:
        for row in rows:
            output.write(json.dumps(row, ensure_ascii=False) + "\n")


# 3. UPSERT SILVER ------------------------------------------------------------

def connect_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.executescript(TABLE_SCHEMA)
    return connection


def upsert_responses(connection: sqlite3.Connection, rows: list[dict]) -> dict:
    ids = [row["response_id"] for row in rows]
    existing = {}
    if ids:
        placeholders = ",".join("?" for _ in ids)
        result = connection.execute(
            f"SELECT response_id, updated_at FROM fact_response "
            f"WHERE response_id IN ({placeholders})",
            ids,
        )
        existing = {row["response_id"]: row["updated_at"] for row in result}

    inserted = sum(row["response_id"] not in existing for row in rows)
    updated = sum(
        row["response_id"] in existing
        and row["updated_at"] > existing[row["response_id"]]
        for row in rows
    )

    with connection:
        connection.executemany(
            """
            INSERT INTO fact_response VALUES (
                :response_id, :customer_id, :journey_stage, :responded_at,
                :updated_at, :event_version, :nps_score, :csat_score,
                :category, :comment
            )
            ON CONFLICT(response_id) DO UPDATE SET
                customer_id = excluded.customer_id,
                journey_stage = excluded.journey_stage,
                responded_at = excluded.responded_at,
                updated_at = excluded.updated_at,
                event_version = excluded.event_version,
                nps_score = excluded.nps_score,
                csat_score = excluded.csat_score,
                category = excluded.category,
                comment = excluded.comment
            WHERE excluded.updated_at > fact_response.updated_at
            """,
            rows,
        )

    return {
        "inserted": inserted,
        "updated": updated,
        "replayed_or_unchanged": len(rows) - inserted - updated,
    }


# 4. PUBLISH GOLD -------------------------------------------------------------

def export_gold_csv(connection: sqlite3.Connection, path: Path) -> int:
    rows = connection.execute(
        """
        SELECT response_id, customer_id, journey_stage, responded_at, updated_at,
               event_version, nps_score, csat_score, category, comment
        FROM fact_response
        ORDER BY response_id
        """
    ).fetchall()

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(rows[0].keys() if rows else ())
        writer.writerows(tuple(row) for row in rows)
    return len(rows)


# 5. COMMIT STATE AND WRITE REPORTS ------------------------------------------

def read_state() -> dict:
    path = ROOT / "runtime" / "watermark.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def text_table(headers: tuple[str, ...], rows: list[tuple]) -> str:
    values = [[str(cell) for cell in row] for row in rows]
    widths = [len(header) for header in headers]
    for row in values:
        widths = [max(width, len(cell)) for width, cell in zip(widths, row)]

    def render(row) -> str:
        return " | ".join(cell.ljust(width) for cell, width in zip(row, widths))

    rule = "-+-".join("-" * width for width in widths)
    return "\n".join([render(headers), rule, *(render(row) for row in values)])


def save_reports(report: dict) -> None:
    validation = report["validation"]
    upsert = report["upsert"]
    details = [
        ("Run", "Load mode", report["mode"]),
        ("Run", "Source cutoff", report["as_of"]),
        ("Extraction", "API pages requested", report["pages_requested"]),
        ("Extraction", "Rows available in a full scan", report["full_scan_rows_at_cutoff"]),
        ("Extraction", "Rows actually extracted", report["extracted_rows"]),
        ("Extraction", "Rows avoided", report["rows_avoided_vs_full_scan"]),
        ("Extraction", "Read reduction", f'{report["read_reduction_pct"]:.1f}%'),
        ("State", "Previous watermark", report["previous_watermark"] or "None"),
        ("State", "Query watermark", report["query_watermark"] or "None"),
        ("State", "Committed watermark", report["committed_watermark"]),
        ("Validation", "Valid latest versions", validation["valid_latest_rows_in_batch"]),
        ("Validation", "Superseded or replayed", validation["superseded_or_replayed_rows_in_batch"]),
        ("Validation", "Sent to quarantine", validation["quarantined_rows"]),
        ("Upsert", "Inserted", upsert["inserted"]),
        ("Upsert", "Updated", upsert["updated"]),
        ("Upsert", "Replayed or unchanged", upsert["replayed_or_unchanged"]),
        ("Output", "Final Gold rows", report["analytical_rows"]),
    ]
    report_path = ROOT / "reports" / f"{report['mode']}_{report['as_of'][:10]}.txt"
    report_path.write_text(
        f"EXERCISE 01 - {report['mode'].upper()} LOAD\n"
        f"Run ID: {report['run_id']}\n\n"
        f"{text_table(('Stage', 'Metric', 'Value'), details)}\n",
        encoding="utf-8",
    )

    history_path = ROOT / "runtime" / "run_history.json"
    history = json.loads(history_path.read_text(encoding="utf-8")) if history_path.exists() else []
    key = (report["mode"], report["as_of"])
    history = [item for item in history if (item["mode"], item["as_of"]) != key]
    history.append(report)
    history.sort(key=lambda item: item["as_of"])
    write_json(history_path, history)

    rows = [
        (
            item["mode"],
            item["as_of"],
            item["full_scan_rows_at_cutoff"],
            item["extracted_rows"],
            item["rows_avoided_vs_full_scan"],
            f'{item["read_reduction_pct"]:.1f}%',
            item["analytical_rows"],
        )
        for item in history
    ]
    (ROOT / "reports" / "load_comparison.txt").write_text(
        text_table(
            ("Mode", "Cutoff", "Full scan", "Rows read", "Avoided", "Reduction", "Gold"),
            rows,
        )
        + "\n",
        encoding="utf-8",
    )


# 6. ORCHESTRATE ONE RUN ------------------------------------------------------

def reset_exercise() -> None:
    runtime = ROOT / "runtime"
    if runtime.exists():
        shutil.rmtree(runtime)

    gold = ROOT / "data" / "gold" / "customer_experience_responses.csv"
    if gold.exists():
        gold.unlink()

    for report in (ROOT / "reports").glob("*.txt"):
        report.unlink()


def run_pipeline(
    base_url: str,
    token: str,
    as_of: str,
    reset: bool = False,
) -> dict:
    if reset:
        reset_exercise()

    state = read_state()
    previous_watermark = state.get("watermark")
    mode = "full" if previous_watermark is None else "incremental"
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    started = time.perf_counter()

    bronze_path = ROOT / "runtime" / "bronze" / run_id / "responses.jsonl"
    extraction = extract_responses(
        base_url,
        token,
        bronze_path,
        as_of,
        previous_watermark,
    )

    resolved = validate_and_resolve(read_jsonl(bronze_path))
    write_jsonl(
        ROOT / "runtime" / "silver" / run_id / "responses.jsonl",
        resolved["valid_latest"],
    )
    write_jsonl(
        ROOT / "runtime" / "quarantine" / run_id / "responses.jsonl",
        resolved["quarantine"],
    )

    database_path = ROOT / "runtime" / "customer_experience.db"
    connection = connect_database(database_path)
    try:
        upsert = upsert_responses(connection, resolved["valid_latest"])
        gold_rows = export_gold_csv(
            connection,
            ROOT / "data" / "gold" / "customer_experience_responses.csv",
        )
    finally:
        connection.close()

    full_scan_rows = count_available_records(base_url, token, as_of)
    rows_read = extraction["extracted_rows"]
    rows_avoided = max(0, full_scan_rows - rows_read)
    committed_watermark = extraction["maximum_updated_at"] or previous_watermark

    report = {
        "run_id": run_id,
        "mode": mode,
        "as_of": as_of,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "full_scan_rows_at_cutoff": full_scan_rows,
        "extracted_rows": rows_read,
        "rows_avoided_vs_full_scan": rows_avoided,
        "read_reduction_pct": (
            round(100 * rows_avoided / full_scan_rows, 1) if full_scan_rows else 0.0
        ),
        "pages_requested": extraction["pages_requested"],
        "previous_watermark": previous_watermark,
        "query_watermark": extraction["query_watermark"],
        "committed_watermark": committed_watermark,
        "validation": resolved["statistics"],
        "upsert": upsert,
        "analytical_rows": gold_rows,
    }

    # The watermark changes only after Bronze, validation, upsert and Gold succeed.
    write_json(
        ROOT / "runtime" / "watermark.json",
        {"watermark": committed_watermark, "run_id": run_id, "as_of": as_of},
    )
    save_reports(report)
    return report


def print_summary(reports: list[dict]) -> None:
    rows = [
        (
            report["mode"],
            report["extracted_rows"],
            report["rows_avoided_vs_full_scan"],
            f'{report["read_reduction_pct"]:.1f}%',
            report["analytical_rows"],
        )
        for report in reports
    ]
    print(
        text_table(
            ("Mode", "Rows read", "Rows avoided", "Reduction", "Gold rows"),
            rows,
        )
    )
    print("\nDetailed tables: reports/")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run CX Exercise 01 locally.")
    parser.add_argument("--base-url", default=API_URL)
    parser.add_argument("--token", default=API_TOKEN)
    parser.add_argument("--as-of", default=INCREMENTAL_CUTOFF)
    parser.add_argument("--reset", action="store_true")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run the historical and incremental loads in sequence.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    base_url = args.base_url.rstrip("/")

    if args.demo:
        reports = [
            run_pipeline(base_url, args.token, FULL_CUTOFF, reset=True),
            run_pipeline(base_url, args.token, INCREMENTAL_CUTOFF),
        ]
    else:
        reports = [
            run_pipeline(base_url, args.token, args.as_of, reset=args.reset)
        ]
    print_summary(reports)


if __name__ == "__main__":
    main()
