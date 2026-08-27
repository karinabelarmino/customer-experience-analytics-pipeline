"""Run Exercise 1 outside Databricks from extraction through CSV export.

Start ``setup_synthetic_api.py`` in another terminal before executing this
file. The pipeline uses only Python's standard library so it can be reproduced
without a platform-specific environment.
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
DEFAULT_BASE_URL = "http://127.0.0.1:8765"
DEFAULT_TOKEN = "synthetic-demo-token"
OVERLAP_MINUTES = 5
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


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-formatted timestamp and normalize it to UTC."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def overlap_watermark(watermark: str | None, minutes: int = OVERLAP_MINUTES) -> str | None:
    """Move the watermark back so boundary events can be replayed safely."""
    if watermark is None:
        return None
    adjusted = parse_timestamp(watermark) - timedelta(minutes=minutes)
    return adjusted.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def request_json(url: str, token: str, attempts: int = 3) -> dict:
    """Request one API page with a small exponential-backoff retry policy."""
    request = Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
    )
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(request, timeout=20) as response:
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError) as error:
            if attempt == attempts:
                raise RuntimeError(
                    f"API request failed after {attempts} attempts. "
                    "Confirm that setup_synthetic_api.py is running."
                ) from error
            time.sleep(0.25 * (2 ** (attempt - 1)))
    raise RuntimeError("Unreachable retry state.")


def count_available_records(base_url: str, token: str, as_of: str) -> int:
    """Read API metadata for the counterfactual full scan at a cutoff."""
    parameters = urlencode({"page": 1, "page_size": 1, "as_of": as_of})
    payload = request_json(f"{base_url}/v1/responses?{parameters}", token)
    return int(payload["pagination"]["total_records"])


def extract_responses(
    base_url: str,
    token: str,
    destination: Path,
    as_of: str,
    watermark: str | None,
    page_size: int = 250,
) -> dict:
    """Extract every API page and land the raw events as Bronze JSON Lines."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    page = 1
    pages_requested = 0
    extracted_rows = 0
    maximum_updated_at = watermark
    query_watermark = overlap_watermark(watermark)

    with destination.open("w", encoding="utf-8", newline="\n") as bronze_file:
        while True:
            parameters = {
                "page": page,
                "page_size": page_size,
                "as_of": as_of,
            }
            if query_watermark is not None:
                parameters["updated_since"] = query_watermark

            payload = request_json(
                f"{base_url}/v1/responses?{urlencode(parameters)}",
                token,
            )
            events = payload.get("data", [])
            for event in events:
                bronze_file.write(
                    json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n"
                )
                extracted_rows += 1
                if (
                    maximum_updated_at is None
                    or event["updated_at"] > maximum_updated_at
                ):
                    maximum_updated_at = event["updated_at"]

            pages_requested += 1
            next_page = payload.get("pagination", {}).get("next_page")
            if next_page is None:
                break
            page = int(next_page)

    return {
        "bronze_path": str(destination),
        "pages_requested": pages_requested,
        "extracted_rows": extracted_rows,
        "previous_watermark": watermark,
        "query_watermark": query_watermark,
        "maximum_updated_at": maximum_updated_at,
    }


def read_json_lines(path: Path) -> list[dict]:
    """Read a JSON Lines file into a list of dictionaries."""
    with path.open("r", encoding="utf-8") as source_file:
        return [json.loads(line) for line in source_file if line.strip()]


def write_json_lines(path: Path, rows: list[dict]) -> None:
    """Write dictionaries to a JSON Lines file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as destination:
        for row in rows:
            destination.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def validate_response(event: dict) -> tuple[bool, str | None]:
    """Validate required fields and the accepted NPS and CSAT ranges."""
    missing_fields = [
        field for field in REQUIRED_FIELDS if event.get(field) in (None, "")
    ]
    if missing_fields:
        return False, f"missing:{','.join(missing_fields)}"
    if not isinstance(event["nps_score"], int) or not 0 <= event["nps_score"] <= 10:
        return False, "invalid_nps_score"
    if not isinstance(event["csat_score"], int) or not 1 <= event["csat_score"] <= 5:
        return False, "invalid_csat_score"
    return True, None


def validate_and_resolve(events: list[dict]) -> dict:
    """Quarantine invalid events and retain the newest version in the batch."""
    latest_by_response: dict[str, dict] = {}
    quarantine: list[dict] = []

    for event in events:
        is_valid, reason = validate_response(event)
        if not is_valid:
            quarantine.append({"reason": reason, "record": event})
            continue

        response_id = event["response_id"]
        candidate_order = (event["updated_at"], int(event["event_version"]))
        current = latest_by_response.get(response_id)
        if current is None or candidate_order > (
            current["updated_at"],
            int(current["event_version"]),
        ):
            latest_by_response[response_id] = event

    valid_latest = sorted(
        latest_by_response.values(),
        key=lambda event: event["response_id"],
    )
    return {
        "valid_latest": valid_latest,
        "quarantine": quarantine,
        "statistics": {
            "extracted_rows": len(events),
            "valid_latest_rows_in_batch": len(valid_latest),
            "superseded_or_replayed_rows_in_batch": (
                len(events) - len(quarantine) - len(valid_latest)
            ),
            "quarantined_rows": len(quarantine),
        },
    }


def connect_database(path: Path) -> sqlite3.Connection:
    """Open the analytical SQLite database and create its target table."""
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.executescript(TABLE_SCHEMA)
    return connection


def upsert_responses(connection: sqlite3.Connection, rows: list[dict]) -> dict:
    """Insert new responses and update only records with a newer timestamp."""
    response_ids = [row["response_id"] for row in rows]
    existing: dict[str, str] = {}
    if response_ids:
        placeholders = ",".join("?" for _ in response_ids)
        result = connection.execute(
            f"SELECT response_id, updated_at FROM fact_response "
            f"WHERE response_id IN ({placeholders})",
            response_ids,
        ).fetchall()
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


def export_gold_csv(connection: sqlite3.Connection, destination: Path) -> int:
    """Export the final analytical table to a platform-independent CSV file."""
    rows = connection.execute(
        """
        SELECT response_id, customer_id, journey_stage, responded_at, updated_at,
               event_version, nps_score, csat_score, category, comment
        FROM fact_response
        ORDER BY response_id
        """
    ).fetchall()
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(rows[0].keys() if rows else ())
        writer.writerows([tuple(row) for row in rows])
    return len(rows)


def write_json(path: Path, payload: dict) -> None:
    """Write a formatted JSON report or pipeline state file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as output_file:
        json.dump(payload, output_file, ensure_ascii=False, indent=2, sort_keys=True)
        output_file.write("\n")


def export_load_comparison() -> None:
    """Build a compact CSV comparison from the available run reports."""
    report_directory = ROOT / "reports"
    rows = []
    for report_path in sorted(report_directory.glob("*.json")):
        report = json.loads(report_path.read_text(encoding="utf-8"))
        rows.append(
            {
                "mode": report["mode"],
                "as_of": report["as_of"],
                "full_scan_rows": report["full_scan_rows_at_cutoff"],
                "rows_read": report["extracted_rows"],
                "rows_avoided": report["rows_avoided_vs_full_scan"],
                "read_reduction_pct": report["read_reduction_pct"],
                "analytical_rows": report["analytical_rows"],
            }
        )

    comparison_path = report_directory / "load_comparison.csv"
    with comparison_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=(
                "mode",
                "as_of",
                "full_scan_rows",
                "rows_read",
                "rows_avoided",
                "read_reduction_pct",
                "analytical_rows",
            ),
        )
        writer.writeheader()
        writer.writerows(rows)


def read_state() -> dict:
    """Read the last successfully committed watermark, when available."""
    state_path = ROOT / "runtime" / "watermark.json"
    if not state_path.exists():
        return {}
    return json.loads(state_path.read_text(encoding="utf-8"))


def reset_runtime() -> None:
    """Remove only the generated runtime area for a clean full-load demonstration."""
    runtime_path = ROOT / "runtime"
    if runtime_path.exists():
        shutil.rmtree(runtime_path)
    gold_path = ROOT / "data" / "gold" / "customer_experience_responses.csv"
    if gold_path.exists():
        gold_path.unlink()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Exercise 1 from API extraction through analytical export."
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--token", default=DEFAULT_TOKEN)
    parser.add_argument("--as-of", default="2026-01-15T23:59:59Z")
    parser.add_argument("--reset", action="store_true")
    return parser.parse_args()


def run_pipeline(
    base_url: str,
    token: str,
    as_of: str,
    reset: bool = False,
) -> dict:
    """Execute one full or incremental run and commit state only after success."""
    if reset:
        reset_runtime()

    state = read_state()
    previous_watermark = state.get("watermark")
    load_mode = "full" if previous_watermark is None else "incremental"
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    started_at = time.perf_counter()

    bronze_path = ROOT / "runtime" / "bronze" / run_id / "responses.jsonl"
    extraction = extract_responses(
        base_url=base_url,
        token=token,
        destination=bronze_path,
        as_of=as_of,
        watermark=previous_watermark,
    )
    transformed = validate_and_resolve(read_json_lines(bronze_path))

    silver_path = ROOT / "runtime" / "silver" / run_id / "responses.jsonl"
    quarantine_path = ROOT / "runtime" / "quarantine" / run_id / "responses.jsonl"
    write_json_lines(silver_path, transformed["valid_latest"])
    write_json_lines(quarantine_path, transformed["quarantine"])

    database_path = ROOT / "runtime" / "customer_experience.db"
    connection = connect_database(database_path)
    try:
        load_statistics = upsert_responses(
            connection,
            transformed["valid_latest"],
        )
        analytical_rows = export_gold_csv(
            connection,
            ROOT / "data" / "gold" / "customer_experience_responses.csv",
        )
    finally:
        connection.close()

    # The state changes only after extraction, validation, load and export succeed.
    committed_watermark = extraction["maximum_updated_at"] or previous_watermark
    write_json(
        ROOT / "runtime" / "watermark.json",
        {
            "watermark": committed_watermark,
            "committed_run_id": run_id,
            "as_of": as_of,
        },
    )

    full_scan_rows = count_available_records(base_url, token, as_of)
    extracted_rows = extraction["extracted_rows"]
    avoided_rows = max(0, full_scan_rows - extracted_rows)
    reduction_pct = (
        round(100 * avoided_rows / full_scan_rows, 1) if full_scan_rows else 0.0
    )
    report = {
        "run_id": run_id,
        "exercise": "01_full_vs_incremental_load",
        "mode": load_mode,
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
        "validation": transformed["statistics"],
        "upsert": load_statistics,
        "analytical_rows": analytical_rows,
        "outputs": {
            "bronze": str(bronze_path.relative_to(ROOT)),
            "silver": str(silver_path.relative_to(ROOT)),
            "quarantine": str(quarantine_path.relative_to(ROOT)),
            "sqlite": str(database_path.relative_to(ROOT)),
            "gold_csv": "data/gold/customer_experience_responses.csv",
        },
    }
    report_path = ROOT / "reports" / f"{load_mode}_{as_of[:10]}.json"
    write_json(report_path, report)
    export_load_comparison()
    return report


def main() -> None:
    args = parse_arguments()
    report = run_pipeline(
        base_url=args.base_url.rstrip("/"),
        token=args.token,
        as_of=args.as_of,
        reset=args.reset,
    )
    print("This output summarizes extraction, validation, upsert, watermark and analytical export results.")
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
