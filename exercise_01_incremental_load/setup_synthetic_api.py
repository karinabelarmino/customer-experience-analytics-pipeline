# Databricks notebook source
"""Generate deterministic synthetic data and expose it through a local API.

This file represents the source system used in Exercise 1. It contains no
company data, endpoint, credential or business rule. When executed locally it
starts an HTTP server. When called with ``%run`` from the companion Databricks
notebook, it starts the same server in a background thread on the driver.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen


API_TOKEN = "synthetic-demo-token"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
STAGES = ("onboarding", "service", "support", "renewal")
CATEGORIES = {
    "onboarding": ("access", "information"),
    "service": ("experience", "schedule"),
    "support": ("resolution", "communication"),
    "renewal": ("value", "relationship"),
}

# 1. BUILD THE SYNTHETIC SOURCE ----------------------------------------------

def iso_timestamp(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def bounded_score(
    generator: random.Random,
    mean: float,
    standard_deviation: float,
    lower_bound: int,
    upper_bound: int,
) -> int:
    score = int(round(generator.gauss(mean, standard_deviation)))
    return max(lower_bound, min(upper_bound, score))


def generate_responses(count: int = 6000, seed: int = 20260826) -> list[dict]:
    """Create 6,000 responses plus revisions and validation errors."""
    generator = random.Random(seed)
    start = datetime(2025, 1, 1, 8, 0, tzinfo=timezone.utc)
    score_profiles = {
        "onboarding": (8.2, 4.2),
        "service": (7.6, 3.9),
        "support": (6.1, 3.3),
        "renewal": (7.0, 3.7),
    }
    comments = (
        "Everything worked as expected.",
        "The information could be clearer.",
        "I needed more than one contact.",
        "The response was quick.",
        "The waiting time affected the experience.",
    )
    events: list[dict] = []

    for index in range(1, count + 1):
        responded_at = start + timedelta(minutes=(index - 1) * 91)
        journey_stage = generator.choices(
            STAGES,
            weights=(0.20, 0.35, 0.30, 0.15),
            k=1,
        )[0]
        nps_mean, csat_mean = score_profiles[journey_stage]
        response = {
            "response_id": f"R{index:06d}",
            "customer_id": f"C{generator.randint(1, 2400):06d}",
            "journey_stage": journey_stage,
            "responded_at": iso_timestamp(responded_at),
            "updated_at": iso_timestamp(
                responded_at + timedelta(minutes=generator.randint(5, 180))
            ),
            "event_version": 1,
            "nps_score": bounded_score(generator, nps_mean, 1.9, 0, 10),
            "csat_score": bounded_score(generator, csat_mean, 0.9, 1, 5),
            "category": generator.choice(CATEGORIES[journey_stage]),
            "comment": generator.choice(comments),
        }
        events.append(response)

        # Some responses are revised later to reproduce a changing source system.
        if index % 17 == 0:
            revision = dict(response)
            revision["updated_at"] = iso_timestamp(
                responded_at + timedelta(days=14, minutes=index % 59)
            )
            revision["event_version"] = 2
            revision["nps_score"] = max(
                0,
                min(10, response["nps_score"] + generator.choice((-1, 0, 1))),
            )
            revision["csat_score"] = max(
                1,
                min(5, response["csat_score"] + generator.choice((-1, 0, 1))),
            )
            events.append(revision)

        # A few malformed events make the validation and quarantine steps observable.
        if index % 233 == 0:
            invalid_event = dict(response)
            invalid_event["updated_at"] = iso_timestamp(responded_at + timedelta(days=2))
            invalid_event["event_version"] = 99
            invalid_event["nps_score"] = 12
            events.append(invalid_event)

    events.sort(
        key=lambda event: (
            event["updated_at"],
            event["response_id"],
            event["event_version"],
        )
    )
    return events


def write_source(path: Path, force: bool = False) -> int:
    if path.exists() and not force:
        with path.open("r", encoding="utf-8") as source_file:
            return sum(1 for line in source_file if line.strip())

    events = generate_responses()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as source_file:
        for event in events:
            source_file.write(
                json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n"
            )
    return len(events)


def load_source(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as source_file:
        return [json.loads(line) for line in source_file if line.strip()]

# 2. SERVE THE SOURCE THROUGH A PAGINATED API --------------------------------

def build_handler(events: list[dict], token: str) -> type[BaseHTTPRequestHandler]:

    class SyntheticAPIHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args) -> None:
            return

        def send_json(self, status: int, payload: dict) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            parsed_url = urlparse(self.path)

            if parsed_url.path == "/health":
                self.send_json(200, {"status": "ok"})
                return

            if self.headers.get("Authorization") != f"Bearer {token}":
                self.send_json(401, {"error": "unauthorized"})
                return

            if parsed_url.path != "/v1/responses":
                self.send_json(404, {"error": "not_found"})
                return

            query = parse_qs(parsed_url.query)
            page = max(1, int(query.get("page", ["1"])[0]))
            page_size = min(500, max(1, int(query.get("page_size", ["200"])[0])))
            as_of = parse_timestamp(
                query.get("as_of", ["2100-01-01T00:00:00Z"])[0]
            )
            watermark_text = query.get("updated_since", [None])[0]
            watermark = parse_timestamp(watermark_text) if watermark_text else None

            eligible_events = []
            for event in events:
                updated_at = parse_timestamp(event["updated_at"])
                if updated_at > as_of:
                    continue
                if watermark is not None and updated_at <= watermark:
                    continue
                eligible_events.append(event)

            start = (page - 1) * page_size
            stop = start + page_size
            next_page = page + 1 if stop < len(eligible_events) else None
            self.send_json(
                200,
                {
                    "data": eligible_events[start:stop],
                    "pagination": {
                        "page": page,
                        "page_size": page_size,
                        "total_records": len(eligible_events),
                        "next_page": next_page,
                    },
                },
            )

    return SyntheticAPIHandler


def create_server(
    source_path: Path,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    token: str = API_TOKEN,
) -> ThreadingHTTPServer:
    handler = build_handler(load_source(source_path), token)
    return ThreadingHTTPServer((host, port), handler)


def api_is_ready(base_url: str) -> bool:
    try:
        with urlopen(f"{base_url}/health", timeout=1) as response:
            return response.status == 200
    except (OSError, URLError):
        return False


def wait_for_api(base_url: str, attempts: int = 40) -> None:
    for _ in range(attempts):
        if api_is_ready(base_url):
            return
        time.sleep(0.1)
    raise RuntimeError("The synthetic API did not become ready.")


def start_api_in_background(
    source_path: Path,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    token: str = API_TOKEN,
) -> ThreadingHTTPServer | None:
    base_url = f"http://{host}:{port}"
    if api_is_ready(base_url):
        return None

    server = create_server(source_path, host=host, port=port, token=token)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    wait_for_api(base_url)
    return server

# 3. START LOCALLY OR INSIDE DATABRICKS --------------------------------------

def local_source_path() -> Path:
    return Path(__file__).resolve().parent / "data" / "source" / "responses.jsonl"


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate the synthetic source and serve the paginated API."
    )
    parser.add_argument("--source", type=Path, default=local_source_path())
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--token", default=API_TOKEN)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--generate-only", action="store_true")
    return parser.parse_args()


def run_locally() -> None:
    args = parse_arguments()
    source_rows = write_source(args.source, force=args.force)

    print(f"Synthetic source ready: {source_rows:,} events")
    print(f"Source file: {args.source}")

    if args.generate_only:
        return

    server = create_server(
        args.source,
        host=args.host,
        port=args.port,
        token=args.token,
    )
    print(f"API listening at http://{args.host}:{args.port}/v1/responses")
    print("Keep this terminal open. Press Ctrl+C to stop the API.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Synthetic API stopped.")
    finally:
        server.server_close()


IN_DATABRICKS = bool(
    os.environ.get("DATABRICKS_RUNTIME_VERSION")
    or os.environ.get("DATABRICKS_SERVERLESS_ENV")
    or ("spark" in globals() and "dbutils" in globals())
)

if IN_DATABRICKS:
    SYNTHETIC_API_BASE_URL = f"http://{DEFAULT_HOST}:{DEFAULT_PORT}"
    SYNTHETIC_API_TOKEN = API_TOKEN
    SYNTHETIC_SOURCE_PATH = Path(
        "/tmp/customer_experience_exercise_01/responses.jsonl"
    )
    SYNTHETIC_SOURCE_ROWS = write_source(SYNTHETIC_SOURCE_PATH)
    SYNTHETIC_API_SERVER = start_api_in_background(SYNTHETIC_SOURCE_PATH)

    print(
        f"Synthetic API ready at {SYNTHETIC_API_BASE_URL} "
        f"({SYNTHETIC_SOURCE_ROWS:,} events)"
    )
elif __name__ == "__main__":
    run_locally()
