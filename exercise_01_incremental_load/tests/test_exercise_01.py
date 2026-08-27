from __future__ import annotations

import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path


EXERCISE_ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, path: Path):
    specification = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(specification)
    assert specification.loader is not None
    specification.loader.exec_module(module)
    return module


setup_api = load_module("setup_synthetic_api", EXERCISE_ROOT / "setup_synthetic_api.py")
pipeline = load_module("run_pipeline_local", EXERCISE_ROOT / "run_pipeline_local.py")


class ExerciseOneTests(unittest.TestCase):
    def test_source_generation_is_deterministic(self) -> None:
        events = setup_api.generate_responses()
        self.assertEqual(len(events), 6377)
        self.assertEqual(len({event["response_id"] for event in events}), 6000)
        self.assertEqual(sum(event["nps_score"] == 12 for event in events), 25)

    def test_overlap_moves_watermark_back_five_minutes(self) -> None:
        self.assertEqual(
            pipeline.overlap_watermark("2026-01-15T10:00:00Z"),
            "2026-01-15T09:55:00Z",
        )

    def test_validation_quarantines_invalid_score_and_keeps_latest_version(self) -> None:
        base = {
            "response_id": "R000001",
            "customer_id": "C000001",
            "journey_stage": "support",
            "responded_at": "2025-01-01T10:00:00Z",
            "updated_at": "2025-01-01T10:05:00Z",
            "event_version": 1,
            "nps_score": 6,
            "csat_score": 3,
            "category": "resolution",
            "comment": "Example",
        }
        revision = dict(
            base,
            updated_at="2025-01-02T10:05:00Z",
            event_version=2,
            nps_score=7,
        )
        invalid = dict(base, response_id="R000002", nps_score=12)
        result = pipeline.validate_and_resolve([base, revision, invalid])

        self.assertEqual(len(result["valid_latest"]), 1)
        self.assertEqual(result["valid_latest"][0]["event_version"], 2)
        self.assertEqual(len(result["quarantine"]), 1)

    def test_upsert_is_idempotent(self) -> None:
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        connection.executescript(pipeline.TABLE_SCHEMA)
        row = {
            "response_id": "R000001",
            "customer_id": "C000001",
            "journey_stage": "support",
            "responded_at": "2025-01-01T10:00:00Z",
            "updated_at": "2025-01-01T10:05:00Z",
            "event_version": 1,
            "nps_score": 6,
            "csat_score": 3,
            "category": "resolution",
            "comment": "Example",
        }

        first = pipeline.upsert_responses(connection, [row])
        second = pipeline.upsert_responses(connection, [row])

        self.assertEqual(first["inserted"], 1)
        self.assertEqual(second["replayed_or_unchanged"], 1)
        self.assertEqual(
            connection.execute("SELECT COUNT(*) FROM fact_response").fetchone()[0],
            1,
        )
        connection.close()

    def test_source_can_be_written_and_loaded(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            source_path = Path(temporary_directory) / "responses.jsonl"
            written_rows = setup_api.write_source(source_path)
            loaded_rows = setup_api.load_source(source_path)

        self.assertEqual(written_rows, 6377)
        self.assertEqual(len(loaded_rows), 6377)


if __name__ == "__main__":
    unittest.main()
