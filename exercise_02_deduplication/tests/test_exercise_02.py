import copy
import json
from pathlib import Path
import random
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from core import canonical, clean_text, explode_like_legacy, metrics, model, normalize, resolve
from run_pipeline_local import run_batch
from setup_synthetic_data import build_source, micro_example, question, response, revised


def delivery(payload, number=1):
    return {"receipt_id": f"test-{number:03d}", "payload": payload}


class ResponseGrainTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows, cls.cases = build_source()
        cls.result = resolve(cls.rows)
        cls.tables = model(cls.result["current"])

    def by_id(self, rid, survey="survey-demo"):
        return next(r for r in self.result["current"] if r["response_id"] == rid and r["survey_id"] == survey)

    def test_micro_example_fanout_changes_nps(self):
        selected = resolve([delivery(p, i) for i, p in enumerate(micro_example())])["current"]
        flat = explode_like_legacy(selected)
        self.assertEqual(len(flat), 8)
        self.assertEqual(sum(r["response_id"] == "R001" for r in flat), 6)
        self.assertAlmostEqual(metrics(model(selected)["responses"])["nps"], 33.333333)
        self.assertEqual(metrics(flat)["nps"], -50)

    def test_generator_reproduces_identical_source(self):
        self.assertEqual(build_source(), (self.rows, self.cases))

    def test_grains_and_foreign_keys_preserve_children(self):
        keys = {(r["survey_id"], r["response_id"]) for r in self.tables["responses"]}
        self.assertEqual(len(keys), 1215)
        self.assertEqual(len(self.tables["responses_wide"]), len(keys))
        for name, suffix in [("response_categories", ("category", "subcategory")),
                             ("question_answers", ("question_id",))]:
            rows = self.tables[name]
            self.assertEqual(len(rows), len({tuple(r[k] for k in ("survey_id", "response_id") + suffix) for r in rows}))
            self.assertTrue(all((r["survey_id"], r["response_id"]) in keys for r in rows))
        self.assertEqual(len(self.by_id("R001")["categories"]), 3)

    def test_response_without_children_is_retained(self):
        self.assertEqual(self.by_id("R003")["questions"], [])
        self.assertIn("R003", {r["response_id"] for r in self.tables["responses_wide"]})

    def test_latest_snapshot_removes_old_children(self):
        r = self.by_id("R004")
        self.assertEqual((r["source_version"], r["score"], r["categories"]), (2, 10, []))
        self.assertEqual([q["question_id"] for q in r["questions"]], ["q_wait"])

    def test_same_customer_and_survey_scoped_ids_survive(self):
        self.assertEqual(self.by_id("R008")["customer_id"], self.by_id("R009")["customer_id"])
        self.assertEqual(len([r for r in self.tables["responses"] if r["response_id"] == "R001"]), 2)

    def test_unicode_and_duplicate_children_normalize(self):
        r = self.by_id("R005")
        self.assertEqual(len(r["categories"]), 1)
        self.assertEqual(len(r["questions"]), 1)
        self.assertEqual(r["questions"][0]["value"], 0)
        self.assertEqual(r["questions"][0]["question_text"], "Tempo de espera")
        self.assertEqual(r["quality_status"], "ok")

    def test_fallback_preserves_arrays_and_structures(self):
        qs = {q["question_id"]: q["value"] for q in self.by_id("R006")["questions"]}
        self.assertEqual(qs["q_channels"], ["App", "Telefone"])
        self.assertEqual(qs["q_detail"]["contact"]["channel"], "App")
        self.assertEqual(self.by_id("R014")["questions"][0]["value"][0]["subOptions"], ["Chat", "Formulário"])

    def test_empty_array_falls_back_and_false_is_valid(self):
        p = response("edge", questions=[question("q_detail", "Detalhe", review=[], answerText="Texto")])
        self.assertEqual(normalize(p)["questions"][0]["value"], "Texto")
        p["additionalQuestions"][0]["review"] = False
        self.assertIs(normalize(p)["questions"][0]["value"], False)

    def test_question_id_prevents_column_name_collision(self):
        r = next(r for r in self.tables["responses_wide"] if r["response_id"] == "R007")
        self.assertEqual((r["answer_wait"], r["answer_wait_alternative"]), ("1", "2"))

    def test_unconfigured_question_remains_in_long_table(self):
        row = next(r for r in self.tables["question_answers"] if r["response_id"] == "R013")
        self.assertEqual(row["question_id"], "q_new")
        self.assertIsNone(row["value_json"])

    def test_conflicting_version_quarantines_both_candidates(self):
        self.assertEqual(self.by_id("R010")["source_version"], 1)
        self.assertEqual(self.by_id("R010")["quality_status"], "has_rejected_history")
        self.assertEqual(sum(r["reason"] == "response_version_conflict" for r in self.result["quarantine"]), 2)

    def test_event_id_conflict_removes_previously_accepted_event(self):
        self.assertNotIn("BAD008", {r["response_id"] for r in self.result["current"]})
        self.assertEqual(sum(r["reason"] == "event_id_payload_conflict" for r in self.result["quarantine"]), 2)

    def test_source_version_resolves_timestamp_tie(self):
        self.assertEqual((self.by_id("R011")["source_version"], self.by_id("R011")["score"]), (2, 10))

    def test_timezone_equivalence_and_missing_zone_rejection(self):
        self.assertEqual(self.by_id("R015")["quality_status"], "ok")
        with self.assertRaisesRegex(ValueError, "timezone"):
            normalize(response("bad-zone", updatedAt="2026-08-01T12:00:00"))

    def test_tombstone_stays_in_history_and_leaves_active_gold(self):
        self.assertTrue(self.by_id("R012")["deleted"])
        self.assertNotIn("R012", {r["response_id"] for r in self.tables["responses"]})

    def test_invalid_latest_preserves_flagged_previous_valid(self):
        self.assertEqual(self.by_id("B0000")["source_version"], 2)
        self.assertEqual(self.by_id("B0000")["quality_status"], "has_rejected_history")
        reasons = {r["reason"] for r in self.result["quarantine"]}
        self.assertTrue({"score_out_of_range", "missing_snapshot_children", "conflicting_question_in_snapshot"} <= reasons)

    def test_order_independence_and_transport_replay(self):
        shuffled = list(self.rows); random.Random(999).shuffle(shuffled)
        self.assertEqual(resolve(shuffled), self.result)
        again = [dict(r, receipt_id="replay-" + r["receipt_id"]) for r in self.rows]
        self.assertEqual(model(resolve(self.rows + again)["current"]), self.tables)

    def test_every_bronze_receipt_has_exactly_one_audit_outcome(self):
        self.assertEqual(len(self.result["audit"]), 1525)
        self.assertEqual({r["receipt_id"] for r in self.result["audit"]}, {r["receipt_id"] for r in self.rows})
        # 7 invalid base snapshots + 20 invalid revisions + 2 version conflicts
        # + both sides of an event-ID collision = 31 rejected deliveries.
        self.assertEqual(len(self.result["quarantine"]), 31)

    def test_persisted_jobs_equal_full_rebuild_and_replay(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / "cx.sqlite"
            run_batch(db, [r for r in self.rows if r["batch_id"] == "initial"])
            incremental = [r for r in self.rows if r["batch_id"] == "incremental"]
            updated = run_batch(db, incremental)
            replayed = run_batch(db, incremental)
            self.assertEqual(updated["tables"], self.tables)
            self.assertEqual(updated, replayed)

    def test_receipt_collision_rolls_back_whole_batch(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / "cx.sqlite"
            p = delivery(response("atomic"))
            before = run_batch(db, [p])
            wrong = copy.deepcopy(p); wrong["payload"]["review"] = 0
            with self.assertRaisesRegex(ValueError, "receipt_id_collision"):
                run_batch(db, [delivery(response("never-commit"), 2), wrong])
            self.assertEqual(run_batch(db, []), before)


if __name__ == "__main__":
    unittest.main()
