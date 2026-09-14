"""Deterministic API-shaped delivery fixture. No network or real records."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import random

SEED = 20260913


def question(qid, text, **answer):
    return {"_id": qid, "text": text, **answer}


def response(rid, score=9, metric="NPS", categories=None, questions=None, **extra):
    return {"_id": rid, "eventId": f"event-{rid}-v1", "version": 1,
            "actionId": "survey-demo", "clientId": f"customer-{rid}",
            "inviteId": f"invite-{rid}", "journeyStage": "support",
            "metric": metric, "review": score, "text": "Como foi sua experiência?",
            "createdAt": "2026-08-01T12:00:00Z", "answerDate": "2026-08-01T12:00:00Z",
            "date": "2026-08-01T12:00:00Z", "updatedAt": "2026-08-01T12:00:00Z",
            "deleted": False, "feedback": "Comentário inteiramente sintético.",
            "categories": categories if categories is not None else [],
            "additionalQuestions": questions if questions is not None else [],
            "treatments": {"status": "open", "feed": []},
            "indicators": [{"column": "unit", "value": "Fictional Unit A"}], **extra}


def revised(original, version=2, **changes):
    result = deepcopy(original)
    result.update(version=version, eventId=f"{original['eventId'].split('-v')[0]}-v{version}",
                  updatedAt=f"2026-08-{version + 1:02}T12:00:00Z")
    result.update(changes)
    return result


def micro_example():
    return [
        response("R001", 3,
                 categories=[{"category": "Atendimento", "subCategory": "Demora"},
                             {"category": "Comunicação", "subCategory": "Retorno"},
                             {"category": "Processo", "subCategory": "Agendamento"}],
                 questions=[question("q_wait", "Tempo de espera", review=40),
                            question("q_resolution", "Problema resolvido?", answerText="Não")]),
        response("R002", 10, categories=[{"category": "Atendimento", "subCategory": "Cordialidade"}],
                 questions=[question("q_resolution", "Problema resolvido?", answerText="Sim")]),
        response("R003", 10),
    ]


def build_source(n_background=1200):
    rng = random.Random(SEED)
    deliveries, cases = [], []

    def add(payload, scenario, batch="initial"):
        index = len(deliveries)
        when = datetime(2026, 8, 10 if batch == "initial" else 11, tzinfo=timezone.utc) + timedelta(seconds=index)
        receipt = {"receipt_id": f"receipt-{index:06d}", "batch_id": batch,
                   "page": index // 100 + 1, "position": index % 100,
                   "received_at": when.isoformat().replace("+00:00", "Z"),
                   "payload": deepcopy(payload)}
        deliveries.append(receipt)
        cases.append({"receipt_id": receipt["receipt_id"], "response_id": payload.get("_id"),
                      "scenario": scenario, "batch_id": batch})

    for p in micro_example():
        add(p, "micro_example_fanout")
    base = micro_example()[0]
    add(base, "same_event_replayed", "incremental")
    p = deepcopy(base); p["eventId"] = "new-envelope-same-R001-content"
    add(p, "new_event_id_same_snapshot", "incremental")

    p = response("R004", 7, categories=[{"category": "Acesso", "subCategory": "Canal"}],
                 questions=[question("q_wait", "Tempo de espera", review=55),
                            question("q_detail", "Detalhe", answerText="Antes")])
    add(p, "revision_original")
    add(revised(p, review=10, categories=[], additionalQuestions=[
        question("q_wait", "Tempo de espera", review=5)]), "revision_removes_category_and_question", "incremental")
    add(p, "late_old_version", "incremental")

    p = response("R005", 4, "CSAT", categories=[{"category": "Atendimento", "subCategory": "Equipe"}] * 2,
                 questions=[question("q_wait", "\ufeffTempo\u00a0de\u00a0espera\u200b", review=0)] * 2)
    add(p, "duplicate_children_unicode_zero")
    p2 = deepcopy(p); p2["eventId"] = "unicode-equivalent-R005"
    p2["additionalQuestions"] = [question("q_wait", "Tempo de espera", review=0)]
    add(p2, "normalized_equivalent_snapshot", "incremental")

    add(response("R006", 5, "CSAT", questions=[
        question("q_channels", "Canais utilizados", review=" ", answerText="", multipleValues=["App", "Telefone"]),
        question("q_detail", "Detalhe", review={"contact": {"channel": "App"}, "resolved": True})]),
        "fallback_array_and_struct")
    add(response("R007", questions=[question("q_wait", "Tempo de espera", review=1),
                                    question("q_wait_alt", "Tempo-de-espera", review=2)]),
        "safe_column_name_collision")
    add(response("R008", clientId="customer-returning"), "same_customer_first_response")
    add(response("R009", clientId="customer-returning"), "same_customer_second_response")
    p = response("R010", 6)
    add(p, "conflict_previous_valid")
    add(revised(p, review=9), "same_version_conflict_a", "incremental")
    add(revised(p, eventId="conflicting-envelope-R010", review=10), "same_version_conflict_b", "incremental")
    p = response("R011", 8)
    add(p, "same_timestamp_original")
    add(revised(p, updatedAt=p["updatedAt"], review=10), "same_timestamp_higher_version", "incremental")
    p = response("R012", 10)
    add(p, "deletion_original")
    add(revised(p, deleted=True, categories=[], additionalQuestions=[]), "full_snapshot_tombstone", "incremental")
    add(response("R013", 10, questions=[question("q_new", "Nova pergunta", multipleValues=[])]),
        "unknown_question_retained_long")
    add(response("R014", 9, questions=[question("q_detail", "Detalhe", multipleValues=[
        {"option": "App", "subOptions": ["Chat", "Formulário"]},
        {"option": "Telefone", "subOptions": []}])]), "array_of_structs")
    p = response("R015", 9); add(p, "timezone_original")
    p2 = deepcopy(p); p2.update(eventId="timezone-alias", updatedAt="2026-08-01T09:00:00-03:00")
    add(p2, "same_instant_different_timezone", "incremental")
    add(response("R001", 5, "CSAT", actionId="survey-other", eventId="event-other-R001-v1"),
        "same_response_id_other_survey")

    # Invalid events are deliberate and remain traceable in Bronze and quarantine.
    add(response("BAD001", 12), "invalid_nps_score")
    add(response("BAD002", 0, "CSAT"), "invalid_csat_score")
    add(response("BAD003", updatedAt="not-a-date"), "invalid_timestamp")
    p = response("BAD004"); p["_id"] = None; add(p, "missing_response_id")
    p = response("BAD005"); p.pop("categories"); add(p, "missing_snapshot_array")
    add(response("BAD006", questions=[question("q_wait", "Tempo", review=1),
                                      question("q_wait", "Tempo", review=9)]), "question_value_conflict")
    add(response("BAD007", questions=[question("q_detail", "\u200b  ", review=1)]), "empty_question_label")
    p = response("BAD008"); add(p, "event_id_collision_a")
    p["review"] = 1; add(p, "event_id_collision_b", "incremental")

    labels = [("Atendimento", "Demora"), ("Comunicação", "Retorno"),
              ("Processo", "Agendamento"), ("Atendimento", "Cordialidade")]
    for i in range(n_background):
        metric = "NPS" if i % 3 else "CSAT"
        score = rng.randint(0, 10) if metric == "NPS" else rng.randint(1, 5)
        # Intentional selection pattern: dissatisfied responses tend to have
        # more classifications. The amount and direction of bias are synthetic.
        ncat = rng.randint(2, 4) if score <= (6 if metric == "NPS" else 3) else rng.randint(0, 2)
        cats = [{"category": a, "subCategory": b} for a, b in labels[:ncat]]
        qs = [question("q_wait", "Tempo de espera", review=rng.randint(0, 90)),
              question("q_resolution", "Problema resolvido?", answerText="Sim" if score >= 8 else "Não"),
              question("q_channels", "Canais utilizados", multipleValues=["App", "Telefone"])]
        p = response(f"B{i:04d}", str(score) if i % 10 == 0 else score, metric,
                     categories=cats, questions=qs[:rng.randint(0, 3)],
                     clientId=f"customer-{i // 2:04d}",
                     journeyStage=["onboarding", "service", "support", "renewal"][i % 4])
        add(p, "background_original")
        if i % 8 == 0:
            new_score = min(score + 1, 10 if metric == "NPS" else 5)
            add(revised(p, review=new_score, categories=cats[:1], additionalQuestions=qs[:1]),
                "background_revision", "incremental")
        if i % 10 == 0:
            add(p, "background_replay", "incremental")
        if i % 60 == 0:
            add(revised(p, version=3, review=99), "invalid_later_revision", "incremental")
    # Simulated pages deliberately deliver versions out of source-time order.
    rng.shuffle(deliveries)
    return deliveries, cases


def write_source(root):
    rows, cases = build_source()
    root = Path(root); root.mkdir(parents=True, exist_ok=True)
    for name, values in [("api_deliveries.jsonl", rows), ("scenario_manifest.jsonl", cases)]:
        with (root / name).open("w", encoding="utf-8") as f:
            for value in values:
                f.write(json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n")
    return rows


if __name__ == "__main__":
    rows = write_source(Path(__file__).parent / "data/source")
    print(f"Synthetic delivery fixture ready: {len(rows):,} receipts (seed {SEED}).")
