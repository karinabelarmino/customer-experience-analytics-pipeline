"""CX exercise 02: explicit snapshot semantics; Python standard library only.

No provider connection. Raw deliveries are immutable; response versions are
complete snapshots (not patches). See docs/DATA_CONTRACT.md before adaptation.
"""
from collections import defaultdict
from datetime import datetime, timezone
from itertools import product
import hashlib
import json
import math
import re
import unicodedata

QUESTION_COLUMNS = {
    "q_wait": "answer_wait",
    "q_resolution": "answer_resolution",
    "q_channels": "answer_channels",
    "q_wait_alt": "answer_wait_alternative",
    "q_detail": "answer_detail",
}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def clean_text(value):
    if not isinstance(value, str):
        raise ValueError("expected_string")
    value = unicodedata.normalize("NFC", value)
    value = re.sub(r"[\u200b-\u200d\ufeff]", "", value)
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def identifier(value):
    # Do not cast a missing ID to the strings 'None' or 'null'.
    if not isinstance(value, str) or not value or clean_text(value) != value:
        raise ValueError("invalid_identifier")
    return value


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("invalid_timestamp")
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("invalid_timestamp") from None
    if dt.tzinfo is None:
        raise ValueError("timestamp_without_timezone")
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def clean_value(value):
    if isinstance(value, str):
        return clean_text(value) or None
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non_finite_value")
        return value
    if isinstance(value, list):
        return [clean_value(x) for x in value]
    if isinstance(value, dict):
        return {k: clean_value(v) for k, v in sorted(value.items())}
    raise ValueError("unsupported_answer_type")


def answer_value(question):
    for field in ("review", "answerText", "multipleValues"):
        value = clean_value(question.get(field))
        if value is not None and value != [] and value != {}:
            # Arrays retain their boundaries; 0 and False remain valid answers.
            return value
    return None


def normalize(payload):
    if not isinstance(payload, dict):
        raise ValueError("payload_not_object")
    rid = identifier(payload.get("_id"))
    survey = identifier(payload.get("actionId"))
    event_id = identifier(payload.get("eventId"))
    customer = identifier(payload.get("clientId"))
    version = payload.get("version")
    if type(version) is not int or version < 1:
        raise ValueError("invalid_source_version")
    metric = payload.get("metric")
    score = payload.get("review")
    if metric not in ("NPS", "CSAT"):
        raise ValueError("invalid_metric")
    # Main score strings emulate JSON sources that stringify numeric answers.
    if isinstance(score, str) and re.fullmatch(r"\d+", score.strip()):
        score = int(score.strip())
    low, high = (0, 10) if metric == "NPS" else (1, 5)
    if type(score) is not int or not low <= score <= high:
        raise ValueError("score_out_of_range")
    updated = timestamp(payload.get("updatedAt"))
    answered = timestamp(payload.get("answerDate"))
    created = timestamp(payload.get("createdAt"))
    if updated < answered or updated < created:
        raise ValueError("update_precedes_answer_or_creation")
    categories, questions = payload.get("categories"), payload.get("additionalQuestions")
    # Missing/null means unknown, not an empty snapshot: quarantine the version.
    if not isinstance(categories, list) or not isinstance(questions, list):
        raise ValueError("missing_snapshot_children")
    category_set = set()
    for category in categories:
        if not isinstance(category, dict):
            raise ValueError("invalid_category")
        label = clean_text(category.get("category"))
        sub = clean_text(category.get("subCategory") or "")
        if not label:
            raise ValueError("empty_category")
        category_set.add((label, sub))
    question_map = {}
    for question in questions:
        if not isinstance(question, dict):
            raise ValueError("invalid_question")
        qid = identifier(question.get("_id"))
        label = clean_text(question.get("text"))
        if not label:
            raise ValueError("empty_question_label")
        normalized_question = {"question_id": qid, "question_text": label,
                               "value": answer_value(question)}
        if qid in question_map and question_map[qid] != normalized_question:
            raise ValueError("conflicting_question_in_snapshot")
        question_map[qid] = normalized_question
    deleted = payload.get("deleted", False)
    if type(deleted) is not bool:
        raise ValueError("invalid_deleted_flag")
    out = {
        "survey_id": survey, "response_id": rid, "customer_id": customer,
        "source_version": version, "updated_at": updated,
        "answered_at": answered, "created_at": created,
        "metric": metric, "score": score,
        "journey_stage": clean_text(payload.get("journeyStage", "unknown")),
        "feedback": clean_value(payload.get("feedback")), "deleted": deleted,
        "categories": [{"category": c, "subcategory": s} for c, s in sorted(category_set)],
        "questions": [question_map[k] for k in sorted(question_map)],
    }
    # Neither transport IDs nor receipt times determine the business version.
    return {**out, "event_id": event_id, "payload_hash": digest(out)}


def resolve(receipts):
    """Resolve all retained Bronze deliveries; independent of arrival order.

    Distinct normalized contents at one response/version are ambiguous: reject
    the whole version, retaining the previous valid snapshot with a quality flag.
    Hashes detect equality, never which conflicting answer is true.
    """
    valid, audit, quarantine = [], [], []
    rejected_keys = set()

    def reject(receipt, reason):
        p = receipt.get("payload", {})
        if isinstance(p, dict) and isinstance(p.get("actionId"), str) and isinstance(p.get("_id"), str):
            rejected_keys.add((p["actionId"], p["_id"]))
        row = {"receipt_id": receipt["receipt_id"], "event_id": p.get("eventId") if isinstance(p, dict) else None,
               "reason": reason, "raw_payload": p}
        quarantine.append(row)
        audit.append({"receipt_id": receipt["receipt_id"], "status": "quarantined", "reason": reason})

    for receipt in receipts:
        try:
            value = normalize(receipt["payload"])
            valid.append((receipt, value))
        except (ValueError, TypeError, KeyError) as exc:
            reject(receipt, str(exc))
    event_hashes, version_hashes = defaultdict(set), defaultdict(set)
    for _, value in valid:
        event_hashes[value["event_id"]].add(value["payload_hash"])
        version_hashes[(value["survey_id"], value["response_id"], value["source_version"])].add(value["payload_hash"])
    unique, candidates = {}, defaultdict(list)
    for receipt, value in sorted(valid, key=lambda pair: pair[0]["receipt_id"]):
        key = (value["survey_id"], value["response_id"], value["source_version"])
        if len(event_hashes[value["event_id"]]) > 1:
            reject(receipt, "event_id_payload_conflict")
        elif len(version_hashes[key]) > 1:
            reject(receipt, "response_version_conflict")
        elif key in unique:
            audit.append({"receipt_id": receipt["receipt_id"], "status": "repeated_snapshot", "reason": "same_business_content"})
        else:
            unique[key] = (receipt, value)
            candidates[key[:2]].append((receipt, value))
    current = []
    for key in sorted(candidates):
        ordered = sorted(candidates[key], key=lambda pair: pair[1]["source_version"], reverse=True)
        for i, (receipt, value) in enumerate(ordered):
            status = ("deleted_current" if value["deleted"] else "selected_current") if i == 0 else "superseded_version"
            audit.append({"receipt_id": receipt["receipt_id"], "status": status, "reason": "source_version_order"})
        value = dict(ordered[0][1])
        value["quality_status"] = "has_rejected_history" if key in rejected_keys else "ok"
        current.append(value)
    return {"current": current, "audit": sorted(audit, key=lambda r: r["receipt_id"]),
            "quarantine": sorted(quarantine, key=lambda r: r["receipt_id"])}


def model(current):
    responses, categories, questions, wide = [], [], [], []
    for value in current:
        if value["deleted"]:
            continue
        key = {"survey_id": value["survey_id"], "response_id": value["response_id"]}
        response = {k: v for k, v in value.items() if k not in ("categories", "questions", "deleted")}
        responses.append(response)
        for category in value["categories"]:
            categories.append({**key, **category, "source_version": value["source_version"]})
        wide_row = {**response, **{column: None for column in QUESTION_COLUMNS.values()},
                    "category_count": len(value["categories"])}
        for question in value["questions"]:
            # An unanswered question remains distinguishable from an absent ID.
            cell = canonical(question["value"]) if question["value"] is not None else None
            questions.append({**key, "question_id": question["question_id"],
                              "question_text": question["question_text"], "value_json": cell,
                              "source_version": value["source_version"]})
            if question["question_id"] in QUESTION_COLUMNS:
                wide_row[QUESTION_COLUMNS[question["question_id"]]] = cell
        wide.append(wide_row)
    return {"responses": responses, "response_categories": categories,
            "question_answers": questions, "responses_wide": wide}


def explode_like_legacy(current):
    """The two original LEFT JOINs, isolated from replays and old versions."""
    rows = []
    for value in current:
        if value["deleted"]:
            continue
        for category, question in product(value["categories"] or [None], value["questions"] or [None]):
            rows.append({"survey_id": value["survey_id"], "response_id": value["response_id"],
                         "metric": value["metric"], "score": value["score"],
                         "category": category["category"] if category else None,
                         "subcategory": category["subcategory"] if category else None,
                         "question_id": question["question_id"] if question else None})
    return rows


def metrics(rows):
    nps = [r["score"] for r in rows if r["metric"] == "NPS"]
    csat = [r["score"] for r in rows if r["metric"] == "CSAT"]
    return {"rows": len(rows), "nps_responses": len(nps),
            "nps": round(100 * (sum(x >= 9 for x in nps) - sum(x <= 6 for x in nps)) / len(nps), 6) if nps else None,
            "csat_responses": len(csat),
            "csat_top2_pct": round(100 * sum(x >= 4 for x in csat) / len(csat), 6) if csat else None}
