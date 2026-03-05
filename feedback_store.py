from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class FeedbackStore:
    def __init__(self, path: str) -> None:
        self.path = path
        self._data = self._load()
        self._ensure_schema()

    def _empty(self) -> dict[str, Any]:
        return {
            "generations": [],
            "feedback": [],
            "summary": {
                "generated": {"night": 0, "morning": 0},
                "feedback": {"good": 0, "bad": 0},
                "per_kind": {
                    "night": {"good": 0, "bad": 0},
                    "morning": {"good": 0, "bad": 0},
                },
                "reasons": {"good": {}, "bad": {}},
                "training": {
                    "samples": 0,
                    "direction_ok": 0,
                    "confidence_sum": 0.0,
                    "good_score_sum": 0.0,
                    "good_score_count": 0,
                    "bad_score_sum": 0.0,
                    "bad_score_count": 0,
                },
            },
        }

    def _load(self) -> dict[str, Any]:
        if not os.path.exists(self.path):
            return self._empty()
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                raw = json.load(handle)
            if not isinstance(raw, dict):
                return self._empty()
            return raw
        except Exception:
            return self._empty()

    def _ensure_schema(self) -> None:
        if "generations" not in self._data or not isinstance(self._data["generations"], list):
            self._data["generations"] = []
        if "feedback" not in self._data or not isinstance(self._data["feedback"], list):
            self._data["feedback"] = []
        if "summary" not in self._data or not isinstance(self._data["summary"], dict):
            self._data["summary"] = self._empty()["summary"]

        summary = self._data["summary"]
        summary.setdefault("generated", {"night": 0, "morning": 0})
        summary.setdefault("feedback", {"good": 0, "bad": 0})
        summary.setdefault(
            "per_kind",
            {
                "night": {"good": 0, "bad": 0},
                "morning": {"good": 0, "bad": 0},
            },
        )
        summary.setdefault("reasons", {"good": {}, "bad": {}})
        summary.setdefault(
            "training",
            {
                "samples": 0,
                "direction_ok": 0,
                "confidence_sum": 0.0,
                "good_score_sum": 0.0,
                "good_score_count": 0,
                "bad_score_sum": 0.0,
                "bad_score_count": 0,
            },
        )

    def _save(self) -> None:
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(self._data, handle, ensure_ascii=False, indent=2)
        os.replace(tmp_path, self.path)

    def record_generation(
        self,
        *,
        kind: str,
        source: str,
        engine: str,
        chat_id: int,
        message_id: int,
        text: str,
        mode: str,
        person_id: int,
        person_name: str,
        emoji_count: int,
        template_idx: int,
        picks: dict[str, int],
        score: float | None,
    ) -> None:
        event = {
            "ts": _utc_now_iso(),
            "kind": kind,
            "source": source,
            "engine": engine,
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "mode": mode,
            "person_id": person_id,
            "person_name": person_name,
            "emoji_count": emoji_count,
            "template_idx": template_idx,
            "picks": picks,
            "score": score,
        }
        self._data["generations"].append(event)
        if len(self._data["generations"]) > 4000:
            self._data["generations"] = self._data["generations"][-4000:]

        generated = self._data["summary"]["generated"]
        if kind not in generated:
            generated[kind] = 0
        generated[kind] += 1
        self._save()

    def _find_generation(self, *, chat_id: int, message_id: int) -> dict[str, Any] | None:
        rows = self._data.get("generations", [])
        for row in reversed(rows):
            if int(row.get("chat_id", 0)) == int(chat_id) and int(row.get("message_id", -1)) == int(message_id):
                return row
        return None

    def record_feedback(
        self,
        *,
        kind: str,
        rating: str,
        reason: str,
        user_id: int,
        chat_id: int,
        source_message_id: int,
        text: str | None = None,
    ) -> None:
        event = {
            "ts": _utc_now_iso(),
            "kind": kind,
            "rating": rating,
            "reason": reason,
            "user_id": user_id,
            "chat_id": chat_id,
            "source_message_id": source_message_id,
            "text": text,
        }
        matched_generation = self._find_generation(chat_id=chat_id, message_id=source_message_id)
        score = None
        if matched_generation is not None:
            score = matched_generation.get("score")
            if isinstance(score, (float, int)):
                event["score"] = float(score)
                event["mode"] = matched_generation.get("mode")
                event["person_id"] = matched_generation.get("person_id")
                event["person_name"] = matched_generation.get("person_name")

        self._data["feedback"].append(event)
        if len(self._data["feedback"]) > 4000:
            self._data["feedback"] = self._data["feedback"][-4000:]

        feedback = self._data["summary"]["feedback"]
        if rating not in feedback:
            feedback[rating] = 0
        feedback[rating] += 1

        per_kind = self._data["summary"]["per_kind"]
        if kind not in per_kind:
            per_kind[kind] = {"good": 0, "bad": 0}
        if rating not in per_kind[kind]:
            per_kind[kind][rating] = 0
        per_kind[kind][rating] += 1

        reasons = self._data["summary"]["reasons"]
        if rating not in reasons:
            reasons[rating] = {}
        reasons[rating][reason] = int(reasons[rating].get(reason, 0)) + 1

        if isinstance(score, (float, int)):
            score_float = float(score)
            expected = 1.0 if rating == "good" else 0.0
            predicted = 1.0 if score_float >= 0.5 else 0.0
            confidence = abs(score_float - 0.5) * 2.0
            training = self._data["summary"]["training"]
            training["samples"] = int(training.get("samples", 0)) + 1
            training["direction_ok"] = int(training.get("direction_ok", 0)) + int(predicted == expected)
            training["confidence_sum"] = float(training.get("confidence_sum", 0.0)) + confidence
            if rating == "good":
                training["good_score_sum"] = float(training.get("good_score_sum", 0.0)) + score_float
                training["good_score_count"] = int(training.get("good_score_count", 0)) + 1
            else:
                training["bad_score_sum"] = float(training.get("bad_score_sum", 0.0)) + score_float
                training["bad_score_count"] = int(training.get("bad_score_count", 0)) + 1
        self._save()

    def summary(self) -> dict[str, Any]:
        return self._data.get("summary", {})

    def recent_feedback(self, limit: int = 10) -> list[dict[str, Any]]:
        rows = self._data.get("feedback", [])
        return rows[-max(1, limit) :]

    def recent_generations(self, *, limit: int = 40, chat_id: int | None = None) -> list[dict[str, Any]]:
        rows = self._data.get("generations", [])
        if not isinstance(rows, list):
            return []
        capped = max(1, limit)
        if chat_id is None:
            return rows[-capped:]
        filtered = [row for row in rows if int(row.get("chat_id", 0)) == int(chat_id)]
        return filtered[-capped:]

    def training_progress(self, target_samples: int = 500) -> dict[str, float]:
        training = self.summary().get("training", {})
        samples = int(training.get("samples", 0))
        direction_ok = int(training.get("direction_ok", 0))
        confidence_sum = float(training.get("confidence_sum", 0.0))

        good_score_sum = float(training.get("good_score_sum", 0.0))
        good_score_count = int(training.get("good_score_count", 0))
        bad_score_sum = float(training.get("bad_score_sum", 0.0))
        bad_score_count = int(training.get("bad_score_count", 0))

        accuracy = (direction_ok / samples) if samples else 0.0
        avg_confidence = (confidence_sum / samples) if samples else 0.0
        good_avg_score = (good_score_sum / good_score_count) if good_score_count else 0.0
        bad_avg_score = (bad_score_sum / bad_score_count) if bad_score_count else 0.0
        progress = min(100.0, (samples / max(1, target_samples)) * 100.0)

        return {
            "samples": float(samples),
            "accuracy": accuracy,
            "avg_confidence": avg_confidence,
            "good_avg_score": good_avg_score,
            "bad_avg_score": bad_avg_score,
            "progress_percent": progress,
            "target_samples": float(max(1, target_samples)),
        }
