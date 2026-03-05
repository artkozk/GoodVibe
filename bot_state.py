from __future__ import annotations

import json
import os
import re
from typing import Any


DEFAULT_PERSON_ID = 0
DEFAULT_MODE = "standard"
VALID_MODES = {"short", "standard", "context"}
DEFAULT_SCHEDULE_MODE = "standard"
VALID_SCHEDULE_MODES = {"short", "standard", "context"}
DEFAULT_GROUP_ACTIVITY_MODE = "normal"
VALID_GROUP_ACTIVITY_MODES = {"quiet", "normal", "active", "question_only"}


class BotStateStore:
    def __init__(self, path: str) -> None:
        self.path = path
        self._data = self._load()
        self._ensure_schema()

    def _empty(self) -> dict[str, Any]:
        return {
            "next_person_id": 1,
            "personas": {
                str(DEFAULT_PERSON_ID): {
                    "id": DEFAULT_PERSON_ID,
                    "name": "общий вариант",
                    "instructions": "",
                }
            },
            "chat_prefs": {},
            "premium_emoji_ids_set": False,
            "premium_emoji_ids": [],
            "liked_emojis": [],
            "favorite_phrases": [],
            "blacklist_phrases": [],
            "schedule_mode": DEFAULT_SCHEDULE_MODE,
            "admin_only_mode": True,
            "access_exceptions": {},
            "runtime_admin_user_id": 0,
            "runtime_group_chat_id": 0,
            "group_fire_reaction_mode": False,
            "group_chat_mode": False,
            "group_activity_mode": DEFAULT_GROUP_ACTIVITY_MODE,
            "style_examples": [],
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
        changed = False
        if "next_person_id" not in self._data or not isinstance(self._data["next_person_id"], int):
            self._data["next_person_id"] = 1
            changed = True
        if "personas" not in self._data or not isinstance(self._data["personas"], dict):
            self._data["personas"] = {}
            changed = True
        if "chat_prefs" not in self._data or not isinstance(self._data["chat_prefs"], dict):
            self._data["chat_prefs"] = {}
            changed = True
        if "premium_emoji_ids_set" not in self._data or not isinstance(self._data["premium_emoji_ids_set"], bool):
            self._data["premium_emoji_ids_set"] = False
            changed = True
        if "premium_emoji_ids" not in self._data or not isinstance(self._data["premium_emoji_ids"], list):
            self._data["premium_emoji_ids"] = []
            changed = True
        if "liked_emojis" not in self._data or not isinstance(self._data["liked_emojis"], list):
            self._data["liked_emojis"] = []
            changed = True
        if "favorite_phrases" not in self._data or not isinstance(self._data["favorite_phrases"], list):
            self._data["favorite_phrases"] = []
            changed = True
        if "blacklist_phrases" not in self._data or not isinstance(self._data["blacklist_phrases"], list):
            self._data["blacklist_phrases"] = []
            changed = True
        if "schedule_mode" not in self._data or str(self._data["schedule_mode"]) not in VALID_SCHEDULE_MODES:
            self._data["schedule_mode"] = DEFAULT_SCHEDULE_MODE
            changed = True
        if "admin_only_mode" not in self._data or not isinstance(self._data["admin_only_mode"], bool):
            self._data["admin_only_mode"] = True
            changed = True
        if "access_exceptions" not in self._data or not isinstance(self._data["access_exceptions"], dict):
            self._data["access_exceptions"] = {}
            changed = True
        if "runtime_admin_user_id" not in self._data or not isinstance(self._data["runtime_admin_user_id"], int):
            self._data["runtime_admin_user_id"] = 0
            changed = True
        if "runtime_group_chat_id" not in self._data or not isinstance(self._data["runtime_group_chat_id"], int):
            self._data["runtime_group_chat_id"] = 0
            changed = True
        if "group_fire_reaction_mode" not in self._data or not isinstance(self._data["group_fire_reaction_mode"], bool):
            self._data["group_fire_reaction_mode"] = False
            changed = True
        if "group_chat_mode" not in self._data or not isinstance(self._data["group_chat_mode"], bool):
            self._data["group_chat_mode"] = False
            changed = True
        if (
            "group_activity_mode" not in self._data
            or str(self._data["group_activity_mode"]).strip().lower() not in VALID_GROUP_ACTIVITY_MODES
        ):
            self._data["group_activity_mode"] = DEFAULT_GROUP_ACTIVITY_MODE
            changed = True
        if "style_examples" not in self._data or not isinstance(self._data["style_examples"], list):
            self._data["style_examples"] = []
            changed = True
        if str(DEFAULT_PERSON_ID) not in self._data["personas"]:
            self._data["personas"][str(DEFAULT_PERSON_ID)] = {
                "id": DEFAULT_PERSON_ID,
                "name": "общий вариант",
                "instructions": "",
            }
            changed = True

        max_person_id = 0
        for key, value in self._data["personas"].items():
            try:
                candidate = int(value.get("id", key)) if isinstance(value, dict) else int(key)
            except Exception:
                continue
            if candidate > max_person_id:
                max_person_id = candidate
        if int(self._data.get("next_person_id", 1)) <= max_person_id:
            self._data["next_person_id"] = max_person_id + 1
            changed = True

        if self._ensure_builtin_personas():
            changed = True
        if self._dedupe_phrase_list("blacklist_phrases"):
            changed = True
        if self._dedupe_phrase_list("favorite_phrases"):
            changed = True

        if changed:
            self._save()

    def _ensure_builtin_personas(self) -> bool:
        builtins = [
            ("подруга", "дружески и мягко, как для близкой подруги"),
            ("друг", "дружелюбно и тепло, как для хорошего друга"),
        ]
        existing_names = set()
        for value in self._data.get("personas", {}).values():
            if not isinstance(value, dict):
                continue
            existing_names.add(str(value.get("name", "")).strip().lower())

        changed = False
        for name, instructions in builtins:
            if name in existing_names:
                continue
            person_id = int(self._data["next_person_id"])
            self._data["next_person_id"] = person_id + 1
            self._data["personas"][str(person_id)] = {
                "id": person_id,
                "name": name,
                "instructions": instructions,
            }
            existing_names.add(name)
            changed = True
        return changed

    def _save(self) -> None:
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(self._data, handle, ensure_ascii=False, indent=2)
        os.replace(tmp_path, self.path)

    @staticmethod
    def _norm_phrase_key(value: str) -> str:
        lowered = str(value).strip().lower().replace("ё", "е")
        cleaned = re.sub(r"[^a-zа-я0-9\s]+", " ", lowered)
        return " ".join(cleaned.split())

    def _dedupe_phrase_list(self, key: str) -> bool:
        raw = self._data.get(key, [])
        if not isinstance(raw, list):
            self._data[key] = []
            return True
        out: list[str] = []
        seen: set[str] = set()
        for item in raw:
            value = " ".join(str(item).strip().split())
            if not value:
                continue
            norm = self._norm_phrase_key(value)
            if not norm or norm in seen:
                continue
            seen.add(norm)
            out.append(value)
        changed = out != raw
        if changed:
            self._data[key] = out
        return changed

    def list_personas(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for key, value in self._data["personas"].items():
            if isinstance(value, dict):
                rows.append(
                    {
                        "id": int(value.get("id", int(key))),
                        "name": str(value.get("name", f"person-{key}")),
                        "instructions": str(value.get("instructions", "")),
                    }
                )
        rows.sort(key=lambda x: x["id"])
        return rows

    def get_person(self, person_id: int) -> dict[str, Any] | None:
        row = self._data["personas"].get(str(person_id))
        if not isinstance(row, dict):
            return None
        return {
            "id": int(row.get("id", person_id)),
            "name": str(row.get("name", f"person-{person_id}")),
            "instructions": str(row.get("instructions", "")),
        }

    def add_person(self, name: str, instructions: str) -> dict[str, Any]:
        clean_name = name.strip()
        clean_instructions = instructions.strip()
        if not clean_name:
            raise ValueError("name is required")

        person_id = int(self._data["next_person_id"])
        self._data["next_person_id"] = person_id + 1
        self._data["personas"][str(person_id)] = {
            "id": person_id,
            "name": clean_name,
            "instructions": clean_instructions,
        }
        self._save()
        return self.get_person(person_id) or {
            "id": person_id,
            "name": clean_name,
            "instructions": clean_instructions,
        }

    def delete_person(self, person_id: int) -> bool:
        if person_id == DEFAULT_PERSON_ID:
            return False
        if str(person_id) not in self._data["personas"]:
            return False
        self._data["personas"].pop(str(person_id), None)

        for chat_key, prefs in self._data["chat_prefs"].items():
            if not isinstance(prefs, dict):
                continue
            if int(prefs.get("person_id", DEFAULT_PERSON_ID)) == person_id:
                prefs["person_id"] = DEFAULT_PERSON_ID
        self._save()
        return True

    def get_chat_prefs(self, chat_id: int) -> dict[str, Any]:
        chat_key = str(chat_id)
        prefs = self._data["chat_prefs"].get(chat_key)
        if not isinstance(prefs, dict):
            prefs = {"mode": DEFAULT_MODE, "person_id": DEFAULT_PERSON_ID, "training_mode": False}
            self._data["chat_prefs"][chat_key] = prefs
            self._save()

        mode = str(prefs.get("mode", DEFAULT_MODE)).strip().lower()
        if mode == "advanced":
            mode = "standard"
        if mode not in VALID_MODES:
            mode = DEFAULT_MODE
        person_id = int(prefs.get("person_id", DEFAULT_PERSON_ID))
        if str(person_id) not in self._data["personas"]:
            person_id = DEFAULT_PERSON_ID
        training_mode = bool(prefs.get("training_mode", False))
        return {"mode": mode, "person_id": person_id, "training_mode": training_mode}

    def set_chat_mode(self, chat_id: int, mode: str) -> str:
        if mode == "advanced":
            mode = "standard"
        if mode not in VALID_MODES:
            raise ValueError("unsupported mode")
        chat_key = str(chat_id)
        prefs = self._data["chat_prefs"].setdefault(chat_key, {})
        prefs["mode"] = mode
        if "person_id" not in prefs:
            prefs["person_id"] = DEFAULT_PERSON_ID
        if "training_mode" not in prefs:
            prefs["training_mode"] = False
        self._save()
        return mode

    def toggle_chat_mode(self, chat_id: int) -> str:
        prefs = self.get_chat_prefs(chat_id)
        mode = prefs["mode"]
        if mode == "short":
            new_mode = "standard"
        elif mode == "standard":
            new_mode = "context"
        else:
            new_mode = "short"
        self.set_chat_mode(chat_id, new_mode)
        return new_mode

    def set_chat_person(self, chat_id: int, person_id: int) -> dict[str, Any]:
        person = self.get_person(person_id)
        if not person:
            raise ValueError("unknown person_id")

        chat_key = str(chat_id)
        prefs = self._data["chat_prefs"].setdefault(chat_key, {})
        if str(prefs.get("mode", DEFAULT_MODE)) not in VALID_MODES:
            prefs["mode"] = DEFAULT_MODE
        prefs["person_id"] = person_id
        if "training_mode" not in prefs:
            prefs["training_mode"] = False
        self._save()
        return person

    def is_chat_training_mode(self, chat_id: int) -> bool:
        prefs = self.get_chat_prefs(chat_id)
        return bool(prefs.get("training_mode", False))

    def set_chat_training_mode(self, chat_id: int, enabled: bool) -> bool:
        chat_key = str(chat_id)
        prefs = self._data["chat_prefs"].setdefault(chat_key, {})
        if str(prefs.get("mode", DEFAULT_MODE)) not in VALID_MODES:
            prefs["mode"] = DEFAULT_MODE
        if "person_id" not in prefs:
            prefs["person_id"] = DEFAULT_PERSON_ID
        prefs["training_mode"] = bool(enabled)
        self._save()
        return bool(enabled)

    def toggle_chat_training_mode(self, chat_id: int) -> bool:
        new_value = not self.is_chat_training_mode(chat_id)
        return self.set_chat_training_mode(chat_id, new_value)

    def list_training_chat_ids(self) -> list[int]:
        out: list[int] = []
        raw = self._data.get("chat_prefs", {})
        if not isinstance(raw, dict):
            return out
        for chat_key, prefs in raw.items():
            if not isinstance(prefs, dict):
                continue
            if not bool(prefs.get("training_mode", False)):
                continue
            try:
                out.append(int(chat_key))
            except Exception:
                continue
        return out

    def get_premium_emoji_ids(self, default_ids: list[str] | None = None) -> list[str]:
        if bool(self._data.get("premium_emoji_ids_set", False)):
            raw = self._data.get("premium_emoji_ids", [])
            if not isinstance(raw, list):
                return []
            return [str(x).strip() for x in raw if str(x).strip()]
        return [x.strip() for x in (default_ids or []) if x.strip()]

    def set_premium_emoji_ids(self, emoji_ids: list[str]) -> None:
        self._data["premium_emoji_ids_set"] = True
        self._data["premium_emoji_ids"] = [str(x).strip() for x in emoji_ids if str(x).strip()]
        self._save()

    def add_premium_emoji_ids(self, emoji_ids: list[str], default_ids: list[str] | None = None) -> int:
        current = self.get_premium_emoji_ids(default_ids)
        existing = set(current)
        added = 0
        for raw in emoji_ids:
            item = str(raw).strip()
            if not item:
                continue
            if item in existing:
                continue
            current.append(item)
            existing.add(item)
            added += 1
        self._data["premium_emoji_ids_set"] = True
        self._data["premium_emoji_ids"] = current
        self._save()
        return added

    def reset_premium_to_default(self) -> None:
        self._data["premium_emoji_ids_set"] = False
        self._data["premium_emoji_ids"] = []
        self._save()

    def get_liked_emojis(self) -> list[str]:
        raw = self._data.get("liked_emojis", [])
        if not isinstance(raw, list):
            return []
        return [str(x) for x in raw if str(x).strip()]

    def add_liked_emojis(self, emojis: list[str], limit: int = 300) -> int:
        current = self.get_liked_emojis()
        existing = set(current)
        added = 0
        for emoji in emojis:
            item = str(emoji).strip()
            if not item or item in existing:
                continue
            current.append(item)
            existing.add(item)
            added += 1
        if len(current) > limit:
            current = current[-limit:]
        self._data["liked_emojis"] = current
        self._save()
        return added

    def reset_liked_emojis(self) -> None:
        self._data["liked_emojis"] = []
        self._save()

    def get_favorite_phrases(self) -> list[str]:
        raw = self._data.get("favorite_phrases", [])
        if not isinstance(raw, list):
            return []
        out: list[str] = []
        for item in raw:
            value = " ".join(str(item).strip().split())
            if value:
                out.append(value)
        return out

    def add_favorite_phrase(self, phrase: str, limit: int = 300) -> bool:
        clean = " ".join(str(phrase).strip().split())
        if not clean:
            return False
        current = self.get_favorite_phrases()
        lowered = {self._norm_phrase_key(x) for x in current}
        if self._norm_phrase_key(clean) in lowered:
            return False
        current.append(clean)
        if len(current) > limit:
            current = current[-limit:]
        self._data["favorite_phrases"] = current
        self._save()
        return True

    def reset_favorite_phrases(self) -> None:
        self._data["favorite_phrases"] = []
        self._save()

    def get_blacklist_phrases(self) -> list[str]:
        raw = self._data.get("blacklist_phrases", [])
        if not isinstance(raw, list):
            return []
        out: list[str] = []
        for item in raw:
            value = str(item).strip()
            if value:
                out.append(value)
        return out

    def add_blacklist_phrase(self, phrase: str, limit: int = 300) -> bool:
        clean = " ".join(str(phrase).strip().split())
        if not clean:
            return False
        current = self.get_blacklist_phrases()
        lowered = {self._norm_phrase_key(x) for x in current}
        if self._norm_phrase_key(clean) in lowered:
            return False
        current.append(clean)
        if len(current) > limit:
            current = current[-limit:]
        self._data["blacklist_phrases"] = current
        self._save()
        return True

    def reset_blacklist_phrases(self) -> None:
        self._data["blacklist_phrases"] = []
        self._save()

    def get_schedule_mode(self) -> str:
        mode = str(self._data.get("schedule_mode", DEFAULT_SCHEDULE_MODE)).strip().lower()
        if mode == "advanced":
            mode = "standard"
        if mode not in VALID_SCHEDULE_MODES:
            mode = DEFAULT_SCHEDULE_MODE
        return mode

    def set_schedule_mode(self, mode: str) -> str:
        clean = str(mode).strip().lower()
        if clean == "advanced":
            clean = "standard"
        if clean not in VALID_SCHEDULE_MODES:
            raise ValueError("unsupported schedule mode")
        self._data["schedule_mode"] = clean
        self._save()
        return clean

    def toggle_schedule_mode(self) -> str:
        now = self.get_schedule_mode()
        if now == "short":
            new_mode = "standard"
        elif now == "standard":
            new_mode = "context"
        else:
            new_mode = "short"
        return self.set_schedule_mode(new_mode)

    def get_effective_admin_user_id(self, configured_admin_user_id: int) -> int:
        if int(configured_admin_user_id):
            return int(configured_admin_user_id)
        return int(self._data.get("runtime_admin_user_id", 0))

    def set_runtime_admin_user_id(self, user_id: int) -> None:
        self._data["runtime_admin_user_id"] = int(user_id)
        self._save()

    def get_effective_group_chat_id(self, configured_group_chat_id: int) -> int:
        if int(configured_group_chat_id):
            return int(configured_group_chat_id)
        return int(self._data.get("runtime_group_chat_id", 0))

    def set_runtime_group_chat_id(self, chat_id: int) -> None:
        self._data["runtime_group_chat_id"] = int(chat_id)
        self._save()

    def is_group_fire_reaction_mode(self) -> bool:
        return bool(self._data.get("group_fire_reaction_mode", False))

    def set_group_fire_reaction_mode(self, enabled: bool) -> bool:
        self._data["group_fire_reaction_mode"] = bool(enabled)
        self._save()
        return bool(enabled)

    def toggle_group_fire_reaction_mode(self) -> bool:
        new_value = not self.is_group_fire_reaction_mode()
        return self.set_group_fire_reaction_mode(new_value)

    def is_group_chat_mode(self) -> bool:
        return bool(self._data.get("group_chat_mode", False))

    def set_group_chat_mode(self, enabled: bool) -> bool:
        self._data["group_chat_mode"] = bool(enabled)
        self._save()
        return bool(enabled)

    def toggle_group_chat_mode(self) -> bool:
        new_value = not self.is_group_chat_mode()
        return self.set_group_chat_mode(new_value)

    def get_group_activity_mode(self) -> str:
        mode = str(self._data.get("group_activity_mode", DEFAULT_GROUP_ACTIVITY_MODE)).strip().lower()
        if mode not in VALID_GROUP_ACTIVITY_MODES:
            mode = DEFAULT_GROUP_ACTIVITY_MODE
        return mode

    def set_group_activity_mode(self, mode: str) -> str:
        clean = str(mode).strip().lower()
        if clean not in VALID_GROUP_ACTIVITY_MODES:
            raise ValueError("unsupported group_activity_mode")
        self._data["group_activity_mode"] = clean
        self._save()
        return clean

    def cycle_group_activity_mode(self) -> str:
        now = self.get_group_activity_mode()
        if now == "quiet":
            new_mode = "normal"
        elif now == "normal":
            new_mode = "active"
        elif now == "active":
            new_mode = "question_only"
        else:
            new_mode = "quiet"
        return self.set_group_activity_mode(new_mode)

    def get_style_examples(self) -> list[str]:
        raw = self._data.get("style_examples", [])
        if not isinstance(raw, list):
            return []
        out: list[str] = []
        for item in raw:
            clean = " ".join(str(item).strip().split())
            if clean:
                out.append(clean)
        return out

    def set_style_examples(self, examples: list[str], limit: int = 2500) -> int:
        out: list[str] = []
        seen: set[str] = set()
        for item in examples:
            clean = " ".join(str(item).strip().split())
            if not clean:
                continue
            norm = self._norm_phrase_key(clean)
            if not norm or norm in seen:
                continue
            seen.add(norm)
            out.append(clean)
        if len(out) > max(1, limit):
            out = out[-max(1, limit):]
        self._data["style_examples"] = out
        self._save()
        return len(out)

    def clear_style_examples(self) -> None:
        self._data["style_examples"] = []
        self._save()

    def is_admin_only_mode(self) -> bool:
        return bool(self._data.get("admin_only_mode", True))

    def set_admin_only_mode(self, enabled: bool) -> bool:
        self._data["admin_only_mode"] = bool(enabled)
        self._save()
        return bool(enabled)

    def toggle_admin_only_mode(self) -> bool:
        new_value = not self.is_admin_only_mode()
        return self.set_admin_only_mode(new_value)

    def list_access_exceptions(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        raw = self._data.get("access_exceptions", {})
        if not isinstance(raw, dict):
            return rows
        for user_id_str, payload in raw.items():
            if not isinstance(payload, dict):
                continue
            try:
                user_id = int(user_id_str)
            except Exception:
                continue
            first_name = str(payload.get("first_name", "")).strip()
            last_name = str(payload.get("last_name", "")).strip()
            rows.append(
                {
                    "user_id": user_id,
                    "first_name": first_name,
                    "last_name": last_name,
                }
            )
        rows.sort(key=lambda x: x["user_id"])
        return rows

    def has_access_exception(self, user_id: int) -> bool:
        return str(int(user_id)) in self._data.get("access_exceptions", {})

    def add_access_exception(self, user_id: int, first_name: str, last_name: str = "") -> None:
        uid = str(int(user_id))
        self._data.setdefault("access_exceptions", {})
        self._data["access_exceptions"][uid] = {
            "first_name": str(first_name).strip(),
            "last_name": str(last_name).strip(),
        }
        self._save()

    def remove_access_exception(self, user_id: int) -> bool:
        uid = str(int(user_id))
        raw = self._data.get("access_exceptions", {})
        if uid not in raw:
            return False
        raw.pop(uid, None)
        self._save()
        return True
