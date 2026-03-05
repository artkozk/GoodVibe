
from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import random
import re
import tempfile
import time
from difflib import SequenceMatcher
from dataclasses import dataclass
from datetime import datetime, time as dtime
from typing import Awaitable, Callable
from zoneinfo import ZoneInfo

import httpx
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, TelegramError, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from adaptive_model import TinyFeedbackModel
from bot_state import BotStateStore, DEFAULT_PERSON_ID
from feedback_store import FeedbackStore
from openai_wishes import (
    OpenAIWishConfig,
    OpenAIWishError,
    generate_openai_chat_reply,
    generate_openai_wish,
)
from wishes import GeneratedWish, estimated_unique_texts, generate_candidate


@dataclass(frozen=True)
class BotConfig:
    token: str
    timezone: ZoneInfo
    base_premium_emoji_ids: list[str]
    admin_user_id: int
    group_chat_id: int
    training_target_samples: int
    feedback_path: str
    model_path: str
    state_path: str
    chat_log_path: str
    openai_enabled: bool
    openai_api_key: str
    openai_base_url: str
    openai_model: str
    openai_temperature: float
    openai_max_tokens: int
    openai_timeout_sec: float
    openai_rules: str


@dataclass(frozen=True)
class _MessageRef:
    message_id: int


NIGHT_TIME = dtime(hour=23, minute=10)
MORNING_TIME = dtime(hour=7, minute=0)
TRAINING_STREAM_INTERVAL_SEC = 8
NON_ADMIN_TEXT = "такое вообще то пишут с душой!"
GROUP_CHAT_REPLY_COOLDOWN_SEC = 25.0
GROUP_CHAT_REPLY_CHANCE = 0.24
GROUP_REACTION_EMOJI = "🔥"
GROUP_QUERY_PREFIX = "@v"
GROUP_QUERY_TIMEOUT_SEC = 12.0
WISH_HEART_EMOJIS = ("❤️", "💖", "💗", "💕", "💞", "💘", "💝", "🩷")
GROUP_CONTEXT_POSITIVE_REACTIONS = ("❤️", "💯", "👏", "🫶", "✨", "👍")
GROUP_CONTEXT_NEUTRAL_REACTIONS = ("👀", "🤔", "🧠", "👌", "🫡", "😌")
GROUP_CONTEXT_NEGATIVE_REACTIONS = ("😏", "🙃", "🧊", "😬", "🤨")
GROUP_CONTEXT_FUN_REACTIONS = ("😂", "🤣", "😹", "🔥")
SOCIAL_WARM_REACTIONS = ("❤️", "💖", "🫶", "🥰", "🤝")
SOCIAL_COLD_REACTIONS = ("🤨", "😏", "🙃", "🧊")
FRIENDLY_MARKERS = (
    "спасибо",
    "благодар",
    "люблю",
    "обнимаю",
    "красава",
    "умница",
    "молодец",
    "доброе утро",
    "спокойной ночи",
    "сладких снов",
    "доброй ночи",
    "пожалуйста",
)
RUDE_MARKERS = (
    "туп",
    "дура",
    "дурак",
    "идиот",
    "дебил",
    "мудак",
    "бесишь",
    "заткни",
    "ненавижу",
    "пошел",
    "пошёл",
    "урод",
    "говно",
)
THANKS_MARKERS = ("спасибо", "благодар", "сенкс", "пасиб")
LAUGH_MARKERS = ("ахах", "лол", "ржу", "смешно", "хаха")
FORGIVE_MARKERS = ("прости", "извини", "сорян", "сорри", "простишь", "прощения")
HOSTILE_FORCE_REPLY_SCORE = -70
HOSTILE_SILENCE_BREAK_CHANCE = 0.65
AMSTERDAM_MARKERS = ("амстердам", "amsterdam")
CITY_TIMEZONE_MAP = {
    "москва": "Europe/Moscow",
    "питер": "Europe/Moscow",
    "санкт петербург": "Europe/Moscow",
    "киев": "Europe/Kyiv",
    "минск": "Europe/Minsk",
    "астана": "Asia/Almaty",
    "алматы": "Asia/Almaty",
    "екатеринбург": "Asia/Yekaterinburg",
    "новосибирск": "Asia/Novosibirsk",
    "владивосток": "Asia/Vladivostok",
    "лондон": "Europe/London",
    "берлин": "Europe/Berlin",
    "париж": "Europe/Paris",
    "рим": "Europe/Rome",
    "мадрид": "Europe/Madrid",
    "амстердам": "Europe/Amsterdam",
    "амстердаме": "Europe/Amsterdam",
    "new york": "America/New_York",
    "нью йорк": "America/New_York",
    "лос анджелес": "America/Los_Angeles",
    "лос анжелес": "America/Los_Angeles",
    "чикаго": "America/Chicago",
    "токио": "Asia/Tokyo",
    "сеул": "Asia/Seoul",
    "дубай": "Asia/Dubai",
}
WISH_REACTION_KEYWORDS = (
    "доброе утро",
    "с добрым утром",
    "хорошего утра",
    "хорошего дня",
    "добрый день",
    "доброго дня",
    "хорошего вечера",
    "спокойной ночи",
    "доброй ночи",
    "сладких снов",
    "приятных снов",
    "хорошего сна",
)
EMOJI_SEQUENCE_RE = re.compile(
    r"(?:[\U0001F1E6-\U0001F1FF]{2}|[\U0001F300-\U0001FAFF\u2600-\u27BF]"
    r"(?:\uFE0F)?(?:\u200D[\U0001F300-\U0001FAFF\u2600-\u27BF](?:\uFE0F)?)*)"
)
HTML_TAG_RE = re.compile(r"<[^>]+>")
MOJIBAKE_CHARS = ("�", "Ð", "Ñ", "Ã", "Ø", "Â")


def _as_bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on", "y", "да"}:
        return True
    if text in {"0", "false", "no", "off", "n", "нет"}:
        return False
    return default


def _load_config() -> BotConfig:
    try:
        import user_config as cfg
    except Exception as exc:
        raise RuntimeError("Не найден user_config.py. Скопируй и заполни его.") from exc

    token = str(getattr(cfg, "BOT_TOKEN", "")).strip()
    admin_user_id = int(getattr(cfg, "ADMIN_USER_ID", 0))
    group_chat_id = int(getattr(cfg, "GROUP_CHAT_ID", 0))
    tz_name = str(getattr(cfg, "TIMEZONE", "Europe/Moscow")).strip()
    training_target_samples = int(getattr(cfg, "TRAINING_TARGET_SAMPLES", 500))
    feedback_path = str(getattr(cfg, "FEEDBACK_PATH", "feedback_stats.json")).strip()
    model_path = str(getattr(cfg, "MODEL_PATH", "adaptive_model_state.json")).strip()
    state_path = str(getattr(cfg, "STATE_PATH", "bot_state.json")).strip()
    chat_log_path = str(getattr(cfg, "CHAT_LOG_PATH", "chat_memory.jsonl")).strip()
    openai_api_key = str(getattr(cfg, "OPENAI_API_KEY", "")).strip()
    openai_base_url = str(getattr(cfg, "OPENAI_BASE_URL", "https://api.openai.com/v1")).strip()
    openai_model = str(getattr(cfg, "OPENAI_MODEL", "gpt-4.1-mini")).strip()
    openai_temperature = float(getattr(cfg, "OPENAI_TEMPERATURE", 0.8))
    openai_max_tokens = int(getattr(cfg, "OPENAI_MAX_TOKENS", 260))
    openai_timeout_sec = float(getattr(cfg, "OPENAI_TIMEOUT_SEC", 25.0))
    openai_rules = str(getattr(cfg, "OPENAI_RULES", "")).strip()
    raw_openai_enabled = getattr(cfg, "OPENAI_ENABLED", None)
    openai_enabled = _as_bool(raw_openai_enabled, default=bool(openai_api_key))

    raw_premium = getattr(cfg, "PREMIUM_EMOJI_IDS", [])
    if isinstance(raw_premium, str):
        base_premium_emoji_ids = [item.strip() for item in raw_premium.split(",") if item.strip()]
    else:
        base_premium_emoji_ids = [str(item).strip() for item in list(raw_premium) if str(item).strip()]

    if not token or "PUT_YOUR_TOKEN" in token:
        raise RuntimeError("Заполни BOT_TOKEN в user_config.py")
    try:
        timezone = ZoneInfo(tz_name)
    except Exception as exc:
        raise RuntimeError(f"Некорректный TIMEZONE: {tz_name}") from exc

    return BotConfig(
        token=token,
        timezone=timezone,
        base_premium_emoji_ids=base_premium_emoji_ids,
        admin_user_id=admin_user_id,
        group_chat_id=group_chat_id,
        training_target_samples=max(10, training_target_samples),
        feedback_path=feedback_path,
        model_path=model_path,
        state_path=state_path,
        chat_log_path=chat_log_path or "chat_memory.jsonl",
        openai_enabled=bool(openai_enabled and openai_api_key),
        openai_api_key=openai_api_key,
        openai_base_url=openai_base_url or "https://api.openai.com/v1",
        openai_model=openai_model or "gpt-4.1-mini",
        openai_temperature=max(0.0, min(1.5, openai_temperature)),
        openai_max_tokens=max(80, min(900, openai_max_tokens)),
        openai_timeout_sec=max(5.0, min(120.0, openai_timeout_sec)),
        openai_rules=openai_rules,
    )


def _get_config(context: ContextTypes.DEFAULT_TYPE) -> BotConfig:
    return context.application.bot_data["config"]


def _get_store(context: ContextTypes.DEFAULT_TYPE) -> FeedbackStore:
    return context.application.bot_data["store"]


def _get_model(context: ContextTypes.DEFAULT_TYPE) -> TinyFeedbackModel:
    return context.application.bot_data["model"]


def _get_state(context: ContextTypes.DEFAULT_TYPE) -> BotStateStore:
    return context.application.bot_data["state"]


def _pending_inputs(context: ContextTypes.DEFAULT_TYPE) -> dict[str, dict]:
    return context.application.bot_data.setdefault("pending_inputs", {})


def _recent_generations(context: ContextTypes.DEFAULT_TYPE) -> dict[str, dict]:
    return context.application.bot_data.setdefault("recent_generations", {})


def _training_waiting(context: ContextTypes.DEFAULT_TYPE) -> dict[str, dict]:
    return context.application.bot_data.setdefault("training_waiting", {})


def _set_training_waiting(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    waiting: bool,
    message_id: int | None = None,
) -> None:
    store = _training_waiting(context)
    key = str(int(chat_id))
    if not waiting:
        store.pop(key, None)
        return
    payload: dict[str, int | bool] = {"waiting": True}
    if message_id:
        payload["message_id"] = int(message_id)
    store[key] = payload


def _is_training_waiting(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> bool:
    key = str(int(chat_id))
    data = _training_waiting(context).get(key, {})
    return bool(data.get("waiting", False))


def _group_dialogue_cache(context: ContextTypes.DEFAULT_TYPE) -> dict[str, list[str]]:
    return context.application.bot_data.setdefault("group_dialogue_cache", {})


def _group_reply_meta(context: ContextTypes.DEFAULT_TYPE) -> dict[str, float]:
    return context.application.bot_data.setdefault("group_reply_meta", {})


def _reaction_cursor(context: ContextTypes.DEFAULT_TYPE) -> dict[str, int]:
    return context.application.bot_data.setdefault("reaction_cursor", {})


def _chat_log_file_path(context: ContextTypes.DEFAULT_TYPE) -> str:
    config = _get_config(context)
    path = str(config.chat_log_path or "chat_memory.jsonl").strip()
    if os.path.isabs(path):
        return path
    return os.path.join(os.getcwd(), path)


def _append_chat_log_row(context: ContextTypes.DEFAULT_TYPE, row: dict) -> None:
    try:
        path = _chat_log_file_path(context)
        folder = os.path.dirname(path)
        if folder:
            os.makedirs(folder, exist_ok=True)
        payload = dict(row)
        payload.setdefault("ts", datetime.now(ZoneInfo("UTC")).isoformat())
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception as exc:
        logging.warning("Cannot write chat log: %s", exc)


def _iter_chat_log_rows(context: ContextTypes.DEFAULT_TYPE, *, reverse: bool = False) -> list[dict]:
    path = _chat_log_file_path(context)
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as handle:
            lines = handle.readlines()
    except Exception:
        return []
    if reverse:
        lines = list(reversed(lines))
    out: list[dict] = []
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if isinstance(row, dict):
            out.append(row)
    return out


def _chat_log_quick_stats(context: ContextTypes.DEFAULT_TYPE) -> tuple[int, int]:
    rows = _iter_chat_log_rows(context, reverse=False)
    users: set[int] = set()
    for row in rows:
        uid = int(row.get("user_id", 0) or 0)
        if uid:
            users.add(uid)
    return len(rows), len(users)


def _message_content_type(message: object) -> str:
    if getattr(message, "text", None):
        return "text"
    if getattr(message, "voice", None):
        return "voice"
    if getattr(message, "video_note", None):
        return "video_note"
    if getattr(message, "video", None):
        return "video"
    if getattr(message, "sticker", None):
        return "sticker"
    if getattr(message, "document", None):
        return "document"
    if getattr(message, "photo", None):
        return "photo"
    if getattr(message, "audio", None):
        return "audio"
    return "other"


def _message_text_or_caption(message: object) -> str:
    text = str(getattr(message, "text", "") or "").strip()
    if text:
        return " ".join(text.split())
    caption = str(getattr(message, "caption", "") or "").strip()
    if caption:
        return " ".join(caption.split())
    return ""


def _log_incoming_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not msg or not chat or not user:
        return

    row = {
        "direction": "incoming",
        "chat_id": int(getattr(chat, "id", 0) or 0),
        "chat_type": str(getattr(chat, "type", "") or ""),
        "chat_title": str(getattr(chat, "title", "") or ""),
        "message_id": int(getattr(msg, "message_id", 0) or 0),
        "reply_to_message_id": int(getattr(getattr(msg, "reply_to_message", None), "message_id", 0) or 0),
        "user_id": int(getattr(user, "id", 0) or 0),
        "username": str(getattr(user, "username", "") or ""),
        "first_name": str(getattr(user, "first_name", "") or ""),
        "last_name": str(getattr(user, "last_name", "") or ""),
        "content_type": _message_content_type(msg),
        "text": _message_text_or_caption(msg),
    }
    _append_chat_log_row(context, row)


def _log_outgoing_message(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    chat_type: str,
    message_id: int,
    text: str,
    source: str,
    reply_to_message_id: int = 0,
    peer_user_id: int = 0,
) -> None:
    me = getattr(context.bot, "id", 0) or 0
    row = {
        "direction": "outgoing",
        "chat_id": int(chat_id),
        "chat_type": str(chat_type or ""),
        "message_id": int(message_id),
        "reply_to_message_id": int(reply_to_message_id or 0),
        "user_id": int(me),
        "username": str(getattr(context.bot, "username", "") or ""),
        "first_name": str(getattr(context.bot, "first_name", "") or ""),
        "last_name": "",
        "content_type": "text",
        "text": " ".join(str(text or "").split()),
        "source": str(source or ""),
        "peer_user_id": int(peer_user_id or 0),
    }
    _append_chat_log_row(context, row)


def _log_users_summary(context: ContextTypes.DEFAULT_TYPE, *, limit: int = 80) -> list[dict]:
    rows = _iter_chat_log_rows(context, reverse=True)
    by_user: dict[int, dict] = {}
    for row in rows:
        user_id = int(row.get("user_id", 0) or 0)
        if not user_id:
            continue
        direction = str(row.get("direction", "")).strip().lower()
        if direction != "incoming":
            continue
        if user_id not in by_user:
            by_user[user_id] = {
                "user_id": user_id,
                "username": str(row.get("username", "") or ""),
                "first_name": str(row.get("first_name", "") or ""),
                "last_name": str(row.get("last_name", "") or ""),
                "last_ts": str(row.get("ts", "") or ""),
                "messages": 0,
            }
        by_user[user_id]["messages"] += 1
    items = list(by_user.values())
    items.sort(key=lambda x: (x.get("last_ts", ""), int(x.get("messages", 0))), reverse=True)
    return items[: max(1, int(limit))]


def _user_export_keyboard(context: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    users = _log_users_summary(context, limit=40)
    rows: list[list[InlineKeyboardButton]] = []
    for row in users:
        uid = int(row.get("user_id", 0))
        if not uid:
            continue
        name = " ".join(
            part
            for part in (
                str(row.get("first_name", "")).strip(),
                str(row.get("last_name", "")).strip(),
            )
            if part
        )
        if not name:
            username = str(row.get("username", "")).strip()
            name = f"@{username}" if username else str(uid)
        label = f"{name} ({uid})"
        if len(label) > 58:
            label = f"{label[:55]}..."
        rows.append([InlineKeyboardButton(label, callback_data=f"menu|export_user|{uid}")])
    if not rows:
        rows.append([InlineKeyboardButton("Пока нет логов", callback_data="menu|noop")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu|settings")])
    return InlineKeyboardMarkup(rows)


def _is_primary_admin_user(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    admin_id = _effective_admin_user_id(context)
    return bool(admin_id and int(user_id) == int(admin_id))


def _safe_relation_delta(context: ContextTypes.DEFAULT_TYPE, user_id: int, delta: int) -> int:
    if _is_primary_admin_user(context, user_id) and int(delta) < 0:
        return 0
    return int(delta)


def _safe_relation_score(context: ContextTypes.DEFAULT_TYPE, user_id: int, score: int) -> int:
    target = int(score)
    if _is_primary_admin_user(context, user_id):
        target = max(40, target)
    return max(-100, min(100, target))


def _ensure_admin_relation_floor(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    state: BotStateStore,
    chat_id: int,
    user_id: int,
) -> dict | None:
    if not _is_primary_admin_user(context, user_id):
        return state.get_relation(chat_id=chat_id, user_id=user_id)
    relation = state.get_relation(chat_id=chat_id, user_id=user_id)
    if relation and int(relation.get("score", 0)) >= 40:
        return relation
    return state.set_relation_score(
        chat_id=chat_id,
        user_id=user_id,
        score=40,
        reason="админ всегда в плюс-статусе",
    )


def _append_group_dialogue(context: ContextTypes.DEFAULT_TYPE, *, chat_id: int, line: str, limit: int = 180) -> None:
    clean = " ".join(str(line).split())
    if not clean:
        return
    cache = _group_dialogue_cache(context)
    key = str(int(chat_id))
    rows = cache.setdefault(key, [])
    rows.append(clean)
    max_items = max(20, int(limit))
    if len(rows) > max_items:
        cache[key] = rows[-max_items:]


def _recent_group_dialogue(context: ContextTypes.DEFAULT_TYPE, *, chat_id: int, limit: int = 14) -> list[str]:
    rows = _group_dialogue_cache(context).get(str(int(chat_id)), [])
    if not isinstance(rows, list):
        return []
    return rows[-max(1, int(limit)) :]


def _mark_group_reply_now(context: ContextTypes.DEFAULT_TYPE, *, chat_id: int) -> None:
    _group_reply_meta(context)[str(int(chat_id))] = time.time()


def _is_group_reply_cooldown_ready(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    cooldown_sec: float = GROUP_CHAT_REPLY_COOLDOWN_SEC,
) -> bool:
    now_ts = time.time()
    last_ts = float(_group_reply_meta(context).get(str(int(chat_id)), 0.0))
    return (now_ts - last_ts) >= max(0.0, float(cooldown_sec))


def _chat_name_or_title(chat: object) -> str:
    title = str(getattr(chat, "title", "") or "").strip()
    if title:
        return title
    first = str(getattr(chat, "first_name", "") or "").strip()
    last = str(getattr(chat, "last_name", "") or "").strip()
    joined = f"{first} {last}".strip()
    return joined or "чат"


def _is_target_group_chat(context: ContextTypes.DEFAULT_TYPE, chat: object) -> bool:
    if not chat:
        return False
    chat_type = str(getattr(chat, "type", "")).strip()
    if chat_type == "private":
        return False
    effective_group_id = _effective_group_chat_id(context)
    if not int(effective_group_id):
        return False
    return int(getattr(chat, "id", 0)) == int(effective_group_id)


def _flatten_export_text(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            parts.append(_flatten_export_text(item))
        return "".join(parts)
    if isinstance(value, dict):
        return _flatten_export_text(value.get("text", ""))
    return ""


def _extract_style_examples_from_export_json(payload: dict, author_user_id: int, limit: int = 5000) -> list[str]:
    raw_messages = payload.get("messages", [])
    if not isinstance(raw_messages, list):
        return []
    author_key = f"user{int(author_user_id)}" if int(author_user_id) else ""
    out: list[str] = []
    seen: set[str] = set()
    for row in raw_messages:
        if not isinstance(row, dict):
            continue
        if str(row.get("type", "")).strip().lower() != "message":
            continue
        from_id = str(row.get("from_id", "")).strip().lower()
        if author_key and from_id != author_key:
            continue
        text = " ".join(_flatten_export_text(row.get("text", "")).split())
        if len(text) < 4:
            continue
        norm = text.lower().replace("ё", "е")
        if norm in seen:
            continue
        seen.add(norm)
        out.append(text)
        if len(out) >= max(50, limit):
            break
    return out


def _extract_style_examples_from_text(raw_text: str, limit: int = 5000) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw_line in str(raw_text).splitlines():
        line = " ".join(raw_line.split())
        if len(line) < 4:
            continue
        norm = line.lower().replace("ё", "е")
        if norm in seen:
            continue
        seen.add(norm)
        out.append(line)
        if len(out) >= max(50, limit):
            break
    return out


def _clean_html_fragment(raw: str) -> str:
    if not raw:
        return ""
    text = re.sub(r"(?i)<br\s*/?>", "\n", str(raw))
    text = re.sub(r"(?i)</p>", "\n", text)
    text = HTML_TAG_RE.sub(" ", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    return " ".join(text.split())


def _extract_style_examples_from_export_html(
    raw_html: str,
    *,
    author_hints: list[str] | None = None,
    limit: int = 5000,
) -> list[str]:
    source = str(raw_html or "")
    if not source:
        return []

    hints = []
    for hint in author_hints or []:
        clean = " ".join(str(hint).strip().lower().replace("ё", "е").split())
        if clean:
            hints.append(clean)

    rows: list[tuple[str, str]] = []
    pattern = re.compile(
        r'<div class="from_name">\s*(.*?)\s*</div>.*?<div class="text"[^>]*>\s*(.*?)\s*</div>',
        re.IGNORECASE | re.DOTALL,
    )
    for from_html, text_html in pattern.findall(source):
        author = _clean_html_fragment(from_html)
        text = _clean_html_fragment(text_html)
        if len(text) < 4:
            continue
        rows.append((author, text))

    if not rows:
        text_pattern = re.compile(r'<div class="text"[^>]*>\s*(.*?)\s*</div>', re.IGNORECASE | re.DOTALL)
        for text_html in text_pattern.findall(source):
            text = _clean_html_fragment(text_html)
            if len(text) >= 4:
                rows.append(("", text))

    if not rows:
        return []

    if hints:
        filtered: list[tuple[str, str]] = []
        for author, text in rows:
            author_norm = " ".join(author.lower().replace("ё", "е").split())
            if any(hint and hint in author_norm for hint in hints):
                filtered.append((author, text))
        if filtered:
            rows = filtered

    out: list[str] = []
    seen: set[str] = set()
    for _, text in rows:
        line = " ".join(text.split())
        if len(line) < 4:
            continue
        norm = line.lower().replace("ё", "е")
        if norm in seen:
            continue
        seen.add(norm)
        out.append(line)
        if len(out) >= max(50, limit):
            break
    return out


def _social_mode_ru(mode: str) -> str:
    clean = str(mode or "").strip().lower()
    if clean == "style_clone":
        return "как я (по экспорту)"
    return "самообучение по людям"


def _relation_status_ru(status: str) -> str:
    clean = str(status or "").strip().lower()
    if clean == "friendly":
        return "дружелюбно"
    if clean == "warm":
        return "тепло"
    if clean == "cold":
        return "холодно"
    if clean == "hostile":
        return "конфликт"
    return "нейтрально"


def _relation_display_name(relation: dict) -> str:
    first = str(relation.get("first_name", "")).strip()
    last = str(relation.get("last_name", "")).strip()
    username = str(relation.get("username", "")).strip()
    full = f"{first} {last}".strip()
    if full:
        return full
    if username:
        return f"@{username}" if not username.startswith("@") else username
    return str(relation.get("user_id", "пользователь"))


def _relation_summary_for_prompt(relation: dict | None) -> str:
    if not relation:
        return "новый участник, рейтинг 0"
    score = int(relation.get("score", 0))
    status = _relation_status_ru(str(relation.get("status", "neutral")))
    friendly_hits = int(relation.get("friendly_hits", 0))
    rude_hits = int(relation.get("rude_hits", 0))
    blocked = bool(relation.get("forgive_blocked", False))
    grudges = relation.get("grudges", [])
    if not isinstance(grudges, list):
        grudges = []
    last_reason = " ".join(str(relation.get("last_reason", "")).split())
    grudge_hint = ", ".join(str(x) for x in grudges[-3:] if str(x).strip())
    parts = [
        f"рейтинг={score}",
        f"статус={status}",
        f"доброжелательных сигналов={friendly_hits}",
        f"грубых сигналов={rude_hits}",
        f"прощение заблокировано={'да' if blocked else 'нет'}",
    ]
    if last_reason:
        parts.append(f"последняя причина={last_reason}")
    if grudge_hint:
        parts.append(f"обиды={grudge_hint}")
    return "; ".join(parts)


def _has_any_marker(plain_text: str, markers: tuple[str, ...]) -> bool:
    return any(marker in plain_text for marker in markers)


def _is_forgive_request(text: str) -> bool:
    plain = _plain_text_for_blacklist(text)
    return _has_any_marker(plain, FORGIVE_MARKERS)


def _relation_signal(text: str) -> tuple[int, str]:
    plain = _plain_text_for_blacklist(text)
    if not plain:
        return 0, ""

    tokens = plain.split()
    positive_hits = sum(1 for marker in FRIENDLY_MARKERS if marker in plain)
    negative_hits = sum(1 for marker in RUDE_MARKERS if marker in plain)
    delta = 0
    reason = ""

    if positive_hits:
        delta += min(12, positive_hits * 4)
        reason = "дружелюбный тон"
    if negative_hits:
        delta -= min(36, negative_hits * 9)
        reason = "грубый тон"
    if not positive_hits and not negative_hits and len(tokens) <= 2 and not _looks_like_question(text):
        delta -= 2
        reason = "сухой тон"
    if _is_wish_like_text(text):
        delta += 3
        reason = "пожелание"
    if _is_forgive_request(text):
        reason = "запрос на прощение"
    return delta, reason


def _forgive_reply_for_relation(*, relation: dict, user_name: str) -> str:
    score = int(relation.get("score", 0))
    blocked = bool(relation.get("forgive_blocked", False))
    prefix = f"{user_name}, " if user_name else ""

    if score <= -65 or blocked:
        choices = [
            "я помню, за что злюсь. Пока не готов отпустить.",
            "нет, тут быстро не починить. Осадок сильный.",
            "пока без прощения. Я это не забыл.",
        ]
        return f"{prefix}{random.choice(choices)}"
    if score < -18:
        choices = [
            "подумаю. Может позже отпущу ситуацию.",
            "может быть, но не сразу. Дай время.",
            "пока под вопросом. Посмотрим по делам.",
        ]
        return f"{prefix}{random.choice(choices)}"

    choices = [
        "та за что, ты меня не обижал. Все нормально.",
        "да все ок, тут и прощать нечего.",
        "спокойно, я не в обиде.",
    ]
    return f"{prefix}{random.choice(choices)}"


def _extract_v_query(text: str) -> str | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    if raw.lower().startswith(GROUP_QUERY_PREFIX):
        payload = raw[len(GROUP_QUERY_PREFIX) :].strip(" :,-")
        return payload or None
    return None


def _city_from_time_query(query: str) -> str | None:
    clean = _plain_text_for_blacklist(query)
    if not clean:
        return None
    if any(marker in clean for marker in AMSTERDAM_MARKERS):
        return "амстердам"
    match = re.search(r"\bв\s+([a-zа-я0-9\-\s]{2,40})$", clean)
    if match:
        return " ".join(match.group(1).split())
    if clean.startswith("время "):
        return " ".join(clean.replace("время", "", 1).split())
    return None


def _resolve_timezone_by_city(city: str) -> str | None:
    clean = " ".join(str(city or "").lower().replace("ё", "е").split())
    if not clean:
        return None
    if clean in CITY_TIMEZONE_MAP:
        return CITY_TIMEZONE_MAP[clean]
    candidates = [
        clean.replace(" ", "_"),
        clean.title().replace(" ", "_"),
        clean.capitalize().replace(" ", "_"),
    ]
    for candidate in candidates:
        for prefix in ("Europe", "Asia", "America", "Africa", "Australia"):
            tz = f"{prefix}/{candidate}"
            try:
                ZoneInfo(tz)
                return tz
            except Exception:
                continue
    return None


def _time_answer_from_query(query: str) -> str | None:
    plain = _plain_text_for_blacklist(query)
    if not plain:
        return None
    if "врем" not in plain and "time" not in plain:
        return None
    city = _city_from_time_query(query)
    if not city:
        return None
    tz_name = _resolve_timezone_by_city(city)
    if not tz_name:
        return None
    try:
        now_local = datetime.now(ZoneInfo(tz_name))
    except Exception:
        return None
    city_title = city.title()
    return f"{city_title}: {now_local.strftime('%H:%M')} ({tz_name}), {now_local.strftime('%d.%m.%Y')}"


async def _web_lookup_answer(query: str) -> str:
    timeout = httpx.Timeout(connect=4.0, read=GROUP_QUERY_TIMEOUT_SEC, write=8.0, pool=4.0)
    params = {
        "q": query,
        "format": "json",
        "no_html": "1",
        "skip_disambig": "1",
        "no_redirect": "1",
        "kl": "ru-ru",
    }
    url = "https://api.duckduckgo.com/"
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(url, params=params)
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP {response.status_code}")
    data = response.json()
    candidates = [
        str(data.get("Answer", "")).strip(),
        str(data.get("AbstractText", "")).strip(),
        str(data.get("Definition", "")).strip(),
    ]
    answer = next((value for value in candidates if value), "")
    if not answer:
        related = data.get("RelatedTopics", [])
        if isinstance(related, list):
            for item in related:
                if isinstance(item, dict) and str(item.get("Text", "")).strip():
                    answer = str(item["Text"]).strip()
                    break
                topics = item.get("Topics") if isinstance(item, dict) else None
                if isinstance(topics, list):
                    for sub in topics:
                        if isinstance(sub, dict) and str(sub.get("Text", "")).strip():
                            answer = str(sub["Text"]).strip()
                            break
                    if answer:
                        break
    if not answer:
        return "Точного ответа не нашел. Уточни запрос."
    if len(answer) > 500:
        answer = f"{answer[:497]}..."
    return answer


async def _answer_v_query(query: str) -> str:
    clean = " ".join(str(query or "").split())
    if not clean:
        return "Напиши запрос после @v."
    time_answer = _time_answer_from_query(clean)
    if time_answer:
        return time_answer
    try:
        return await _web_lookup_answer(clean)
    except Exception as exc:
        logging.warning("@v lookup failed: %s", exc)
        return "Не смог найти ответ сейчас. Попробуй переформулировать запрос."


def _is_wish_like_text(text: str) -> bool:
    plain = _plain_text_for_blacklist(text)
    if not plain:
        return False
    return any(marker in plain for marker in WISH_REACTION_KEYWORDS)


def _kind_code(kind: str) -> str:
    if kind == "night":
        return "n"
    if kind == "morning":
        return "m"
    raise ValueError(f"Unsupported kind: {kind}")


def _kind_from_code(code: str) -> str:
    if code == "n":
        return "night"
    if code == "m":
        return "morning"
    raise ValueError(f"Unsupported code: {code}")


def _mode_name(mode: str) -> str:
    clean = str(mode).strip().lower()
    if clean == "advanced":
        return "standard"
    return clean if clean in {"short", "standard", "context"} else "standard"


def _mode_ru(mode: str) -> str:
    clean = _mode_name(mode)
    if clean == "short":
        return "коротко"
    if clean == "context":
        return "по смыслу (эмодзи в тексте)"
    return "стандарт (с пожеланием)"


def _group_activity_ru(mode: str) -> str:
    clean = str(mode or "").strip().lower()
    if clean == "quiet":
        return "тихо"
    if clean == "active":
        return "каждое сообщение"
    if clean == "question_only":
        return "только вопрос"
    return "норм"


def _mode_label(mode: str) -> str:
    clean = _mode_name(mode)
    if clean == "short":
        return "Режим: коротко"
    if clean == "context":
        return "Режим: по смыслу"
    return "Режим: стандарт"


def _on_off_ru(enabled: bool) -> str:
    return "ВКЛ" if enabled else "ВЫКЛ"


def _plain_text_for_blacklist(text: str) -> str:
    no_custom = _strip_tg_emoji_tags(text or "")
    no_tags = re.sub(r"<[^>]+>", " ", no_custom)
    lowered = no_tags.lower().replace("ё", "е")
    cleaned = re.sub(r"[^a-zа-я0-9\s]+", " ", lowered)
    return " ".join(cleaned.split())


def _looks_like_bad_language_output(text: str) -> bool:
    raw = " ".join(str(text or "").split())
    if not raw:
        return True
    if any(ch in raw for ch in MOJIBAKE_CHARS):
        return True

    letters = [ch for ch in raw if ch.isalpha()]
    if not letters:
        return False

    def _is_cjk(ch: str) -> bool:
        code = ord(ch)
        return (
            0x4E00 <= code <= 0x9FFF
            or 0x3400 <= code <= 0x4DBF
            or 0x3040 <= code <= 0x309F
            or 0x30A0 <= code <= 0x30FF
            or 0xAC00 <= code <= 0xD7AF
        )

    cjk_count = sum(1 for ch in letters if _is_cjk(ch))
    if cjk_count >= 2 and (cjk_count / max(1, len(letters))) >= 0.12:
        return True

    cyr_or_lat_count = sum(1 for ch in letters if ("а" <= ch.lower() <= "я") or ("a" <= ch.lower() <= "z"))
    if cyr_or_lat_count / max(1, len(letters)) < 0.55:
        return True
    return False


def _blacklist_tokens(text: str) -> list[str]:
    return [tok for tok in _plain_text_for_blacklist(text).split() if len(tok) > 1]


def _phrase_matches_segment(phrase: str, segment: str) -> bool:
    p = _plain_text_for_blacklist(phrase)
    s = _plain_text_for_blacklist(segment)
    if not p or not s:
        return False
    if p in s:
        return True
    p_tokens_list = _blacklist_tokens(p)
    s_tokens_list = _blacklist_tokens(s)
    p_tokens = set(p_tokens_list)
    s_tokens = set(s_tokens_list)
    if p_tokens:
        coverage = len(p_tokens & s_tokens) / max(1, len(p_tokens))
        if coverage >= 0.75:
            return True

    # For one-word phrases use fuzzy token matching (e.g. "подруга" vs "подружка").
    if len(p_tokens_list) == 1 and s_tokens_list:
        needle = p_tokens_list[0]
        for token in s_tokens_list:
            if needle in token or token in needle:
                return True
            if SequenceMatcher(a=needle, b=token).ratio() >= 0.72:
                return True

    # For multi-word phrases count fuzzy token overlaps.
    if len(p_tokens_list) >= 2 and s_tokens_list:
        matched = 0
        for p_tok in p_tokens_list:
            for s_tok in s_tokens_list:
                ratio = SequenceMatcher(a=p_tok, b=s_tok).ratio()
                if ratio >= 0.84:
                    matched += 1
                    break
        if matched / max(1, len(p_tokens_list)) >= 0.75:
            return True

    if len(p) >= 8:
        ratio = SequenceMatcher(a=p, b=s).ratio()
        if ratio >= 0.82:
            return True
    return False


def _contains_blacklisted_phrase(text: str, blacklist: list[str]) -> bool:
    if not blacklist:
        return False
    plain = _plain_text_for_blacklist(text)
    segments = [seg for seg in re.split(r"[.!?]+", plain) if seg.strip()]
    if not segments:
        segments = [plain]
    for phrase in blacklist:
        if not str(phrase).strip():
            continue
        for segment in segments:
            if _phrase_matches_segment(phrase, segment):
                return True
    return False


def _strip_blacklisted_sentences(text: str, blacklist: list[str]) -> str:
    if not blacklist:
        return text
    source = str(text or "").strip()
    if not source:
        return ""
    sentences = [seg.strip() for seg in re.split(r"(?<=[.!?…])\s+", source) if seg.strip()]
    if not sentences:
        sentences = [source]
    kept: list[str] = []
    for sentence in sentences:
        if _contains_blacklisted_phrase(sentence, blacklist):
            continue
        kept.append(sentence)
    return " ".join(kept).strip()


def _safe_fallback_text(kind: str, blacklist: list[str]) -> str:
    if kind == "morning":
        options = [
            "Доброе утро. Пусть день будет спокойным и светлым. ✨🤍☀️",
            "С добрым утром. Пусть сегодня все складывается мягко и по силам. 🌤️🤍✨",
            "Доброе утро. Бережного тебе дня и хорошего ритма. 🌼🤍☕",
        ]
    else:
        options = [
            "Спокойной ночи. Пусть ночь принесет отдых и тишину. ✨🤍☁️",
            "Доброй ночи. Мягкого сна и спокойного сердца. 🌙🤍✨",
            "Спи спокойно. Пусть усталость уйдет, а силы вернутся к утру. 💤🤍☁️",
        ]
    for option in options:
        if not _contains_blacklisted_phrase(option, blacklist):
            return option
    # If blacklist became overly broad, return a minimal neutral fallback.
    return "Доброе утро." if kind == "morning" else "Спокойной ночи."


def _snippet_hint(snippet: str) -> str:
    tokens = _blacklist_tokens(snippet)
    if not tokens:
        return "Понял, буду избегать похожих формулировок."
    if len(tokens) <= 2:
        return "Понял, буду избегать коротких похожих оборотов."
    return "Понял, буду анализировать похожие фразы по словам и близости формулировки."


def _append_favorite_phrase(text: str, phrase: str) -> str:
    clean = " ".join(str(phrase).strip().split())
    if not clean:
        return text
    if _plain_text_for_blacklist(clean) in _plain_text_for_blacklist(text):
        return text
    ending = clean if re.search(r"[.!?…]$", clean) else f"{clean}."
    return f"{text} {html.escape(ending)}"


def _favorite_phrase_matches_kind(phrase: str, kind: str) -> bool:
    plain = _plain_text_for_blacklist(phrase)
    if not plain:
        return False
    if kind == "morning":
        # Don't append explicitly night phrases to morning wishes.
        night_markers = (
            "спокойной ночи",
            "доброй ночи",
            "сладких снов",
            "хорошего сна",
            "спокойного сна",
            "ноч",
            "снов",
            "спи ",
        )
        return not any(marker in plain for marker in night_markers)
    if kind == "night":
        # Don't append explicitly morning phrases to night wishes.
        morning_markers = (
            "доброе утро",
            "с добрым утром",
            "хорошего утра",
            "утро",
            "утра",
            "просып",
        )
        return not any(marker in plain for marker in morning_markers)
    return True


def _recent_chat_texts(store: FeedbackStore, chat_id: int, limit: int = 60) -> list[str]:
    rows = store.recent_generations(chat_id=chat_id, limit=limit)
    out: list[str] = []
    for row in rows:
        text = str(row.get("text", "")).strip()
        if text:
            out.append(text)
    return out


def _text_similarity(a: str, b: str) -> float:
    left = _plain_text_for_blacklist(a)
    right = _plain_text_for_blacklist(b)
    if not left or not right:
        return 0.0
    return SequenceMatcher(a=left, b=right).ratio()


def _max_similarity_to_recent(text: str, recent_texts: list[str]) -> float:
    if not recent_texts:
        return 0.0
    return max(_text_similarity(text, old) for old in recent_texts)


def _is_too_similar_to_recent(text: str, recent_texts: list[str], mode: str) -> bool:
    similarity = _max_similarity_to_recent(text, recent_texts)
    threshold = 0.86 if mode == "short" else 0.82
    return similarity >= threshold


def _openai_runtime_cfg(config: BotConfig) -> OpenAIWishConfig:
    return OpenAIWishConfig(
        api_key=config.openai_api_key,
        base_url=config.openai_base_url,
        model=config.openai_model,
        timeout_sec=config.openai_timeout_sec,
        temperature=config.openai_temperature,
        max_tokens=config.openai_max_tokens,
        rules=config.openai_rules,
    )


def _openai_features(kind: str, mode: str, audience: str, text: str, person_id: int) -> dict[str, float]:
    out: dict[str, float] = {
        f"kind:{kind}": 1.0,
        f"mode:{mode}": 1.0,
        f"audience:{audience}": 1.0,
        "source:openai": 1.0,
        f"template:{kind}:{mode}:-2": 1.0,
    }
    if person_id:
        out[f"person:{person_id}"] = 1.0
    words = re.findall(r"[a-zа-я0-9]{3,}", _plain_text_for_blacklist(text))
    for token in words[:18]:
        out[f"tok:{token}"] = 1.0
    return out


def _openai_generated_wish(
    *,
    kind: str,
    mode: str,
    audience: str,
    person_id: int,
    person_name: str,
    text: str,
) -> GeneratedWish:
    emoji_count = len(_extract_unicode_emojis(_strip_tg_emoji_tags(text)))
    features = _openai_features(kind, mode, audience, text, person_id)
    return GeneratedWish(
        kind=kind,
        mode=mode,
        person_id=person_id,
        person_name=person_name,
        text=text,
        template_idx=-2,
        picks={"openai": 1},
        features=features,
        emoji_count=emoji_count,
    )


def _short_person(name: str) -> str:
    clean = (name or "общий вариант").strip()
    if len(clean) <= 14:
        return clean
    return f"{clean[:11]}..."


def _effective_premium_ids(context: ContextTypes.DEFAULT_TYPE) -> list[str]:
    config = _get_config(context)
    state = _get_state(context)
    return state.get_premium_emoji_ids(config.base_premium_emoji_ids)


def _effective_liked_emojis(context: ContextTypes.DEFAULT_TYPE) -> list[str]:
    return _get_state(context).get_liked_emojis()


def _effective_admin_user_id(context: ContextTypes.DEFAULT_TYPE) -> int:
    config = _get_config(context)
    state = _get_state(context)
    return state.get_effective_admin_user_id(config.admin_user_id)


def _effective_group_chat_id(context: ContextTypes.DEFAULT_TYPE) -> int:
    config = _get_config(context)
    state = _get_state(context)
    return state.get_effective_group_chat_id(config.group_chat_id)


def _extract_unicode_emojis(text: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for match in EMOJI_SEQUENCE_RE.findall(text or ""):
        if not match or match in seen:
            continue
        seen.add(match)
        out.append(match)
    return out


def _extract_custom_emoji_ids(message: object) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []

    for bucket_name in ("entities", "caption_entities"):
        entities = getattr(message, bucket_name, None) or []
        for entity in entities:
            entity_type = getattr(entity, "type", "")
            is_custom = str(entity_type) == "custom_emoji" or str(getattr(entity_type, "value", "")) == "custom_emoji"
            if not is_custom:
                continue
            emoji_id = str(getattr(entity, "custom_emoji_id", "") or "").strip()
            if not emoji_id or emoji_id in seen:
                continue
            seen.add(emoji_id)
            out.append(emoji_id)

    sticker = getattr(message, "sticker", None)
    if sticker:
        sticker_custom_id = str(getattr(sticker, "custom_emoji_id", "") or "").strip()
        if sticker_custom_id and sticker_custom_id not in seen:
            seen.add(sticker_custom_id)
            out.append(sticker_custom_id)
    return out


def _is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    admin_id = _effective_admin_user_id(context)
    user = update.effective_user
    return bool(admin_id and user and user.id == admin_id)


def _is_allowed_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.effective_user
    if not user:
        return False
    if _is_admin(update, context):
        return True
    state = _get_state(context)
    chat = update.effective_chat
    if chat and chat.type == "private" and state.is_public_private_chat_mode():
        return True
    if not state.is_admin_only_mode():
        return True
    return state.has_access_exception(user.id)


async def _safe_query_answer(query, text: str | None = None, *, show_alert: bool = False) -> None:
    if not query:
        return
    try:
        if text is None:
            await query.answer()
        else:
            await query.answer(text, show_alert=show_alert)
    except TelegramError as exc:
        logging.warning("answerCallbackQuery failed: %s", exc)


class _SafeCallbackQueryProxy:
    def __init__(self, query):
        self._query = query

    def __getattr__(self, name: str):
        return getattr(self._query, name)

    async def answer(self, text: str | None = None, show_alert: bool = False) -> None:
        await _safe_query_answer(self._query, text, show_alert=show_alert)


async def _reject_non_admin(update: Update) -> None:
    query = update.callback_query
    if query:
        await _safe_query_answer(query, NON_ADMIN_TEXT, show_alert=True)
        if query.message:
            await query.message.reply_text(NON_ADMIN_TEXT)
        return
    if update.effective_message:
        await update.effective_message.reply_text(NON_ADMIN_TEXT)


async def _reject_no_access(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    state = _get_state(context)
    text = "Иш чё выдумала, тыкает она тут"
    if state.is_admin_only_mode():
        text = "Иш чё выдумала, тыкает она тут"

    query = update.callback_query
    if query:
        await _safe_query_answer(query, text, show_alert=True)
        if query.message:
            await query.message.reply_text(text)
        return
    if update.effective_message:
        await update.effective_message.reply_text(text)


async def _ensure_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if _is_admin(update, context):
        return True

    state = _get_state(context)
    current_admin = _effective_admin_user_id(context)
    user = update.effective_user
    chat = update.effective_chat
    if not current_admin and user and chat and chat.type == "private":
        state.set_runtime_admin_user_id(user.id)
        if update.effective_message:
            await update.effective_message.reply_text(
                "Первичный админ настроен автоматически. Теперь управление доступно этому аккаунту."
            )
        elif update.callback_query:
            await _safe_query_answer(update.callback_query, "Ты назначен админом", show_alert=True)
        return True

    await _reject_non_admin(update)
    return False


async def _ensure_access(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if _is_allowed_user(update, context):
        return True
    await _reject_no_access(update, context)
    return False


def _is_public_private_chat_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private":
        return False
    if _is_admin(update, context):
        return False
    return _get_state(context).is_public_private_chat_mode()


def _reason_label(rating_code: str, reason_code: str) -> str:
    good_reasons = {
        "a": "понравилось всё",
        "w": "классное пожелание",
        "s": "приятный стиль",
        "e": "крутые эмодзи",
        "h": "прям с душой",
        "m": "мило и уютно",
        "n": "без комментария",
    }
    bad_reasons = {
        "w": "слабое пожелание",
        "c": "нестыковка слов/падежей",
        "e": "плохие эмодзи",
        "l": "слишком длинно/коротко",
        "t": "тон не подошел",
        "b": "не зашла часть текста",
        "n": "без комментария",
    }
    if rating_code == "g":
        return good_reasons.get(reason_code, "другое")
    return bad_reasons.get(reason_code, "другое")

def _home_inline_keyboard(*, is_admin: bool, training_mode: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton("🌙 Спокойной ночи", callback_data="menu|send|n"),
            InlineKeyboardButton("☀️ Доброе утро", callback_data="menu|send|m"),
        ],
        [
            InlineKeyboardButton("ℹ️ Режимы", callback_data="menu|modes_help"),
            InlineKeyboardButton("📊 Статистика", callback_data="menu|stats"),
        ],
    ]
    if is_admin:
        label = f"🎓 Обучение: {_on_off_ru(training_mode)}"
        rows.append([InlineKeyboardButton(label, callback_data="menu|training_toggle")])
    rows.append([InlineKeyboardButton("⚙️ Настройки", callback_data="menu|settings")])
    return InlineKeyboardMarkup(rows)


def _send_target_keyboard(*, kind_code: str, is_admin: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton("👭 Девочке в личку", callback_data=f"menu|dispatch|{kind_code}|p|g"),
            InlineKeyboardButton("👬 Мальчику в личку", callback_data=f"menu|dispatch|{kind_code}|p|b"),
        ],
    ]
    if is_admin:
        rows.append([InlineKeyboardButton("👥 В группу", callback_data=f"menu|dispatch|{kind_code}|g|x")])
        rows.append([InlineKeyboardButton("📨 В личку и группу", callback_data=f"menu|dispatch|{kind_code}|b|x")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu|home")])
    return InlineKeyboardMarkup(rows)


def _settings_keyboard(
    mode: str,
    person_name: str,
    schedule_mode: str,
    admin_only_mode: bool,
    exceptions_count: int,
    blacklist_count: int,
    group_reaction_mode: bool = False,
    group_chat_mode: bool = False,
    public_private_chat_mode: bool = False,
    group_activity_mode: str = "normal",
    style_examples_count: int = 0,
    social_mode: str = "self_learning",
    roast_words_count: int = 0,
    relations_count: int = 0,
) -> InlineKeyboardMarkup:
    access_label = "🔒 Только админ: ВКЛ" if admin_only_mode else "🔓 Только админ: ВЫКЛ"
    schedule_label = f"🕒 Рассылка: {_mode_ru(schedule_mode)}"
    reaction_label = f"🔥 Реакции на voice/video: {_on_off_ru(group_reaction_mode)}"
    chat_mode_label = f"💬 Общение в группе: {_on_off_ru(group_chat_mode)}"
    public_chat_label = f"🗨 Личка для всех: {_on_off_ru(public_private_chat_mode)}"
    group_activity_label = f"🎚 Активность: {_group_activity_ru(group_activity_mode)}"
    social_mode_label = f"🧠 Соц режим: {_social_mode_ru(social_mode)}"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(_mode_label(mode), callback_data="menu|toggle_mode"),
                InlineKeyboardButton(f"Кому: {_short_person(person_name)}", callback_data="menu|persons"),
            ],
            [
                InlineKeyboardButton(schedule_label, callback_data="menu|toggle_schedule_mode"),
                InlineKeyboardButton(access_label, callback_data="menu|toggle_admin_only"),
            ],
            [
                InlineKeyboardButton("➕ Исключение по ID", callback_data="menu|add_exception"),
                InlineKeyboardButton(f"👥 Исключения ({exceptions_count})", callback_data="menu|list_exceptions"),
            ],
            [
                InlineKeyboardButton("➕ Добавить человека", callback_data="menu|add_person"),
                InlineKeyboardButton("🗑 Удалить человека", callback_data="menu|del_persons"),
            ],
            [
                InlineKeyboardButton(reaction_label, callback_data="menu|toggle_group_reaction"),
                InlineKeyboardButton(chat_mode_label, callback_data="menu|toggle_group_chat_mode"),
            ],
            [InlineKeyboardButton(public_chat_label, callback_data="menu|toggle_public_private_chat_mode")],
            [InlineKeyboardButton(group_activity_label, callback_data="menu|toggle_group_activity")],
            [
                InlineKeyboardButton(social_mode_label, callback_data="menu|toggle_social_mode"),
                InlineKeyboardButton(f"👤 Рейтинг людей ({relations_count})", callback_data="menu|relations"),
            ],
            [InlineKeyboardButton(f"🗯 Подколы/обзывалки ({roast_words_count})", callback_data="menu|roast")],
            [
                InlineKeyboardButton("📥 Импорт стиля общения", callback_data="menu|import_style_examples"),
                InlineKeyboardButton(f"🧹 Очистить стиль ({style_examples_count})", callback_data="menu|clear_style_examples"),
            ],
            [InlineKeyboardButton(f"🚫 Черный список ({blacklist_count})", callback_data="menu|blacklist")],
            [InlineKeyboardButton("📤 Экспорт переписок", callback_data="menu|export_chats")],
            [InlineKeyboardButton("📌 Сделать этот чат группой", callback_data="menu|set_group_here")],
            [InlineKeyboardButton("⭐ Премиум и эмодзи", callback_data="menu|premium")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="menu|home")],
        ]
    )


def _settings_markup_for_chat(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    mode: str,
    person_name: str,
) -> InlineKeyboardMarkup:
    state = _get_state(context)
    relation_chat_id = _effective_group_chat_id(context) or chat_id
    relations_count = len(state.list_chat_relations(chat_id=relation_chat_id, limit=9999))
    return _settings_keyboard(
        mode=mode,
        person_name=person_name,
        schedule_mode=state.get_schedule_mode(),
        admin_only_mode=state.is_admin_only_mode(),
        exceptions_count=len(state.list_access_exceptions()),
        blacklist_count=len(state.get_blacklist_phrases()),
        group_reaction_mode=state.is_group_fire_reaction_mode(),
        group_chat_mode=state.is_group_chat_mode(),
        public_private_chat_mode=state.is_public_private_chat_mode(),
        group_activity_mode=state.get_group_activity_mode(),
        style_examples_count=len(state.get_style_examples()),
        social_mode=state.get_social_mode(),
        roast_words_count=len(state.get_roast_words()),
        relations_count=relations_count,
    )


def _exceptions_keyboard(state: BotStateStore) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for row in state.list_access_exceptions():
        first = str(row.get("first_name", "")).strip()
        last = str(row.get("last_name", "")).strip()
        full_name = f"{first} {last}".strip() or "Без имени"
        label = f"{full_name} ({row['user_id']})"
        if len(label) > 58:
            label = f"{label[:55]}..."
        rows.append([InlineKeyboardButton(label, callback_data=f"menu|del_exception|{row['user_id']}")])
    if not rows:
        rows = [[InlineKeyboardButton("Список пуст", callback_data="menu|noop")]]
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu|settings")])
    return InlineKeyboardMarkup(rows)


def _premium_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("👀 Показать сохраненные эмодзи", callback_data="menu|premium_show")],
            [InlineKeyboardButton("🧹 Очистить обычные эмодзи", callback_data="menu|liked_reset")],
            [InlineKeyboardButton("♻️ Сбросить премиум-эмодзи к user_config.py", callback_data="menu|premium_reset")],
            [InlineKeyboardButton("🧹 Очистить любимые обороты", callback_data="menu|favorite_reset")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="menu|settings")],
        ]
    )


def _blacklist_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("👀 Показать фразы", callback_data="menu|blacklist_show")],
            [InlineKeyboardButton("🧹 Очистить черный список", callback_data="menu|blacklist_reset")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="menu|settings")],
        ]
    )


def _roast_keyboard(state: BotStateStore) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"👀 Показать список ({len(state.get_roast_words())})", callback_data="menu|roast_show")],
            [InlineKeyboardButton("➕ Добавить слова", callback_data="menu|roast_add")],
            [InlineKeyboardButton("♻️ Сбросить к базовым", callback_data="menu|roast_reset")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="menu|settings")],
        ]
    )


def _relations_keyboard(*, state: BotStateStore, relation_chat_id: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for rel in state.list_chat_relations(chat_id=relation_chat_id, limit=30):
        user_id = int(rel.get("user_id", 0))
        if not user_id:
            continue
        name = _relation_display_name(rel)
        score = int(rel.get("score", 0))
        status = _relation_status_ru(str(rel.get("status", "neutral")))
        label = f"{name} | {score:+d} ({status})"
        if len(label) > 58:
            label = f"{label[:55]}..."
        rows.append(
            [
                InlineKeyboardButton(
                    label,
                    callback_data=f"menu|rel_user|{relation_chat_id}|{user_id}",
                )
            ]
        )
    if not rows:
        rows.append([InlineKeyboardButton("Пока нет данных", callback_data="menu|noop")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu|settings")])
    return InlineKeyboardMarkup(rows)


def _relation_adjust_keyboard(*, relation_chat_id: int, user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("➖25", callback_data=f"menu|rel_adj|{relation_chat_id}|{user_id}|-25"),
                InlineKeyboardButton("➖10", callback_data=f"menu|rel_adj|{relation_chat_id}|{user_id}|-10"),
                InlineKeyboardButton("➖5", callback_data=f"menu|rel_adj|{relation_chat_id}|{user_id}|-5"),
            ],
            [
                InlineKeyboardButton("➕5", callback_data=f"menu|rel_adj|{relation_chat_id}|{user_id}|5"),
                InlineKeyboardButton("➕10", callback_data=f"menu|rel_adj|{relation_chat_id}|{user_id}|10"),
                InlineKeyboardButton("➕25", callback_data=f"menu|rel_adj|{relation_chat_id}|{user_id}|25"),
            ],
            [InlineKeyboardButton("↩️ Сбросить в 0", callback_data=f"menu|rel_set|{relation_chat_id}|{user_id}|0")],
            [InlineKeyboardButton("✍️ Ввести рейтинг вручную", callback_data=f"menu|rel_prompt|{relation_chat_id}|{user_id}")],
            [InlineKeyboardButton("⬅️ К списку людей", callback_data=f"menu|relations|{relation_chat_id}")],
        ]
    )


def _main_wish_keyboard(
    kind: str,
    mode: str,
    person_name: str,
    *,
    show_training_stop: bool = False,
) -> InlineKeyboardMarkup:
    code = _kind_code(kind)
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton("Еще вариант", callback_data=f"wish|regen|{code}")],
        [
            InlineKeyboardButton("✅ Понравилось всё", callback_data=f"wish|rate_all|{code}"),
        ],
        [
            InlineKeyboardButton("✨ Отметить хороший оборот", callback_data=f"wish|like_phrase|{code}"),
            InlineKeyboardButton("🙁 Не понравилась часть", callback_data=f"wish|dislike_part|{code}"),
        ],
        [
            InlineKeyboardButton(_mode_label(mode), callback_data=f"wish|toggle_mode|{code}"),
            InlineKeyboardButton(f"Кому: {_short_person(person_name)}", callback_data=f"wish|pick_person|{code}"),
        ],
    ]
    if show_training_stop:
        rows.append([InlineKeyboardButton("⏹ Остановить обучение", callback_data="menu|training_toggle")])
    return InlineKeyboardMarkup(rows)


def _after_good_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🏠 В меню", callback_data="menu|home")]])


def _person_select_keyboard(
    *,
    scope: str,
    kind_code: str,
    state: BotStateStore,
    current_person_id: int,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for person in state.list_personas():
        marker = "✅ " if person["id"] == current_person_id else ""
        rows.append(
            [
                InlineKeyboardButton(
                    f"{marker}{person['id']}: {person['name']}",
                    callback_data=f"{scope}|set_person|{kind_code}|{person['id']}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("Закрыть", callback_data="menu|noop")])
    return InlineKeyboardMarkup(rows)


def _person_delete_keyboard(state: BotStateStore) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for person in state.list_personas():
        if person["id"] == DEFAULT_PERSON_ID:
            continue
        rows.append(
            [
                InlineKeyboardButton(
                    f"Удалить {person['id']}: {person['name']}",
                    callback_data=f"menu|del_person|{person['id']}",
                )
            ]
        )
    if not rows:
        rows = [[InlineKeyboardButton("Список пуст", callback_data="menu|noop")]]
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu|settings")])
    return InlineKeyboardMarkup(rows)


def _set_pending_input(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    user_id: int,
    action: str,
    payload: dict | None = None,
) -> None:
    key = f"{chat_id}:{user_id}"
    record = {"action": action}
    if isinstance(payload, dict):
        record.update(payload)
    _pending_inputs(context)[key] = record


def _pop_pending_input(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    user_id: int,
) -> dict | None:
    key = f"{chat_id}:{user_id}"
    return _pending_inputs(context).pop(key, None)


def _store_generation_snapshot(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    message_id: int,
    generated: GeneratedWish,
    text_override: str | None = None,
) -> None:
    cache = _recent_generations(context)
    key = f"{chat_id}:{message_id}"
    payload = generated.to_record()
    if text_override is not None:
        payload["text"] = text_override
    cache[key] = payload
    while len(cache) > 5000:
        oldest_key = next(iter(cache))
        cache.pop(oldest_key, None)


def _find_generation_snapshot(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    message_id: int,
) -> dict | None:
    key = f"{chat_id}:{message_id}"
    return _recent_generations(context).get(key)


def _training_job_name(chat_id: int) -> str:
    return f"training_stream:{int(chat_id)}"


def _stop_training_job(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    name = _training_job_name(chat_id)
    for job in context.job_queue.get_jobs_by_name(name):
        job.schedule_removal()


def _ensure_training_job(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    name = _training_job_name(chat_id)
    existing = context.job_queue.get_jobs_by_name(name)
    if existing:
        return
    context.job_queue.run_repeating(
        callback=training_stream_tick,
        interval=TRAINING_STREAM_INTERVAL_SEC,
        first=1,
        name=name,
        data={"chat_id": int(chat_id)},
    )


def _strip_tg_emoji_tags(text: str) -> str:
    # If custom emoji IDs are invalid, Telegram rejects the whole message.
    return re.sub(r'<tg-emoji[^>]*>(.*?)</tg-emoji>', r"\1", text)


def _is_markup_error(exc: BadRequest) -> bool:
    msg = str(exc)
    lowered = msg.lower()
    return "document_invalid" in lowered or "can't parse entities" in lowered


async def _send_html_safe(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
    edit_message_id: int | None = None,
) -> tuple[object, str]:
    async def _send(text_to_send: str, *, use_html: bool) -> object:
        if edit_message_id:
            kwargs = {
                "chat_id": chat_id,
                "message_id": int(edit_message_id),
                "text": text_to_send,
                "reply_markup": reply_markup,
            }
            if use_html:
                kwargs["parse_mode"] = ParseMode.HTML
            msg = await context.bot.edit_message_text(**kwargs)
            return msg if hasattr(msg, "message_id") else _MessageRef(message_id=int(edit_message_id))
        if use_html:
            return await context.bot.send_message(
                chat_id=chat_id,
                text=text_to_send,
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
            )
        return await context.bot.send_message(
            chat_id=chat_id,
            text=text_to_send,
            reply_markup=reply_markup,
        )

    try:
        msg = await _send(text, use_html=True)
        return msg, text
    except BadRequest as exc:
        if not _is_markup_error(exc):
            raise

        safe_text = _strip_tg_emoji_tags(text)
        try:
            msg = await _send(safe_text, use_html=True)
            return msg, safe_text
        except BadRequest:
            msg = await _send(safe_text, use_html=False)
            return msg, safe_text


def _chat_person_mode(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> tuple[str, dict]:
    state = _get_state(context)
    prefs = state.get_chat_prefs(chat_id)
    mode = _mode_name(str(prefs.get("mode", "standard")))
    person_id = int(prefs.get("person_id", DEFAULT_PERSON_ID))
    person = state.get_person(person_id)
    if not person:
        person = state.get_person(DEFAULT_PERSON_ID) or {
            "id": DEFAULT_PERSON_ID,
            "name": "общий вариант",
            "instructions": "",
        }
    return mode, person


async def _send_wish(
    *,
    chat_id: int,
    kind: str,
    context: ContextTypes.DEFAULT_TYPE,
    source: str,
    mode_override: str | None = None,
    person_override: dict | None = None,
    edit_message_id: int | None = None,
    before_send: Callable[[], Awaitable[None]] | None = None,
) -> None:
    config = _get_config(context)
    model = _get_model(context)
    store = _get_store(context)
    state = _get_state(context)
    premium_ids = _effective_premium_ids(context)
    liked_emojis = _effective_liked_emojis(context)
    if person_override is not None:
        person = dict(person_override)
        mode = _mode_name(mode_override or "standard")
    else:
        mode, person = _chat_person_mode(context, chat_id)
        if mode_override:
            mode = _mode_name(mode_override)
    person_id = int(person.get("id", DEFAULT_PERSON_ID))
    person_name = str(person.get("name", "")).strip() if person_id != DEFAULT_PERSON_ID else ""
    person_instructions = str(person.get("instructions", "")).strip()
    blacklist = state.get_blacklist_phrases()
    favorite_phrases = state.get_favorite_phrases()
    recent_texts = _recent_chat_texts(store, chat_id, limit=80)
    group_id = _effective_group_chat_id(context)
    audience = "group" if int(chat_id) < 0 or (group_id and int(chat_id) == int(group_id)) else "single"

    def _pick_best_candidate(
        *,
        candidate_person_id: int,
        candidate_person_name: str,
        candidate_person_instructions: str,
    ) -> GeneratedWish | None:
        best: GeneratedWish | None = None
        best_rank = -10_000.0
        attempts = 64 if mode != "short" else 44

        for _ in range(attempts):
            candidate = generate_candidate(
                kind,
                mode=mode,
                audience=audience,
                person_id=candidate_person_id,
                person_name=candidate_person_name,
                person_instructions=candidate_person_instructions,
                premium_emoji_ids=premium_ids,
                extra_emojis=liked_emojis,
            )
            if _contains_blacklisted_phrase(candidate.text, blacklist):
                continue
            base_score = model.predict(candidate.features)
            similarity = _max_similarity_to_recent(candidate.text, recent_texts)
            novelty = 1.0 - similarity
            rank = (base_score * 0.63) + (novelty * 0.37)
            if _is_too_similar_to_recent(candidate.text, recent_texts, mode):
                rank -= 0.30
            if best is None or rank > best_rank:
                best = candidate
                best_rank = rank

        return best

    generated = _pick_best_candidate(
        candidate_person_id=person_id,
        candidate_person_name=person_name,
        candidate_person_instructions=person_instructions,
    )
    if generated is None:
        generated = _pick_best_candidate(
            candidate_person_id=DEFAULT_PERSON_ID,
            candidate_person_name="",
            candidate_person_instructions="",
        )
    if generated is None:
        generated = generate_candidate(
            kind,
            mode=mode,
            audience=audience,
            person_id=DEFAULT_PERSON_ID,
            person_name="",
            person_instructions="",
            premium_emoji_ids=premium_ids,
            extra_emojis=liked_emojis,
        )

    outgoing_text = generated.text
    openai_used = False
    if config.openai_enabled:
        try:
            openai_text = await asyncio.wait_for(
                generate_openai_wish(
                    cfg=_openai_runtime_cfg(config),
                    kind=kind,
                    mode=mode,
                    audience=audience,
                    person_name=person_name if audience == "single" else "",
                    person_instructions=person_instructions if audience == "single" else "",
                    blacklist=blacklist,
                    recent_texts=recent_texts[-12:],
                    preferred_emojis=liked_emojis[-40:],
                ),
                timeout=min(18.0, max(6.0, float(config.openai_timeout_sec))),
            )
            candidate_text = " ".join(openai_text.split())
            reject_reason = ""
            if not candidate_text:
                reject_reason = "empty"
            elif _looks_like_bad_language_output(candidate_text):
                reject_reason = "bad_language"
            elif _contains_blacklisted_phrase(candidate_text, blacklist):
                stripped = _strip_blacklisted_sentences(candidate_text, blacklist)
                if stripped and not _contains_blacklisted_phrase(stripped, blacklist):
                    candidate_text = stripped
                else:
                    reject_reason = "blacklist"

            if candidate_text and not reject_reason:
                similarity = _max_similarity_to_recent(candidate_text, recent_texts)
                if _is_too_similar_to_recent(candidate_text, recent_texts, mode):
                    logging.info(
                        "LLM text is similar to recent (%.3f), still using it",
                        similarity,
                    )
                openai_person_id = person_id if audience == "single" else 0
                openai_person_name = person_name if audience == "single" else ""
                generated = _openai_generated_wish(
                    kind=kind,
                    mode=mode,
                    audience=audience,
                    person_id=openai_person_id,
                    person_name=openai_person_name,
                    text=candidate_text,
                )
                outgoing_text = candidate_text
                openai_used = True
            elif reject_reason:
                logging.info(
                    "LLM text rejected reason=%s kind=%s mode=%s audience=%s",
                    reject_reason,
                    kind,
                    mode,
                    audience,
                )
        except OpenAIWishError as exc:
            logging.warning(
                "LLM wish generation failed endpoint=%s model=%s: %s",
                config.openai_base_url,
                config.openai_model,
                exc,
            )
        except asyncio.TimeoutError:
            logging.warning(
                "LLM wish generation timeout endpoint=%s model=%s",
                config.openai_base_url,
                config.openai_model,
            )
        except Exception as exc:
            logging.exception(
                "Unexpected LLM error endpoint=%s model=%s: %s",
                config.openai_base_url,
                config.openai_model,
                exc,
            )

    if (
        not openai_used
        and mode in {"standard", "context"}
        and favorite_phrases
        and audience != "group"
        and random.random() < 0.45
    ):
        compatible_favorites = [
            phrase
            for phrase in favorite_phrases[-40:]
            if _favorite_phrase_matches_kind(phrase, kind)
        ]
        if compatible_favorites:
            phrase = random.choice(compatible_favorites)
            candidate_text = _append_favorite_phrase(outgoing_text, phrase)
            if not _contains_blacklisted_phrase(candidate_text, blacklist):
                outgoing_text = candidate_text

    generation_engine = "openai" if openai_used else "local_patterns"

    # Hard safety: never send text that still matches blacklist.
    if _contains_blacklisted_phrase(outgoing_text, blacklist):
        logging.info(
            "Generated text matched blacklist, applying safety fallback kind=%s mode=%s audience=%s",
            kind,
            mode,
            audience,
        )
        replacement: GeneratedWish | None = None
        for _ in range(180):
            candidate = generate_candidate(
                kind,
                mode=mode,
                audience=audience,
                person_id=DEFAULT_PERSON_ID,
                person_name="",
                person_instructions="",
                premium_emoji_ids=premium_ids,
                extra_emojis=liked_emojis,
            )
            if _contains_blacklisted_phrase(candidate.text, blacklist):
                continue
            replacement = candidate
            break
        if replacement is not None:
            generated = replacement
            outgoing_text = replacement.text
            generation_engine = "local_safety_fallback"
        else:
            stripped = _strip_blacklisted_sentences(outgoing_text, blacklist)
            if stripped and not _contains_blacklisted_phrase(stripped, blacklist):
                outgoing_text = stripped
                generation_engine = "openai_stripped" if openai_used else "local_stripped"
            else:
                outgoing_text = _safe_fallback_text(kind, blacklist)
                generation_engine = "safe_fallback"

    logging.info(
        "Wish generation source=%s kind=%s mode=%s audience=%s chat_id=%s",
        generation_engine,
        kind,
        mode,
        audience,
        chat_id,
    )

    score = model.predict(generated.features)
    training_mode_enabled = state.is_chat_training_mode(chat_id)
    if before_send:
        try:
            await before_send()
        except Exception as exc:
            logging.warning("before_send hook failed: %s", exc)
    sent, sent_text = await _send_html_safe(
        context=context,
        chat_id=chat_id,
        text=outgoing_text,
        reply_markup=_main_wish_keyboard(
            kind,
            mode,
            person.get("name", "общий вариант"),
            show_training_stop=training_mode_enabled,
        ),
        edit_message_id=edit_message_id,
    )
    sent_message_id = int(getattr(sent, "message_id", edit_message_id or 0))
    if not sent_message_id:
        logging.warning("Cannot resolve message_id for generation snapshot chat_id=%s", chat_id)
        return
    _log_outgoing_message(
        context=context,
        chat_id=chat_id,
        chat_type="group" if int(chat_id) < 0 else "private",
        message_id=sent_message_id,
        text=sent_text,
        source=f"wish_{source}",
        reply_to_message_id=0,
        peer_user_id=0,
    )
    if training_mode_enabled and source in {"training_stream", "inline_regenerate", "feedback_bad_text", "inline_person_switch"}:
        _set_training_waiting(context, chat_id=chat_id, waiting=True, message_id=sent_message_id)
    _store_generation_snapshot(
        context,
        chat_id=chat_id,
        message_id=sent_message_id,
        generated=generated,
        text_override=sent_text,
    )
    store.record_generation(
        kind=kind,
        source=source,
        engine=generation_engine,
        chat_id=chat_id,
        message_id=sent_message_id,
        text=sent_text,
        mode=generated.mode,
        person_id=generated.person_id,
        person_name=generated.person_name,
        emoji_count=generated.emoji_count,
        template_idx=generated.template_idx,
        picks=generated.picks,
        score=score,
    )


async def _animate_generating_status(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    stop_event: asyncio.Event,
) -> None:
    frames = ("Генерирую", "Генерирую.", "Генерирую..", "Генерирую...")
    frame_index = 0
    while not stop_event.is_set():
        frame = frames[frame_index % len(frames)]
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=frame,
            )
        except TelegramError:
            return
        frame_index += 1
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=0.65)
        except asyncio.TimeoutError:
            continue


async def _send_wish_with_progress(
    *,
    chat_id: int,
    kind: str,
    context: ContextTypes.DEFAULT_TYPE,
    source: str,
    mode_override: str | None = None,
    person_override: dict | None = None,
) -> None:
    status = await context.bot.send_message(chat_id=chat_id, text="Генерирую")
    stop_event = asyncio.Event()
    animation_task = asyncio.create_task(
        _animate_generating_status(
            context=context,
            chat_id=chat_id,
            message_id=status.message_id,
            stop_event=stop_event,
        )
    )

    async def _before_send() -> None:
        stop_event.set()
        try:
            await animation_task
        except Exception:
            pass

    try:
        await _send_wish(
            chat_id=chat_id,
            kind=kind,
            context=context,
            source=source,
            mode_override=mode_override,
            person_override=person_override,
            edit_message_id=status.message_id,
            before_send=_before_send,
        )
    except Exception:
        stop_event.set()
        try:
            await animation_task
        except Exception:
            pass
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=status.message_id,
                text="Не смог сгенерировать, попробуй еще раз.",
            )
        except TelegramError:
            pass
        raise


def _home_markup_for(update: Update, context: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    state = _get_state(context)
    chat_id = update.effective_chat.id if update.effective_chat else 0
    is_admin = _is_admin(update, context)
    training_mode = state.is_chat_training_mode(chat_id) if (is_admin and chat_id) else False
    return _home_inline_keyboard(is_admin=is_admin, training_mode=training_mode)


async def _show_home(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message:
        await update.effective_message.reply_text(
            "Выбирай действие:",
            reply_markup=_home_markup_for(update, context),
        )


async def _show_settings(*, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    config = _get_config(context)
    state = _get_state(context)
    mode, person = _chat_person_mode(context, chat_id)
    schedule_mode = state.get_schedule_mode()
    admin_id = _effective_admin_user_id(context)
    group_id = _effective_group_chat_id(context)
    group_line = str(group_id) if group_id else "не задана"
    admin_only_mode = state.is_admin_only_mode()
    exceptions_count = len(state.list_access_exceptions())
    liked_count = len(state.get_liked_emojis())
    premium_count = len(_effective_premium_ids(context))
    favorite_count = len(state.get_favorite_phrases())
    blacklist_count = len(state.get_blacklist_phrases())
    group_reaction_mode = state.is_group_fire_reaction_mode()
    group_chat_mode = state.is_group_chat_mode()
    public_private_chat_mode = state.is_public_private_chat_mode()
    group_activity_mode = state.get_group_activity_mode()
    style_examples_count = len(state.get_style_examples())
    social_mode = state.get_social_mode()
    roast_words_count = len(state.get_roast_words())
    relations_count = len(state.list_chat_relations(chat_id=group_id or chat_id, limit=9999))
    log_rows_count, log_users_count = _chat_log_quick_stats(context)
    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            f"Настройки чата:\n"
            f"- режим: {_mode_ru(mode)}\n"
            f"- режим авто-рассылки: {_mode_ru(schedule_mode)}\n"
            f"- человек: {person.get('name', 'общий вариант')}\n"
            f"- доступ только админ/исключения: {'ВКЛ' if admin_only_mode else 'ВЫКЛ'}\n"
            f"- исключений: {exceptions_count}\n"
            f"- ID админа: {admin_id or 'не задан'}\n"
            f"- группа для рассылки: {group_line}\n"
            f"- сохраненных обычных эмодзи: {liked_count}\n"
            f"- сохраненных премиум-эмодзи (ID): {premium_count}\n"
            f"- любимых оборотов: {favorite_count}\n"
            f"- фраз в черном списке: {blacklist_count}\n"
            f"- 🔥 реакции на voice/video в группе: {_on_off_ru(group_reaction_mode)}\n"
            f"- 💬 режим общения в группе: {_on_off_ru(group_chat_mode)}\n"
            f"- 🗨 личный чат для всех: {_on_off_ru(public_private_chat_mode)}\n"
            f"- 🎚 активность ответов: {_group_activity_ru(group_activity_mode)}\n"
            f"- 🧠 социальный режим: {_social_mode_ru(social_mode)}\n"
            f"- 🗯 дружеских подколов: {roast_words_count}\n"
            f"- 👤 людей в рейтинге: {relations_count}\n"
            f"- примеров стиля из экспорта: {style_examples_count}\n"
            f"- записей в логе переписки: {log_rows_count}\n"
            f"- пользователей в логе: {log_users_count}\n"
            f"- GPT генерация: {'ВКЛ' if config.openai_enabled else 'ВЫКЛ'}\n"
            f"- GPT модель: {config.openai_model}\n"
            f"- GPT endpoint: {config.openai_base_url}"
        ),
        reply_markup=_settings_markup_for_chat(
            context=context,
            chat_id=chat_id,
            mode=mode,
            person_name=person.get("name", "общий вариант"),
        ),
    )


async def _send_modes_help(*, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    state = _get_state(context)
    mode, person = _chat_person_mode(context, chat_id)
    schedule_mode = state.get_schedule_mode()
    text = (
        "Режимы генерации:\n"
        "- коротко: только «доброе утро/спокойной ночи» + 4-8 эмодзи.\n"
        "- стандарт (с пожеланием): полноценное теплое пожелание + 4-8 эмодзи.\n\n"
        "- по смыслу: эмодзи ставятся внутри текста по контексту (звезды, солнце, уют и т.д.).\n\n"
        f"Сейчас в этом чате: {_mode_ru(mode)}.\n"
        f"Сейчас для авто-рассылки: {_mode_ru(schedule_mode)}.\n"
        f"Текущий профиль «кому»: {person.get('name', 'общий вариант')}.\n\n"
        "Отправка в несколько мест:\n"
        "- «В личку+группу» отправляет сразу в твой личный чат и в группу.\n"
        "- «Подруга/Друг» переключают профиль текста.\n\n"
        "Фидбек:\n"
        "- «Отметить хороший оборот» — отправляешь удачную фразу.\n"
        "- «Не понравилась часть» — отправляешь неудачный кусок, он уходит в черный список.\n\n"
        "Группа:\n"
        "- «🔥 Реакции на voice/video» — бот ставит огонек на голосовые и видео.\n"
        "- «💬 Общение в группе» — бот периодически отвечает в беседе.\n"
        "- «🗨 Личка для всех» — неадмины могут просто болтать с ботом в личке.\n"
        "- «🎚 Активность» — тихо / норм / каждое сообщение / только вопрос.\n"
        "- «📥 Импорт стиля общения» — загрузи JSON/TXT/HTML экспорт, чтобы бот писал ближе к твоему стилю.\n"
        "- «🧠 Соц режим»:\n"
        "  самообучение — учитывает рейтинг человека и прошлое общение,\n"
        "  как я — сильнее копирует твой стиль по экспорту.\n"
        "- «🗯 Подколы/обзывалки» — список слов для дружеского стеба."
    )
    await context.bot.send_message(chat_id=chat_id, text=text)


async def _send_stats(*, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    store = _get_store(context)
    model = _get_model(context)
    state = _get_state(context)
    config = _get_config(context)

    summary = store.summary()
    progress = store.training_progress(config.training_target_samples)

    generated = summary.get("generated", {})
    feedback = summary.get("feedback", {})
    reasons = summary.get("reasons", {})
    prefs = state.get_chat_prefs(chat_id)
    person = state.get_person(int(prefs.get("person_id", DEFAULT_PERSON_ID))) or {"name": "общий вариант"}
    admin_id = _effective_admin_user_id(context)
    group_id = _effective_group_chat_id(context)
    admin_only_mode = state.is_admin_only_mode()
    exceptions_count = len(state.list_access_exceptions())
    liked_count = len(state.get_liked_emojis())
    schedule_mode = state.get_schedule_mode()
    favorite_count = len(state.get_favorite_phrases())
    blacklist_count = len(state.get_blacklist_phrases())
    group_reaction_mode = state.is_group_fire_reaction_mode()
    group_chat_mode = state.is_group_chat_mode()
    public_private_chat_mode = state.is_public_private_chat_mode()
    group_activity_mode = state.get_group_activity_mode()
    style_examples_count = len(state.get_style_examples())
    social_mode = state.get_social_mode()
    roast_words_count = len(state.get_roast_words())
    relation_chat_id = group_id or chat_id
    relations = state.list_chat_relations(chat_id=relation_chat_id, limit=500)
    log_rows_count, log_users_count = _chat_log_quick_stats(context)
    recent_generations = store.recent_generations(limit=4000)
    openai_generated = 0
    local_generated = 0
    for row in recent_generations:
        engine = str(row.get("engine", "")).strip().lower()
        picks = row.get("picks", {})
        template_idx = int(row.get("template_idx", 0)) if str(row.get("template_idx", "")).lstrip("-").isdigit() else 0
        is_openai = engine.startswith("openai")
        if not is_openai and isinstance(picks, dict):
            is_openai = int(picks.get("openai", 0)) > 0
        if not is_openai and template_idx == -2:
            is_openai = True
        if is_openai:
            openai_generated += 1
        else:
            local_generated += 1

    def _top_reason_lines(bucket: dict[str, int]) -> str:
        if not bucket:
            return "нет данных"
        pairs = sorted(bucket.items(), key=lambda x: x[1], reverse=True)
        return ", ".join(f"{name}: {count}" for name, count in pairs[:5])

    def _top_relations_lines() -> str:
        if not relations:
            return "нет данных"
        lines: list[str] = []
        for row in relations[:10]:
            score = int(row.get("score", 0))
            status = _relation_status_ru(str(row.get("status", "neutral")))
            name = _relation_display_name(row)
            lines.append(f"{name}: {score:+d} ({status})")
        return "\n".join(lines)

    text = (
        "Статистика:\n"
        f"- отправлено пожеланий на ночь: {generated.get('night', 0)}\n"
        f"- отправлено пожеланий утром: {generated.get('morning', 0)}\n"
        f"- из нейросети: {openai_generated}\n"
        f"- из локальных паттернов: {local_generated}\n"
        f"- оценок «хорошо»: {feedback.get('good', 0)}\n"
        f"- оценок «плохо»: {feedback.get('bad', 0)}\n"
        f"- частые причины «хорошо»: {_top_reason_lines(reasons.get('good', {}))}\n"
        f"- частые причины «плохо»: {_top_reason_lines(reasons.get('bad', {}))}\n"
        "\nПрогресс обучения:\n"
        f"- обучающих примеров: {int(progress['samples'])}/{int(progress['target_samples'])}\n"
        f"- прогресс до цели: {progress['progress_percent']:.1f}%\n"
        f"- точность направления оценок: {progress['accuracy'] * 100:.1f}%\n"
        f"- средняя уверенность: {progress['avg_confidence'] * 100:.1f}%\n"
        f"- средний балл у «хорошо»: {progress['good_avg_score']:.3f}\n"
        f"- средний балл у «плохо»: {progress['bad_avg_score']:.3f}\n"
        f"- модель: {model.model_info()}\n"
        "\nНастройки чата:\n"
        f"- режим: {_mode_ru(str(prefs.get('mode', 'standard')))}\n"
        f"- режим авто-рассылки: {_mode_ru(schedule_mode)}\n"
        f"- выбранный человек: {person.get('name', 'общий вариант')}\n"
        f"- всего профилей людей: {len(state.list_personas())}\n"
        f"- сохраненных обычных эмодзи: {liked_count}\n"
        f"- активных премиум-эмодзи (ID): {len(_effective_premium_ids(context))}\n"
        f"- любимых оборотов: {favorite_count}\n"
        f"- фраз в черном списке: {blacklist_count}\n"
        f"- 🔥 реакции на voice/video: {_on_off_ru(group_reaction_mode)}\n"
        f"- 💬 режим общения в группе: {_on_off_ru(group_chat_mode)}\n"
        f"- 🗨 личный чат для всех: {_on_off_ru(public_private_chat_mode)}\n"
        f"- 🎚 активность ответов: {_group_activity_ru(group_activity_mode)}\n"
        f"- 🧠 социальный режим: {_social_mode_ru(social_mode)}\n"
        f"- 🗯 подколов/обзывалок: {roast_words_count}\n"
        f"- примеров стиля из экспорта: {style_examples_count}\n"
        f"- записей в логе переписки: {log_rows_count}\n"
        f"- пользователей в логе: {log_users_count}\n"
        f"- режим «только админ»: {_on_off_ru(admin_only_mode)}\n"
        f"- пользователей в исключениях: {exceptions_count}\n"
        f"- текущий ID админа: {admin_id or 'не задан'}\n"
        f"- текущий ID группы: {group_id or 'не задан'}\n"
        f"- GPT генерация: {_on_off_ru(config.openai_enabled)}\n"
        f"- GPT модель: {config.openai_model}\n"
        "\nРейтинг контактов:\n"
        f"- чат рейтинга: {relation_chat_id}\n"
        f"- людей в базе: {len(relations)}\n"
        f"{_top_relations_lines()}"
    )
    await context.bot.send_message(chat_id=chat_id, text=text)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if _effective_admin_user_id(context):
        if not await _ensure_access(update, context):
            return
    else:
        if not await _ensure_admin(update, context):
            return

    config = _get_config(context)
    admin_only_mode = _get_state(context).is_admin_only_mode()
    admin_id = _effective_admin_user_id(context)
    group_id = _effective_group_chat_id(context)
    state = _get_state(context)
    chat_id = update.effective_chat.id if update.effective_chat else 0
    schedule_mode = state.get_schedule_mode()
    training_mode = state.is_chat_training_mode(chat_id) if chat_id else False
    night_short = estimated_unique_texts("night", mode="short")
    night_std = estimated_unique_texts("night", mode="standard")
    night_ctx = estimated_unique_texts("night", mode="context")
    morning_short = estimated_unique_texts("morning", mode="short")
    morning_std = estimated_unique_texts("morning", mode="standard")
    morning_ctx = estimated_unique_texts("morning", mode="context")

    text = (
        "Готово. Теперь работа только кнопками.\n\n"
        "Авто-рассылка:\n"
        f"- спокойной ночи: 23:10 ({config.timezone.key})\n"
        f"- доброе утро: 07:00 ({config.timezone.key})\n"
        f"- режим авто-рассылки: {_mode_ru(schedule_mode)}\n\n"
        "Вариативность:\n"
        f"- ночь (коротко): {night_short}\n"
        f"- ночь (стандарт): {night_std}\n"
        f"- ночь (по смыслу): {night_ctx}\n"
        f"- утро (коротко): {morning_short}\n"
        f"- утро (стандарт): {morning_std}\n"
        f"- утро (по смыслу): {morning_ctx}\n"
        "\n"
        "Текущая автонастройка:\n"
        f"- ID админа: {admin_id or 'не задан'}\n"
        f"- ID группы: {group_id or 'не задан'}\n"
        f"- режим «только админ»: {_on_off_ru(admin_only_mode)}\n"
        f"- режим обучения в этом чате: {_on_off_ru(training_mode)}\n"
        f"- GPT генерация: {_on_off_ru(config.openai_enabled)}\n"
        f"- GPT модель: {config.openai_model}"
    )
    if _is_public_private_chat_user(update, context):
        await update.effective_message.reply_text(
            "Привет. Можем общаться в обычном режиме.\n"
            "Я запоминаю контекст и стиль диалога."
        )
        return
    await update.effective_message.reply_text(text)
    await _show_home(update, context)


async def audit_message_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message:
        return
    _log_incoming_message(update, context)


async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message or not chat or not user:
        return

    if chat.type != "private":
        _get_state(context).set_runtime_group_chat_id(chat.id)
        return

    if not await _ensure_access(update, context):
        return

    pending = _pop_pending_input(context, chat_id=chat.id, user_id=user.id)
    text = (message.text or "").strip()
    if pending:
        action = pending.get("action")
        state = _get_state(context)
        if action == "import_style_examples":
            if not await _ensure_admin(update, context):
                return
            examples = _extract_style_examples_from_text(text)
            if not examples:
                _set_pending_input(context, chat_id=chat.id, user_id=user.id, action="import_style_examples")
                await message.reply_text(
                    "Не вижу подходящего текста.\n"
                    "Отправь JSON/TXT файлом или вставь текст (по одной фразе на строку)."
                )
                return
            count = state.set_style_examples(examples)
            await message.reply_text(
                f"Готово. Загрузил примеров стиля: {count}.\n"
                "Теперь можно включить «💬 Общение в группе» в настройках."
            )
            return
        if action == "add_person":
            if not await _ensure_admin(update, context):
                return
            if "|" not in text:
                _set_pending_input(context, chat_id=chat.id, user_id=user.id, action="add_person")
                await message.reply_text("Формат: Имя | инструкция\nПример: Алина | пиши тепло и с чаем.")
                return
            name, instructions = [part.strip() for part in text.split("|", maxsplit=1)]
            if not name:
                _set_pending_input(context, chat_id=chat.id, user_id=user.id, action="add_person")
                await message.reply_text("Имя пустое. Попробуй еще раз.")
                return
            if len(name) > 40:
                _set_pending_input(context, chat_id=chat.id, user_id=user.id, action="add_person")
                await message.reply_text("Имя слишком длинное (до 40 символов).")
                return
            if len(instructions) > 260:
                _set_pending_input(context, chat_id=chat.id, user_id=user.id, action="add_person")
                await message.reply_text("Инструкция слишком длинная (до 260 символов).")
                return
            person = state.add_person(name=name, instructions=instructions)
            await message.reply_text(
                f"Добавлено: {person['id']} - {person['name']}.\n"
                "Теперь можно выбрать через кнопку 'Кому: ...'.",
            )
            return

        if action == "add_exception_id":
            if not await _ensure_admin(update, context):
                return
            raw_id = re.findall(r"-?\d+", text)
            if not raw_id:
                _set_pending_input(context, chat_id=chat.id, user_id=user.id, action="add_exception_id")
                await message.reply_text("Не вижу ID. Отправь только цифры ID пользователя.")
                return
            target_user_id = int(raw_id[0])
            if target_user_id == _effective_admin_user_id(context):
                await message.reply_text("Это админ. Он и так имеет доступ.")
                return

            first_name = ""
            last_name = ""
            try:
                info = await context.bot.get_chat(target_user_id)
                first_name = str(getattr(info, "first_name", "") or "").strip()
                last_name = str(getattr(info, "last_name", "") or "").strip()
            except Exception:
                _set_pending_input(context, chat_id=chat.id, user_id=user.id, action="add_exception_id")
                await message.reply_text(
                    "Не смог получить имя и фамилию по этому ID.\n"
                    "Пусть пользователь сначала напишет боту или появится в общей группе с ботом, затем повтори."
                )
                return

            if not first_name:
                first_name = f"user_{target_user_id}"
            state.add_access_exception(target_user_id, first_name=first_name, last_name=last_name)
            await message.reply_text(
                f"Добавил исключение: {first_name} {last_name}".strip(),
            )
            return

        if action == "roast_add":
            if not await _ensure_admin(update, context):
                return
            raw_parts = re.split(r"[,;\n]+", text)
            words = [" ".join(part.split()) for part in raw_parts if " ".join(part.split())]
            if not words:
                _set_pending_input(context, chat_id=chat.id, user_id=user.id, action="roast_add")
                await message.reply_text("Пришли 1+ слова/фразы через запятую или с новой строки.")
                return
            added = state.add_roast_words(words)
            await message.reply_text(
                f"Добавил слов: {added}. Всего в списке: {len(state.get_roast_words())}.",
                reply_markup=_roast_keyboard(state),
            )
            return

        if action == "set_relation_score":
            if not await _ensure_admin(update, context):
                return
            relation_chat_id = int(pending.get("relation_chat_id", 0) or 0)
            target_user_id = int(pending.get("target_user_id", 0) or 0)
            if not relation_chat_id or not target_user_id:
                await message.reply_text("Не смог понять кого обновлять. Открой карточку заново.")
                return
            numbers = re.findall(r"-?\d+", text)
            if not numbers:
                _set_pending_input(
                    context,
                    chat_id=chat.id,
                    user_id=user.id,
                    action="set_relation_score",
                    payload={"relation_chat_id": relation_chat_id, "target_user_id": target_user_id},
                )
                await message.reply_text("Нужен числовой рейтинг от -100 до 100. Пример: 35 | за активную помощь")
                return
            score = _safe_relation_score(context, target_user_id, int(numbers[0]))
            reason = text
            relation = state.set_relation_score(
                chat_id=relation_chat_id,
                user_id=target_user_id,
                score=score,
                reason=reason or "админ вручную",
            )
            await message.reply_text(
                f"Рейтинг обновлен: {_relation_display_name(relation)} -> {int(relation.get('score', 0)):+d} "
                f"({_relation_status_ru(str(relation.get('status', 'neutral')))})"
            )
            return

        if action == "set_premium_ids":
            await message.reply_text(
                "Ручной ввод ID отключен.\n"
                "Просто отправь эмодзи сообщением, бот сам запомнит их."
            )
            return
        if action == "good_snippet":
            raw_kind = str(pending.get("kind", "n"))
            if raw_kind in {"n", "m"}:
                kind = _kind_from_code(raw_kind)
            else:
                kind = raw_kind if raw_kind in {"night", "morning"} else "night"
            source_message_id = int(pending.get("source_message_id", message.message_id))
            snippet = " ".join(text.split())

            snapshot = _find_generation_snapshot(
                context,
                chat_id=chat.id,
                message_id=source_message_id,
            )
            store = _get_store(context)
            store.record_feedback(
                kind=kind,
                rating="good",
                reason="очень хороший оборот речи",
                user_id=user.id,
                chat_id=chat.id,
                source_message_id=source_message_id,
                text=snapshot["text"] if snapshot else None,
            )
            if snapshot and isinstance(snapshot.get("features"), dict):
                model = _get_model(context)
                model.train(snapshot["features"], target=1.0, epochs=14)
                model.save(_get_config(context).model_path)

            if snippet and snippet not in {"-", "нет", "пропуск", "skip"}:
                added = state.add_favorite_phrase(snippet)
                if added:
                    await message.reply_text("Сохранил оборот как очень удачный. Буду чаще брать похожий стиль.")
                else:
                    await message.reply_text("Этот оборот уже есть в списке удачных.")
            else:
                await message.reply_text("Принял оценку «нравится».")

            _set_training_waiting(context, chat_id=chat.id, waiting=False)
            await message.reply_text("Спасибо за фидбек.", reply_markup=_after_good_keyboard())
            return
        if action == "bad_snippet":
            raw_kind = str(pending.get("kind", "n"))
            if raw_kind in {"n", "m"}:
                kind = _kind_from_code(raw_kind)
            else:
                kind = raw_kind if raw_kind in {"night", "morning"} else "night"
            source_message_id = int(pending.get("source_message_id", message.message_id))
            snippet = " ".join(text.split())

            snapshot = _find_generation_snapshot(
                context,
                chat_id=chat.id,
                message_id=source_message_id,
            )
            store = _get_store(context)
            store.record_feedback(
                kind=kind,
                rating="bad",
                reason="не зашла часть текста",
                user_id=user.id,
                chat_id=chat.id,
                source_message_id=source_message_id,
                text=snapshot["text"] if snapshot else None,
            )
            if snapshot and isinstance(snapshot.get("features"), dict):
                model = _get_model(context)
                model.train(snapshot["features"], target=0.0, epochs=14)
                model.save(_get_config(context).model_path)

            if snippet and snippet not in {"-", "нет", "пропуск", "skip"}:
                added = state.add_blacklist_phrase(snippet)
                if added:
                    await message.reply_text(
                        "Добавил фразу в черный список.\n"
                        f"{_snippet_hint(snippet)}"
                    )
                else:
                    await message.reply_text("Эта фраза уже есть в черном списке.")
            else:
                await message.reply_text("Пропустили добавление в черный список.")

            _set_training_waiting(context, chat_id=chat.id, waiting=False)
            await _send_wish(chat_id=chat.id, kind=kind, context=context, source="feedback_bad_text")
            return

    if _is_public_private_chat_user(update, context):
        state = _get_state(context)
        incoming_text = text or _message_text_or_caption(message) or "[без текста]"
        user_name = str(getattr(user, "first_name", "") or getattr(user, "username", "") or "").strip()
        _append_group_dialogue(context, chat_id=chat.id, line=f"{user_name or 'Пользователь'}: {incoming_text}")

        relation = state.get_or_create_relation(
            chat_id=chat.id,
            user_id=user.id,
            first_name=str(getattr(user, "first_name", "") or ""),
            last_name=str(getattr(user, "last_name", "") or ""),
            username=str(getattr(user, "username", "") or ""),
        )
        relation = _ensure_admin_relation_floor(
            context=context,
            state=state,
            chat_id=chat.id,
            user_id=user.id,
        ) or relation

        delta, reason = _relation_signal(incoming_text)
        safe_delta = _safe_relation_delta(context, user.id, delta)
        if safe_delta:
            relation = state.adjust_relation_score(
                chat_id=chat.id,
                user_id=user.id,
                delta=safe_delta,
                reason=reason or "личный чат",
                text=incoming_text,
                first_name=str(getattr(user, "first_name", "") or ""),
                last_name=str(getattr(user, "last_name", "") or ""),
                username=str(getattr(user, "username", "") or ""),
            )
            relation = _ensure_admin_relation_floor(
                context=context,
                state=state,
                chat_id=chat.id,
                user_id=user.id,
            ) or relation

        if _is_forgive_request(incoming_text):
            reply_text = _forgive_reply_for_relation(relation=relation, user_name="")
            sent = await message.reply_text(reply_text)
            _append_group_dialogue(context, chat_id=chat.id, line=f"Бот: {reply_text}")
            _log_outgoing_message(
                context=context,
                chat_id=chat.id,
                chat_type="private",
                message_id=int(getattr(sent, "message_id", 0) or 0),
                text=reply_text,
                source="private_forgive_reply",
                reply_to_message_id=message.message_id,
                peer_user_id=user.id,
            )
            return

        reply_text, source = await _generate_private_chat_reply(
            context=context,
            chat_id=chat.id,
            incoming_text=incoming_text,
            user_name=user_name,
            relation=relation,
        )
        sent = await message.reply_text(reply_text)
        _append_group_dialogue(context, chat_id=chat.id, line=f"Бот: {reply_text}")
        _log_outgoing_message(
            context=context,
            chat_id=chat.id,
            chat_type="private",
            message_id=int(getattr(sent, "message_id", 0) or 0),
            text=reply_text,
            source=f"private_chat_{source}",
            reply_to_message_id=message.message_id,
            peer_user_id=user.id,
        )
        return

    query_text = _extract_v_query(text)
    if query_text:
        answer = await _answer_v_query(query_text)
        sent = await message.reply_text(answer)
        _log_outgoing_message(
            context=context,
            chat_id=chat.id,
            chat_type="private",
            message_id=int(getattr(sent, "message_id", 0) or 0),
            text=answer,
            source="private_v_query",
            reply_to_message_id=message.message_id,
            peer_user_id=user.id,
        )
        return

    state = _get_state(context)
    unicode_emojis = _extract_unicode_emojis(text)
    custom_emoji_ids = _extract_custom_emoji_ids(message)

    added_unicode = 0
    added_custom = 0
    if unicode_emojis:
        added_unicode = state.add_liked_emojis(unicode_emojis)
    if custom_emoji_ids:
        added_custom = state.add_premium_emoji_ids(
            custom_emoji_ids,
            default_ids=_get_config(context).base_premium_emoji_ids,
        )

    if unicode_emojis or custom_emoji_ids:
        total_unicode = len(state.get_liked_emojis())
        total_custom = len(_effective_premium_ids(context))
        lines = ["Сохранил эмодзи для генерации пожеланий."]
        if unicode_emojis:
            if added_unicode:
                lines.append(f"- обычные эмодзи: +{added_unicode} (всего {total_unicode})")
            else:
                lines.append(f"- обычные эмодзи: уже были сохранены (всего {total_unicode})")
        if custom_emoji_ids:
            if added_custom:
                lines.append(f"- премиум-эмодзи по ID: +{added_custom} (всего {total_custom})")
            else:
                lines.append(f"- премиум-эмодзи по ID: уже были сохранены (всего {total_custom})")
        lines.append("Буду подмешивать их в новые тексты.")
        await message.reply_text("\n".join(lines))
        return
    await message.reply_text("Используй inline-кнопки из меню. Нажми /start если меню потерялось.")


async def _set_message_reaction(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    reaction: list[dict[str, str]],
    is_big: bool = False,
) -> bool:
    try:
        await context.bot._post(
            "setMessageReaction",
            data={
                "chat_id": int(chat_id),
                "message_id": int(message_id),
                "reaction": reaction,
                "is_big": bool(is_big),
            },
        )
        return True
    except Exception:
        return False


def _next_wish_reaction_payload(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
) -> list[dict[str, str]]:
    key = str(int(chat_id))
    cursor = _reaction_cursor(context)
    idx = int(cursor.get(key, 0))
    cursor[key] = idx + 1

    premium_ids = [x.strip() for x in _effective_premium_ids(context) if str(x).strip()]
    use_premium = bool(premium_ids) and (idx % 2 == 1)
    if use_premium:
        premium_id = premium_ids[idx % len(premium_ids)]
        return [{"type": "custom_emoji", "custom_emoji_id": premium_id}]

    emoji = WISH_HEART_EMOJIS[idx % len(WISH_HEART_EMOJIS)]
    return [{"type": "emoji", "emoji": emoji}]


async def _set_fire_reaction(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
) -> None:
    ok = await _set_message_reaction(
        context=context,
        chat_id=chat_id,
        message_id=message_id,
        reaction=[{"type": "emoji", "emoji": GROUP_REACTION_EMOJI}],
        is_big=False,
    )
    if not ok:
        logging.debug("Cannot set fire reaction chat=%s message=%s", chat_id, message_id)


async def _set_wish_heart_reaction(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
) -> None:
    payload = _next_wish_reaction_payload(context=context, chat_id=chat_id)
    ok = await _set_message_reaction(
        context=context,
        chat_id=chat_id,
        message_id=message_id,
        reaction=payload,
        is_big=False,
    )
    if ok:
        return
    fallback = [{"type": "emoji", "emoji": "❤️"}]
    await _set_message_reaction(
        context=context,
        chat_id=chat_id,
        message_id=message_id,
        reaction=fallback,
        is_big=False,
    )


async def _set_social_relation_reaction(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    delta: int,
) -> None:
    pool = SOCIAL_WARM_REACTIONS if int(delta) > 0 else SOCIAL_COLD_REACTIONS
    emoji = random.choice(pool)
    await _set_message_reaction(
        context=context,
        chat_id=chat_id,
        message_id=message_id,
        reaction=[{"type": "emoji", "emoji": emoji}],
        is_big=False,
    )


def _pick_contextual_reaction_emoji(
    *,
    text: str,
    relation_score: int,
    delta: int,
) -> str | None:
    plain = _plain_text_for_blacklist(text)
    if not plain:
        return None

    if any(marker in plain for marker in THANKS_MARKERS):
        return random.choice(("❤️", "🫶", "👏", "✨"))
    if any(marker in plain for marker in LAUGH_MARKERS):
        return random.choice(GROUP_CONTEXT_FUN_REACTIONS)
    if _looks_like_question(text):
        return random.choice(("🤔", "🧠", "👀"))

    if int(delta) <= -8 or int(relation_score) <= -45:
        return random.choice(GROUP_CONTEXT_NEGATIVE_REACTIONS)
    if int(delta) >= 4 or int(relation_score) >= 24:
        return random.choice(GROUP_CONTEXT_POSITIVE_REACTIONS)
    return random.choice(GROUP_CONTEXT_NEUTRAL_REACTIONS)


async def _maybe_set_contextual_group_reaction(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    text: str,
    relation_score: int,
    delta: int,
    is_wish_text: bool,
) -> None:
    if is_wish_text:
        return

    plain = _plain_text_for_blacklist(text)
    if not plain:
        return

    base_chance = 0.12
    if _looks_like_question(text):
        base_chance += 0.14
    if abs(int(delta)) >= 8:
        base_chance += 0.28
    if int(relation_score) <= -45 or int(relation_score) >= 30:
        base_chance += 0.18
    if any(marker in plain for marker in THANKS_MARKERS):
        base_chance += 0.15
    if any(marker in plain for marker in LAUGH_MARKERS):
        base_chance += 0.15
    if random.random() > min(0.88, base_chance):
        return

    emoji = _pick_contextual_reaction_emoji(
        text=text,
        relation_score=relation_score,
        delta=delta,
    )
    if not emoji:
        return
    await _set_message_reaction(
        context=context,
        chat_id=chat_id,
        message_id=message_id,
        reaction=[{"type": "emoji", "emoji": emoji}],
        is_big=False,
    )


def _is_group_voice_or_video(message: object) -> bool:
    return bool(
        getattr(message, "voice", None)
        or getattr(message, "video_note", None)
        or getattr(message, "video", None)
    )


def _is_direct_group_addressing(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    message: object,
    text: str,
) -> bool:
    lower = str(text or "").lower()
    bot_username = str(getattr(context.bot, "username", "") or "").strip().lower()
    if bot_username and f"@{bot_username}" in lower:
        return True
    reply_to = getattr(message, "reply_to_message", None)
    bot_id = int(getattr(context.bot, "id", 0) or 0)
    return bool(reply_to and getattr(reply_to, "from_user", None) and int(getattr(reply_to.from_user, "id", 0) or 0) == bot_id)


def _looks_like_question(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    if "?" in lowered:
        return True
    starters = ("как ", "почему ", "зачем ", "когда ", "где ", "кто ", "что ", "можно ", "стоит ")
    return lowered.startswith(starters)


def _detect_group_wish_kind(text: str) -> str | None:
    plain = _plain_text_for_blacklist(text)
    if not plain:
        return None
    if "доброе утро" in plain or "с добрым утром" in plain:
        return "morning"
    if "спокойной ночи" in plain or "доброй ночи" in plain or "сладких снов" in plain:
        return "night"
    return None


def _group_special_wish_reply(*, kind: str, user_name: str) -> str:
    morning = [
        "Доброе утро! Пусть день будет лёгким ☀️",
        "С добрым утром! Пусть сегодня всё сложится 🌤️",
        "Доброе утро, пусть настроение держится весь день ✨",
    ]
    night = [
        "Спокойной ночи! Пусть сон будет крепким 🌙",
        "Доброй ночи, высыпайтесь и набирайтесь сил ✨",
        "Сладких снов! Пусть ночь пройдет спокойно 💤",
    ]
    pool = morning if kind == "morning" else night
    prefix = f"{user_name}, " if user_name else ""
    return f"{prefix}{random.choice(pool)}"


def _group_reply_cooldown_for_mode(activity_mode: str) -> float:
    mode = str(activity_mode or "").strip().lower()
    if mode == "active":
        return 0.0
    if mode == "question_only":
        return 8.0
    if mode == "quiet":
        return 35.0
    return GROUP_CHAT_REPLY_COOLDOWN_SEC


def _should_reply_in_group(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    message: object,
    text: str,
    activity_mode: str,
    relation_score: int = 0,
) -> bool:
    mode = str(activity_mode or "").strip().lower()
    if mode == "active":
        return True
    if mode == "question_only":
        return _looks_like_question(text)

    if int(relation_score) <= HOSTILE_FORCE_REPLY_SCORE and random.random() < HOSTILE_SILENCE_BREAK_CHANCE:
        return True

    if _is_direct_group_addressing(context=context, message=message, text=text):
        return True

    if len(" ".join(str(text or "").split())) <= 2:
        return False

    chance = 0.10 if mode == "quiet" else GROUP_CHAT_REPLY_CHANCE
    if _looks_like_question(text):
        chance += 0.20
    if int(relation_score) <= -45:
        chance += 0.35
    return random.random() < min(1.0, chance)


def _local_group_reply(
    *,
    incoming_text: str,
    user_name: str,
    relation: dict | None,
    social_mode: str,
    roast_words: list[str],
    force_hostile: bool = False,
) -> str:
    relation = relation or {}
    score = int(relation.get("score", 0))
    status = str(relation.get("status", "neutral")).strip().lower()
    question = _looks_like_question(incoming_text)
    prefix = f"{user_name}, " if user_name else ""

    question_replies = [
        "вопрос хороший, давай разложим по шагам.",
        "я бы пошел от простого варианта и проверил на практике.",
        "сначала собери факты, потом решим без суеты.",
        "давай глянем контекст, тогда ответ будет точнее.",
    ]
    neutral_replies = [
        "понял тебя.",
        "принял, звучит логично.",
        "нормальная мысль, поддерживаю.",
        "окей, зафиксировал.",
        "согласен, это важно.",
    ]
    warm_replies = [
        "ты красавчик, хороший ход мысли.",
        "мне нравится, как ты это сформулировал.",
        "очень по делу, давай так и сделаем.",
        "круто сказал, я с тобой.",
    ]
    cold_replies = [
        "тон мерзкий. сбавь обороты.",
        "сначала научись говорить по-человечески.",
        "звучит как наезд. переделай формулировку.",
        "ты вечно с претензией. надоело.",
    ]
    hostile_replies = [
        "опять поток отмазок. звучишь жалко.",
        "твой тон нулевой, аргументов тоже не вижу.",
        "ты уже глубоко в минусе по доверию.",
        "с таким поведением нормального диалога не будет.",
        "прежде чем писать, включи голову и уважение.",
        "каждый твой наезд просто сливает разговор в мусор.",
    ]
    hostile_forced_replies = [
        "ахах, снова ты с этим шумом. скучно и слабо.",
        "очередной вброс ни о чем. стабильность уровня дна.",
        "не удивил. как всегда мимо и с претензией.",
        "вместо драмы попробуй говорить по делу.",
        "с тобой каждый раз один и тот же кринж.",
    ]

    if social_mode == "style_clone":
        if question:
            pool = question_replies
        else:
            pool = warm_replies if score >= 20 else neutral_replies
        return f"{prefix}{random.choice(pool)}"

    is_hostile = force_hostile or status == "hostile" or score <= -45
    if is_hostile:
        pool = hostile_replies
        if score <= HOSTILE_FORCE_REPLY_SCORE:
            pool = hostile_forced_replies + hostile_replies
        if roast_words and random.random() < 0.52:
            roast = random.choice(roast_words)
            pool.append(f"{roast}. тон прежний: токсичный и пустой.")
    elif status == "cold" or score <= -18:
        pool = cold_replies
    elif status in {"friendly", "warm"} or score >= 12:
        pool = question_replies if question else warm_replies
    else:
        pool = question_replies if question else neutral_replies

    return f"{prefix}{random.choice(pool)}"


async def _generate_group_chat_reply(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    incoming_text: str,
    user_name: str,
    relation: dict | None,
    social_mode: str,
    force_hostile: bool = False,
) -> tuple[str, str]:
    state = _get_state(context)
    config = _get_config(context)
    recent_dialogue = _recent_group_dialogue(context, chat_id=chat_id, limit=14)
    style_examples = state.get_style_examples()
    blacklist = state.get_blacklist_phrases()
    relation_summary = _relation_summary_for_prompt(relation)
    roast_words = state.get_roast_words()
    prefer_local_hostile = bool(force_hostile and social_mode == "self_learning")

    if config.openai_enabled and not prefer_local_hostile:
        try:
            text = await asyncio.wait_for(
                generate_openai_chat_reply(
                    cfg=_openai_runtime_cfg(config),
                    incoming_text=incoming_text,
                    recent_dialogue=recent_dialogue,
                    style_examples=style_examples,
                    bot_name=str(getattr(context.bot, "first_name", "") or getattr(context.bot, "username", "") or "бот"),
                    social_mode=social_mode,
                    relation_summary=relation_summary,
                    roast_words=roast_words,
                ),
                timeout=min(12.0, max(4.0, float(config.openai_timeout_sec))),
            )
            clean = " ".join(str(text).split())
            if clean:
                if _looks_like_bad_language_output(clean):
                    logging.info("Group chat LLM text rejected: bad_language")
                elif _contains_blacklisted_phrase(clean, blacklist):
                    stripped = _strip_blacklisted_sentences(clean, blacklist)
                    if stripped and not _contains_blacklisted_phrase(stripped, blacklist):
                        return stripped, "openai_stripped"
                else:
                    return clean, "openai"
        except OpenAIWishError as exc:
            logging.warning("Group chat LLM failed: %s", exc)
        except asyncio.TimeoutError:
            logging.warning("Group chat LLM failed: timeout")
        except Exception as exc:
            logging.exception("Unexpected group chat LLM error: %s", exc)

    fallback = _local_group_reply(
        incoming_text=incoming_text,
        user_name=user_name,
        relation=relation,
        social_mode=social_mode,
        roast_words=roast_words,
        force_hostile=force_hostile,
    )
    if _contains_blacklisted_phrase(fallback, blacklist):
        fallback = "Понял тебя."
    return fallback, "local"


async def _generate_private_chat_reply(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    incoming_text: str,
    user_name: str,
    relation: dict | None,
) -> tuple[str, str]:
    state = _get_state(context)
    social_mode = state.get_social_mode()
    score = int((relation or {}).get("score", 0))
    force_hostile = (
        social_mode == "self_learning"
        and score <= HOSTILE_FORCE_REPLY_SCORE
        and random.random() < 0.72
    )
    return await _generate_group_chat_reply(
        context=context,
        chat_id=chat_id,
        incoming_text=incoming_text,
        user_name=user_name,
        relation=relation,
        social_mode=social_mode,
        force_hostile=force_hostile,
    )


async def group_text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message or not chat or not user:
        return
    if str(chat.type) == "private":
        return

    state = _get_state(context)
    state.set_runtime_group_chat_id(chat.id)
    if not _is_target_group_chat(context, chat):
        return

    if getattr(user, "is_bot", False):
        return
    text = " ".join(str(message.text or "").split())
    if not text:
        return

    is_wish_text = _is_wish_like_text(text)
    if is_wish_text:
        await _set_wish_heart_reaction(context=context, chat_id=chat.id, message_id=message.message_id)

    user_name = str(getattr(user, "first_name", "") or getattr(user, "username", "") or "").strip()
    _append_group_dialogue(context, chat_id=chat.id, line=f"{user_name or 'Участник'}: {text}")

    relation = state.get_or_create_relation(
        chat_id=chat.id,
        user_id=user.id,
        first_name=str(getattr(user, "first_name", "") or ""),
        last_name=str(getattr(user, "last_name", "") or ""),
        username=str(getattr(user, "username", "") or ""),
    )
    relation = _ensure_admin_relation_floor(
        context=context,
        state=state,
        chat_id=chat.id,
        user_id=user.id,
    ) or relation
    delta, reason = _relation_signal(text)
    reaction_done = False
    safe_delta = _safe_relation_delta(context, user.id, delta)
    if safe_delta:
        relation = state.adjust_relation_score(
            chat_id=chat.id,
            user_id=user.id,
            delta=safe_delta,
            reason=reason or "сигнал общения",
            text=text,
            first_name=str(getattr(user, "first_name", "") or ""),
            last_name=str(getattr(user, "last_name", "") or ""),
            username=str(getattr(user, "username", "") or ""),
        )
        relation = _ensure_admin_relation_floor(
            context=context,
            state=state,
            chat_id=chat.id,
            user_id=user.id,
        ) or relation
        if not is_wish_text and abs(safe_delta) >= 8 and random.random() < 0.45:
            await _set_social_relation_reaction(
                context=context,
                chat_id=chat.id,
                message_id=message.message_id,
                delta=safe_delta,
            )
            reaction_done = True

    relation_score = int(relation.get("score", 0))
    if not reaction_done:
        await _maybe_set_contextual_group_reaction(
            context=context,
            chat_id=chat.id,
            message_id=message.message_id,
            text=text,
            relation_score=relation_score,
            delta=safe_delta,
            is_wish_text=is_wish_text,
        )

    v_query = _extract_v_query(text)
    if v_query:
        answer = await _answer_v_query(v_query)
        kwargs = {
            "chat_id": chat.id,
            "text": answer,
            "reply_to_message_id": message.message_id,
        }
        thread_id = getattr(message, "message_thread_id", None)
        if isinstance(thread_id, int):
            kwargs["message_thread_id"] = thread_id
        sent = await context.bot.send_message(**kwargs)
        _append_group_dialogue(context, chat_id=chat.id, line=f"Бот: {answer}")
        _mark_group_reply_now(context, chat_id=chat.id)
        _log_outgoing_message(
            context=context,
            chat_id=chat.id,
            chat_type=str(chat.type),
            message_id=int(getattr(sent, "message_id", 0) or 0),
            text=answer,
            source="group_v_query",
            reply_to_message_id=message.message_id,
            peer_user_id=user.id,
        )
        logging.info("Group @v reply chat_id=%s message_id=%s", chat.id, sent.message_id)
        return

    if not state.is_group_chat_mode():
        return

    wish_kind = _detect_group_wish_kind(text)
    social_mode = state.get_social_mode()

    activity_mode = state.get_group_activity_mode()
    if _is_forgive_request(text):
        reply_text = _forgive_reply_for_relation(relation=relation, user_name=user_name)
        if int(relation.get("score", 0)) < -18 or bool(relation.get("forgive_blocked", False)):
            adj = _safe_relation_delta(context, user.id, -1)
            state.adjust_relation_score(
                chat_id=chat.id,
                user_id=user.id,
                delta=adj,
                reason="просьба о прощении при негативном фоне",
                text=text,
                first_name=str(getattr(user, "first_name", "") or ""),
                last_name=str(getattr(user, "last_name", "") or ""),
                username=str(getattr(user, "username", "") or ""),
            )
        else:
            adj = _safe_relation_delta(context, user.id, 3)
            state.adjust_relation_score(
                chat_id=chat.id,
                user_id=user.id,
                delta=adj,
                reason="позитивная просьба о прощении",
                text=text,
                first_name=str(getattr(user, "first_name", "") or ""),
                last_name=str(getattr(user, "last_name", "") or ""),
                username=str(getattr(user, "username", "") or ""),
            )
        reply_source = "social_forgive"
    elif wish_kind:
        reply_text = _group_special_wish_reply(kind=wish_kind, user_name=user_name)
        reply_source = f"special_{wish_kind}"
    else:
        force_hostile = (
            social_mode == "self_learning"
            and relation_score <= HOSTILE_FORCE_REPLY_SCORE
            and random.random() < 0.72
        )
        cooldown_sec = 0.0 if (force_hostile and activity_mode != "quiet") else _group_reply_cooldown_for_mode(activity_mode)
        if cooldown_sec > 0 and not _is_group_reply_cooldown_ready(context, chat_id=chat.id, cooldown_sec=cooldown_sec):
            return
        if not _should_reply_in_group(
            context=context,
            message=message,
            text=text,
            activity_mode=activity_mode,
            relation_score=relation_score,
        ):
            return
        reply_text, reply_source = await _generate_group_chat_reply(
            context=context,
            chat_id=chat.id,
            incoming_text=text,
            user_name=user_name,
            relation=relation,
            social_mode=social_mode,
            force_hostile=force_hostile,
        )
        if force_hostile:
            reply_source = f"{reply_source}_forced"
    if not reply_text:
        return

    kwargs = {
        "chat_id": chat.id,
        "text": reply_text,
        "reply_to_message_id": message.message_id,
    }
    thread_id = getattr(message, "message_thread_id", None)
    if isinstance(thread_id, int):
        kwargs["message_thread_id"] = thread_id
    sent = await context.bot.send_message(**kwargs)
    _append_group_dialogue(context, chat_id=chat.id, line=f"Бот: {reply_text}")
    _mark_group_reply_now(context, chat_id=chat.id)
    _log_outgoing_message(
        context=context,
        chat_id=chat.id,
        chat_type=str(chat.type),
        message_id=int(getattr(sent, "message_id", 0) or 0),
        text=reply_text,
        source=f"group_reply_{reply_source}",
        reply_to_message_id=message.message_id,
        peer_user_id=user.id,
    )
    logging.info(
        "Group chat reply source=%s social_mode=%s chat_id=%s message_id=%s",
        reply_source,
        social_mode,
        chat.id,
        sent.message_id,
    )


async def media_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message or not chat or not user:
        return
    if str(chat.type) == "private":
        return

    state = _get_state(context)
    state.set_runtime_group_chat_id(chat.id)
    if not _is_target_group_chat(context, chat):
        return
    if not state.is_group_fire_reaction_mode():
        return
    if getattr(user, "is_bot", False):
        return
    if not _is_group_voice_or_video(message):
        return

    await _set_fire_reaction(context=context, chat_id=chat.id, message_id=message.message_id)


async def document_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message or not chat or not user:
        return
    if str(chat.type) != "private":
        return
    if not await _ensure_access(update, context):
        return

    pending = _pop_pending_input(context, chat_id=chat.id, user_id=user.id)
    if not pending:
        return
    action = str(pending.get("action", "")).strip()
    if action != "import_style_examples":
        _set_pending_input(context, chat_id=chat.id, user_id=user.id, action=action, payload=pending)
        await message.reply_text("Сейчас жду текст, а не файл.")
        return
    if not await _ensure_admin(update, context):
        return

    document = getattr(message, "document", None)
    if not document:
        _set_pending_input(context, chat_id=chat.id, user_id=user.id, action="import_style_examples")
        await message.reply_text("Файл не найден. Отправь JSON/TXT/HTML экспортом.")
        return

    file_name = str(getattr(document, "file_name", "") or "").strip()
    ext = os.path.splitext(file_name.lower())[1]
    if ext not in {".json", ".txt", ".html", ".htm"}:
        _set_pending_input(context, chat_id=chat.id, user_id=user.id, action="import_style_examples")
        await message.reply_text("Поддерживаю JSON, TXT и HTML (экспорт Telegram).")
        return

    tmp_path = os.path.join(
        tempfile.gettempdir(),
        f"style_{chat.id}_{user.id}_{int(time.time())}{ext}",
    )

    try:
        tg_file = await context.bot.get_file(document.file_id)
        await tg_file.download_to_drive(custom_path=tmp_path)

        examples: list[str] = []
        if ext == ".json":
            with open(tmp_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            if not isinstance(payload, dict):
                raise ValueError("Некорректный JSON")
            admin_id = _effective_admin_user_id(context)
            examples = _extract_style_examples_from_export_json(payload, admin_id)
        elif ext == ".txt":
            with open(tmp_path, "r", encoding="utf-8", errors="ignore") as handle:
                raw_text = handle.read()
            examples = _extract_style_examples_from_text(raw_text)
        else:
            with open(tmp_path, "r", encoding="utf-8", errors="ignore") as handle:
                raw_html = handle.read()
            author_hints = [
                str(getattr(user, "first_name", "") or "").strip(),
                str(getattr(user, "last_name", "") or "").strip(),
                str(getattr(user, "username", "") or "").strip(),
            ]
            full_name = " ".join(
                part
                for part in (
                    str(getattr(user, "first_name", "") or "").strip(),
                    str(getattr(user, "last_name", "") or "").strip(),
                )
                if part
            )
            if full_name:
                author_hints.append(full_name)
            examples = _extract_style_examples_from_export_html(raw_html, author_hints=author_hints)

        if not examples:
            _set_pending_input(context, chat_id=chat.id, user_id=user.id, action="import_style_examples")
            await message.reply_text(
                "Не смог извлечь примеры стиля.\n"
                "Проверь экспорт и попробуй еще раз."
            )
            return

        count = _get_state(context).set_style_examples(examples)
        await message.reply_text(
            f"Импорт готов: {count} примеров стиля.\n"
            "Теперь включи «💬 Общение в группе» в настройках."
        )
    except Exception as exc:
        logging.exception("Style import failed: %s", exc)
        _set_pending_input(context, chat_id=chat.id, user_id=user.id, action="import_style_examples")
        await message.reply_text("Не смог прочитать файл. Попробуй другой JSON/TXT/HTML.")
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _ensure_access(update, context):
        return
    query = update.callback_query
    if not query or not query.data or not query.message:
        return
    query = _SafeCallbackQueryProxy(query)

    if query.message.chat.type != "private":
        _get_state(context).set_runtime_group_chat_id(query.message.chat_id)

    parts = query.data.split("|")
    scope = parts[0]

    if scope == "menu":
        action = parts[1] if len(parts) > 1 else ""
        chat_id = query.message.chat_id
        state = _get_state(context)

        if action == "noop":
            await query.answer()
            return
        if action == "home":
            await query.answer()
            await query.message.reply_text("Главное меню:", reply_markup=_home_markup_for(update, context))
            return
        if action == "training_toggle":
            if not await _ensure_admin(update, context):
                return
            enabled = state.toggle_chat_training_mode(chat_id)
            if enabled:
                _set_training_waiting(context, chat_id=chat_id, waiting=False)
                _ensure_training_job(context, chat_id)
                await query.answer("Режим обучения включен")
                await query.message.reply_text(
                    "Режим обучения включен.\n"
                    "Буду отправлять следующий текст после твоего фидбека.",
                    reply_markup=_home_inline_keyboard(is_admin=True, training_mode=True),
                )
            else:
                _stop_training_job(context, chat_id)
                _set_training_waiting(context, chat_id=chat_id, waiting=False)
                await query.answer("Режим обучения выключен")
                await query.message.reply_text(
                    "Режим обучения выключен.",
                    reply_markup=_home_inline_keyboard(is_admin=True, training_mode=False),
                )
            return
        if action == "send" and len(parts) == 3:
            kind_code = parts[2]
            kind_title = "Спокойной ночи" if kind_code == "n" else "Доброе утро"
            await query.answer()
            await query.message.reply_text(
                f"{kind_title}: выбери, кому отправить:",
                reply_markup=_send_target_keyboard(
                    kind_code=kind_code,
                    is_admin=_is_admin(update, context),
                ),
            )
            return
        if action == "dispatch" and len(parts) == 5:
            kind = _kind_from_code(parts[2])
            target_code = parts[3]  # p=private, g=group, b=both
            person_code = parts[4]  # g=girl, b=boy, x=keep current

            mode, current_person = _chat_person_mode(context, chat_id)
            person_override = current_person
            if person_code in {"g", "b"}:
                target_name = "подруга" if person_code == "g" else "друг"
                picked_person = None
                for row in state.list_personas():
                    if str(row.get("name", "")).strip().lower() == target_name:
                        picked_person = row
                        break
                if picked_person is None:
                    instructions = (
                        "пиши мягко и нежно, как близкой подруге"
                        if target_name == "подруга"
                        else "пиши дружелюбно и уверенно, как хорошему другу"
                    )
                    picked_person = state.add_person(target_name, instructions)
                person_override = picked_person

            if target_code == "p":
                target_chat_id = chat_id
                if query.message.chat.type != "private":
                    target_chat_id = _effective_admin_user_id(context) or chat_id
                await query.answer("Отправляю в личку")
                if int(target_chat_id) == int(chat_id):
                    await _send_wish_with_progress(
                        chat_id=target_chat_id,
                        kind=kind,
                        context=context,
                        source="button_dispatch_private",
                        mode_override=mode,
                        person_override=person_override,
                    )
                else:
                    await _send_wish(
                        chat_id=target_chat_id,
                        kind=kind,
                        context=context,
                        source="button_dispatch_private",
                        mode_override=mode,
                        person_override=person_override,
                    )
                return

            if target_code == "g":
                if not await _ensure_admin(update, context):
                    return
                group_id = _effective_group_chat_id(context)
                if not group_id:
                    await query.answer("Группа не задана", show_alert=True)
                    return
                await query.answer("Отправляю в группу")
                await _send_wish(
                    chat_id=group_id,
                    kind=kind,
                    context=context,
                    source="button_dispatch_group",
                    mode_override=mode,
                    person_override=person_override,
                )
                return

            if target_code == "b":
                if not await _ensure_admin(update, context):
                    return
                targets: list[int] = []
                admin_id = _effective_admin_user_id(context)
                group_id = _effective_group_chat_id(context)
                for target_id in (admin_id, group_id):
                    if target_id and target_id not in targets:
                        targets.append(target_id)
                if not targets:
                    await query.answer("Нет цели отправки", show_alert=True)
                    return
                await query.answer("Отправляю в личку и группу")
                for target_id in targets:
                    await _send_wish(
                        chat_id=target_id,
                        kind=kind,
                        context=context,
                        source="button_dispatch_both",
                        mode_override=mode,
                        person_override=person_override,
                    )
                return

            await query.answer("Неизвестная цель", show_alert=True)
            return
        if action == "send_both" and len(parts) == 3:
            if not await _ensure_admin(update, context):
                return
            kind = _kind_from_code(parts[2])
            mode, person = _chat_person_mode(context, chat_id)
            targets = []
            admin_id = _effective_admin_user_id(context)
            group_id = _effective_group_chat_id(context)
            for target_id in (admin_id, group_id):
                if target_id and target_id not in targets:
                    targets.append(target_id)
            if not targets:
                await query.answer("Нет цели отправки", show_alert=True)
                return
            await query.answer("Отправляю в личку и группу")
            for target_id in targets:
                await _send_wish(
                    chat_id=target_id,
                    kind=kind,
                    context=context,
                    source="button_send_both",
                    mode_override=mode,
                    person_override=person,
                )
            return
        if action in {"examples", "modes_help"}:
            await query.answer()
            await _send_modes_help(chat_id=chat_id, context=context)
            return
        if action == "stats":
            await query.answer()
            await _send_stats(chat_id=chat_id, context=context)
            return
        if action == "settings":
            if not await _ensure_admin(update, context):
                return
            await query.answer()
            await _show_settings(chat_id=chat_id, context=context)
            return
        if action == "toggle_mode":
            if not await _ensure_admin(update, context):
                return
            new_mode = state.toggle_chat_mode(chat_id)
            _, person = _chat_person_mode(context, chat_id)
            await query.answer(f"Режим: {_mode_ru(new_mode)}")
            try:
                await query.message.edit_reply_markup(
                    reply_markup=_settings_markup_for_chat(
                        context=context,
                        chat_id=chat_id,
                        mode=new_mode,
                        person_name=person.get("name", "общий вариант"),
                    )
                )
            except Exception:
                pass
            return
        if action == "toggle_schedule_mode":
            if not await _ensure_admin(update, context):
                return
            new_schedule_mode = state.toggle_schedule_mode()
            mode, person = _chat_person_mode(context, chat_id)
            await query.answer(f"Режим рассылки: {_mode_ru(new_schedule_mode)}")
            try:
                await query.message.edit_reply_markup(
                    reply_markup=_settings_markup_for_chat(
                        context=context,
                        chat_id=chat_id,
                        mode=mode,
                        person_name=person.get("name", "общий вариант"),
                    )
                )
            except Exception:
                pass
            return
        if action == "toggle_admin_only":
            if not await _ensure_admin(update, context):
                return
            enabled = state.toggle_admin_only_mode()
            mode, person = _chat_person_mode(context, chat_id)
            await query.answer(f"Только админ: {_on_off_ru(enabled)}")
            try:
                await query.message.edit_reply_markup(
                    reply_markup=_settings_markup_for_chat(
                        context=context,
                        chat_id=chat_id,
                        mode=mode,
                        person_name=person.get("name", "общий вариант"),
                    )
                )
            except Exception:
                pass
            return
        if action == "toggle_group_reaction":
            if not await _ensure_admin(update, context):
                return
            enabled = state.toggle_group_fire_reaction_mode()
            mode, person = _chat_person_mode(context, chat_id)
            await query.answer(f"🔥 Реакции: {_on_off_ru(enabled)}")
            try:
                await query.message.edit_reply_markup(
                    reply_markup=_settings_markup_for_chat(
                        context=context,
                        chat_id=chat_id,
                        mode=mode,
                        person_name=person.get("name", "общий вариант"),
                    )
                )
            except Exception:
                pass
            return
        if action == "toggle_group_chat_mode":
            if not await _ensure_admin(update, context):
                return
            enabled = state.toggle_group_chat_mode()
            mode, person = _chat_person_mode(context, chat_id)
            await query.answer(f"💬 Общение: {_on_off_ru(enabled)}")
            try:
                await query.message.edit_reply_markup(
                    reply_markup=_settings_markup_for_chat(
                        context=context,
                        chat_id=chat_id,
                        mode=mode,
                        person_name=person.get("name", "общий вариант"),
                    )
                )
            except Exception:
                pass
            return
        if action == "toggle_public_private_chat_mode":
            if not await _ensure_admin(update, context):
                return
            enabled = state.toggle_public_private_chat_mode()
            mode, person = _chat_person_mode(context, chat_id)
            await query.answer(f"Личка для всех: {_on_off_ru(enabled)}")
            try:
                await query.message.edit_reply_markup(
                    reply_markup=_settings_markup_for_chat(
                        context=context,
                        chat_id=chat_id,
                        mode=mode,
                        person_name=person.get("name", "общий вариант"),
                    )
                )
            except Exception:
                pass
            return
        if action == "toggle_group_activity":
            if not await _ensure_admin(update, context):
                return
            new_mode = state.cycle_group_activity_mode()
            mode, person = _chat_person_mode(context, chat_id)
            await query.answer(f"🎚 Активность: {_group_activity_ru(new_mode)}")
            try:
                await query.message.edit_reply_markup(
                    reply_markup=_settings_markup_for_chat(
                        context=context,
                        chat_id=chat_id,
                        mode=mode,
                        person_name=person.get("name", "общий вариант"),
                    )
                )
            except Exception:
                pass
            return
        if action == "toggle_social_mode":
            if not await _ensure_admin(update, context):
                return
            new_mode = state.cycle_social_mode()
            mode, person = _chat_person_mode(context, chat_id)
            await query.answer(f"Соц режим: {_social_mode_ru(new_mode)}")
            try:
                await query.message.edit_reply_markup(
                    reply_markup=_settings_markup_for_chat(
                        context=context,
                        chat_id=chat_id,
                        mode=mode,
                        person_name=person.get("name", "общий вариант"),
                    )
                )
            except Exception:
                pass
            return
        if action == "roast":
            if not await _ensure_admin(update, context):
                return
            await query.answer()
            await query.message.reply_text(
                "Список дружеских подколов/обзывалок.\n"
                "Используются аккуратно в режиме самообучения.",
                reply_markup=_roast_keyboard(state),
            )
            return
        if action == "roast_show":
            if not await _ensure_admin(update, context):
                return
            await query.answer()
            roast_words = state.get_roast_words()
            text_lines = [f"{idx}. {item}" for idx, item in enumerate(roast_words, start=1)]
            await query.message.reply_text("Подколы/обзывалки:\n" + "\n".join(text_lines))
            return
        if action == "roast_add":
            if not await _ensure_admin(update, context):
                return
            await query.answer()
            _set_pending_input(context, chat_id=chat_id, user_id=query.from_user.id, action="roast_add")
            await query.message.reply_text(
                "Пришли слова/фразы для дружеских подколов.\n"
                "Можно через запятую или каждую с новой строки."
            )
            return
        if action == "roast_reset":
            if not await _ensure_admin(update, context):
                return
            state.reset_roast_words()
            await query.answer("Сбросил")
            await query.message.reply_text("Список подколов сброшен к базовому.", reply_markup=_roast_keyboard(state))
            return
        if action == "relations":
            if not await _ensure_admin(update, context):
                return
            relation_chat_id = _effective_group_chat_id(context) or chat_id
            if len(parts) == 3:
                try:
                    relation_chat_id = int(parts[2])
                except Exception:
                    relation_chat_id = _effective_group_chat_id(context) or chat_id
            await query.answer()
            await query.message.reply_text(
                f"Рейтинг людей (чат {relation_chat_id}):",
                reply_markup=_relations_keyboard(state=state, relation_chat_id=relation_chat_id),
            )
            return
        if action == "rel_user" and len(parts) == 4:
            if not await _ensure_admin(update, context):
                return
            try:
                relation_chat_id = int(parts[2])
                target_user_id = int(parts[3])
            except Exception:
                await query.answer("Ошибка")
                return
            relation = state.get_relation(chat_id=relation_chat_id, user_id=target_user_id)
            if not relation:
                await query.answer("Нет данных")
                return
            grudges = relation.get("grudges", [])
            if not isinstance(grudges, list):
                grudges = []
            grudge_line = ", ".join(str(item) for item in grudges[-5:] if str(item).strip()) or "нет"
            await query.answer()
            await query.message.reply_text(
                f"Профиль: {_relation_display_name(relation)}\n"
                f"- user_id: {target_user_id}\n"
                f"- рейтинг: {int(relation.get('score', 0)):+d}\n"
                f"- статус: {_relation_status_ru(str(relation.get('status', 'neutral')))}\n"
                f"- доброжелательных сигналов: {int(relation.get('friendly_hits', 0))}\n"
                f"- грубых сигналов: {int(relation.get('rude_hits', 0))}\n"
                f"- прощение заблокировано: {'да' if relation.get('forgive_blocked', False) else 'нет'}\n"
                f"- последняя причина: {relation.get('last_reason', 'нет')}\n"
                f"- обиды: {grudge_line}",
                reply_markup=_relation_adjust_keyboard(
                    relation_chat_id=relation_chat_id,
                    user_id=target_user_id,
                ),
            )
            return
        if action == "rel_adj" and len(parts) == 5:
            if not await _ensure_admin(update, context):
                return
            try:
                relation_chat_id = int(parts[2])
                target_user_id = int(parts[3])
                delta = int(parts[4])
            except Exception:
                await query.answer("Ошибка")
                return
            safe_delta = _safe_relation_delta(context, target_user_id, delta)
            relation = state.adjust_relation_score(
                chat_id=relation_chat_id,
                user_id=target_user_id,
                delta=safe_delta,
                reason="админ изменил рейтинг",
                text="manual_adjustment",
            )
            relation = _ensure_admin_relation_floor(
                context=context,
                state=state,
                chat_id=relation_chat_id,
                user_id=target_user_id,
            ) or relation
            await query.answer(f"Рейтинг: {int(relation.get('score', 0)):+d}")
            await query.message.reply_text(
                f"Обновил: {_relation_display_name(relation)} -> {int(relation.get('score', 0)):+d} "
                f"({_relation_status_ru(str(relation.get('status', 'neutral')))})",
                reply_markup=_relation_adjust_keyboard(
                    relation_chat_id=relation_chat_id,
                    user_id=target_user_id,
                ),
            )
            return
        if action == "rel_set" and len(parts) == 5:
            if not await _ensure_admin(update, context):
                return
            try:
                relation_chat_id = int(parts[2])
                target_user_id = int(parts[3])
                score = int(parts[4])
            except Exception:
                await query.answer("Ошибка")
                return
            safe_score = _safe_relation_score(context, target_user_id, score)
            relation = state.set_relation_score(
                chat_id=relation_chat_id,
                user_id=target_user_id,
                score=safe_score,
                reason="админ задал рейтинг",
            )
            await query.answer(f"Рейтинг: {int(relation.get('score', 0)):+d}")
            await query.message.reply_text(
                f"Установил рейтинг: {_relation_display_name(relation)} -> {int(relation.get('score', 0)):+d}",
                reply_markup=_relation_adjust_keyboard(
                    relation_chat_id=relation_chat_id,
                    user_id=target_user_id,
                ),
            )
            return
        if action == "rel_prompt" and len(parts) == 4:
            if not await _ensure_admin(update, context):
                return
            try:
                relation_chat_id = int(parts[2])
                target_user_id = int(parts[3])
            except Exception:
                await query.answer("Ошибка")
                return
            await query.answer()
            _set_pending_input(
                context,
                chat_id=chat_id,
                user_id=query.from_user.id,
                action="set_relation_score",
                payload={"relation_chat_id": relation_chat_id, "target_user_id": target_user_id},
            )
            await query.message.reply_text("Напиши новый рейтинг от -100 до 100. Можно с причиной: 35 | за хороший диалог")
            return
        if action == "import_style_examples":
            if not await _ensure_admin(update, context):
                return
            await query.answer()
            _set_pending_input(context, chat_id=chat_id, user_id=query.from_user.id, action="import_style_examples")
            await query.message.reply_text(
                "Отправь JSON/TXT/HTML файл с экспортом переписки.\n"
                "JSON: telegram desktop export (result.json).\n"
                "HTML: telegram desktop export (messages.html).\n"
                "TXT: по одной фразе на строку.\n"
                "Возьму стиль твоих сообщений и применю в режиме общения."
            )
            return
        if action == "clear_style_examples":
            if not await _ensure_admin(update, context):
                return
            state.clear_style_examples()
            await query.answer("Очистил стиль")
            await query.message.reply_text("Примеры стиля очищены.")
            return
        if action == "quick_person" and len(parts) == 3:
            if not await _ensure_admin(update, context):
                return
            variant = parts[2]
            target_name = "подруга" if variant == "girl" else "друг"
            target_person = None
            for row in state.list_personas():
                if str(row.get("name", "")).strip().lower() == target_name:
                    target_person = row
                    break
            if target_person is None:
                instructions = (
                    "пиши мягко и нежно, как близкой подруге"
                    if target_name == "подруга"
                    else "пиши дружелюбно и уверенно, как хорошему другу"
                )
                target_person = state.add_person(target_name, instructions)
            state.set_chat_person(chat_id, int(target_person["id"]))
            await query.answer(f"Кому: {target_person['name']}")
            await query.message.reply_text(
                f"Переключил профиль на: {target_person['name']}.",
                reply_markup=_home_inline_keyboard(
                    is_admin=True,
                    training_mode=state.is_chat_training_mode(chat_id),
                ),
            )
            return
        if action == "persons":
            if not await _ensure_admin(update, context):
                return
            prefs = state.get_chat_prefs(chat_id)
            current_person_id = int(prefs.get("person_id", DEFAULT_PERSON_ID))
            await query.answer()
            await query.message.reply_text(
                "Выбери человека для этого чата:",
                reply_markup=_person_select_keyboard(
                    scope="menu",
                    kind_code="n",
                    state=state,
                    current_person_id=current_person_id,
                ),
            )
            return
        if action == "set_person" and len(parts) == 4:
            if not await _ensure_admin(update, context):
                return
            try:
                person_id = int(parts[3])
                person = state.set_chat_person(chat_id, person_id)
            except Exception:
                await query.answer("Такого ID нет")
                return
            await query.answer(f"Кому: {person['name']}")
            await query.message.reply_text(f"Теперь активен: {person['name']}.")
            return
        if action == "add_person":
            if not await _ensure_admin(update, context):
                return
            await query.answer()
            _set_pending_input(context, chat_id=chat_id, user_id=query.from_user.id, action="add_person")
            await query.message.reply_text(
                "Напиши одним сообщением:\nИмя | инструкция\n\n"
                "Пример:\nАлина | дружески, с теплом, можно чай и плед"
            )
            return
        if action == "add_exception":
            if not await _ensure_admin(update, context):
                return
            await query.answer()
            _set_pending_input(context, chat_id=chat_id, user_id=query.from_user.id, action="add_exception_id")
            await query.message.reply_text(
                "Отправь ID пользователя для исключения.\n"
                "Я попробую получить имя и фамилию и добавить в разрешенные."
            )
            return
        if action == "list_exceptions":
            if not await _ensure_admin(update, context):
                return
            await query.answer()
            await query.message.reply_text(
                "Исключения доступа (нажми, чтобы удалить):",
                reply_markup=_exceptions_keyboard(state),
            )
            return
        if action == "del_exception" and len(parts) == 3:
            if not await _ensure_admin(update, context):
                return
            try:
                user_id = int(parts[2])
            except Exception:
                await query.answer("Ошибка")
                return
            removed = state.remove_access_exception(user_id)
            if removed:
                await query.answer("Удалил из исключений")
            else:
                await query.answer("Пользователь не найден")
            try:
                await query.message.edit_reply_markup(reply_markup=_exceptions_keyboard(state))
            except Exception:
                pass
            return
        if action == "del_persons":
            if not await _ensure_admin(update, context):
                return
            await query.answer()
            await query.message.reply_text(
                "Кого удалить?",
                reply_markup=_person_delete_keyboard(state),
            )
            return
        if action == "del_person" and len(parts) == 3:
            if not await _ensure_admin(update, context):
                return
            try:
                person_id = int(parts[2])
            except Exception:
                await query.answer("Ошибка")
                return
            if person_id == DEFAULT_PERSON_ID:
                await query.answer("Общий вариант удалять нельзя")
                return
            removed = state.delete_person(person_id)
            if removed:
                await query.answer("Удалил")
                await query.message.reply_text(f"Удалил ID {person_id}.")
            else:
                await query.answer("Такого ID нет")
            return
        if action == "set_group_here":
            if not await _ensure_admin(update, context):
                return
            if query.message.chat.type == "private":
                await query.answer("Открой эту кнопку в группе", show_alert=True)
                return
            state.set_runtime_group_chat_id(chat_id)
            await query.answer("Группа закреплена")
            await query.message.reply_text(f"Сохранил группу для авторассылки: {chat_id}")
            return
        if action == "premium":
            if not await _ensure_admin(update, context):
                return
            await query.answer()
            await query.message.reply_text(
                "Настройка эмодзи.\n"
                "Можно просто прислать боту эмодзи сообщением, и он запомнит их автоматически.",
                reply_markup=_premium_keyboard(),
            )
            return
        if action == "blacklist":
            if not await _ensure_admin(update, context):
                return
            await query.answer()
            await query.message.reply_text(
                "Черный список фраз.\n"
                "Если фраза в списке, бот старается не использовать похожие куски текста.",
                reply_markup=_blacklist_keyboard(),
            )
            return
        if action == "export_chats":
            if not await _ensure_admin(update, context):
                return
            await query.answer()
            await query.message.reply_text(
                "Выбери пользователя для экспорта переписки:",
                reply_markup=_user_export_keyboard(context),
            )
            return
        if action == "export_user" and len(parts) == 3:
            if not await _ensure_admin(update, context):
                return
            try:
                target_user_id = int(parts[2])
            except Exception:
                await query.answer("Ошибка")
                return
            await query.answer("Готовлю экспорт")
            rows = _iter_chat_log_rows(context, reverse=False)
            user_rows = [
                row
                for row in rows
                if int(row.get("user_id", 0) or 0) == target_user_id
                or int(row.get("peer_user_id", 0) or 0) == target_user_id
            ]
            if not user_rows:
                await query.message.reply_text("Для этого пользователя пока нет записей.")
                return
            lines: list[str] = []
            for row in user_rows:
                ts = str(row.get("ts", "") or "")
                chat_id_row = int(row.get("chat_id", 0) or 0)
                direction = str(row.get("direction", "") or "")
                username = str(row.get("username", "") or "")
                first_name = str(row.get("first_name", "") or "")
                last_name = str(row.get("last_name", "") or "")
                name = " ".join(part for part in (first_name, last_name) if part).strip()
                if username:
                    uname = f"@{username}"
                    name = f"{name} ({uname})" if name else uname
                if not name:
                    if direction == "outgoing":
                        name = "бот"
                    else:
                        name = str(target_user_id)
                text_value = " ".join(str(row.get("text", "") or "").split())
                content_type = str(row.get("content_type", "") or "text")
                if not text_value:
                    text_value = f"[{content_type}]"
                source = str(row.get("source", "") or "")
                src_suffix = f" source={source}" if source else ""
                lines.append(f"[{ts}] chat={chat_id_row} {direction} {name}:{src_suffix} {text_value}")
            export_text = "\n".join(lines)
            tmp_path = os.path.join(
                tempfile.gettempdir(),
                f"chat_export_{target_user_id}_{int(time.time())}.txt",
            )
            try:
                with open(tmp_path, "w", encoding="utf-8") as handle:
                    handle.write(export_text)
                with open(tmp_path, "rb") as handle:
                    await context.bot.send_document(
                        chat_id=chat_id,
                        document=handle,
                        filename=f"chat_export_{target_user_id}.txt",
                        caption=f"Экспорт сообщений пользователя {target_user_id}",
                    )
            finally:
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception:
                    pass
            return
        if action == "blacklist_show":
            if not await _ensure_admin(update, context):
                return
            await query.answer()
            phrases = state.get_blacklist_phrases()
            if not phrases:
                await query.message.reply_text("Черный список пуст.")
                return
            top = phrases[-40:]
            lines = [f"{idx}. {value}" for idx, value in enumerate(top, start=1)]
            if len(phrases) > len(top):
                lines.append(f"... и еще {len(phrases) - len(top)}")
            await query.message.reply_text("Фразы в черном списке:\n" + "\n".join(lines))
            return
        if action == "blacklist_reset":
            if not await _ensure_admin(update, context):
                return
            await query.answer("Очистил")
            state.reset_blacklist_phrases()
            await query.message.reply_text("Черный список очищен.")
            return
        if action == "premium_show":
            if not await _ensure_admin(update, context):
                return
            await query.answer()
            ids = _effective_premium_ids(context)
            liked = state.get_liked_emojis()
            favorite_phrases = state.get_favorite_phrases()

            max_custom_show = 40
            shown_ids = ids[:max_custom_show]
            custom_text = "\n".join(shown_ids) if shown_ids else "нет"
            if len(ids) > max_custom_show:
                custom_text = f"{custom_text}\n... и еще {len(ids) - max_custom_show}"

            max_liked_show = 60
            shown_liked = liked[-max_liked_show:]
            liked_text = " ".join(shown_liked) if shown_liked else "нет"
            if len(liked) > max_liked_show:
                liked_text = f"{liked_text}\n... и еще {len(liked) - max_liked_show}"

            await query.message.reply_text(
                "Сохраненные эмодзи для генерации:\n"
                f"- обычные эмодзи: {len(liked)}\n{liked_text}\n\n"
                f"- премиум-эмодзи (ID): {len(ids)}\n{custom_text}\n\n"
                f"- любимых оборотов речи: {len(favorite_phrases)}\n\n"
                "Чтобы добавить новые эмодзи, просто отправь боту эмодзи сообщением."
            )
            return
        if action == "liked_reset":
            if not await _ensure_admin(update, context):
                return
            await query.answer("Очистил")
            state.reset_liked_emojis()
            await query.message.reply_text("Обычные эмодзи очищены.")
            return
        if action == "favorite_reset":
            if not await _ensure_admin(update, context):
                return
            await query.answer("Очистил")
            state.reset_favorite_phrases()
            await query.message.reply_text("Список любимых оборотов очищен.")
            return
        if action == "premium_reset":
            if not await _ensure_admin(update, context):
                return
            await query.answer("Сбросил")
            state.reset_premium_to_default()
            await query.message.reply_text("ID премиум-эмодзи сброшены к списку из user_config.py.")
            return

    if scope == "wish":
        action = parts[1] if len(parts) > 1 else ""
        chat_id = query.message.chat_id
        state = _get_state(context)

        if action == "regen" and len(parts) == 3:
            await query.answer()
            kind = _kind_from_code(parts[2])
            await _send_wish_with_progress(
                chat_id=chat_id,
                kind=kind,
                context=context,
                source="inline_regenerate",
            )
            return

        if action == "toggle_mode" and len(parts) == 3:
            if not await _ensure_admin(update, context):
                return
            kind = _kind_from_code(parts[2])
            new_mode = state.toggle_chat_mode(chat_id)
            _, person = _chat_person_mode(context, chat_id)
            await query.answer(f"Режим: {_mode_ru(new_mode)}")
            try:
                await query.message.edit_reply_markup(
                    reply_markup=_main_wish_keyboard(kind, new_mode, person.get("name", "общий вариант"))
                )
            except Exception:
                pass
            return

        if action == "pick_person" and len(parts) == 3:
            if not await _ensure_admin(update, context):
                return
            kind_code = parts[2]
            prefs = state.get_chat_prefs(chat_id)
            current_person_id = int(prefs.get("person_id", DEFAULT_PERSON_ID))
            await query.answer()
            await query.message.reply_text(
                "Выбери человека:",
                reply_markup=_person_select_keyboard(
                    scope="wish",
                    kind_code=kind_code,
                    state=state,
                    current_person_id=current_person_id,
                ),
            )
            return

        if action == "set_person" and len(parts) == 4:
            if not await _ensure_admin(update, context):
                return
            try:
                kind = _kind_from_code(parts[2])
                person_id = int(parts[3])
                person = state.set_chat_person(chat_id, person_id)
            except Exception:
                await query.answer("Ошибка выбора")
                return
            await query.answer(f"Кому: {person['name']}")
            await _send_wish_with_progress(
                chat_id=chat_id,
                kind=kind,
                context=context,
                source="inline_person_switch",
            )
            return

        if action == "rate_all" and len(parts) == 3:
            kind_code = parts[2]
            try:
                kind = _kind_from_code(kind_code)
                source_message_id = int(query.message.message_id)
            except Exception:
                await query.answer("Ошибка")
                return

            snapshot = _find_generation_snapshot(
                context,
                chat_id=query.message.chat_id,
                message_id=source_message_id,
            )
            store = _get_store(context)
            store.record_feedback(
                kind=kind,
                rating="good",
                reason="понравилось всё",
                user_id=query.from_user.id,
                chat_id=query.message.chat_id,
                source_message_id=source_message_id,
                text=snapshot["text"] if snapshot else None,
            )
            if snapshot and isinstance(snapshot.get("features"), dict):
                model = _get_model(context)
                model.train(snapshot["features"], target=1.0, epochs=14)
                model.save(_get_config(context).model_path)

            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

            _set_training_waiting(context, chat_id=chat_id, waiting=False)
            await query.answer("Спасибо!")
            await query.message.reply_text("Супер, принял: понравилось всё.", reply_markup=_after_good_keyboard())
            return

        if action == "like_phrase" and len(parts) == 3:
            kind_code = parts[2]
            source_message_id = int(query.message.message_id)
            _set_pending_input(
                context,
                chat_id=chat_id,
                user_id=query.from_user.id,
                action="good_snippet",
                payload={"kind": kind_code, "source_message_id": source_message_id},
            )
            await query.answer()
            await query.message.reply_text(
                "Пришли фразу/оборот, который особенно понравился.\n"
                "Я отмечу это как удачный стиль.\n"
                "Если не хочешь писать фразу, отправь: -"
            )
            return

        if action == "dislike_part" and len(parts) == 3:
            kind_code = parts[2]
            source_message_id = int(query.message.message_id)
            _set_pending_input(
                context,
                chat_id=chat_id,
                user_id=query.from_user.id,
                action="bad_snippet",
                payload={"kind": kind_code, "source_message_id": source_message_id},
            )
            await query.answer()
            await query.message.reply_text(
                "Пришли кусочек текста, который не понравился.\n"
                "Я добавлю его в черный список и после этого сгенерирую новый вариант.\n"
                "Если не хочешь добавлять, отправь: -"
            )
            return

        # Совместимость со старыми кнопками в уже отправленных сообщениях.
        if action == "rate" and len(parts) == 4:
            kind_code = parts[2]
            rating_code = parts[3]
            if rating_code == "g":
                _set_pending_input(
                    context,
                    chat_id=chat_id,
                    user_id=query.from_user.id,
                    action="good_snippet",
                    payload={"kind": kind_code, "source_message_id": query.message.message_id},
                )
                await query.answer()
                await query.message.reply_text("Пришли фразу, которая понравилась. Или отправь: -")
                return
            _set_pending_input(
                context,
                chat_id=chat_id,
                user_id=query.from_user.id,
                action="bad_snippet",
                payload={"kind": kind_code, "source_message_id": query.message.message_id},
            )
            await query.answer()
            await query.message.reply_text("Пришли кусочек текста, который не понравился. Или отправь: -")
            return

        if action in {"rsn", "badpart"}:
            await query.answer("Эти кнопки устарели")
            await query.message.reply_text("Это старые кнопки. Нажми «Еще вариант», чтобы получить новое сообщение с актуальным меню.")
            return


async def training_stream_tick(context: ContextTypes.DEFAULT_TYPE) -> None:
    job = context.job
    if not job or not isinstance(job.data, dict):
        return
    chat_id = int(job.data.get("chat_id", 0) or 0)
    if not chat_id:
        return

    state = _get_state(context)
    if not state.is_chat_training_mode(chat_id):
        _set_training_waiting(context, chat_id=chat_id, waiting=False)
        _stop_training_job(context, chat_id)
        return
    if _is_training_waiting(context, chat_id):
        return

    kind = "night" if random.random() < 0.5 else "morning"
    _set_training_waiting(context, chat_id=chat_id, waiting=True)
    try:
        await _send_wish(
            chat_id=chat_id,
            kind=kind,
            context=context,
            source="training_stream",
        )
    except TelegramError as exc:
        _set_training_waiting(context, chat_id=chat_id, waiting=False)
        logging.exception("Training stream failed for chat %s: %s", chat_id, exc)


async def _broadcast(kind: str, context: ContextTypes.DEFAULT_TYPE) -> None:
    state = _get_state(context)
    schedule_mode = state.get_schedule_mode()
    admin_chat_id = _effective_admin_user_id(context)
    group_chat_id = _effective_group_chat_id(context)
    targets = []
    for chat_id in (admin_chat_id, group_chat_id):
        if chat_id and chat_id not in targets:
            targets.append(chat_id)
    for chat_id in targets:
        try:
            await _send_wish(
                chat_id=chat_id,
                kind=kind,
                context=context,
                source=f"scheduled_{kind}",
                mode_override=schedule_mode,
            )
        except TelegramError as exc:
            logging.exception("Failed to send %s wish to chat %s: %s", kind, chat_id, exc)


async def scheduled_night(context: ContextTypes.DEFAULT_TYPE) -> None:
    await _broadcast("night", context)


async def scheduled_morning(context: ContextTypes.DEFAULT_TYPE) -> None:
    await _broadcast("morning", context)


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    if isinstance(err, TimedOut):
        logging.warning("Telegram timeout in update handler: %s", err)
        return
    logging.exception("Unhandled error in update handler", exc_info=err)

    text = "Произошла ошибка при обработке. Попробуй еще раз."
    if isinstance(err, BadRequest) and _is_markup_error(err):
        text = (
            "Ошибка отправки из-за ID премиум-эмодзи.\n"
            "Проверь список в Настройки -> Эмодзи.\n"
            "Сейчас бот попробует работать без премиум-эмодзи."
        )

    chat_id = None
    if isinstance(update, Update):
        if update.effective_chat:
            chat_id = update.effective_chat.id
        elif update.callback_query and update.callback_query.message:
            chat_id = update.callback_query.message.chat_id

    if chat_id:
        try:
            await context.bot.send_message(chat_id=chat_id, text=text)
        except Exception:
            pass


def _schedule_jobs(app: Application, timezone: ZoneInfo) -> None:
    days = (0, 1, 2, 3, 4, 5, 6)
    app.job_queue.run_daily(
        callback=scheduled_night,
        time=dtime(hour=NIGHT_TIME.hour, minute=NIGHT_TIME.minute, tzinfo=timezone),
        days=days,
        name="scheduled_night",
    )
    app.job_queue.run_daily(
        callback=scheduled_morning,
        time=dtime(hour=MORNING_TIME.hour, minute=MORNING_TIME.minute, tzinfo=timezone),
        days=days,
        name="scheduled_morning",
    )


def _schedule_training_jobs(app: Application, state: BotStateStore) -> None:
    for chat_id in state.list_training_chat_ids():
        name = _training_job_name(chat_id)
        existing = app.job_queue.get_jobs_by_name(name)
        if existing:
            continue
        app.job_queue.run_repeating(
            callback=training_stream_tick,
            interval=TRAINING_STREAM_INTERVAL_SEC,
            first=2,
            name=name,
            data={"chat_id": int(chat_id)},
        )


def main() -> None:
    logging.basicConfig(
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        level=logging.INFO,
    )

    config = _load_config()
    app = Application.builder().token(config.token).build()

    app.bot_data["config"] = config
    app.bot_data["store"] = FeedbackStore(config.feedback_path)
    app.bot_data["model"] = TinyFeedbackModel.load_or_create(config.model_path)
    app.bot_data["state"] = BotStateStore(config.state_path)
    app.bot_data["recent_generations"] = {}
    app.bot_data["pending_inputs"] = {}
    app.bot_data["training_waiting"] = {}

    _schedule_jobs(app, config.timezone)
    _schedule_training_jobs(app, app.bot_data["state"])

    app.add_handler(MessageHandler(filters.ALL, audit_message_router), group=-1)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(callback_router))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, text_router))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.Sticker.ALL, text_router))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.Document.ALL, document_router))
    app.add_handler(MessageHandler((~filters.ChatType.PRIVATE) & filters.TEXT & ~filters.COMMAND, group_text_router))
    app.add_handler(
        MessageHandler(
            (~filters.ChatType.PRIVATE) & (filters.VOICE | filters.VIDEO | filters.VIDEO_NOTE),
            media_router,
        )
    )
    app.add_error_handler(on_error)

    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
