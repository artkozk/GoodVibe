
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
from datetime import time as dtime
from typing import Awaitable, Callable
from zoneinfo import ZoneInfo

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
WISH_HEART_EMOJIS = ("❤️", "💖", "💗", "💕", "💞", "💘", "💝", "🩷")
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
    group_activity_mode: str = "normal",
    style_examples_count: int = 0,
) -> InlineKeyboardMarkup:
    access_label = "🔒 Только админ: ВКЛ" if admin_only_mode else "🔓 Только админ: ВЫКЛ"
    schedule_label = f"🕒 Рассылка: {_mode_ru(schedule_mode)}"
    reaction_label = f"🔥 Реакции на voice/video: {_on_off_ru(group_reaction_mode)}"
    chat_mode_label = f"💬 Общение в группе: {_on_off_ru(group_chat_mode)}"
    group_activity_label = f"🎚 Активность: {_group_activity_ru(group_activity_mode)}"
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
            [InlineKeyboardButton(group_activity_label, callback_data="menu|toggle_group_activity")],
            [
                InlineKeyboardButton("📥 Импорт стиля общения", callback_data="menu|import_style_examples"),
                InlineKeyboardButton(f"🧹 Очистить стиль ({style_examples_count})", callback_data="menu|clear_style_examples"),
            ],
            [InlineKeyboardButton(f"🚫 Черный список ({blacklist_count})", callback_data="menu|blacklist")],
            [InlineKeyboardButton("📌 Сделать этот чат группой", callback_data="menu|set_group_here")],
            [InlineKeyboardButton("⭐ Премиум и эмодзи", callback_data="menu|premium")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="menu|home")],
        ]
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
            openai_text = await generate_openai_wish(
                cfg=_openai_runtime_cfg(config),
                kind=kind,
                mode=mode,
                audience=audience,
                person_name=person_name if audience == "single" else "",
                person_instructions=person_instructions if audience == "single" else "",
                blacklist=blacklist,
                recent_texts=recent_texts[-12:],
                preferred_emojis=liked_emojis[-40:],
            )
            candidate_text = " ".join(openai_text.split())
            reject_reason = ""
            if not candidate_text:
                reject_reason = "empty"
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
    group_activity_mode = state.get_group_activity_mode()
    style_examples_count = len(state.get_style_examples())
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
            f"- 🎚 активность ответов: {_group_activity_ru(group_activity_mode)}\n"
            f"- примеров стиля из экспорта: {style_examples_count}\n"
            f"- GPT генерация: {'ВКЛ' if config.openai_enabled else 'ВЫКЛ'}\n"
            f"- GPT модель: {config.openai_model}\n"
            f"- GPT endpoint: {config.openai_base_url}"
        ),
        reply_markup=_settings_keyboard(
            mode,
            person.get("name", "общий вариант"),
            schedule_mode,
            admin_only_mode,
            exceptions_count,
            blacklist_count,
            group_reaction_mode,
            group_chat_mode,
            group_activity_mode,
            style_examples_count,
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
        "- «🎚 Активность» — тихо / норм / каждое сообщение / только вопрос.\n"
        "- «📥 Импорт стиля общения» — загрузи JSON/TXT экспорт, чтобы бот писал ближе к твоему стилю."
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
    group_activity_mode = state.get_group_activity_mode()
    style_examples_count = len(state.get_style_examples())
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
        f"- 🎚 активность ответов: {_group_activity_ru(group_activity_mode)}\n"
        f"- примеров стиля из экспорта: {style_examples_count}\n"
        f"- режим «только админ»: {_on_off_ru(admin_only_mode)}\n"
        f"- пользователей в исключениях: {exceptions_count}\n"
        f"- текущий ID админа: {admin_id or 'не задан'}\n"
        f"- текущий ID группы: {group_id or 'не задан'}\n"
        f"- GPT генерация: {_on_off_ru(config.openai_enabled)}\n"
        f"- GPT модель: {config.openai_model}"
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
    await update.effective_message.reply_text(text)
    await _show_home(update, context)


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
) -> bool:
    mode = str(activity_mode or "").strip().lower()
    if mode == "active":
        return True
    if mode == "question_only":
        return _looks_like_question(text)

    if _is_direct_group_addressing(context=context, message=message, text=text):
        return True

    chance = 0.10 if mode == "quiet" else GROUP_CHAT_REPLY_CHANCE
    if _looks_like_question(text):
        chance += 0.20
    return random.random() < min(1.0, chance)


def _local_group_reply(*, incoming_text: str, user_name: str) -> str:
    question_replies = [
        "Сложный вопрос, но звучит интересно.",
        "Я бы попробовал так, как тебе сейчас спокойнее.",
        "Нормальный вопрос, давай разберем по шагам.",
        "Я бы пошел через самый простой вариант.",
    ]
    plain_replies = [
        "Да, понимаю тебя.",
        "Звучит хорошо, мне нравится ход мысли.",
        "Поддерживаю, нормальная идея.",
        "Окей, принято.",
        "Хорошо сказано.",
        "Согласен, это важно.",
    ]
    pool = question_replies if "?" in incoming_text else plain_replies
    prefix = f"{user_name}, " if user_name else ""
    return f"{prefix}{random.choice(pool)}"


async def _generate_group_chat_reply(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    incoming_text: str,
    user_name: str,
) -> tuple[str, str]:
    state = _get_state(context)
    config = _get_config(context)
    recent_dialogue = _recent_group_dialogue(context, chat_id=chat_id, limit=14)
    style_examples = state.get_style_examples()
    blacklist = state.get_blacklist_phrases()

    if config.openai_enabled:
        try:
            text = await generate_openai_chat_reply(
                cfg=_openai_runtime_cfg(config),
                incoming_text=incoming_text,
                recent_dialogue=recent_dialogue,
                style_examples=style_examples,
                bot_name=_chat_name_or_title(getattr(context, "bot", None)),
            )
            clean = " ".join(str(text).split())
            if clean:
                if _contains_blacklisted_phrase(clean, blacklist):
                    stripped = _strip_blacklisted_sentences(clean, blacklist)
                    if stripped and not _contains_blacklisted_phrase(stripped, blacklist):
                        return stripped, "openai_stripped"
                else:
                    return clean, "openai"
        except OpenAIWishError as exc:
            logging.warning("Group chat LLM failed: %s", exc)
        except Exception as exc:
            logging.exception("Unexpected group chat LLM error: %s", exc)

    fallback = _local_group_reply(incoming_text=incoming_text, user_name=user_name)
    if _contains_blacklisted_phrase(fallback, blacklist):
        fallback = "Понял тебя."
    return fallback, "local"


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

    if _is_wish_like_text(text):
        await _set_wish_heart_reaction(context=context, chat_id=chat.id, message_id=message.message_id)

    user_name = str(getattr(user, "first_name", "") or getattr(user, "username", "") or "").strip()
    _append_group_dialogue(context, chat_id=chat.id, line=f"{user_name or 'Участник'}: {text}")

    if not state.is_group_chat_mode():
        return

    wish_kind = _detect_group_wish_kind(text)

    activity_mode = state.get_group_activity_mode()
    if wish_kind:
        reply_text = _group_special_wish_reply(kind=wish_kind, user_name=user_name)
        reply_source = f"special_{wish_kind}"
    else:
        cooldown_sec = _group_reply_cooldown_for_mode(activity_mode)
        if cooldown_sec > 0 and not _is_group_reply_cooldown_ready(context, chat_id=chat.id, cooldown_sec=cooldown_sec):
            return
        if not _should_reply_in_group(context=context, message=message, text=text, activity_mode=activity_mode):
            return
        reply_text, reply_source = await _generate_group_chat_reply(
            context=context,
            chat_id=chat.id,
            incoming_text=text,
            user_name=user_name,
        )
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
    logging.info("Group chat reply source=%s chat_id=%s message_id=%s", reply_source, chat.id, sent.message_id)


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
        await message.reply_text("Файл не найден. Отправь JSON/TXT экспортом.")
        return

    file_name = str(getattr(document, "file_name", "") or "").strip()
    ext = os.path.splitext(file_name.lower())[1]
    if ext not in {".json", ".txt"}:
        _set_pending_input(context, chat_id=chat.id, user_id=user.id, action="import_style_examples")
        await message.reply_text("Поддерживаю только JSON или TXT.")
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
        else:
            with open(tmp_path, "r", encoding="utf-8", errors="ignore") as handle:
                raw_text = handle.read()
            examples = _extract_style_examples_from_text(raw_text)

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
        await message.reply_text("Не смог прочитать файл. Попробуй другой JSON/TXT.")
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
            exceptions_count = len(state.list_access_exceptions())
            blacklist_count = len(state.get_blacklist_phrases())
            await query.answer(f"Режим: {_mode_ru(new_mode)}")
            try:
                await query.message.edit_reply_markup(
                    reply_markup=_settings_keyboard(
                        new_mode,
                        person.get("name", "общий вариант"),
                        state.get_schedule_mode(),
                        state.is_admin_only_mode(),
                        exceptions_count,
                        blacklist_count,
                        state.is_group_fire_reaction_mode(),
                        state.is_group_chat_mode(),
                        state.get_group_activity_mode(),
                        len(state.get_style_examples()),
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
            exceptions_count = len(state.list_access_exceptions())
            blacklist_count = len(state.get_blacklist_phrases())
            await query.answer(f"Режим рассылки: {_mode_ru(new_schedule_mode)}")
            try:
                await query.message.edit_reply_markup(
                    reply_markup=_settings_keyboard(
                        mode,
                        person.get("name", "общий вариант"),
                        new_schedule_mode,
                        state.is_admin_only_mode(),
                        exceptions_count,
                        blacklist_count,
                        state.is_group_fire_reaction_mode(),
                        state.is_group_chat_mode(),
                        state.get_group_activity_mode(),
                        len(state.get_style_examples()),
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
            exceptions_count = len(state.list_access_exceptions())
            blacklist_count = len(state.get_blacklist_phrases())
            await query.answer(f"Только админ: {_on_off_ru(enabled)}")
            try:
                await query.message.edit_reply_markup(
                    reply_markup=_settings_keyboard(
                        mode,
                        person.get("name", "общий вариант"),
                        state.get_schedule_mode(),
                        enabled,
                        exceptions_count,
                        blacklist_count,
                        state.is_group_fire_reaction_mode(),
                        state.is_group_chat_mode(),
                        state.get_group_activity_mode(),
                        len(state.get_style_examples()),
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
            exceptions_count = len(state.list_access_exceptions())
            blacklist_count = len(state.get_blacklist_phrases())
            await query.answer(f"🔥 Реакции: {_on_off_ru(enabled)}")
            try:
                await query.message.edit_reply_markup(
                    reply_markup=_settings_keyboard(
                        mode,
                        person.get("name", "общий вариант"),
                        state.get_schedule_mode(),
                        state.is_admin_only_mode(),
                        exceptions_count,
                        blacklist_count,
                        enabled,
                        state.is_group_chat_mode(),
                        state.get_group_activity_mode(),
                        len(state.get_style_examples()),
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
            exceptions_count = len(state.list_access_exceptions())
            blacklist_count = len(state.get_blacklist_phrases())
            await query.answer(f"💬 Общение: {_on_off_ru(enabled)}")
            try:
                await query.message.edit_reply_markup(
                    reply_markup=_settings_keyboard(
                        mode,
                        person.get("name", "общий вариант"),
                        state.get_schedule_mode(),
                        state.is_admin_only_mode(),
                        exceptions_count,
                        blacklist_count,
                        state.is_group_fire_reaction_mode(),
                        enabled,
                        state.get_group_activity_mode(),
                        len(state.get_style_examples()),
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
            exceptions_count = len(state.list_access_exceptions())
            blacklist_count = len(state.get_blacklist_phrases())
            await query.answer(f"🎚 Активность: {_group_activity_ru(new_mode)}")
            try:
                await query.message.edit_reply_markup(
                    reply_markup=_settings_keyboard(
                        mode,
                        person.get("name", "общий вариант"),
                        state.get_schedule_mode(),
                        state.is_admin_only_mode(),
                        exceptions_count,
                        blacklist_count,
                        state.is_group_fire_reaction_mode(),
                        state.is_group_chat_mode(),
                        new_mode,
                        len(state.get_style_examples()),
                    )
                )
            except Exception:
                pass
            return
        if action == "import_style_examples":
            if not await _ensure_admin(update, context):
                return
            await query.answer()
            _set_pending_input(context, chat_id=chat_id, user_id=query.from_user.id, action="import_style_examples")
            await query.message.reply_text(
                "Отправь JSON/TXT файл с экспортом переписки.\n"
                "JSON: telegram desktop export (result.json).\n"
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
