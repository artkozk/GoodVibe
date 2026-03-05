from __future__ import annotations

import html
import random
from typing import Sequence

TEMPLATES = [
    "{greeting}, {appeal}. {wish}. {comfort}. {signoff} {emojis}",
    "{greeting}, {appeal}! {comfort}. {dream}. {signoff} {emojis}",
    "{greeting}, {appeal}. {wish}. {dream}. {care}. {emojis}",
    "{greeting}, {appeal}. {care}. {comfort}. {signoff} {emojis}",
    "{greeting}, {appeal}. {dream}. {wish}. {signoff} {emojis}",
]

GREETINGS = [
    "Спокойной ночи",
    "Доброй ночи",
    "Сладких снов",
    "Теплой ночи",
    "Нежной ночи",
]

APPEALS = [
    "моя хорошая",
    "красотка",
    "солнышко",
    "звездочка",
    "девчонка",
]

WISHES = [
    "Пусть кроватка будет мягкой, как облачко",
    "Пусть подушка будет мягче облачка",
    "Пусть одеяло укроет тебя так, будто само небо обнимает",
    "Пусть в комнате будет тихо, уютно и спокойно",
    "Пусть тревоги уходят, как волны обратно в море",
    "Пусть сон заберет усталость и оставит только легкость",
    "Пусть мысли станут мягкими и светлыми",
    "Пусть сердцу станет спокойно и тепло",
    "Пусть утро встретит тебя очень бережно",
]

COMFORT_LINES = [
    "День уже позади, и сейчас можно просто выдохнуть",
    "Никуда не нужно спешить, только отдыхать",
    "Если бы мог, укутал бы тебя в плед и налил чай с медом",
    "Пусть время немного замедлится и даст тебе тишину",
    "Если сегодня было сложно, ночь все аккуратно разгладит",
    "Пусть рядом останутся только уют и спокойствие",
]

DREAM_LINES = [
    "Пусть приснится летняя прогулка по полю и теплый ветер",
    "Пусть приснится вечер у моря в цветах заката",
    "Пусть приснятся смех, свет и любимые люди рядом",
    "Пусть снится что-то доброе, легкое и очень уютное",
    "Пусть во сне все будет так, как тебе хочется",
]

CARE_LINES = [
    "Кому-то очень важно, чтобы ты засыпала с улыбкой",
    "Я мысленно обнимаю тебя и отправляю много тепла",
    "Если вдруг проснешься ночью, просто помни: все хорошо",
    "Ты умеешь согревать одним словом, так что береги себя",
    "Пусть этой ночью мир будет только про твой отдых",
]

SIGNOFFS = [
    "Обнимаю крепко",
    "Я рядом мыслями",
    "Пиши утром, как спалось",
    "Пусть эта ночь будет твоей перезагрузкой",
    "Отдыхай, ты умница",
]

FALLBACK_EMOJIS = [
    "🩷",
    "💖",
    "💗",
    "💞",
    "💕",
    "🫶",
    "🤍",
    "✨",
    "🌟",
    "⭐",
    "☁️",
    "🧸",
    "🍯",
    "☕",
    "🍵",
    "🌸",
    "🌷",
    "🪻",
    "🦋",
    "🐰",
    "🐣",
    "🐱",
    "🍓",
    "🍪",
    "🍬",
    "🌈",
    "🎀",
    "💫",
    "🌙",
    "🌌",
    "🫧",
]


def _tg_emoji_tag(emoji_id: str, fallback_symbol: str) -> str:
    safe_id = html.escape(emoji_id, quote=True)
    safe_fallback = html.escape(fallback_symbol)
    return f'<tg-emoji emoji-id="{safe_id}">{safe_fallback}</tg-emoji>'


def _build_emoji_block(rng: random.Random, premium_emoji_ids: Sequence[str]) -> str:
    count = rng.randint(3, 6)
    if premium_emoji_ids:
        tags = [
            _tg_emoji_tag(rng.choice(premium_emoji_ids), rng.choice(FALLBACK_EMOJIS))
            for _ in range(count)
        ]
        return " ".join(tags)

    return " ".join(rng.choice(FALLBACK_EMOJIS) for _ in range(count))


def compose_goodnight(
    premium_emoji_ids: Sequence[str] | None = None,
    rng: random.Random | None = None,
) -> str:
    """Compose one emotional goodnight phrase in HTML format for Telegram."""
    rnd = rng or random.Random()
    premium_ids = [x.strip() for x in (premium_emoji_ids or []) if x.strip()]

    parts = {
        "greeting": html.escape(rnd.choice(GREETINGS)),
        "appeal": html.escape(rnd.choice(APPEALS)),
        "wish": html.escape(rnd.choice(WISHES)),
        "comfort": html.escape(rnd.choice(COMFORT_LINES)),
        "dream": html.escape(rnd.choice(DREAM_LINES)),
        "care": html.escape(rnd.choice(CARE_LINES)),
        "signoff": html.escape(rnd.choice(SIGNOFFS)),
        "emojis": _build_emoji_block(rnd, premium_ids),
    }
    template = rnd.choice(TEMPLATES)
    return template.format(**parts)


def compose_batch(
    amount: int,
    premium_emoji_ids: Sequence[str] | None = None,
    seed: int | None = None,
) -> list[str]:
    rnd = random.Random(seed)
    return [compose_goodnight(premium_emoji_ids=premium_emoji_ids, rng=rnd) for _ in range(amount)]
