from __future__ import annotations

import html
import random
import re
from dataclasses import dataclass
from functools import reduce
from operator import mul
from typing import Protocol, Sequence


VALID_KINDS = {"night", "morning"}
VALID_MODES = {"short", "standard", "context"}


class ScoreModel(Protocol):
    def predict(self, features: dict[str, float]) -> float:
        ...


@dataclass(frozen=True)
class PatternSet:
    templates: list[str]
    greetings: list[str]
    appeals: list[str]
    wishes: list[str]
    comforts: list[str]
    dreams: list[str]
    cares: list[str]
    signoffs: list[str]


@dataclass(frozen=True)
class StorySet:
    templates: list[str]
    greetings: list[str]
    appeals: list[str]
    openers: list[str]
    scenes: list[str]
    wishes: list[str]
    cares: list[str]
    endings: list[str]


@dataclass(frozen=True)
class GeneratedWish:
    kind: str
    mode: str
    person_id: int
    person_name: str
    text: str
    template_idx: int
    picks: dict[str, int]
    features: dict[str, float]
    emoji_count: int

    def to_record(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "mode": self.mode,
            "person_id": self.person_id,
            "person_name": self.person_name,
            "text": self.text,
            "template_idx": self.template_idx,
            "picks": dict(self.picks),
            "features": dict(self.features),
            "emoji_count": self.emoji_count,
        }


def _expand_templates_for_kind(kind: str, base_templates: Sequence[str], target_count: int = 920) -> list[str]:
    out: list[str] = [str(x) for x in base_templates if str(x).strip()]
    seen = set(out)
    if len(out) >= target_count:
        return out

    openers_by_kind = {
        "night": [
            "{greeting}, {appeal}.",
            "{greeting}, {appeal}!",
            "{greeting}, {appeal} -",
            "{greeting}, {appeal}, сегодня все только про твой отдых.",
            "{greeting}.",
            "{greeting}, {appeal}. На сегодня хватит.",
            "{greeting}, {appeal}. Просто выдохни.",
        ],
        "morning": [
            "{greeting}, {appeal}.",
            "{greeting}, {appeal}!",
            "{greeting}, {appeal} -",
            "{greeting}, {appeal}, сегодня пусть день будет бережным к тебе.",
            "{greeting}.",
            "{greeting}, {appeal}. Пусть утро начнется спокойно.",
            "{greeting}, {appeal}. Легкого старта тебе.",
        ],
    }
    openings = openers_by_kind.get(kind, openers_by_kind["night"])

    clause_orders = [
        ("{wish}", "{comfort}", "{dream}", "{care}", "{signoff}"),
        ("{comfort}", "{wish}", "{care}", "{dream}", "{signoff}"),
        ("{dream}", "{wish}", "{comfort}", "{care}", "{signoff}"),
        ("{care}", "{comfort}", "{wish}", "{dream}", "{signoff}"),
        ("{wish}", "{dream}", "{care}", "{comfort}", "{signoff}"),
        ("{comfort}", "{dream}", "{wish}", "{care}", "{signoff}"),
        ("{dream}", "{care}", "{wish}", "{comfort}", "{signoff}"),
        ("{care}", "{wish}", "{dream}", "{comfort}", "{signoff}"),
        ("{wish}", "{care}", "{comfort}", "{dream}", "{signoff}"),
        ("{comfort}", "{care}", "{dream}", "{wish}", "{signoff}"),
        ("{wish}", "{dream}", "{comfort}", "{care}", "{signoff}"),
        ("{comfort}", "{wish}", "{dream}", "{care}", "{signoff}"),
        ("{dream}", "{comfort}", "{wish}", "{care}", "{signoff}"),
        ("{care}", "{dream}", "{wish}", "{comfort}", "{signoff}"),
        ("{wish}", "{care}", "{dream}", "{comfort}", "{signoff}"),
        ("{comfort}", "{care}", "{wish}", "{dream}", "{signoff}"),
        ("{dream}", "{wish}", "{care}", "{comfort}", "{signoff}"),
        ("{care}", "{wish}", "{comfort}", "{dream}", "{signoff}"),
        ("{wish}", "{comfort}", "{care}", "{dream}", "{signoff}"),
        ("{dream}", "{comfort}", "{care}", "{wish}", "{signoff}"),
    ]
    # Keep transitions natural: no artificial prefixes like
    # "И в итоге:", "А еще:", "Плюс:".
    bridge_a = [""]
    bridge_b = [""]
    bridge_c = [""]
    end_marks = [".", "!", "..."]

    for opener in openings:
        for order in clause_orders:
            c1, c2, c3, c4, c5 = order
            for b1 in bridge_a:
                for b2 in bridge_b:
                    for b3 in bridge_c:
                        for end in end_marks:
                            variants = [
                                f"{opener} {c1}. {b1}{c2}. {b2}{c3}. {b3}{c4}. {c5}{end} {{emojis}}",
                                f"{opener} {c1}. {b1}{c2}. {b2}{c3}. {c5}{end} {{emojis}}",
                                f"{opener} {c1}. {b1}{c2}. {b3}{c4}. {c5}{end} {{emojis}}",
                            ]
                            for template in variants:
                                clean = " ".join(template.split())
                                if clean in seen:
                                    continue
                                out.append(clean)
                                seen.add(clean)
                                if len(out) >= target_count:
                                    return out
    return out


def _compose_opened_wishes(openers: Sequence[str], sleep_wishes: Sequence[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for opener in openers:
        for tail in sleep_wishes:
            phrase = " ".join(f"{opener} {tail}".split())
            if phrase in seen:
                continue
            seen.add(phrase)
            out.append(phrase)
    return out


NIGHT_GREETINGS = [
    "Спокойной ночи",
    "Сладких снов",
    "Доброй ночи",
    "Хорошего сна",
    "Приятных снов",
    "Спи спокойно",
    "Спокойного сна",
    "Хорошей ночи",
    "Пусть тебе хорошо спится",
    "Пусть сон будет спокойным",
]

NIGHT_APPEALS_FEMALE = [
    "подруга",
    "моя хорошая",
    "солнышко",
    "родная",
    "дорогая",
    "моя дорогая",
    "милая",
    "красавица",
    "девочка",
    "моя милая",
    "моя родная",
    "светлая моя",
    "хорошая моя",
    "тыковка",
    "звезда моя",
]

NIGHT_APPEALS_MALE = [
    "друг",
    "брат",
    "бро",
    "дружище",
    "родной",
    "дорогой",
    "мой хороший",
    "мой друг",
    "братец",
    "человек",
    "дружок",
    "мой дорогой",
]

NIGHT_APPEALS_NEUTRAL = [
    "друг",
    "подруга",
    "человек",
    "мой друг",
    "моя хорошая",
]

NIGHT_OPENERS = [
    "Пусть",
    "Пускай",
    "Пусть этой ночью",
    "Пусть сегодня",
    "Пускай сегодня",
]

NIGHT_SLEEP_WISHES = [
    "сон будет крепким",
    "сон будет спокойным",
    "сон будет глубоким",
    "тебе будет легко уснуть",
    "тебе хорошо спится",
    "ночь принесет отдых",
    "ночь снимет усталость",
    "ты хорошо выспишься",
    "силы восстановятся",
    "тревоги отступят",
    "мысли улягутся",
    "голова отдохнет",
    "душа успокоится",
    "сердце станет спокойнее",
    "утро встретит тебя легко",
]

NIGHT_COMFORT_LINES = [
    "День уже закончился, теперь можно просто отдохнуть",
    "Все на сегодня уже позади",
    "Пора выключить мысли и дать себе отдых",
    "Пусть эта ночь немного разгрузит тебя",
    "Сейчас главное - выдохнуть и отдыхать",
    "Никуда не нужно спешить до самого утра",
    "Пусть все лишнее останется за пределами этой ночи",
    "Пусть вечерняя тишина поможет тебе успокоиться",
    "Сегодня уже достаточно, остальное потом",
    "Просто отдохни как следует",
    "Пусть эта ночь будет временем восстановления",
    "Ночь для того и нужна, чтобы вернуть силы",
    "Отложи все мысли на завтра",
    "Сейчас лучшее, что можно сделать, - хорошо выспаться",
    "Пусть тело и голова наконец расслабятся",
    "Если бы мог, укутал бы тебя в плед и налил чай с медом",
]

NIGHT_DREAM_LINES = [
    "Пусть приснится что-то доброе",
    "Пусть приснится что-то светлое",
    "Пусть приснятся спокойные сны",
    "Пусть во сне будет легко и спокойно",
    "Пусть приснится что-то хорошее",
    "Пусть сон будет без тревог",
    "Пусть этой ночью тебе снится только хорошее",
    "Пусть сны будут приятными",
    "Пусть ночь пройдет тихо и спокойно",
    "Пусть сон принесет тебе легкость",
    "Пусть приснится что-то, от чего захочется улыбнуться",
    "Пусть во сне будет уютно и спокойно",
]

NIGHT_CARE_LINES_FEMALE = [
    "Береги себя",
    "Отдыхай как следует",
    "Я очень хочу, чтобы ты выспалась",
    "Пусть тебе этой ночью будет спокойно",
    "Ты заслужила нормальный отдых",
    "Пусть у тебя будет тихая и спокойная ночь",
    "Очень хочется, чтобы тебе стало легче",
    "Пусть эта ночь тебя немного восстановит",
    "Обнимаю тебя мысленно",
    "Я рядом мыслями",
    "Пусть тебе будет тепло и спокойно",
    "Спи и ни о чем не переживай",
]

NIGHT_CARE_LINES_MALE = [
    "Отдыхай как следует",
    "Набирайся сил",
    "Выспись нормально",
    "Пусть ночь тебя перезагрузит",
    "Завтра будет новый день",
    "Хорошо отдохни",
    "Пусть голова отдохнет",
    "Пусть эта ночь вернет тебе силы",
    "Спи спокойно",
    "Нормально выспись",
    "Пусть утро будет бодрее",
    "Перезагрузи голову и отдыхай",
]

NIGHT_SIGNOFFS_SOFT = [
    "Обнимаю",
    "Крепко обнимаю",
    "Я рядом мыслями",
    "Спи спокойно",
    "Хорошего сна",
    "До завтра",
    "Сладких снов",
    "Спокойной ночи",
    "Пусть ночь пройдет спокойно",
    "Пиши утром, как спалось",
]

NIGHT_SIGNOFFS_FRIENDLY = [
    "Спокойной",
    "До завтра",
    "Отдыхай",
    "Высыпайся",
    "Спи спокойно",
    "Набирайся сил",
    "Хорошего сна",
    "Пусть утро будет легким",
    "Увидимся завтра",
    "Потом расскажешь, как спалось",
]

NIGHT_TEMPLATES_FEMALE = [
    "{greeting}, {appeal}. {comfort}. {dream}. {signoff}. {emojis}",
    "{greeting}, {appeal}. {wish}. {signoff}. {emojis}",
    "{greeting}. {comfort}. {care}. {signoff}. {emojis}",
    "{greeting}, {appeal}. {wish}. {care}. {emojis}",
    "{greeting}. {dream}. {care}. {signoff}. {emojis}",
]

NIGHT_TEMPLATES_MALE = [
    "{greeting}, {appeal}. {comfort}. {signoff}. {emojis}",
    "{greeting}. {wish}. {care}. {emojis}",
    "{greeting}, {appeal}. {dream}. {signoff}. {emojis}",
    "{greeting}. {comfort}. {care}. {emojis}",
    "{greeting}, {appeal}. {wish}. {signoff}. {emojis}",
]

NIGHT_WISHES = _compose_opened_wishes(NIGHT_OPENERS, NIGHT_SLEEP_WISHES)

NIGHT_STANDARD = PatternSet(
    templates=NIGHT_TEMPLATES_FEMALE + NIGHT_TEMPLATES_MALE,
    greetings=NIGHT_GREETINGS,
    appeals=NIGHT_APPEALS_NEUTRAL,
    wishes=NIGHT_WISHES,
    comforts=NIGHT_COMFORT_LINES,
    dreams=NIGHT_DREAM_LINES,
    cares=list(dict.fromkeys(NIGHT_CARE_LINES_FEMALE + NIGHT_CARE_LINES_MALE)),
    signoffs=list(dict.fromkeys(NIGHT_SIGNOFFS_SOFT + NIGHT_SIGNOFFS_FRIENDLY)),
)

NIGHT_STANDARD_FEMALE = PatternSet(
    templates=NIGHT_TEMPLATES_FEMALE,
    greetings=NIGHT_GREETINGS,
    appeals=NIGHT_APPEALS_FEMALE,
    wishes=NIGHT_WISHES,
    comforts=NIGHT_COMFORT_LINES,
    dreams=NIGHT_DREAM_LINES,
    cares=NIGHT_CARE_LINES_FEMALE,
    signoffs=NIGHT_SIGNOFFS_SOFT,
)

NIGHT_STANDARD_MALE = PatternSet(
    templates=NIGHT_TEMPLATES_MALE,
    greetings=NIGHT_GREETINGS,
    appeals=NIGHT_APPEALS_MALE,
    wishes=NIGHT_WISHES,
    comforts=NIGHT_COMFORT_LINES,
    dreams=NIGHT_DREAM_LINES,
    cares=NIGHT_CARE_LINES_MALE,
    signoffs=NIGHT_SIGNOFFS_FRIENDLY,
)

NIGHT_APPEALS_GROUP = [
    "девочки",
    "красавицы",
    "дорогие",
    "милые",
]

NIGHT_GREETINGS_GROUP = [
    "Спокойной ночи",
    "Сладких снов",
    "Доброй ночи",
    "Хорошего сна",
    "Приятных снов",
    "Пусть вам хорошо спится",
    "Пусть сон будет спокойным",
]

NIGHT_WISHES_GROUP = [
    "Пусть вы спокойно отдохнете этой ночью",
    "Пусть вам будет легко уснуть",
    "Пусть ваши мысли улягутся и станет тише внутри",
    "Пусть эта ночь снимет с вас усталость",
    "Пусть утро встретит вас более бодрыми",
]

NIGHT_COMFORT_GROUP = [
    "День уже закончился, теперь вам можно просто выдохнуть",
    "Никуда не нужно спешить до самого утра",
    "Пусть эта ночь пройдет без лишней суеты",
    "Пусть все лишнее останется за пределами этой ночи",
    "Отложите все мысли на завтра и дайте себе отдых",
]

NIGHT_DREAM_GROUP = [
    "Пусть вам приснится что-то хорошее",
    "Пусть ваши сны будут спокойными",
    "Пусть ночь пройдет тихо и ровно",
    "Пусть сон будет без тревог",
]

NIGHT_CARE_GROUP = [
    "Берегите себя и отдыхайте как следует",
    "Пусть вам этой ночью будет спокойно",
    "Пусть у вас будет тихая и добротная ночь",
    "Я рядом мыслями, всем тепла и отдыха",
]

NIGHT_SIGNOFF_GROUP = [
    "Спокойной ночи вам",
    "Хорошего сна всем",
    "До завтра, девочки",
    "Пишите утром, как спалось",
]

NIGHT_TEMPLATES_GROUP = [
    "{greeting}, {appeal}. {comfort}. {dream}. {signoff}. {emojis}",
    "{greeting}. {wish}. {care}. {signoff}. {emojis}",
    "{greeting}, {appeal}. {comfort}. {care}. {emojis}",
    "{greeting}. {dream}. {care}. {signoff}. {emojis}",
]

NIGHT_STANDARD_GROUP = PatternSet(
    templates=NIGHT_TEMPLATES_GROUP,
    greetings=NIGHT_GREETINGS_GROUP,
    appeals=NIGHT_APPEALS_GROUP,
    wishes=NIGHT_WISHES_GROUP,
    comforts=NIGHT_COMFORT_GROUP,
    dreams=NIGHT_DREAM_GROUP,
    cares=NIGHT_CARE_GROUP,
    signoffs=NIGHT_SIGNOFF_GROUP,
)

MORNING_GREETINGS = [
    "Доброе утро",
    "С добрым утром",
    "Хорошего утра",
    "Пусть утро будет добрым",
    "Пусть утро начнется спокойно",
    "С новым утром",
    "Хорошего начала дня",
    "Пусть день начнется легко",
    "Приятного утра",
    "Пусть утро будет спокойным",
]

MORNING_APPEALS_FEMALE = [
    "подруга",
    "моя хорошая",
    "солнышко",
    "родная",
    "дорогая",
    "моя дорогая",
    "милая",
    "красавица",
    "девочка",
    "моя милая",
    "моя родная",
    "светлая моя",
    "хорошая моя",
    "звезда моя",
]

MORNING_APPEALS_MALE = [
    "друг",
    "брат",
    "бро",
    "дружище",
    "родной",
    "дорогой",
    "мой хороший",
    "мой друг",
    "братец",
    "человек",
    "мой дорогой",
]

MORNING_APPEALS_NEUTRAL = [
    "друг",
    "подруга",
    "мой друг",
    "моя хорошая",
    "родная",
]

MORNING_OPENERS = [
    "Пусть",
    "Пускай",
    "Хочу, чтобы",
    "Желаю, чтобы",
    "Пусть сегодня",
    "Пускай сегодня",
    "Пусть это утро",
]

MORNING_WISHES_CORE = [
    "утро начнется спокойно",
    "день начнется легко",
    "тебе сегодня было хорошо с самого утра",
    "кофе будет вкусным",
    "сонливость уйдет быстрее",
    "сил сегодня хватило на все нужное",
    "день пройдет спокойно",
    "сегодня будет больше ясности и меньше суеты",
    "утро даст тебе заряд сил",
    "ты быстро проснешься и соберешься",
    "настроение с утра будет хорошим",
    "сегодня все будет идти ровнее",
    "утро получится легким",
    "мысли с утра будут ясными",
    "день сложится удачно",
]

MORNING_COMFORT_LINES = [
    "Новый день уже начался, пусть он будет к тебе добрым",
    "Не спеши с самого утра, входи в день спокойно",
    "Пусть утро будет без лишней суеты",
    "Пусть день начнется в твоем темпе",
    "Собирайся спокойно, все успеется",
    "Пусть это утро пройдет без раздражения и спешки",
    "Начни день бережно к себе",
    "Пусть с утра будет чуть больше тишины и порядка",
    "Пускай первая половина дня пройдет ровно",
    "Пусть утро даст тебе немного воздуха и спокойствия",
    "Сегодня лучше без рывков, просто спокойно войди в день",
    "Пусть с самого утра все идет без лишнего напряжения",
    "Не загружай себя сразу всем подряд",
    "Пусть утро будет временем нормального старта, а не гонки",
    "Пусть сегодняшний день начнется мягко и по-человечески",
]

MORNING_ENERGY_LINES = [
    "Пусть сил сегодня будет больше, чем вчера",
    "Пусть энергии хватит на все важное",
    "Пусть голова быстро проснется",
    "Пусть тело быстрее включится в день",
    "Пусть сегодня будет больше бодрости",
    "Пусть утро соберет тебя по кусочкам",
    "Пусть сонливость уйдет, а ясность останется",
    "Пусть день начнется с нормального ритма",
    "Пусть у тебя сегодня будет внутренний запас сил",
    "Пусть утро зарядит тебя на хороший день",
    "Пусть сегодня будет легче, чем ты думаешь",
    "Пусть бодрость приходит без спешки",
]

MORNING_CARE_FEMALE = [
    "Береги себя сегодня",
    "Пусть день будет к тебе бережным",
    "Не перегружай себя с самого утра",
    "Я хочу, чтобы у тебя сегодня был хороший день",
    "Пусть с утра тебе будет спокойно",
    "Пусть сегодня тебе будет легче",
    "Будь к себе помягче",
    "Пусть в этом дне будет больше хорошего",
    "Обнимаю тебя мысленно",
    "Я рядом мыслями",
    "Пусть сегодня у тебя будет повод улыбнуться",
    "Хочется, чтобы у тебя сегодня все сложилось",
]

MORNING_CARE_MALE = [
    "Набирай темп спокойно",
    "Пусть день будет нормальным и без перегруза",
    "Береги силы",
    "Пусть сегодня все решается без лишней нервотрепки",
    "Хорошего тебе хода на весь день",
    "Пусть с утра все складывается ровно",
    "Не рви с места, просто нормально войди в день",
    "Пусть день пройдет без лишней суеты",
    "Сил тебе на сегодня",
    "Пусть все важное получится",
    "Нормального тебе старта",
    "Удачи на весь день",
]

MORNING_SIGNOFF_SOFT = [
    "Обнимаю",
    "Крепко обнимаю",
    "Я рядом мыслями",
    "Хорошего дня",
    "Пусть день пройдет спокойно",
    "Пиши, как проснешься окончательно",
    "Пусть все сегодня получится",
    "Легкого тебе дня",
    "Удачного утра",
    "Пусть утро будет добрым",
]

MORNING_SIGNOFF_FRIENDLY = [
    "Хорошего дня",
    "Держись бодро",
    "Удачи сегодня",
    "Пусть день пойдет нормально",
    "Легкого старта",
    "Нормального тебе дня",
    "Пусть все сложится",
    "Давай, просыпайся",
    "Потом расскажешь, как день",
    "Не зевай там",
]

MORNING_TEMPLATES_FEMALE = [
    "{greeting}, {appeal}. {comfort}. {signoff}. {emojis}",
    "{greeting}, {appeal}. {wish}. {signoff}. {emojis}",
    "{greeting}. {comfort}. {care}. {signoff}. {emojis}",
    "{greeting}, {appeal}. {dream}. {care}. {emojis}",
    "{greeting}. {wish}. {care}. {signoff}. {emojis}",
]

MORNING_TEMPLATES_MALE = [
    "{greeting}, {appeal}. {comfort}. {signoff}. {emojis}",
    "{greeting}. {wish}. {care}. {emojis}",
    "{greeting}, {appeal}. {dream}. {signoff}. {emojis}",
    "{greeting}. {comfort}. {care}. {emojis}",
    "{greeting}, {appeal}. {wish}. {signoff}. {emojis}",
]

MORNING_WISHES = _compose_opened_wishes(MORNING_OPENERS, MORNING_WISHES_CORE)

MORNING_STANDARD = PatternSet(
    templates=MORNING_TEMPLATES_FEMALE + MORNING_TEMPLATES_MALE,
    greetings=MORNING_GREETINGS,
    appeals=MORNING_APPEALS_NEUTRAL,
    wishes=MORNING_WISHES,
    comforts=MORNING_COMFORT_LINES,
    dreams=MORNING_ENERGY_LINES,
    cares=list(dict.fromkeys(MORNING_CARE_FEMALE + MORNING_CARE_MALE)),
    signoffs=list(dict.fromkeys(MORNING_SIGNOFF_SOFT + MORNING_SIGNOFF_FRIENDLY)),
)

MORNING_STANDARD_FEMALE = PatternSet(
    templates=MORNING_TEMPLATES_FEMALE,
    greetings=MORNING_GREETINGS,
    appeals=MORNING_APPEALS_FEMALE,
    wishes=MORNING_WISHES,
    comforts=MORNING_COMFORT_LINES,
    dreams=MORNING_ENERGY_LINES,
    cares=MORNING_CARE_FEMALE,
    signoffs=MORNING_SIGNOFF_SOFT,
)

MORNING_STANDARD_MALE = PatternSet(
    templates=MORNING_TEMPLATES_MALE,
    greetings=MORNING_GREETINGS,
    appeals=MORNING_APPEALS_MALE,
    wishes=MORNING_WISHES,
    comforts=MORNING_COMFORT_LINES,
    dreams=MORNING_ENERGY_LINES,
    cares=MORNING_CARE_MALE,
    signoffs=MORNING_SIGNOFF_FRIENDLY,
)

MORNING_APPEALS_GROUP = [
    "девочки",
    "дорогие",
    "красавицы",
    "девчата",
]

MORNING_WISHES_GROUP = [
    "Пусть у вас утро начнется спокойно",
    "Пусть ваш день начнется легко",
    "Пусть вам сегодня будет проще собраться",
    "Пусть у вас с утра будет больше ясности и меньше суеты",
    "Пусть у вас хватит сил на все важное",
]

MORNING_COMFORT_GROUP = [
    "Новый день уже начался, пусть он будет к вам добрым",
    "Не спешите с самого утра, входите в день спокойно",
    "Пусть у вас утро пройдет без лишней суеты",
    "Пусть день начнется в вашем темпе",
    "Собирайтесь спокойно, все успеется",
]

MORNING_ENERGY_GROUP = [
    "Пусть у вас сегодня будет больше бодрости",
    "Пусть энергии вам хватит на все важное",
    "Пусть сонливость уйдет, а ясность останется",
    "Пусть у вас будет нормальный ритм с самого утра",
]

MORNING_CARE_GROUP = [
    "Берегите себя сегодня",
    "Пусть день будет к вам бережным",
    "Пусть вам сегодня будет легче",
    "Я рядом мыслями и желаю вам хорошего дня",
]

MORNING_SIGNOFF_GROUP = [
    "Хорошего дня вам",
    "Легкого старта, девочки",
    "Пусть у вас все сложится",
    "Потом расскажете, как день",
]

MORNING_TEMPLATES_GROUP = [
    "{greeting}, {appeal}. {comfort}. {signoff}. {emojis}",
    "{greeting}. {wish}. {care}. {emojis}",
    "{greeting}, {appeal}. {dream}. {signoff}. {emojis}",
    "{greeting}. {comfort}. {care}. {signoff}. {emojis}",
]

MORNING_STANDARD_GROUP = PatternSet(
    templates=MORNING_TEMPLATES_GROUP,
    greetings=MORNING_GREETINGS,
    appeals=MORNING_APPEALS_GROUP,
    wishes=MORNING_WISHES_GROUP,
    comforts=MORNING_COMFORT_GROUP,
    dreams=MORNING_ENERGY_GROUP,
    cares=MORNING_CARE_GROUP,
    signoffs=MORNING_SIGNOFF_GROUP,
)

# Build a large template bank to reduce repeats in real chat usage.
NIGHT_STANDARD = PatternSet(
    templates=_expand_templates_for_kind("night", NIGHT_STANDARD.templates, target_count=920),
    greetings=NIGHT_STANDARD.greetings,
    appeals=NIGHT_STANDARD.appeals,
    wishes=NIGHT_STANDARD.wishes,
    comforts=NIGHT_STANDARD.comforts,
    dreams=NIGHT_STANDARD.dreams,
    cares=NIGHT_STANDARD.cares,
    signoffs=NIGHT_STANDARD.signoffs,
)

NIGHT_STANDARD_FEMALE = PatternSet(
    templates=_expand_templates_for_kind("night", NIGHT_STANDARD_FEMALE.templates, target_count=920),
    greetings=NIGHT_STANDARD_FEMALE.greetings,
    appeals=NIGHT_STANDARD_FEMALE.appeals,
    wishes=NIGHT_STANDARD_FEMALE.wishes,
    comforts=NIGHT_STANDARD_FEMALE.comforts,
    dreams=NIGHT_STANDARD_FEMALE.dreams,
    cares=NIGHT_STANDARD_FEMALE.cares,
    signoffs=NIGHT_STANDARD_FEMALE.signoffs,
)

NIGHT_STANDARD_MALE = PatternSet(
    templates=_expand_templates_for_kind("night", NIGHT_STANDARD_MALE.templates, target_count=920),
    greetings=NIGHT_STANDARD_MALE.greetings,
    appeals=NIGHT_STANDARD_MALE.appeals,
    wishes=NIGHT_STANDARD_MALE.wishes,
    comforts=NIGHT_STANDARD_MALE.comforts,
    dreams=NIGHT_STANDARD_MALE.dreams,
    cares=NIGHT_STANDARD_MALE.cares,
    signoffs=NIGHT_STANDARD_MALE.signoffs,
)

NIGHT_STANDARD_GROUP = PatternSet(
    templates=NIGHT_STANDARD_GROUP.templates,
    greetings=NIGHT_STANDARD_GROUP.greetings,
    appeals=NIGHT_STANDARD_GROUP.appeals,
    wishes=NIGHT_STANDARD_GROUP.wishes,
    comforts=NIGHT_STANDARD_GROUP.comforts,
    dreams=NIGHT_STANDARD_GROUP.dreams,
    cares=NIGHT_STANDARD_GROUP.cares,
    signoffs=NIGHT_STANDARD_GROUP.signoffs,
)

MORNING_STANDARD = PatternSet(
    templates=_expand_templates_for_kind("morning", MORNING_STANDARD.templates, target_count=920),
    greetings=MORNING_STANDARD.greetings,
    appeals=MORNING_STANDARD.appeals,
    wishes=MORNING_STANDARD.wishes,
    comforts=MORNING_STANDARD.comforts,
    dreams=MORNING_STANDARD.dreams,
    cares=MORNING_STANDARD.cares,
    signoffs=MORNING_STANDARD.signoffs,
)

MORNING_STANDARD_FEMALE = PatternSet(
    templates=_expand_templates_for_kind("morning", MORNING_STANDARD_FEMALE.templates, target_count=920),
    greetings=MORNING_STANDARD_FEMALE.greetings,
    appeals=MORNING_STANDARD_FEMALE.appeals,
    wishes=MORNING_STANDARD_FEMALE.wishes,
    comforts=MORNING_STANDARD_FEMALE.comforts,
    dreams=MORNING_STANDARD_FEMALE.dreams,
    cares=MORNING_STANDARD_FEMALE.cares,
    signoffs=MORNING_STANDARD_FEMALE.signoffs,
)

MORNING_STANDARD_MALE = PatternSet(
    templates=_expand_templates_for_kind("morning", MORNING_STANDARD_MALE.templates, target_count=920),
    greetings=MORNING_STANDARD_MALE.greetings,
    appeals=MORNING_STANDARD_MALE.appeals,
    wishes=MORNING_STANDARD_MALE.wishes,
    comforts=MORNING_STANDARD_MALE.comforts,
    dreams=MORNING_STANDARD_MALE.dreams,
    cares=MORNING_STANDARD_MALE.cares,
    signoffs=MORNING_STANDARD_MALE.signoffs,
)

MORNING_STANDARD_GROUP = PatternSet(
    templates=MORNING_STANDARD_GROUP.templates,
    greetings=MORNING_STANDARD_GROUP.greetings,
    appeals=MORNING_STANDARD_GROUP.appeals,
    wishes=MORNING_STANDARD_GROUP.wishes,
    comforts=MORNING_STANDARD_GROUP.comforts,
    dreams=MORNING_STANDARD_GROUP.dreams,
    cares=MORNING_STANDARD_GROUP.cares,
    signoffs=MORNING_STANDARD_GROUP.signoffs,
)

NIGHT_ADVANCED = StorySet(
    templates=[
        "{greeting}, {appeal}. {opener}. {scene}. {wish}. {care}. {ending} {emojis}",
        "{greeting}, {appeal}. {scene}. {opener}. {wish}. {ending}. {care} {emojis}",
        "{greeting}, {appeal}. {opener}. {wish}. {scene}. {care}. {ending} {emojis}",
    ],
    greetings=[
        "Спокойной ночи",
        "Доброй ночи",
        "Нежной ночи",
        "Самой уютной ночи",
    ],
    appeals=[
        "подружка",
        "моя хорошая",
        "красотка",
        "солнышко",
        "девчонка",
    ],
    openers=[
        "Пусть этот вечер мягко снимет с тебя все лишнее",
        "Ночь как раз для того, чтобы выдохнуть и перезагрузиться",
        "Сейчас можно отпустить весь дневной шум и просто отдохнуть",
        "Сегодня пусть будет только тишина, тепло и твой комфорт",
    ],
    scenes=[
        "Представь мягкий плед, теплый чай и комнату, где наконец спокойно",
        "Пусть подушка станет облачком, а одеяло укроет как объятие неба",
        "Пусть за окном будет тишина, а в голове медленно станет ясно и легко",
        "Пусть все тревоги уйдут, как волны в море, и оставят ровный покой",
    ],
    wishes=[
        "Пусть сон придет быстро, будет глубоким и добрым",
        "Пусть сердце успокоится, а мысли станут светлыми",
        "Пусть эта ночь вернет тебе силы и внутреннее тепло",
        "Пусть тело расслабится, а усталость уйдет без остатка",
    ],
    cares=[
        "Ты заслуживаешь засыпать с легкостью и улыбкой",
        "Очень хочется, чтобы тебе было уютно и спокойно",
        "Если вдруг проснешься ночью, просто помни: все хорошо",
        "Я мысленно рядом и отправляю тебе много тепла",
    ],
    endings=[
        "Пусть до самого утра рядом будет ощущение, что мир сегодня на твоей стороне",
        "Пусть луна бережно хранит твой покой до первых лучей",
        "Пусть утро встретит тебя мягким светом и ясной головой",
        "Пусть ночь пройдет красиво, тихо и только про твой отдых",
    ],
)

MORNING_ADVANCED = StorySet(
    templates=[
        "{greeting}, {appeal}. {opener}. {scene}. {wish}. {care}. {ending} {emojis}",
        "{greeting}, {appeal}. {scene}. {opener}. {wish}. {ending}. {care} {emojis}",
        "{greeting}, {appeal}. {opener}. {wish}. {scene}. {care}. {ending} {emojis}",
    ],
    greetings=[
        "Доброе утро",
        "С добрым утром",
        "Уютного утра",
        "Солнечного утра",
    ],
    appeals=[
        "подружка",
        "моя хорошая",
        "красотка",
        "солнышко",
        "девчонка",
    ],
    openers=[
        "Пусть день начнется в спокойном темпе, без лишней суеты",
        "Утро - хороший момент, чтобы выдохнуть и настроиться на свое",
        "Сегодня хочется пожелать тебе легкого и теплого старта",
        "Пусть первые минуты дня будут мягкими и добрыми",
    ],
    scenes=[
        "Пусть чай или кофе сегодня будет особенно вкусным и уютным",
        "Пусть солнечный свет потихоньку соберет тебя в ресурсное состояние",
        "Пусть в делах будет порядок, а внутри - спокойная уверенность",
        "Пусть день раскроется как маршрут, где все нужное появляется вовремя",
    ],
    wishes=[
        "Пусть сегодня у тебя все складывается легче, чем ожидалось",
        "Пусть энергии хватит на важное, а настроение держится теплым",
        "Пусть тебе встречаются люди и события, которые поддерживают",
        "Пусть день подарит ощущение, что ты на своем месте",
    ],
    cares=[
        "Ты реально заслуживаешь бережного и классного дня",
        "Пусть тебе сегодня будет комфортно в себе и в своем ритме",
        "Если что-то пойдет не по плану, ты все равно справишься",
        "Ты умеешь делать мир теплее, не забывай это",
    ],
    endings=[
        "Пусть к вечеру останется приятная усталость и чувство, что день был не зря",
        "Пусть в течение дня будет больше поводов улыбнуться по-настоящему",
        "Пусть сегодня все важное двигается без надрыва и спешки",
        "Пусть этот день будет по-настоящему твоим и очень живым",
    ],
)

STANDARD_BY_KIND = {
    "night": NIGHT_STANDARD,
    "morning": MORNING_STANDARD,
}

ADVANCED_BY_KIND = {
    "night": NIGHT_ADVANCED,
    "morning": MORNING_ADVANCED,
}


def _person_profile(person_name: str, person_instructions: str) -> str:
    text = f"{person_name} {person_instructions}".lower().replace("ё", "е")
    tokens = re.findall(r"[a-zа-я0-9]+", text)
    has_female = any(
        token.startswith(("подруг", "дев", "жен", "мила", "дорог", "родн"))
        for token in tokens
    )
    has_male = any(
        token.startswith(("друг", "брат", "бро", "друж", "парен", "муж", "мальч"))
        for token in tokens
    )
    if has_female and not has_male:
        return "female"
    if has_male and not has_female:
        return "male"
    if has_female:
        return "female"
    if has_male:
        return "male"
    return "neutral"


def _standard_set_for_kind(kind: str, person_name: str, person_instructions: str, audience: str) -> PatternSet:
    if kind == "night":
        if audience == "group":
            return NIGHT_STANDARD_GROUP
        profile = _person_profile(person_name, person_instructions)
        if profile == "male":
            return NIGHT_STANDARD_MALE
        if profile == "female":
            return NIGHT_STANDARD_FEMALE
        return NIGHT_STANDARD

    if kind == "morning":
        if audience == "group":
            return MORNING_STANDARD_GROUP
        profile = _person_profile(person_name, person_instructions)
        if profile == "male":
            return MORNING_STANDARD_MALE
        if profile == "female":
            return MORNING_STANDARD_FEMALE
        return MORNING_STANDARD

    return STANDARD_BY_KIND[kind]


EMOJIS = [
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
    "☀️",
    "🌼",
    "🍀",
    "🍋",
    "🧃",
]

SHORT_TEMPLATES = {
    "night": [
        "{greeting}, {appeal}! {emojis}",
        "{greeting}! {emojis}",
        "{greeting}, {appeal}. {emojis}",
        "{greeting}, {appeal}, отдыхай сладко {emojis}",
        "{greeting}, {appeal}, пусть ночь будет мягкой {emojis}",
        "{greeting}, {appeal}, тебе самых уютных снов {emojis}",
        "{greeting}, {appeal}, пусть усталость уйдет {emojis}",
        "{greeting}, {appeal}, обнимаю мысленно {emojis}",
        "{greeting}, {appeal}, пусть все будет спокойно {emojis}",
        "{greeting}, {appeal}, до утра и тепла {emojis}",
    ],
    "morning": [
        "{greeting}, {appeal}! {emojis}",
        "{greeting}! {emojis}",
        "{greeting}, {appeal}. {emojis}",
        "{greeting}, {appeal}, легкого старта {emojis}",
        "{greeting}, {appeal}, пусть день пойдет мягко {emojis}",
        "{greeting}, {appeal}, больше поводов улыбнуться {emojis}",
        "{greeting}, {appeal}, пусть все складывается {emojis}",
        "{greeting}, {appeal}, теплого тебе дня {emojis}",
        "{greeting}, {appeal}, вперед в хорошем ритме {emojis}",
        "{greeting}, {appeal}, пусть сегодня везет {emojis}",
    ],
}

CONTEXT_EMOJI_HINTS: list[tuple[tuple[str, ...], tuple[str, ...]]] = [
    (("звезд", "неб", "луна", "ноч"), ("⭐", "✨", "🌙", "🌌")),
    (("утр", "солн", "рассвет", "луч"), ("☀️", "🌤️", "🌼", "✨")),
    (("чай", "коф", "напит"), ("☕", "🍵", "🧃")),
    (("обним", "тепл", "уют", "плед"), ("🫶", "🤍", "🧸", "💞")),
    (("сон", "сн", "отдых"), ("💤", "🌙", "🫧")),
    (("море", "волны", "океан"), ("🌊", "🫧", "✨")),
    (("цвет", "весн", "сад"), ("🌸", "🌷", "🪻", "🍀")),
]

CONTEXT_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def _tg_emoji_tag(emoji_id: str, fallback_symbol: str) -> str:
    safe_id = html.escape(emoji_id, quote=True)
    safe_fallback = html.escape(fallback_symbol)
    return f'<tg-emoji emoji-id="{safe_id}">{safe_fallback}</tg-emoji>'


def _pick_index(rng: random.Random, values: list[str]) -> tuple[int, str]:
    idx = rng.randrange(len(values))
    return idx, values[idx]


def _merge_emoji_pool(extra_emojis: Sequence[str] | None = None) -> list[str]:
    pool = list(EMOJIS)
    seen = set(pool)
    for raw in extra_emojis or []:
        item = str(raw).strip()
        if not item or item in seen:
            continue
        pool.append(item)
        seen.add(item)
    return pool


def _emoji_block(
    rng: random.Random,
    premium_emoji_ids: Sequence[str],
    emoji_pool: list[str],
    *,
    min_count: int,
    max_count: int,
) -> tuple[str, list[str], int]:
    count = rng.randint(min_count, max_count)
    count = min(count, len(emoji_pool))
    symbols = rng.sample(emoji_pool, k=count)

    if premium_emoji_ids:
        pool = [x for x in premium_emoji_ids if str(x).strip()]
        if pool:
            # Mix premium and regular emojis so a couple of added premium IDs
            # don't fully replace the whole visual variety.
            max_premium = min(count, max(1, count // 2 + 1))
            premium_count = rng.randint(1, max_premium)
            if count > 1:
                premium_count = min(premium_count, count - 1)
            premium_positions = set(rng.sample(range(count), k=max(0, premium_count)))
            mixed: list[str] = []
            for idx, symbol in enumerate(symbols):
                if idx in premium_positions:
                    mixed.append(_tg_emoji_tag(rng.choice(pool), symbol))
                else:
                    mixed.append(symbol)
            return " ".join(mixed), symbols, count

    return " ".join(symbols), symbols, count


def _emoji_symbol_markup(rng: random.Random, symbol: str, premium_emoji_ids: Sequence[str]) -> str:
    if premium_emoji_ids and rng.random() < 0.45:
        emoji_id = rng.choice(list(premium_emoji_ids))
        return _tg_emoji_tag(emoji_id, symbol)
    return symbol


def _collect_context_candidates(sentence: str, emoji_pool: list[str]) -> list[str]:
    lowered = sentence.lower()
    out: list[str] = []
    for keywords, symbols in CONTEXT_EMOJI_HINTS:
        if any(key in lowered for key in keywords):
            for symbol in symbols:
                if symbol in emoji_pool and symbol not in out:
                    out.append(symbol)
    return out


def _inject_contextual_emojis(
    *,
    text: str,
    rng: random.Random,
    premium_emoji_ids: Sequence[str],
    emoji_pool: list[str],
    min_count: int = 2,
    max_count: int = 6,
) -> tuple[str, list[str], int]:
    sentences = [x for x in CONTEXT_SENTENCE_SPLIT_RE.split(text.strip()) if x.strip()]
    if not sentences:
        return text, [], 0

    used_symbols: list[str] = []
    for idx, sentence in enumerate(sentences):
        if len(used_symbols) >= max_count:
            break
        candidates = _collect_context_candidates(sentence, emoji_pool)
        if not candidates:
            continue
        symbol = rng.choice(candidates)
        markup = _emoji_symbol_markup(rng, symbol, premium_emoji_ids)
        sentences[idx] = f"{sentence.rstrip()} {markup}"
        used_symbols.append(symbol)

    while len(used_symbols) < min_count:
        symbol = rng.choice(emoji_pool)
        markup = _emoji_symbol_markup(rng, symbol, premium_emoji_ids)
        sentences[-1] = f"{sentences[-1].rstrip()} {markup}"
        used_symbols.append(symbol)

    return " ".join(sentences), used_symbols, len(used_symbols)


def _personal_instruction_line(instructions: str, mode: str) -> str:
    line = instructions.strip()
    if not line:
        return ""
    limit = 160 if mode == "advanced" else 100
    if len(line) > limit:
        line = f"{line[:limit].strip()}..."
    return f"Личный акцент: {line}"


def _ensure_group_pronouns(text: str, kind: str) -> str:
    lowered = text.lower().replace("ё", "е")
    if re.search(r"\b(вы|вам|вас|ваш|ваша|ваше|ваши)\b", lowered):
        return text
    addon = (
        "Пусть у вас сегодня все сложится."
        if kind == "morning"
        else "Пусть вам сегодня будет спокойно."
    )
    return f"{text.rstrip()} {addon}"


def _features(
    *,
    kind: str,
    mode: str,
    template_idx: int,
    picks: dict[str, int],
    emoji_symbols: list[str],
    person_id: int,
    has_person_instructions: bool,
    text: str,
) -> dict[str, float]:
    out: dict[str, float] = {
        f"kind:{kind}": 1.0,
        f"mode:{mode}": 1.0,
        f"template:{kind}:{mode}:{template_idx}": 1.0,
    }
    for key, value in picks.items():
        out[f"{key}:{kind}:{mode}:{value}"] = 1.0
    for symbol in emoji_symbols:
        out[f"emoji:{symbol}"] = 1.0
    if person_id != 0:
        out[f"person:{person_id}"] = 1.0
    if has_person_instructions:
        out["person_instructions:1"] = 1.0
    # Let the tiny model learn not only IDs of picked buckets, but also
    # recurring lexical patterns from the final text.
    words = re.findall(r"[a-zа-я0-9]{3,}", text.lower().replace("ё", "е"))
    uniq_words: list[str] = []
    seen_words: set[str] = set()
    for word in words:
        if word in seen_words:
            continue
        seen_words.add(word)
        uniq_words.append(word)
        if len(uniq_words) >= 18:
            break
    for token in uniq_words:
        out[f"tok:{token}"] = 1.0
    return out


def _render_standard(
    *,
    kind: str,
    pattern_set: PatternSet,
    rng: random.Random,
    person_name: str,
    person_instructions: str,
    premium_emoji_ids: Sequence[str],
    emoji_pool: list[str],
) -> tuple[str, int, dict[str, int], list[str], int]:
    template_idx, template = _pick_index(rng, pattern_set.templates)
    greeting_idx, greeting = _pick_index(rng, pattern_set.greetings)
    wish_idx, wish = _pick_index(rng, pattern_set.wishes)
    comfort_idx, comfort = _pick_index(rng, pattern_set.comforts)
    dream_idx, dream = _pick_index(rng, pattern_set.dreams)
    care_idx, care = _pick_index(rng, pattern_set.cares)
    signoff_idx, signoff = _pick_index(rng, pattern_set.signoffs)

    if person_name.strip():
        appeal = person_name.strip()
        appeal_idx = -1
    else:
        appeal_idx, appeal = _pick_index(rng, pattern_set.appeals)

    emojis, emoji_symbols, emoji_count = _emoji_block(
        rng,
        premium_emoji_ids,
        emoji_pool,
        min_count=4,
        max_count=8,
    )
    personal_line = _personal_instruction_line(person_instructions, mode="standard")

    text = template.format(
        greeting=html.escape(greeting),
        appeal=html.escape(appeal),
        wish=html.escape(wish),
        comfort=html.escape(comfort),
        dream=html.escape(dream),
        care=html.escape(care),
        signoff=html.escape(signoff),
        emojis=emojis,
    )
    if personal_line:
        text = f"{text} {html.escape(personal_line)}"

    picks = {
        "greeting": greeting_idx,
        "appeal": appeal_idx,
        "wish": wish_idx,
        "comfort": comfort_idx,
        "dream": dream_idx,
        "care": care_idx,
        "signoff": signoff_idx,
    }
    return text, template_idx, picks, emoji_symbols, emoji_count


def _render_short(
    *,
    kind: str,
    pattern_set: PatternSet,
    rng: random.Random,
    person_name: str,
    premium_emoji_ids: Sequence[str],
    emoji_pool: list[str],
) -> tuple[str, int, dict[str, int], list[str], int]:
    templates = SHORT_TEMPLATES[kind]
    template_idx, template = _pick_index(rng, templates)
    greeting_idx, greeting = _pick_index(rng, pattern_set.greetings)

    if person_name.strip():
        appeal = person_name.strip()
        appeal_idx = -1
    else:
        appeal_idx, appeal = _pick_index(rng, pattern_set.appeals)

    emojis, emoji_symbols, emoji_count = _emoji_block(
        rng,
        premium_emoji_ids,
        emoji_pool,
        min_count=4,
        max_count=8,
    )

    text = template.format(
        greeting=html.escape(greeting),
        appeal=html.escape(appeal),
        emojis=emojis,
    )
    picks = {
        "greeting": greeting_idx,
        "appeal": appeal_idx,
    }
    return text, template_idx, picks, emoji_symbols, emoji_count


def _render_context(
    *,
    kind: str,
    pattern_set: PatternSet,
    rng: random.Random,
    person_name: str,
    person_instructions: str,
    premium_emoji_ids: Sequence[str],
    emoji_pool: list[str],
) -> tuple[str, int, dict[str, int], list[str], int]:
    template_idx, template = _pick_index(rng, pattern_set.templates)
    greeting_idx, greeting = _pick_index(rng, pattern_set.greetings)
    wish_idx, wish = _pick_index(rng, pattern_set.wishes)
    comfort_idx, comfort = _pick_index(rng, pattern_set.comforts)
    dream_idx, dream = _pick_index(rng, pattern_set.dreams)
    care_idx, care = _pick_index(rng, pattern_set.cares)
    signoff_idx, signoff = _pick_index(rng, pattern_set.signoffs)

    if person_name.strip():
        appeal = person_name.strip()
        appeal_idx = -1
    else:
        appeal_idx, appeal = _pick_index(rng, pattern_set.appeals)

    text = template.format(
        greeting=html.escape(greeting),
        appeal=html.escape(appeal),
        wish=html.escape(wish),
        comfort=html.escape(comfort),
        dream=html.escape(dream),
        care=html.escape(care),
        signoff=html.escape(signoff),
        emojis="",
    ).strip()
    text, emoji_symbols, emoji_count = _inject_contextual_emojis(
        text=text,
        rng=rng,
        premium_emoji_ids=premium_emoji_ids,
        emoji_pool=emoji_pool,
        min_count=2,
        max_count=6,
    )

    personal_line = _personal_instruction_line(person_instructions, mode="context")
    if personal_line:
        text = f"{text} {html.escape(personal_line)}"

    picks = {
        "greeting": greeting_idx,
        "appeal": appeal_idx,
        "wish": wish_idx,
        "comfort": comfort_idx,
        "dream": dream_idx,
        "care": care_idx,
        "signoff": signoff_idx,
    }
    return text, template_idx, picks, emoji_symbols, emoji_count


def _render_advanced(
    *,
    kind: str,
    story_set: StorySet,
    rng: random.Random,
    person_name: str,
    person_instructions: str,
    premium_emoji_ids: Sequence[str],
    emoji_pool: list[str],
) -> tuple[str, int, dict[str, int], list[str], int]:
    template_idx, template = _pick_index(rng, story_set.templates)
    greeting_idx, greeting = _pick_index(rng, story_set.greetings)
    opener_idx, opener = _pick_index(rng, story_set.openers)
    scene_idx, scene = _pick_index(rng, story_set.scenes)
    wish_idx, wish = _pick_index(rng, story_set.wishes)
    care_idx, care = _pick_index(rng, story_set.cares)
    ending_idx, ending = _pick_index(rng, story_set.endings)

    if person_name.strip():
        appeal = person_name.strip()
        appeal_idx = -1
    else:
        appeal_idx, appeal = _pick_index(rng, story_set.appeals)

    emojis, emoji_symbols, emoji_count = _emoji_block(
        rng,
        premium_emoji_ids,
        emoji_pool,
        min_count=4,
        max_count=8,
    )
    personal_line = _personal_instruction_line(person_instructions, mode="advanced")

    text = template.format(
        greeting=html.escape(greeting),
        appeal=html.escape(appeal),
        opener=html.escape(opener),
        scene=html.escape(scene),
        wish=html.escape(wish),
        care=html.escape(care),
        ending=html.escape(ending),
        emojis=emojis,
    )
    if personal_line:
        text = f"{text} {html.escape(personal_line)}"

    picks = {
        "greeting": greeting_idx,
        "appeal": appeal_idx,
        "opener": opener_idx,
        "scene": scene_idx,
        "wish": wish_idx,
        "care": care_idx,
        "ending": ending_idx,
    }
    return text, template_idx, picks, emoji_symbols, emoji_count


def generate_candidate(
    kind: str,
    *,
    mode: str = "standard",
    audience: str = "single",
    person_id: int = 0,
    person_name: str = "",
    person_instructions: str = "",
    premium_emoji_ids: Sequence[str] | None = None,
    extra_emojis: Sequence[str] | None = None,
    rng: random.Random | None = None,
) -> GeneratedWish:
    if kind not in VALID_KINDS:
        raise ValueError(f"Unsupported kind: {kind}")
    if mode not in VALID_MODES:
        raise ValueError(f"Unsupported mode: {mode}")
    if audience not in {"single", "group"}:
        raise ValueError(f"Unsupported audience: {audience}")

    rnd = rng or random.Random()
    effective_person_id = person_id
    effective_person_name = person_name
    effective_person_instructions = person_instructions
    if audience == "group":
        # Group wishes must stay in plural form and should not inherit
        # personal "друг/подруга" profile from chat settings.
        effective_person_id = 0
        effective_person_name = ""
        effective_person_instructions = ""

    premium_ids = [x.strip() for x in (premium_emoji_ids or []) if x.strip()]
    emoji_pool = _merge_emoji_pool(extra_emojis)
    standard = _standard_set_for_kind(kind, effective_person_name, effective_person_instructions, audience)

    if mode == "short":
        text, template_idx, picks, emoji_symbols, emoji_count = _render_short(
            kind=kind,
            pattern_set=standard,
            rng=rnd,
            person_name=effective_person_name,
            premium_emoji_ids=premium_ids,
            emoji_pool=emoji_pool,
        )
    elif mode == "standard":
        text, template_idx, picks, emoji_symbols, emoji_count = _render_standard(
            kind=kind,
            pattern_set=standard,
            rng=rnd,
            person_name=effective_person_name,
            person_instructions=effective_person_instructions,
            premium_emoji_ids=premium_ids,
            emoji_pool=emoji_pool,
        )
    else:
        text, template_idx, picks, emoji_symbols, emoji_count = _render_context(
            kind=kind,
            pattern_set=standard,
            rng=rnd,
            person_name=effective_person_name,
            person_instructions=effective_person_instructions,
            premium_emoji_ids=premium_ids,
            emoji_pool=emoji_pool,
        )

    if audience == "group":
        text = _ensure_group_pronouns(text, kind)

    features = _features(
        kind=kind,
        mode=mode,
        template_idx=template_idx,
        picks=picks,
        emoji_symbols=emoji_symbols,
        person_id=effective_person_id,
        has_person_instructions=bool(effective_person_instructions.strip()),
        text=text,
    )
    features[f"audience:{audience}"] = 1.0

    return GeneratedWish(
        kind=kind,
        mode=mode,
        person_id=effective_person_id,
        person_name=effective_person_name.strip(),
        text=text,
        template_idx=template_idx,
        picks=picks,
        features=features,
        emoji_count=emoji_count,
    )


def generate_best(
    kind: str,
    *,
    mode: str = "standard",
    audience: str = "single",
    person_id: int = 0,
    person_name: str = "",
    person_instructions: str = "",
    premium_emoji_ids: Sequence[str] | None = None,
    extra_emojis: Sequence[str] | None = None,
    model: ScoreModel | None = None,
    rng: random.Random | None = None,
    candidate_pool: int = 6,
) -> GeneratedWish:
    rnd = rng or random.Random()
    pool_size = max(1, candidate_pool)
    candidates = [
        generate_candidate(
            kind,
            mode=mode,
            audience=audience,
            person_id=person_id,
            person_name=person_name,
            person_instructions=person_instructions,
            premium_emoji_ids=premium_emoji_ids,
            extra_emojis=extra_emojis,
            rng=rnd,
        )
        for _ in range(pool_size)
    ]
    if not model:
        return rnd.choice(candidates)
    return max(candidates, key=lambda c: model.predict(c.features))


def generate_batch(
    kind: str,
    amount: int,
    *,
    mode: str = "standard",
    audience: str = "single",
    person_id: int = 0,
    person_name: str = "",
    person_instructions: str = "",
    premium_emoji_ids: Sequence[str] | None = None,
    extra_emojis: Sequence[str] | None = None,
    model: ScoreModel | None = None,
    seed: int | None = None,
) -> list[GeneratedWish]:
    rnd = random.Random(seed)
    return [
        generate_best(
            kind,
            mode=mode,
            audience=audience,
            person_id=person_id,
            person_name=person_name,
            person_instructions=person_instructions,
            premium_emoji_ids=premium_emoji_ids,
            extra_emojis=extra_emojis,
            model=model,
            rng=rnd,
        )
        for _ in range(amount)
    ]


def estimated_unique_texts(kind: str, mode: str = "standard") -> int:
    if kind not in VALID_KINDS:
        raise ValueError(f"Unsupported kind: {kind}")
    if mode not in VALID_MODES:
        raise ValueError(f"Unsupported mode: {mode}")

    if mode == "short":
        set_data = STANDARD_BY_KIND[kind]
        sizes = [
            len(SHORT_TEMPLATES[kind]),
            len(set_data.greetings),
            len(set_data.appeals),
        ]
        return reduce(mul, sizes, 1)

    if mode == "standard":
        set_data = STANDARD_BY_KIND[kind]
        sizes = [
            len(set_data.templates),
            len(set_data.greetings),
            len(set_data.appeals),
            len(set_data.wishes),
            len(set_data.comforts),
            len(set_data.dreams),
            len(set_data.cares),
            len(set_data.signoffs),
        ]
        return reduce(mul, sizes, 1)

    if mode == "context":
        # Contextual mode reuses standard text pools and inserts emojis by sentence meaning.
        return estimated_unique_texts(kind, mode="standard")

    raise ValueError(f"Unsupported mode: {mode}")
