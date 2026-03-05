"""
Настройка бота без .env

Заполни только BOT_TOKEN.
ADMIN_USER_ID и GROUP_CHAT_ID можно оставить 0:
- ADMIN подтянется автоматически при первом /start в личке.
- GROUP можно закрепить кнопкой в настройках внутри нужной группы.
"""

# Обязательно: токен от @BotFather
BOT_TOKEN = "8764643677:AAGMmZaY43A2PvHAVZjedGMe08Z7_orHgrc"

# Необязательно: если 0, назначится автоматически при первом /start в личке
ADMIN_USER_ID = 0

# Необязательно: если 0, выбери группу кнопкой "Сделать этот чат группой"
GROUP_CHAT_ID = 0

# Необязательно: часовой пояс (по умолчанию Москва)
TIMEZONE = "Europe/Moscow"

# Необязательно: ID премиум-эмодзи (можно потом менять кнопками в боте)
PREMIUM_EMOJI_IDS = []

# Необязательно: сколько оценок считать за 100% прогресса обучения
TRAINING_TARGET_SAMPLES = 10000

# OpenAI (GPT) генерация. Если ключ заполнен, GPT можно включить.
# Ключ API (пример: "sk-...")
OPENAI_API_KEY = "io-v2-eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJvd25lciI6ImQ2NTQxNGJlLThlZmYtNDAzNC1hYzg2LWViODNlZTQyOTliOSIsImV4cCI6NDkyNjMwNTUzMH0.KcqoJuoSgyU3-QeUAdj4bdnrB9XB5lFzFYd2kmKt6tgXxGK9w_vLxdo3QYhvpSNk5zbI9KfO2emK9IeD0okxtQ"
# OpenAI-совместимый endpoint.
# Для OpenAI: "https://api.openai.com/v1"
# Для io-cloud поставь endpoint из их кабинета (обычно что-то вроде ".../v1")
OPENAI_BASE_URL = "https://api.intelligence.io.solutions/api/v1"

# Вкл/выкл GPT-генерацию. Если None, берется автоматически: ВКЛ при наличии ключа.
OPENAI_ENABLED = True

# Модель OpenAI
OPENAI_MODEL = "meta-llama/Llama-3.3-70B-Instruct"

# Параметры генерации
OPENAI_TEMPERATURE = 0.8
OPENAI_MAX_TOKENS = 260
OPENAI_TIMEOUT_SEC = 45.0

# Свои правила стиля для GPT (одной строкой или многострочно)
OPENAI_RULES = (
    "Тон: дружеский и естественный. "
    "Без пафоса, без шаблонных фраз. "

)

# Необязательно: служебные файлы
FEEDBACK_PATH = "feedback_stats.json"
MODEL_PATH = "adaptive_model_state.json"
STATE_PATH = "bot_state.json"
CHAT_LOG_PATH = "chat_memory.jsonl"
