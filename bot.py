"""
bot.py — Анти-стресс_Clo v0.4.0
Изменения v0.4.0:
  - Добавлена команда /admin — показывает все админ-команды
  - Обновлены вопросы опросов: одна тема, но разные формулировки
  - Добавлена команда /broadcast — оповещение всех пользователей об обновлении
  - Убрана информация об авторе и проекте из раздела «О боте»
  - Добавлен раздел «Практики» с 5 техниками снижения стресса

На сервере рядом с bot.py:
  facts_day.txt, moon_photos/, morning_images/

Env: BOT_TOKEN, DB_PATH (опц., по умолчанию /data/antistress.db)
"""

import asyncio, csv, io, logging, os, random, re, sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timedelta

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile, CallbackQuery, FSInputFile,
    InlineKeyboardButton, KeyboardButton, Message, ReplyKeyboardMarkup,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ================================================================
#  КОНФИГУРАЦИЯ
# ================================================================

BOT_TOKEN  = os.getenv("BOT_TOKEN", "YOUR_TOKEN_HERE")
ADMIN_ID   = 7498442456
VERSION    = "0.4.0"
BOT_NAME   = "Анти-стресс_Clo"
TIMEZONE   = "Europe/Moscow"
MSK        = pytz.timezone(TIMEZONE)

ZONE_GREEN  = (8,  16)
ZONE_YELLOW = (17, 28)
ZONE_RED    = (29, 40)

COOLDOWN_EXPRESS   = 3600
COOLDOWN_BREATHING = 1800

POINTS_SURVEY       = 15
POINTS_STREAK_BONUS = 10
POINTS_EXPRESS      = 10
POINTS_BREATH_FIRST = 5
POINTS_BREATH_NEXT  = 1

TRIGGER_QUESTIONS = {5, 8}
TRIGGER_VALUE     = 5
RED_STREAK_ALERT  = 3

FACTS_FILE  = "facts_day.txt"
MOON_DIR    = "moon_photos"
MORNING_DIR = "morning_images"
FACTS_TIME  = "13:30"

DB_PATH = os.getenv("DB_PATH", "/data/antistress.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ================================================================
#  БАЗА ДАННЫХ
# ================================================================

@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id           INTEGER PRIMARY KEY,
            username          TEXT,
            first_name        TEXT,
            gender            TEXT,
            points            INTEGER DEFAULT 0,
            streak            INTEGER DEFAULT 0,
            last_survey_date  TEXT,
            survey_time       TEXT DEFAULT '20:00',
            morning_time      TEXT,
            red_zone_streak   INTEGER DEFAULT 0,
            registered_at     TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS moods (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL,
            score      INTEGER NOT NULL,
            zone       TEXT    NOT NULL,
            mood_type  TEXT    NOT NULL,
            created_at TEXT    DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );
        CREATE TABLE IF NOT EXISTS mood_details (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            mood_id      INTEGER NOT NULL,
            question_num INTEGER NOT NULL,
            answer       INTEGER NOT NULL,
            FOREIGN KEY (mood_id) REFERENCES moods(id)
        );
        CREATE TABLE IF NOT EXISTS daily_tasks (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id       INTEGER NOT NULL,
            task_date     TEXT    NOT NULL,
            task_type     TEXT    NOT NULL,
            points_earned INTEGER DEFAULT 0,
            done_at       TEXT    DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );
        """)
    logger.info("БД готова: %s", DB_PATH)

def upsert_user(user_id, username, first_name):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO users (user_id, username, first_name) VALUES (?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username, first_name=excluded.first_name
        """, (user_id, username or "", first_name or ""))

def get_user(user_id):
    with get_conn() as conn:
        return conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()

def set_gender(user_id, gender):
    with get_conn() as conn:
        conn.execute("UPDATE users SET gender=? WHERE user_id=?", (gender, user_id))

def set_survey_time(user_id, t):
    with get_conn() as conn:
        conn.execute("UPDATE users SET survey_time=? WHERE user_id=?", (t, user_id))

def set_morning_time(user_id, t):
    with get_conn() as conn:
        conn.execute("UPDATE users SET morning_time=? WHERE user_id=?", (t, user_id))

def add_points(user_id, pts):
    with get_conn() as conn:
        conn.execute("UPDATE users SET points=points+? WHERE user_id=?", (pts, user_id))
        row = conn.execute("SELECT points FROM users WHERE user_id=?", (user_id,)).fetchone()
        return row["points"] if row else 0

def set_points_value(user_id, pts):
    with get_conn() as conn:
        conn.execute("UPDATE users SET points=? WHERE user_id=?", (pts, user_id))

def get_all_users():
    with get_conn() as conn:
        return conn.execute("SELECT * FROM users").fetchall()

def get_users_by_survey_time(t):
    with get_conn() as conn:
        return conn.execute("SELECT * FROM users WHERE survey_time=?", (t,)).fetchall()

def get_users_by_morning_time(t):
    with get_conn() as conn:
        return conn.execute("SELECT * FROM users WHERE morning_time=?", (t,)).fetchall()

def save_mood(user_id, score, zone, mood_type, answers):
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO moods (user_id,score,zone,mood_type) VALUES (?,?,?,?)",
            (user_id, score, zone, mood_type)
        )
        mid = cur.lastrowid
        if answers:
            conn.executemany(
                "INSERT INTO mood_details (mood_id,question_num,answer) VALUES (?,?,?)",
                [(mid, i+1, a) for i, a in enumerate(answers)]
            )
        return mid

def get_last_moods(user_id, mood_type, limit=7):
    with get_conn() as conn:
        return conn.execute("""
            SELECT score,zone,created_at FROM moods
            WHERE user_id=? AND mood_type=?
            ORDER BY created_at DESC LIMIT ?
        """, (user_id, mood_type, limit)).fetchall()

def get_last_mood_dt(user_id, mood_type):
    with get_conn() as conn:
        row = conn.execute("""
            SELECT created_at FROM moods
            WHERE user_id=? AND mood_type=?
            ORDER BY created_at DESC LIMIT 1
        """, (user_id, mood_type)).fetchone()
        return datetime.fromisoformat(row["created_at"]) if row else None

def update_streak(user_id):
    today     = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    with get_conn() as conn:
        u = conn.execute(
            "SELECT last_survey_date,streak,red_zone_streak FROM users WHERE user_id=?",
            (user_id,)
        ).fetchone()
        last   = u["last_survey_date"] if u else None
        streak = u["streak"] if u else 0
        if last == today:
            return streak
        streak = (streak + 1) if last == yesterday else 1
        row = conn.execute("""
            SELECT zone FROM moods WHERE user_id=? AND mood_type='main'
            ORDER BY created_at DESC LIMIT 1
        """, (user_id,)).fetchone()
        red = u["red_zone_streak"] if u else 0
        red = (red + 1) if (row and row["zone"] == "red") else 0
        conn.execute(
            "UPDATE users SET streak=?,last_survey_date=?,red_zone_streak=? WHERE user_id=?",
            (streak, today, red, user_id)
        )
        return streak

def get_red_streak(user_id):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT red_zone_streak FROM users WHERE user_id=?", (user_id,)
        ).fetchone()
        return row["red_zone_streak"] if row else 0

def task_done_today(user_id, task_type):
    today = date.today().isoformat()
    with get_conn() as conn:
        return conn.execute("""
            SELECT id FROM daily_tasks
            WHERE user_id=? AND task_date=? AND task_type=? LIMIT 1
        """, (user_id, today, task_type)).fetchone() is not None

def log_task(user_id, task_type, pts):
    today = date.today().isoformat()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO daily_tasks (user_id,task_date,task_type,points_earned) VALUES (?,?,?,?)",
            (user_id, today, task_type, pts)
        )

def get_today_tasks(user_id):
    today = date.today().isoformat()
    with get_conn() as conn:
        return conn.execute("""
            SELECT task_type,points_earned FROM daily_tasks
            WHERE user_id=? AND task_date=? ORDER BY done_at
        """, (user_id, today)).fetchall()

def admin_general_stats():
    with get_conn() as conn:
        total  = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
        active = conn.execute("""
            SELECT COUNT(DISTINCT user_id) AS c FROM moods
            WHERE created_at >= datetime('now','-7 days')
        """).fetchone()["c"]
        avg_sc = conn.execute("""
            SELECT ROUND(AVG(score),1) AS a FROM moods WHERE mood_type='main'
        """).fetchone()["a"]
        zones   = conn.execute("""
            SELECT zone,COUNT(*) AS c FROM moods WHERE mood_type='main' GROUP BY zone
        """).fetchall()
        genders = conn.execute(
            "SELECT gender,COUNT(*) AS c FROM users GROUP BY gender"
        ).fetchall()
    return {
        "total": total, "active_7d": active, "avg_score": avg_sc,
        "zones":   {r["zone"]: r["c"] for r in zones},
        "genders": {(r["gender"] or "не указан"): r["c"] for r in genders},
    }

def admin_all_users():
    with get_conn() as conn:
        return conn.execute("""
            SELECT user_id,username,first_name,gender,
                   points,streak,survey_time,morning_time
            FROM users ORDER BY points DESC
        """).fetchall()

def export_moods_csv(days=30):
    with get_conn() as conn:
        return conn.execute("""
            SELECT m.id,m.user_id,u.username,m.score,m.zone,m.mood_type,m.created_at
            FROM moods m LEFT JOIN users u ON u.user_id=m.user_id
            WHERE m.created_at >= datetime('now', ? || ' days')
            ORDER BY m.created_at DESC
        """, (f"-{days}",)).fetchall()

# ================================================================
#  ТЕКСТЫ
# ================================================================

WELCOME = (
    f"Привет! Я *{BOT_NAME}* 🤍\n\n"
    "Помогу отслеживать уровень стресса и находить баланс — "
    "каждый день, в твоём темпе.\n\n"
    "Сначала пара вопросов для старта 👇"
)
ASK_GENDER = (
    "👤 Укажи свой пол — только для анонимной статистики.\n"
    "_Никто кроме тебя не увидит эти данные._"
)
GENDER_SAVED     = "Сохранил ✅\n\nХочешь прямо сейчас пройти пробный вечерний опрос?"
ASK_SURVEY_TIME  = "⏰ В какое время тебе удобно проходить ежедневный опрос?\n\nНапиши время в формате *ЧЧ:ММ*, например `20:00`"
ASK_MORNING_TIME = "🌅 Хочешь получать утреннюю карточку?\n\nНапиши время в формате *ЧЧ:ММ* или /skip чтобы пропустить."
SETUP_DONE       = "🎉 Всё готово! Ты в игре.\n\nПользуйся меню ниже 👇"
TIME_INVALID     = "⚠️ Неверный формат. Попробуй ещё раз, например: `20:30`"
TIME_SAVED       = "✅ Время сохранено: *{time}*"
TIME_MORNING_OFF = "✅ Утренняя рассылка отключена."
TIME_SETTINGS    = "⏰ *Настройка времени*\n\n🌆 Вечерний опрос: *{survey_time}*\n🌅 Утренняя рассылка: *{morning_time}*"

HELP_TEXT = (
    f"ℹ️ *{BOT_NAME}*  v{VERSION}\n\n"
    "*Главное меню:*\n"
    "📊 Статистика — очки, серия, история\n"
    "🌿 Практики — дыхание, упражнения, луна\n"
    "ℹ️ О боте — справка и настройка времени\n\n"
    "*Как работает бот:*\n"
    "Каждый вечер в заданное время бот пришлёт опрос из 8 вопросов. "
    "По итогам ты получишь оценку уровня стресса (🟢🟡🔴) и очки.\n\n"
    "*Система очков:*\n"
    "• Вечерний опрос — 15 очков\n"
    "• Экспресс-тест — 10 очков\n"
    "• Дыхательная практика — 5 очков\n"
    "• Бонус за серию дней — 10 очков"
)

# ИЗМЕНЕНИЕ 4: убрана информация об авторе и проекте
ABOUT_TEXT = (
    f"🤍 *{BOT_NAME}*  v{VERSION}\n\n"
    "Этот бот помогает отслеживать уровень стресса и заботиться о себе — "
    "каждый день, в привычном темпе.\n\n"
    "📋 *Как это работает:*\n"
    "• Каждый вечер в выбранное тобой время — опрос из 8 вопросов\n"
    "• По результатам ты узнаёшь свою зону стресса: 🟢 🟡 🔴\n"
    "• Дыхательные практики и экспресс-тесты — в любое время\n"
    "• В 13:30 приходит научный факт о стрессе\n\n"
    "🏆 *Система очков:*\n"
    "• Вечерний опрос — 15 очков\n"
    "• Экспресс-тест — 10 очков\n"
    "• Дыхательная практика — 5 очков\n"
    "• Бонус за серию дней — 10 очков\n\n"
    "🌙 *О луне:*\n"
    "Фазы луны добавлены для интереса — "
    "научного влияния на уровень стресса они не имеют.\n\n"
    "_Нужна помощь? Напиши /help_"
)

# ИЗМЕНЕНИЕ 2: Обновлённые вопросы — одна тема (уровень стресса),
# но каждый раз случайная формулировка из пула
SURVEY_QUESTIONS_POOL = [
    # Блок 1 — Телесное напряжение (варианты)
    [
        "🙆 *Вопрос 1 из 8 — Тело*\n\nНасколько ты чувствуешь напряжение в теле прямо сейчас?\n\n_1 — совсем нет  |  5 — очень сильное_",
        "🙆 *Вопрос 1 из 8 — Тело*\n\nЕсть ли зажатость в плечах, шее или спине сегодня?\n\n_1 — совсем нет  |  5 — очень сильная_",
        "🙆 *Вопрос 1 из 8 — Тело*\n\nКак бы ты оценил физическое напряжение в теле за сегодня?\n\n_1 — полностью расслаблен  |  5 — очень напряжён_",
    ],
    # Блок 2 — Цифровой шум (варианты)
    [
        "📱 *Вопрос 2 из 8 — Цифровой шум*\n\nНасколько тебя утомляют уведомления, новости, соцсети сегодня?\n\n_1 — совсем нет  |  5 — очень сильно_",
        "📱 *Вопрос 2 из 8 — Цифровой шум*\n\nСколько раз сегодня ты бездумно тянулся к телефону?\n\n_1 — почти не тянулся  |  5 — постоянно_",
        "📱 *Вопрос 2 из 8 — Цифровой шум*\n\nЧувствуешь ли перегрузку от потока информации сегодня?\n\n_1 — совсем нет  |  5 — сильная перегрузка_",
    ],
    # Блок 3 — Навязчивые мысли (варианты)
    [
        "🌀 *Вопрос 3 из 8 — Мысли*\n\nКак часто сегодня крутились одни и те же тревожные мысли?\n\n_1 — совсем нет  |  5 — постоянно_",
        "🌀 *Вопрос 3 из 8 — Мысли*\n\nТяжело ли было «отключить голову» сегодня?\n\n_1 — легко  |  5 — очень тяжело_",
        "🌀 *Вопрос 3 из 8 — Мысли*\n\nВозникало ли ощущение, что мысли «идут по кругу»?\n\n_1 — совсем нет  |  5 — почти всё время_",
    ],
    # Блок 4 — Концентрация (варианты)
    [
        "⚡ *Вопрос 4 из 8 — Концентрация*\n\nКак сложно было сегодня сосредоточиться и удерживать внимание?\n\n_1 — легко  |  5 — очень тяжело_",
        "⚡ *Вопрос 4 из 8 — Концентрация*\n\nОтвлекался ли ты сегодня больше обычного?\n\n_1 — совсем нет  |  5 — очень часто_",
        "⚡ *Вопрос 4 из 8 — Концентрация*\n\nУдавалось ли удерживать фокус на задачах?\n\n_1 — без проблем  |  5 — совсем не удавалось_",
    ],
    # Блок 5 — Эмоциональный фон (варианты)
    [
        "💛 *Вопрос 5 из 8 — Эмоции*\n\nНасколько сильными были негативные эмоции сегодня?\n\n_1 — совсем нет  |  5 — очень интенсивные_",
        "💛 *Вопрос 5 из 8 — Эмоции*\n\nЧувствовал ли ты раздражительность или тревогу сегодня?\n\n_1 — совсем нет  |  5 — очень сильно_",
        "💛 *Вопрос 5 из 8 — Эмоции*\n\nКак бы ты описал своё эмоциональное состояние за день?\n\n_1 — спокойное, ровное  |  5 — взволнованное, напряжённое_",
    ],
    # Блок 6 — Сон (варианты)
    [
        "🌙 *Вопрос 6 из 8 — Сон*\n\nКак ты оцениваешь свой прошлый сон?\n\n_1 — отлично  |  5 — очень плохо_",
        "🌙 *Вопрос 6 из 8 — Сон*\n\nПросыпался ли ты отдохнувшим сегодня утром?\n\n_1 — да, хорошо  |  5 — совсем нет_",
        "🌙 *Вопрос 6 из 8 — Сон*\n\nХватило ли тебе сна прошлой ночью?\n\n_1 — вполне  |  5 — совсем не хватило_",
    ],
    # Блок 7 — Общение (варианты)
    [
        "💬 *Вопрос 7 из 8 — Общение*\n\nНасколько тебе сегодня хотелось избегать людей?\n\n_1 — совсем нет  |  5 — очень сильно_",
        "💬 *Вопрос 7 из 8 — Общение*\n\nРаздражало ли тебя сегодня общение с окружающими?\n\n_1 — совсем нет  |  5 — очень сильно_",
        "💬 *Вопрос 7 из 8 — Общение*\n\nЧувствовал ли ты желание побыть в одиночестве больше обычного?\n\n_1 — нет  |  5 — очень сильно_",
    ],
    # Блок 8 — Ощущение контроля (варианты)
    [
        "🎯 *Вопрос 8 из 8 — Контроль*\n\nНасколько ты чувствовал себя беспомощным сегодня?\n\n_1 — совсем нет  |  5 — очень сильно_",
        "🎯 *Вопрос 8 из 8 — Контроль*\n\nБыло ли ощущение, что события вышли из-под контроля?\n\n_1 — нет, всё под контролем  |  5 — полностью вышли_",
        "🎯 *Вопрос 8 из 8 — Контроль*\n\nУдавалось ли тебе управлять ситуацией сегодня?\n\n_1 — легко  |  5 — совсем не удавалось_",
    ],
]

def get_random_survey_questions():
    """Возвращает список из 8 вопросов — по одному случайному из каждого блока."""
    return [random.choice(block) for block in SURVEY_QUESTIONS_POOL]

EXPRESS_QUESTIONS_POOL = SURVEY_QUESTIONS_POOL[:4]

def get_random_express_questions():
    return [random.choice(block) for block in EXPRESS_QUESTIONS_POOL]

RESULT_GREEN  = "🟢 *Зелёная зона — Баланс!*\n\nКрасавчик! Ты в хорошем состоянии сегодня 🙌\nСохраняй этот ритм — ты справляешься.\n\n_{score} баллов из 40_"
RESULT_YELLOW = "🟡 *Жёлтая зона — Умеренный стресс*\n\nЧувствуется нагрузка, но ты держишься 💪\nПопробуй технику дыхания — 5 минут могут изменить вечер.\n\n_{score} баллов из 40_"
RESULT_RED    = "🔴 *Красная зона — Высокий стресс*\n\nЗвучит тяжело. Это нормально — бывает.\nПопробуй прямо сейчас: 🧘 *Дыхательная гимнастика*.\nЕсли так несколько дней — поговори с кем-то, кому доверяешь.\n\n_{score} баллов из 40_"
POINTS_ADDED  = "\n\n✨ *+{pts} очков* начислено!"
STREAK_BONUS  = "🔥 Серия {streak} дней подряд! Бонус *+10 очков*"

EXPRESS_START    = "⚡ *Экспресс-тест* — 4 быстрых вопроса\n\nЗаймёт меньше минуты. Поехали 👇"
EXPRESS_RESULT   = "⚡ *Результат экспресс-теста*\n\nСумма: *{score} из 20*\nСредний балл: *{avg}*\n\n_{hint}_\n\n✨ *+10 очков* начислено!"
EXPRESS_COOLDOWN = "⏳ Экспресс-тест можно проходить *раз в час*.\nСледующий доступен через *{minutes} мин.*"

BREATHING_MENU  = "🧘 *Дыхательная гимнастика*\n\nВыбери упражнение — и дай себе пару минут тишины 🌿"
BREATHING_TEXTS = {
    "breath_square":
        "🔲 *Квадратное дыхание*\n\n"
        "Простая и мощная техника для быстрого снятия напряжения.\n\n"
        "1️⃣ Вдох — *4 секунды*\n"
        "2️⃣ Задержи дыхание — *4 секунды*\n"
        "3️⃣ Выдох — *4 секунды*\n"
        "4️⃣ Задержи дыхание — *4 секунды*\n\n"
        "Повтори 4–6 раз. Концентрируйся только на счёте 🎯",
    "breath_478":
        "4️⃣ *Дыхание 4-7-8*\n\n"
        "Метод доктора Эндрю Вейла — расслабляет нервную систему за минуты.\n\n"
        "1️⃣ Вдох через нос — *4 секунды*\n"
        "2️⃣ Задержи дыхание — *7 секунд*\n"
        "3️⃣ Выдох через рот со звуком — *8 секунд*\n\n"
        "Повтори 3–4 раза. Можно делать лёжа 🛏",
    "breath_diaphragm":
        "🫁 *Диафрагмальное дыхание*\n\n"
        "Самый естественный способ дышать — как в детстве.\n\n"
        "1️⃣ Положи руку на живот\n"
        "2️⃣ Вдохни носом — *живот поднимается*, грудь почти не двигается\n"
        "3️⃣ Выдыхай медленно через рот — *живот опускается*\n\n"
        "5 минут такого дыхания снижают кортизол 📉",
    "breath_relax":
        "😌 *Расслабляющее дыхание*\n\n"
        "Выдох длиннее вдоха — это сигнал телу «всё хорошо».\n\n"
        "1️⃣ Вдох — *4 секунды*\n"
        "2️⃣ Выдох — *6–8 секунд*\n\n"
        "Повтори 8–10 раз. Хорошо работает перед сном 🌙",
    "breath_nostril":
        "👃 *Дыхание через ноздри (Нади Шодхана)*\n\n"
        "Балансирует левое и правое полушария мозга.\n\n"
        "1️⃣ Закрой правую ноздрю большим пальцем\n"
        "2️⃣ Вдохни через левую — *4 секунды*\n"
        "3️⃣ Закрой обе, задержи — *4 секунды*\n"
        "4️⃣ Открой правую, выдохни — *4 секунды*\n"
        "5️⃣ Вдохни через правую — *4 секунды*, затем смени ноздрю\n\n"
        "5 циклов = полная перезагрузка 🔄",
}
BREATHING_COOLDOWN = "🧘 Описание доступно, но очки получишь через *{minutes} мин.*\n\n{text}"
BREATHING_POINTS   = "\n\n✨ *+{pts} очков* за практику!"

# ИЗМЕНЕНИЕ 5: Тексты для практик снижения стресса
PRACTICES_TEXTS = {
    "practice_grounding":
        "🌍 *Техника заземления 5-4-3-2-1*\n\n"
        "Возвращает в «здесь и сейчас» за 2–3 минуты.\n\n"
        "Назови вслух или мысленно:\n"
        "👁 *5 вещей*, которые ты видишь\n"
        "✋ *4 вещи*, которые можешь потрогать\n"
        "👂 *3 звука*, которые слышишь\n"
        "👃 *2 запаха*, которые ощущаешь\n"
        "👅 *1 вкус*, который чувствуешь\n\n"
        "Фокусируйся на каждом ощущении — спешить не нужно 🌿",
    "practice_cold_water":
        "🧊 *Метод холодной воды*\n\n"
        "Аварийный «тормоз» тревоги — работает за 30–60 секунд.\n\n"
        "1️⃣ Подойди к раковине\n"
        "2️⃣ Смочи запястья холодной водой\n"
        "3️⃣ Или умойся — уделяя внимание ощущению холода\n\n"
        "Холод активирует рефлекс ныряния — сердечный ритм замедляется, "
        "тревога снижается. Это физиология, а не магия 🧬",
    "practice_muscle_relax":
        "💪 *Прогрессивная мышечная релаксация*\n\n"
        "Снимает накопившееся телесное напряжение за 10 минут.\n\n"
        "Для каждой группы мышц:\n"
        "1️⃣ Напряги — *7 секунд* (сильно, но без боли)\n"
        "2️⃣ Резко расслабь — *20 секунд*, чувствуй разницу\n\n"
        "Порядок: ступни → голени → бёдра → живот → руки → плечи → лицо\n\n"
        "_Метод Джекобсона — один из наиболее изученных способов снижения тревожности_ 🔬",
    "practice_mind_dump":
        "📝 *Выгрузка мыслей*\n\n"
        "Освобождает «оперативную память» мозга.\n\n"
        "1️⃣ Возьми лист бумаги или открой заметки\n"
        "2️⃣ Поставь таймер на *3 минуты*\n"
        "3️⃣ Пиши всё, что тебя беспокоит — без цензуры, потоком\n"
        "4️⃣ Когда время выйдет — можно смять и выбросить\n\n"
        "Не нужно анализировать написанное. Сам акт выгрузки снижает тревогу ✍️",
    "practice_mindful_walk":
        "🌅 *Осознанная прогулка*\n\n"
        "Сочетание движения и присутствия снижает кортизол лучше, чем просто ходьба.\n\n"
        "1️⃣ Выйди на улицу — *5–10 минут* достаточно\n"
        "2️⃣ Убери телефон\n"
        "3️⃣ Фокусируйся только на ощущениях:\n"
        "   • Как чувствуется каждый шаг?\n"
        "   • Какой воздух — тёплый, холодный?\n"
        "   • Что видишь, что слышишь?\n\n"
        "_Если выйти нельзя — подойдёт медленная ходьба по комнате_ 🚶",
}

PRACTICES_MENU_TEXT = "🧠 *Практики снижения стресса*\n\nВыбери технику — каждая займёт не больше 10 минут 👇"

STATS_TEMPLATE = (
    "📊 *Твоя статистика*\n\n"
    "🏆 Очков: *{points}*\n"
    "🔥 Серия: *{streak} дн.*\n\n"
    "Последние опросы:\n{survey_list}\n\n"
    "Последние экспресс-тесты:\n{express_list}"
)
STATS_EMPTY = "пока нет данных"
ZONE_ICONS  = {"green": "🟢", "yellow": "🟡", "red": "🔴"}

TASKS_TEMPLATE = (
    "📋 *Задания на сегодня*\n\n"
    "{survey_status} Вечерний опрос         (+15 очков)\n"
    "{breath_status} Дыхательная гимнастика (+5 очков)\n"
    "{express_status} Экспресс-тест          (+10 очков)\n\n"
    "Всего очков: *{points}*"
)
DONE_ICON = "✅"
TODO_ICON = "⬜"

MOON_PHASES = {
    "new":             ("🌑", "Новолуние"),
    "waxing_crescent": ("🌒", "Растущий серп"),
    "first_quarter":   ("🌓", "Первая четверть"),
    "waxing_gibbous":  ("🌔", "Растущая луна"),
    "full":            ("🌕", "Полнолуние"),
    "waning_gibbous":  ("🌖", "Убывающая луна"),
    "last_quarter":    ("🌗", "Последняя четверть"),
    "waning_crescent": ("🌘", "Убывающий серп"),
}
MOON_DISCLAIMER = "\n\n_Научных доказательств влияния фазы луны на уровень стресса не существует. Это просто красиво 🌌_"

EVENING_PUSH     = "🌆 *Время подвести итоги дня!*\n\nПройди короткий опрос — займёт меньше 2 минут. Нажми на кнопку ниже 👇"
SURVEY_START_BTN = "📝 Начать опрос"
FACT_PREFIX      = "🧠 *Факт о стрессе*\n\n"

ADMIN_TRIGGER = "⚠️ *Триггер!*\nПользователь `{uid}` ({name}) поставил *5* в вопросе {q}.\nДата: {dt}"
ADMIN_RED     = "🚨 *Длительный стресс!*\nПользователь `{uid}` ({name}) — *{days} дня подряд* в красной зоне."

# ИЗМЕНЕНИЕ 1: текст со всеми админ-командами
ADMIN_HELP_TEXT = (
    f"🛠 *Админ-панель — {BOT_NAME}*\n\n"
    "*Команды статистики:*\n"
    "/admin\\_stats — общая статистика бота\n"
    "/admin\\_users — список всех пользователей\n"
    "/export\\_stats — экспорт настроений за 30 дней (CSV)\n\n"
    "*Управление очками:*\n"
    "/add\\_points `<user_id>` `<очки>` — начислить очки\n"
    "/set\\_points `<user_id>` `<очки>` — установить очки\n\n"
    "*Рассылки:*\n"
    "/broadcast `<текст>` — отправить сообщение всем пользователям\n\n"
    "_Все команды доступны только администратору._"
)

GROUP_STATS_TEXT = "📊 *Статистика*\n\nЗдесь ты можешь посмотреть результаты, задания на сегодня и быстро проверить состояние."
GROUP_RELAX_TEXT = "🌿 *Практики и релакс*\n\nДыхательные упражнения, научно обоснованные практики и немного астрономии 🌙"
GROUP_INFO_TEXT  = "ℹ️ *Меню «О боте»*\n\nЗдесь ты найдёшь информацию о боте и настройку времени рассылок."

MORNING_CAPTIONS = [
    "🌅 Доброе утро! Сегодня — новый шанс быть в балансе 🌿",
    "☀️ Привет! Одна маленькая дыхательная практика — и день начнётся отлично 🧘",
    "🌸 Новый день — новые возможности. Ты справишься! 💪",
    "🌤 Сделай что-то маленькое для себя сегодня. Начни с дыхания ✨",
    "🌻 Доброе утро! Стресс временен, ты постоянен 🤍",
]

# ================================================================
#  КНОПКИ
# ================================================================

BTN_STATS_GROUP = "📊 Статистика"
BTN_RELAX_GROUP = "🌿 Практики"
BTN_INFO_GROUP  = "ℹ️ О боте"

BTN_MY_STATS    = "📊 Моя статистика"
BTN_TASKS       = "📋 Мои задания"
BTN_EXPRESS     = "⚡ Экспресс-тест"

BTN_BREATHING   = "🧘 Дыхательная гимнастика"
BTN_PRACTICES   = "🧠 Практики"
BTN_MOON        = "🌙 Фаза луны"

BTN_ABOUT       = "📖 О боте"
BTN_TIME        = "⏰ Настроить время"

BTN_BACK        = "← Главное меню"

# ================================================================
#  КЛАВИАТУРЫ
# ================================================================

def main_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_STATS_GROUP)],
            [KeyboardButton(text=BTN_RELAX_GROUP)],
            [KeyboardButton(text=BTN_INFO_GROUP)],
        ],
        resize_keyboard=True, is_persistent=True,
    )

def stats_submenu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_MY_STATS), KeyboardButton(text=BTN_TASKS)],
            [KeyboardButton(text=BTN_EXPRESS)],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True, is_persistent=True,
    )

def relax_submenu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_BREATHING), KeyboardButton(text=BTN_PRACTICES)],
            [KeyboardButton(text=BTN_MOON)],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True, is_persistent=True,
    )

def info_submenu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_ABOUT), KeyboardButton(text=BTN_TIME)],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True, is_persistent=True,
    )

def gender_kb():
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="👦 Мужской", callback_data="gender:male"),
        InlineKeyboardButton(text="👧 Женский", callback_data="gender:female"),
        InlineKeyboardButton(text="🤷 Другое",  callback_data="gender:other"),
    )
    return b.as_markup()

def trial_kb():
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="✅ Да, пройти сейчас", callback_data="trial:yes"),
        InlineKeyboardButton(text="⏭ Позже",              callback_data="trial:no"),
    )
    return b.as_markup()

def likert_kb(prefix):
    b = InlineKeyboardBuilder()
    for i in range(1, 6):
        b.button(text=str(i), callback_data=f"{prefix}:{i}")
    b.adjust(5)
    return b.as_markup()

def breathing_kb():
    b = InlineKeyboardBuilder()
    for cb, label in [
        ("breath_square",    "🔲 Квадратное дыхание"),
        ("breath_478",       "4️⃣ Дыхание 4-7-8"),
        ("breath_diaphragm", "🫁 Диафрагмальное"),
        ("breath_relax",     "😌 Расслабляющее"),
        ("breath_nostril",   "👃 Через ноздри"),
    ]:
        b.button(text=label, callback_data=cb)
    b.adjust(1)
    return b.as_markup()

def practices_kb():
    b = InlineKeyboardBuilder()
    for cb, label in [
        ("practice_grounding",     "🌍 Заземление 5-4-3-2-1"),
        ("practice_cold_water",    "🧊 Холодная вода"),
        ("practice_muscle_relax",  "💪 Мышечная релаксация"),
        ("practice_mind_dump",     "📝 Выгрузка мыслей"),
        ("practice_mindful_walk",  "🌅 Осознанная прогулка"),
    ]:
        b.button(text=label, callback_data=cb)
    b.adjust(1)
    return b.as_markup()

def time_settings_kb():
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="🌆 Время опроса",       callback_data="time:survey"),
        InlineKeyboardButton(text="🌅 Утреннее время",     callback_data="time:morning"),
    )
    b.row(
        InlineKeyboardButton(text="❌ Отключить утреннюю", callback_data="time:morning_off"),
    )
    return b.as_markup()

def survey_start_kb():
    b = InlineKeyboardBuilder()
    b.button(text=SURVEY_START_BTN, callback_data="survey:start_main")
    return b.as_markup()

# ================================================================
#  FSM
# ================================================================

class RegSt(StatesGroup):
    gender       = State()
    trial        = State()
    survey_time  = State()
    morning_time = State()

class SurveySt(StatesGroup):
    main_q    = State()
    express_q = State()

class TimeSt(StatesGroup):
    survey  = State()
    morning = State()

class BroadcastSt(StatesGroup):
    waiting_text = State()

# ================================================================
#  УТИЛИТЫ
# ================================================================

TIME_RE = re.compile(r"^(\d{1,2}):(\d{2})$")

def norm_time(raw):
    m = TIME_RE.match((raw or "").strip())
    if not m: return None
    h, mn = int(m.group(1)), int(m.group(2))
    return f"{h:02d}:{mn:02d}" if (0 <= h <= 23 and 0 <= mn <= 59) else None

def det_zone(score):
    if score <= ZONE_GREEN[1]:  return "green"
    if score <= ZONE_RED[0]-1:  return "yellow"
    return "red"

def res_text(score, zone):
    return {"green": RESULT_GREEN, "yellow": RESULT_YELLOW, "red": RESULT_RED}[zone].format(score=score)

def expr_hint(score):
    if score <= 8:  return "Отличный баланс! Продолжай в том же духе 🌿"
    if score <= 14: return "Небольшое напряжение — попробуй дыхательную гимнастику 🧘"
    return "Высокая нагрузка. Сделай паузу и подыши 🫁"

def load_facts():
    if not os.path.exists(FACTS_FILE):
        return ["Стресс — нормальная реакция организма. Главное — научиться с ним работать 🌿"]
    with open(FACTS_FILE, encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip()]
    return lines or ["Краткий стресс (эустресс) может повышать продуктивность!"]

def moon_phase_key():
    import math
    today = date.today()
    y, m, d = today.year, today.month, today.day
    if m < 3:
        y -= 1
        m += 12
    a = int(y / 100)
    b = 2 - a + int(a / 4)
    jd = int(365.25 * (y + 4716)) + int(30.6001 * (m + 1)) + d + b - 1524.5
    days_since_new = (jd - 2451549.5) % 29.53059
    frac = days_since_new / 29.53059
    if frac < 0.03:   return "new"
    if frac < 0.25:   return "waxing_crescent"
    if frac < 0.27:   return "first_quarter"
    if frac < 0.48:   return "waxing_gibbous"
    if frac < 0.52:   return "full"
    if frac < 0.73:   return "waning_gibbous"
    if frac < 0.77:   return "last_quarter"
    if frac < 0.97:   return "waning_crescent"
    return "new"

MOON_FILE_MAP = {
    "new":             "new_moon",
    "waxing_crescent": "waxing_crescent",
    "first_quarter":   "first_quarter",
    "waxing_gibbous":  "waxing_gibbous",
    "full":            "full_moon",
    "waning_gibbous":  "waning_gibbous",
    "last_quarter":    "third_quarter",
    "waning_crescent": "waning_crescent",
}

def moon_photo(phase_key):
    filename = MOON_FILE_MAP.get(phase_key, phase_key)
    for ext in ("jpg", "jpeg", "png", "webp"):
        p = os.path.join(MOON_DIR, f"{filename}.{ext}")
        if os.path.exists(p): return p
    return None

def rand_morning_img():
    if not os.path.isdir(MORNING_DIR): return None
    files = [f for f in os.listdir(MORNING_DIR)
             if f.lower().endswith((".jpg",".jpeg",".png",".webp"))]
    return os.path.join(MORNING_DIR, random.choice(files)) if files else None

def fmt_moods(rows):
    if not rows: return STATS_EMPTY
    lines = []
    for r in rows:
        icon = ZONE_ICONS.get(r["zone"], "⚪")
        dt   = r["created_at"][:10]
        lines.append(f"{icon} {r['score']} баллов — {dt}")
    return "\n".join(lines)

# ================================================================
#  РОУТЕР
# ================================================================

router = Router()
MD = "Markdown"

# ── Навигация ─────────────────────────────────────────────────

@router.message(F.text == BTN_STATS_GROUP)
async def open_stats(msg: Message):
    await msg.answer(GROUP_STATS_TEXT, parse_mode=MD, reply_markup=stats_submenu())

@router.message(F.text == BTN_RELAX_GROUP)
async def open_relax(msg: Message):
    await msg.answer(GROUP_RELAX_TEXT, parse_mode=MD, reply_markup=relax_submenu())

@router.message(F.text == BTN_INFO_GROUP)
async def open_info(msg: Message):
    await msg.answer(GROUP_INFO_TEXT, parse_mode=MD, reply_markup=info_submenu())

@router.message(F.text == BTN_BACK)
async def go_back(msg: Message):
    await msg.answer("🏠 Главное меню", reply_markup=main_menu())

# ── /start ────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    upsert_user(msg.from_user.id, msg.from_user.username, msg.from_user.first_name)
    u = get_user(msg.from_user.id)
    if u and u["gender"]:
        await msg.answer(
            f"С возвращением, {msg.from_user.first_name}! 👋\nВсё готово — пользуйся меню 👇",
            reply_markup=main_menu()
        )
        await state.clear()
        return
    await msg.answer(WELCOME, parse_mode=MD)
    await msg.answer(ASK_GENDER, parse_mode=MD, reply_markup=gender_kb())
    await state.set_state(RegSt.gender)

@router.message(Command("help"))
async def cmd_help(msg: Message):
    await msg.answer(HELP_TEXT, parse_mode=MD)

@router.callback_query(RegSt.gender, F.data.startswith("gender:"))
async def cb_gender(call: CallbackQuery, state: FSMContext):
    set_gender(call.from_user.id, call.data.split(":")[1])
    await call.message.edit_reply_markup()
    await call.message.answer(GENDER_SAVED, parse_mode=MD, reply_markup=trial_kb())
    await state.set_state(RegSt.trial)
    await call.answer()

@router.callback_query(RegSt.trial, F.data.startswith("trial:"))
async def cb_trial(call: CallbackQuery, state: FSMContext):
    await call.message.edit_reply_markup()
    await state.update_data(run_trial=(call.data.split(":")[1] == "yes"))
    await call.message.answer(ASK_SURVEY_TIME, parse_mode=MD)
    await state.set_state(RegSt.survey_time)
    await call.answer()

@router.message(RegSt.survey_time)
async def reg_survey_time(msg: Message, state: FSMContext):
    t = norm_time(msg.text)
    if not t:
        await msg.answer(TIME_INVALID, parse_mode=MD); return
    set_survey_time(msg.from_user.id, t)
    await msg.answer(TIME_SAVED.format(time=t), parse_mode=MD)
    await msg.answer(ASK_MORNING_TIME, parse_mode=MD)
    await state.set_state(RegSt.morning_time)

@router.message(RegSt.morning_time)
async def reg_morning_time(msg: Message, state: FSMContext):
    text = (msg.text or "").strip()
    data = await state.get_data()
    if text.lower() == "/skip":
        set_morning_time(msg.from_user.id, None)
    else:
        t = norm_time(text)
        if not t:
            await msg.answer(TIME_INVALID, parse_mode=MD); return
        set_morning_time(msg.from_user.id, t)
        await msg.answer(TIME_SAVED.format(time=t), parse_mode=MD)
    await msg.answer(SETUP_DONE, parse_mode=MD, reply_markup=main_menu())
    await state.clear()
    if data.get("run_trial"):
        await _start_survey(msg, state)

# ── Настройка времени ─────────────────────────────────────────

@router.message(F.text == BTN_TIME)
async def menu_time(msg: Message):
    u = get_user(msg.from_user.id)
    if not u: return
    await msg.answer(
        TIME_SETTINGS.format(
            survey_time=u["survey_time"],
            morning_time=u["morning_time"] or "отключена"
        ),
        parse_mode=MD, reply_markup=time_settings_kb()
    )

@router.callback_query(F.data == "time:survey")
async def cbt_survey(call: CallbackQuery, state: FSMContext):
    await call.message.answer("✏️ Введи новое время вечернего опроса (формат *ЧЧ:ММ*):", parse_mode=MD)
    await state.set_state(TimeSt.survey); await call.answer()

@router.callback_query(F.data == "time:morning")
async def cbt_morning(call: CallbackQuery, state: FSMContext):
    await call.message.answer("✏️ Введи время утренней рассылки (формат *ЧЧ:ММ*):", parse_mode=MD)
    await state.set_state(TimeSt.morning); await call.answer()

@router.callback_query(F.data == "time:morning_off")
async def cbt_morning_off(call: CallbackQuery):
    set_morning_time(call.from_user.id, None)
    await call.message.answer(TIME_MORNING_OFF); await call.answer()

@router.message(TimeSt.survey)
async def edit_survey(msg: Message, state: FSMContext):
    t = norm_time(msg.text)
    if not t:
        await msg.answer(TIME_INVALID, parse_mode=MD); return
    set_survey_time(msg.from_user.id, t)
    await msg.answer(TIME_SAVED.format(time=t), parse_mode=MD)
    await state.clear()

@router.message(TimeSt.morning)
async def edit_morning(msg: Message, state: FSMContext):
    t = norm_time(msg.text)
    if not t:
        await msg.answer(TIME_INVALID, parse_mode=MD); return
    set_morning_time(msg.from_user.id, t)
    await msg.answer(TIME_SAVED.format(time=t), parse_mode=MD)
    await state.clear()

# ── Основной опрос ────────────────────────────────────────────

async def _start_survey(msg: Message, state: FSMContext):
    questions = get_random_survey_questions()
    await state.set_state(SurveySt.main_q)
    await state.update_data(answers=[], questions=questions)
    await msg.answer(questions[0], parse_mode=MD, reply_markup=likert_kb("mq"))

@router.callback_query(F.data == "survey:start_main")
async def cb_survey_start(call: CallbackQuery, state: FSMContext):
    await call.message.edit_reply_markup()
    await _start_survey(call.message, state)
    await call.answer()

@router.callback_query(SurveySt.main_q, F.data.startswith("mq:"))
async def cb_main_q(call: CallbackQuery, state: FSMContext, bot: Bot):
    value   = int(call.data.split(":")[1])
    data    = await state.get_data()
    answers = data.get("answers", [])
    questions = data.get("questions", get_random_survey_questions())
    answers.append(value)
    await call.message.edit_reply_markup()

    if len(answers) < len(questions):
        await state.update_data(answers=answers)
        await call.message.answer(
            questions[len(answers)], parse_mode=MD, reply_markup=likert_kb("mq")
        )
        await call.answer(); return

    uid    = call.from_user.id
    score  = sum(answers)
    zone   = det_zone(score)
    save_mood(uid, score, zone, "main", answers)
    streak = update_streak(uid)
    add_points(uid, POINTS_SURVEY)
    log_task(uid, "survey", POINTS_SURVEY)

    text = res_text(score, zone) + POINTS_ADDED.format(pts=POINTS_SURVEY)
    if streak > 1:
        add_points(uid, POINTS_STREAK_BONUS)
        text += "\n" + STREAK_BONUS.format(streak=streak)

    await call.message.answer(text, parse_mode=MD)
    await state.clear()
    await call.answer()

    name = call.from_user.first_name or str(uid)
    dt   = datetime.now().strftime("%d.%m.%Y %H:%M")
    for q in TRIGGER_QUESTIONS:
        if q <= len(answers) and answers[q-1] == TRIGGER_VALUE:
            try:
                await bot.send_message(ADMIN_ID, ADMIN_TRIGGER.format(uid=uid, name=name, q=q, dt=dt), parse_mode=MD)
            except Exception: pass
    rs = get_red_streak(uid)
    if rs >= RED_STREAK_ALERT:
        try:
            await bot.send_message(ADMIN_ID, ADMIN_RED.format(uid=uid, name=name, days=rs), parse_mode=MD)
        except Exception: pass

# ── Экспресс-тест ─────────────────────────────────────────────

@router.message(F.text == BTN_EXPRESS)
async def menu_express(msg: Message, state: FSMContext):
    uid = msg.from_user.id
    ldt = get_last_mood_dt(uid, "express")
    if ldt:
        diff = (datetime.now(pytz.utc) - ldt.replace(tzinfo=pytz.utc)).total_seconds()
        if diff < COOLDOWN_EXPRESS:
            mins = int((COOLDOWN_EXPRESS - diff) // 60) + 1
            await msg.answer(EXPRESS_COOLDOWN.format(minutes=mins), parse_mode=MD); return
    questions = get_random_express_questions()
    await state.set_state(SurveySt.express_q)
    await state.update_data(answers=[], questions=questions)
    await msg.answer(EXPRESS_START, parse_mode=MD)
    await msg.answer(questions[0], parse_mode=MD, reply_markup=likert_kb("eq"))

@router.callback_query(SurveySt.express_q, F.data.startswith("eq:"))
async def cb_express_q(call: CallbackQuery, state: FSMContext):
    value   = int(call.data.split(":")[1])
    data    = await state.get_data()
    answers = data.get("answers", [])
    questions = data.get("questions", get_random_express_questions())
    answers.append(value)
    await call.message.edit_reply_markup()

    if len(answers) < len(questions):
        await state.update_data(answers=answers)
        await call.message.answer(
            questions[len(answers)], parse_mode=MD, reply_markup=likert_kb("eq")
        )
        await call.answer(); return

    uid   = call.from_user.id
    score = sum(answers)
    avg   = round(score / len(answers), 1)
    save_mood(uid, score, "yellow", "express", answers)
    add_points(uid, POINTS_EXPRESS)
    log_task(uid, "express", POINTS_EXPRESS)
    await call.message.answer(
        EXPRESS_RESULT.format(score=score, avg=avg, hint=expr_hint(score)), parse_mode=MD
    )
    await state.clear(); await call.answer()

# ── Дыхание ───────────────────────────────────────────────────

@router.message(F.text == BTN_BREATHING)
async def menu_breathing(msg: Message):
    await msg.answer(BREATHING_MENU, parse_mode=MD, reply_markup=breathing_kb())

@router.callback_query(F.data.startswith("breath_"))
async def cb_breath(call: CallbackQuery):
    text = BREATHING_TEXTS.get(call.data)
    if not text:
        await call.answer("Неизвестное упражнение"); return
    uid = call.from_user.id
    ldt = get_last_mood_dt(uid, "breathing")
    can = True
    if ldt:
        diff = (datetime.now(pytz.utc) - ldt.replace(tzinfo=pytz.utc)).total_seconds()
        can  = diff >= COOLDOWN_BREATHING
    if can:
        pts = POINTS_BREATH_FIRST if not task_done_today(uid, "breathing") else POINTS_BREATH_NEXT
        add_points(uid, pts); log_task(uid, "breathing", pts)
        save_mood(uid, 0, "green", "breathing", [])
        out = text + BREATHING_POINTS.format(pts=pts)
    else:
        diff = (datetime.now(pytz.utc) - ldt.replace(tzinfo=pytz.utc)).total_seconds()
        mins = int((COOLDOWN_BREATHING - diff) // 60) + 1
        out  = BREATHING_COOLDOWN.format(minutes=mins, text=text)
    await call.message.answer(out, parse_mode=MD); await call.answer()

# ── Практики (ИЗМЕНЕНИЕ 5) ────────────────────────────────────

@router.message(F.text == BTN_PRACTICES)
async def menu_practices(msg: Message):
    await msg.answer(PRACTICES_MENU_TEXT, parse_mode=MD, reply_markup=practices_kb())

@router.callback_query(F.data.startswith("practice_"))
async def cb_practice(call: CallbackQuery):
    text = PRACTICES_TEXTS.get(call.data)
    if not text:
        await call.answer("Неизвестная практика"); return
    await call.message.answer(text, parse_mode=MD)
    await call.answer()

# ── Статистика ────────────────────────────────────────────────

@router.message(F.text == BTN_MY_STATS)
async def menu_stats(msg: Message):
    u = get_user(msg.from_user.id)
    if not u:
        await msg.answer("Сначала зарегистрируйся — нажми /start"); return
    await msg.answer(
        STATS_TEMPLATE.format(
            points       = u["points"],
            streak       = u["streak"],
            survey_list  = fmt_moods(get_last_moods(msg.from_user.id, "main",    7)),
            express_list = fmt_moods(get_last_moods(msg.from_user.id, "express", 7)),
        ),
        parse_mode=MD
    )

@router.message(F.text == BTN_TASKS)
async def menu_tasks(msg: Message):
    u = get_user(msg.from_user.id)
    if not u:
        await msg.answer("Сначала зарегистрируйся — нажми /start"); return
    done = {t["task_type"] for t in get_today_tasks(msg.from_user.id)}
    await msg.answer(
        TASKS_TEMPLATE.format(
            survey_status  = DONE_ICON if "survey"    in done else TODO_ICON,
            breath_status  = DONE_ICON if "breathing" in done else TODO_ICON,
            express_status = DONE_ICON if "express"   in done else TODO_ICON,
            points         = u["points"],
        ),
        parse_mode=MD
    )

# ── Фаза луны ─────────────────────────────────────────────────

@router.message(F.text == BTN_MOON)
async def menu_moon(msg: Message):
    key = moon_phase_key()
    if key is None:
        await msg.answer(
            "🌙 Не удалось рассчитать фазу луны.\n"
            "_Попробуй позже._",
            parse_mode=MD
        )
        return

    icon, name = MOON_PHASES.get(key, ("🌙", "Неизвестная фаза"))
    caption    = f"{icon} *{name}*\n\n_{date.today().strftime('%d.%m.%Y')}_"
    if random.random() < 0.30:
        caption += MOON_DISCLAIMER

    photo = moon_photo(key)
    try:
        if photo:
            await msg.answer_photo(FSInputFile(photo), caption=caption, parse_mode=MD)
        else:
            await msg.answer(
                caption + "\n\n_(картинка не найдена в папке moon\\_photos)_",
                parse_mode=MD
            )
    except Exception as e:
        logger.error("moon send error: %s", e)
        await msg.answer(caption, parse_mode=MD)

# ── О боте ─────────────────────────────────────────────────────

@router.message(F.text == BTN_ABOUT)
async def menu_about(msg: Message):
    await msg.answer(ABOUT_TEXT, parse_mode=MD)

# ── Администратор ─────────────────────────────────────────────

def _adm(msg): return msg.from_user.id == ADMIN_ID

# ИЗМЕНЕНИЕ 1: команда /admin — показывает все доступные команды
@router.message(Command("admin"))
async def cmd_admin(msg: Message):
    if not _adm(msg): return
    await msg.answer(ADMIN_HELP_TEXT, parse_mode=MD)

@router.message(Command("admin_stats"))
async def cmd_admin_stats(msg: Message):
    if not _adm(msg): return
    s = admin_general_stats()
    z = s["zones"]
    await msg.answer(
        f"📊 *Общая статистика*\n\n"
        f"👥 Всего: *{s['total']}*\n"
        f"📆 Активны за 7 дней: *{s['active_7d']}*\n"
        f"📈 Средний балл: *{s['avg_score']}*\n\n"
        f"Зоны: 🟢 {z.get('green',0)}  🟡 {z.get('yellow',0)}  🔴 {z.get('red',0)}\n\n"
        f"Пол:\n" + "\n".join(f"  {k}: {v}" for k, v in s["genders"].items()),
        parse_mode=MD
    )

@router.message(Command("admin_users"))
async def cmd_admin_users(msg: Message):
    if not _adm(msg): return
    users = admin_all_users()
    if not users:
        await msg.answer("Нет пользователей."); return
    lines = [
        f"`{u['user_id']}` @{u['username'] or '-нет юза-'} | "
        f"🏆{u['points']} | 🔥{u['streak']} | ⏰{u['survey_time']}"
        for u in users
    ]
    buf = []
    for line in lines:
        buf.append(line)
        if len("\n".join(buf)) > 3800:
            await msg.answer("\n".join(buf[:-1]), parse_mode=MD)
            buf = [buf[-1]]
    if buf:
        await msg.answer("\n".join(buf), parse_mode=MD)

@router.message(Command("export_stats"))
async def cmd_export(msg: Message):
    if not _adm(msg): return
    rows = export_moods_csv(30)
    buf  = io.StringIO()
    w    = csv.writer(buf)
    w.writerow(["id","user_id","username","score","zone","type","created_at"])
    for r in rows:
        w.writerow([r["id"],r["user_id"],r["username"],r["score"],r["zone"],r["mood_type"],r["created_at"]])
    await msg.answer_document(
        BufferedInputFile(buf.getvalue().encode("utf-8"), filename="stats_30d.csv"),
        caption="📂 Экспорт за 30 дней"
    )

@router.message(Command("add_points"))
async def cmd_add_points(msg: Message):
    if not _adm(msg): return
    parts = (msg.text or "").split()
    if len(parts) != 3:
        await msg.answer("Формат: /add_points <user_id> <points>"); return
    try:
        uid, pts = int(parts[1]), int(parts[2])
    except ValueError:
        await msg.answer("Неверные аргументы."); return
    total = add_points(uid, pts)
    await msg.answer(f"✅ Начислено *{pts}* очков `{uid}`. Итого: *{total}*", parse_mode=MD)

@router.message(Command("set_points"))
async def cmd_set_points(msg: Message):
    if not _adm(msg): return
    parts = (msg.text or "").split()
    if len(parts) != 3:
        await msg.answer("Формат: /set_points <user_id> <points>"); return
    try:
        uid, pts = int(parts[1]), int(parts[2])
    except ValueError:
        await msg.answer("Неверные аргументы."); return
    set_points_value(uid, pts)
    await msg.answer(f"✅ Очки `{uid}` установлены: *{pts}*", parse_mode=MD)

# ИЗМЕНЕНИЕ 3: команда /broadcast — рассылка всем пользователям
@router.message(Command("broadcast"))
async def cmd_broadcast(msg: Message, state: FSMContext):
    if not _adm(msg): return
    # Если текст указан сразу — /broadcast <текст>
    parts = (msg.text or "").split(None, 1)
    if len(parts) > 1 and parts[1].strip():
        await _do_broadcast(msg, parts[1].strip())
    else:
        await msg.answer(
            "✉️ *Рассылка*\n\nОтправь текст сообщения для рассылки всем пользователям.\n\n"
            "_Для отмены напиши /cancel_",
            parse_mode=MD
        )
        await state.set_state(BroadcastSt.waiting_text)

@router.message(BroadcastSt.waiting_text)
async def broadcast_text_received(msg: Message, state: FSMContext, bot: Bot):
    if not _adm(msg): return
    text = (msg.text or "").strip()
    if text.lower() in ("/cancel", "отмена"):
        await msg.answer("❌ Рассылка отменена.")
        await state.clear()
        return
    await state.clear()
    await _do_broadcast(msg, text, bot)

async def _do_broadcast(msg: Message, text: str, bot: Bot = None):
    """Отправляет сообщение всем пользователям. bot передаётся при вызове из FSM."""
    # Получаем bot из msg если не передан явно
    _bot = bot or msg.bot
    users = get_all_users()
    sent = 0
    failed = 0
    broadcast_text = f"📢 *Новое обновление!*\n\n{text}"
    for u in users:
        try:
            await _bot.send_message(u["user_id"], broadcast_text, parse_mode=MD)
            sent += 1
        except Exception:
            failed += 1
    await msg.answer(
        f"✅ *Рассылка завершена*\n\n"
        f"📤 Отправлено: *{sent}*\n"
        f"❌ Не доставлено: *{failed}*",
        parse_mode=MD
    )

# ================================================================
#  ПЛАНИРОВЩИК
# ================================================================

async def job_facts(bot: Bot):
    fact  = random.choice(load_facts())
    for u in get_all_users():
        try:
            await bot.send_message(u["user_id"], FACT_PREFIX + fact, parse_mode=MD)
        except Exception: pass

async def job_evening(bot: Bot):
    now = datetime.now(MSK).strftime("%H:%M")
    for u in get_users_by_survey_time(now):
        try:
            await bot.send_message(
                u["user_id"], EVENING_PUSH, parse_mode=MD,
                reply_markup=survey_start_kb()
            )
        except Exception: pass

async def job_morning(bot: Bot):
    now = datetime.now(MSK).strftime("%H:%M")
    img = rand_morning_img()
    cap = random.choice(MORNING_CAPTIONS)
    for u in get_users_by_morning_time(now):
        try:
            if img:
                await bot.send_photo(u["user_id"], FSInputFile(img), caption=cap, parse_mode=MD)
            else:
                await bot.send_message(u["user_id"], cap, parse_mode=MD)
        except Exception: pass

def setup_scheduler(bot: Bot):
    s = AsyncIOScheduler(timezone=MSK)
    h, m = map(int, FACTS_TIME.split(":"))
    s.add_job(job_facts,   "cron", hour=h, minute=m, kwargs={"bot": bot})
    s.add_job(job_evening, "cron", minute="*",        kwargs={"bot": bot})
    s.add_job(job_morning, "cron", minute="*",        kwargs={"bot": bot})
    return s

# ================================================================
#  ЗАПУСК
# ================================================================

async def main():
    init_db()
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN),
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    sched = setup_scheduler(bot)
    sched.start()
    logger.info("Бот %s v%s запущен, БД: %s", BOT_NAME, VERSION, DB_PATH)

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        sched.shutdown()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
