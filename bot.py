
import os
import re
import json
import asyncio
import hashlib
import logging
from html import escape
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import aiosqlite
from dotenv import load_dotenv
from telethon import TelegramClient
from openai import AsyncOpenAI

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

# =========================================================
# FITPULSE PRO v2.0
# Telegram Fitness & Health Intelligence System
# =========================================================

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

log = logging.getLogger("fitpulse")

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
BOT_TOKEN = os.environ["TG_BOT_TOKEN"]
OWNER_ID = int(os.environ["OWNER_ID"])
TARGET = os.environ["TARGET_CHANNEL"]

TIMEZONE = ZoneInfo(os.getenv("TIMEZONE", "Europe/Paris"))
POLL_MINUTES = int(os.getenv("POLL_MINUTES", "5"))
PUBLISH_INTERVAL_HOURS = int(
    os.getenv("PUBLISH_INTERVAL_HOURS", "6")
)

DB_PATH = os.getenv("DB_PATH", "fitpulse.db")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is required.")

if OWNER_ID <= 0:
    raise RuntimeError("Set a valid OWNER_ID.")

if PUBLISH_INTERVAL_HOURS < 1 or 24 % PUBLISH_INTERVAL_HOURS:
    raise RuntimeError("Publishing interval must divide 24.")

ai = AsyncOpenAI(api_key=OPENAI_API_KEY)
publish_lock = asyncio.Lock()

CATEGORIES = {
    "🏆 اخبار ورزشی": [
        "فوتبال", "بسکتبال", "والیبال", "بدنسازی",
        "فیتنس", "ورزش", "لیگ", "مسابقه", "قهرمان",
        "football", "basketball", "volleyball",
        "fitness", "gym", "sport", "match", "league",
    ],
    "🥗 سلامت و تغذیه": [
        "سلامت", "تغذیه", "پروتئین", "ویتامین",
        "مکمل", "کالری", "رژیم", "خواب", "ریکاوری",
        "health", "nutrition", "protein", "supplement",
        "recovery", "diet", "sleep", "medical",
    ],
    "🌱 سبک زندگی سالم": [
        "سبک زندگی", "عادت", "پیاده‌روی",
        "مدیتیشن", "استرس", "آب", "lifestyle",
        "habit", "walking", "mindfulness",
        "healthy", "stress", "hydration",
    ],
}


# =========================================================
# UTILITIES
# =========================================================

def clean(text):
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"@\w+", "", text)
    return re.sub(r"\s+", " ", text).strip()


def categorize(text):
    low = text.lower()

    scores = {
        category: sum(
            1 for word in keywords if word.lower() in low
        )
        for category, keywords in CATEGORIES.items()
    }

    best = max(scores, key=scores.get)

    return best if scores[best] else "🌱 سبک زندگی سالم"


async def summarize(text):
    try:
        response = await ai.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.3,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "متن ورودی را به فارسی روان و دقیق ترجمه کن "
                        "و در ۲ تا ۳ جمله خلاصه کن. "
                        "نام‌ها، اعداد، تاریخ‌ها و نتایج را حفظ کن. "
                        "هیچ ادعای جدیدی نساز. "
                        "توصیه پزشکی شخصی ارائه نکن. "
                        "محدودیت شواهد علمی را حفظ کن. "
                        "خروجی فقط متن خلاصه باشد."
                    ),
                },
                {
                    "role": "user",
                    "content": text[:12000],
                },
            ],
        )

        result = response.choices[0].message.content

        if not result:
            raise ValueError("Empty AI response")

        return result.strip()

    except Exception:
        log.exception("AI summarization failed")
        return "⚠️ خلاصه‌سازی هوشمند ناموفق بود؛ متن اصلی: " + clean(text)[:500]


# =========================================================
# DATABASE
# =========================================================

async def db_execute(query, params=()):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(query, params)
        await db.commit()


async def get_setting(key, default=None):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT value FROM settings WHERE key=?",
            (key,),
        )
        row = await cur.fetchone()

    return row[0] if row else default


async def set_setting(key, value):
    await db_execute(
        """
        INSERT INTO settings(key,value)
        VALUES(?,?)
        ON CONFLICT(key)
        DO UPDATE SET value=excluded.value
        """,
        (key, str(value)),
    )


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                source_msg_id INTEGER NOT NULL,
                digest TEXT UNIQUE NOT NULL,
                category TEXT NOT NULL,
                summary TEXT NOT NULL,
                url TEXT,
                created_at TEXT NOT NULL,
                published INTEGER DEFAULT 0,
                UNIQUE(source, source_msg_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS state (
                source TEXT PRIMARY KEY,
                last_id INTEGER DEFAULT 0
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

        await db.commit()


async def get_sources():
    saved = await get_setting("sources")

    if saved:
        return json.loads(saved)

    return [
        x.strip()
        for x in os.getenv("SOURCE_CHANNELS", "").split(",")
        if x.strip()
    ]


async def save_sources(sources):
    await set_setting(
        "sources",
        json.dumps(sources, ensure_ascii=False),
    )


# =========================================================
# NEWS COLLECTION
# =========================================================

async def add_post(source, msg_id, body, url):
    normalized = clean(body).lower()

    digest = hashlib.sha256(
        normalized.encode("utf-8")
    ).hexdigest()

    category = categorize(body)
    summary = await summarize(body)

    try:
        await db_execute(
            """
            INSERT INTO posts(
                source,source_msg_id,digest,
                category,summary,url,created_at
            )
            VALUES(?,?,?,?,?,?,?)
            """,
            (
                source,
                msg_id,
                digest,
                category,
                summary,
                url,
                datetime.now(timezone.utc).isoformat(),
            ),
        )

        return True

    except aiosqlite.IntegrityError:
        return False


async def collect(client):
    sources = await get_sources()

    for source in sources:
        try:
            entity = await client.get_entity(source)

            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute(
                    "SELECT last_id FROM state WHERE source=?",
                    (source,),
                )
                row = await cur.fetchone()

            last_id = row[0] if row else 0
            newest = last_id

            async for msg in client.iter_messages(
                entity,
                min_id=last_id,
                reverse=True,
                limit=100,
            ):
                newest = max(newest, msg.id)

                body = msg.message or ""

                if len(clean(body)) < 45:
                    continue

                username = source.lstrip("@").split("/")[-1]

                url = (
                    f"https://t.me/{username}/{msg.id}"
                    if source.startswith("@")
                    else ""
                )

                inserted = await add_post(
                    source,
                    msg.id,
                    body,
                    url,
                )

                if inserted:
                    log.info("Collected: %s / %s", source, msg.id)

            await db_execute(
                """
                INSERT INTO state(source,last_id)
                VALUES(?,?)
                ON CONFLICT(source)
                DO UPDATE SET last_id=excluded.last_id
                """,
                (source, newest),
            )

        except Exception:
            log.exception("Collection failed: %s", source)


# =========================================================
# PUBLISHING ENGINE
# =========================================================

async def publish_digest(bot):
    async with publish_lock:

        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("""
                SELECT id,category,summary,url
                FROM posts
                WHERE published=0
                ORDER BY created_at ASC
                LIMIT 60
            """)

            rows = await cur.fetchall()

        if not rows:
            log.info("No pending news")
            return False

        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")

        groups = {}

        for row in rows:
            groups.setdefault(row[1], []).append(row)

        header = (
            f"🔥 <b>FITPULSE PRO</b>\n"
            f"📅 <code>{today}</code>\n"
            f"━━━━━━━━━━━━━━━━━━"
        )

        chunks = []
        current = header

        for category, items in groups.items():
            section = f"\n\n<b>{escape(category)}</b>\n"

            if len(current) + len(section) > 3800:
                chunks.append(current)
                current = header

            current += section

            for item in items:
                summary = escape(item[2][:1200])

                line = f"\n• {summary}"

                if item[3]:
                    url = escape(item[3], quote=True)
                    line += f'\n<a href="{url}">🔗 مشاهده منبع</a>'

                if len(current) + len(line) > 3800:
                    chunks.append(current)
                    current = header

                current += line

        current += (
            "\n\n━━━━━━━━━━━━━━━━━━"
            "\n<i>گردآوری و خلاصه‌سازی خودکار | FITPULSE PRO</i>"
        )

        if current:
            chunks.append(current)

        for chunk in chunks:
            await bot.send_message(
                chat_id=TARGET,
                text=chunk,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )

        async with aiosqlite.connect(DB_PATH) as db:
            await db.executemany(
                "UPDATE posts SET published=1 WHERE id=?",
                [(row[0],) for row in rows],
            )
            await db.commit()

        log.info("Published %d news items", len(rows))

        return True


# =========================================================
# ADMIN SECURITY
# =========================================================

def is_owner(update):
    user = update.effective_user
    chat = update.effective_chat

    return bool(
        user
        and chat
        and user.id == OWNER_ID
        and chat.type == "private"
    )


async def deny(update):
    if update.callback_query:
        await update.callback_query.answer(
            "⛔ دسترسی غیرمجاز",
            show_alert=True,
        )
    elif update.effective_message:
        await update.effective_message.reply_text(
            "⛔ شما اجازه مدیریت این ربات را ندارید."
        )


def admin_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "📊 آمار سیستم",
                callback_data="stats",
            ),
            InlineKeyboardButton(
                "📰 صف اخبار",
                callback_data="queue",
            ),
        ],
        [
            InlineKeyboardButton(
                "🚀 انتشار فوری",
                callback_data="publish",
            ),
            InlineKeyboardButton(
                "📡 منابع خبری",
                callback_data="sources",
            ),
        ],
        [
            InlineKeyboardButton(
                "🧠 تنظیمات هوش مصنوعی",
                callback_data="ai",
            ),
            InlineKeyboardButton(
                "⏰ زمان‌بندی",
                callback_data="schedule",
            ),
        ],
        [
            InlineKeyboardButton(
                "🔄 بروزرسانی پنل",
                callback_data="home",
            ),
        ],
    ])


async def admin_panel(update, context):
    if not is_owner(update):
        await deny(update)
        return

    await update.effective_message.reply_text(
        "🔥 <b>FITPULSE PRO CONTROL PANEL</b>\n\n"
        "به پنل مدیریت خوش آمدید.\n"
        "تمام بخش‌های مدیریتی از این قسمت قابل کنترل هستند.",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_keyboard(),
    )


async def stats_text():
    async with aiosqlite.connect(DB_PATH) as db:

        cur = await db.execute(
            "SELECT COUNT(*) FROM posts"
        )
        total = (await cur.fetchone())[0]

        cur = await db.execute(
            "SELECT COUNT(*) FROM posts WHERE published=1"
        )
        published = (await cur.fetchone())[0]

        cur = await db.execute(
            "SELECT COUNT(*) FROM posts WHERE published=0"
        )
        pending = (await cur.fetchone())[0]

        cur = await db.execute(
            "SELECT COUNT(*) FROM state"
        )
        sources = (await cur.fetchone())[0]

    return (
        "📊 <b>گزارش عملکرد FITPULSE</b>\n\n"
        f"📰 کل اخبار: <b>{total}</b>\n"
        f"✅ منتشرشده: <b>{published}</b>\n"
        f"⏳ در صف انتظار: <b>{pending}</b>\n"
        f"📡 منابع ثبت‌شده در وضعیت جمع‌آوری: <b>{sources}</b>"
    )


# =========================================================
# ADMIN CALLBACKS
# =========================================================

async def admin_callback(update, context):
    query = update.callback_query

    if not is_owner(update):
        await deny(update)
        return

    await query.answer()

    action = query.data

    if action == "home":
        await query.edit_message_text(
            "🔥 <b>FITPULSE PRO CONTROL PANEL</b>\n"
            "مدیریت مرکزی سیستم",
            parse_mode=ParseMode.HTML,
            reply_markup=admin_keyboard(),
        )

    elif action == "stats":
        await query.edit_message_text(
            await stats_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(
                        "🔙 بازگشت",
                        callback_data="home",
                    )
                ]
            ]),
        )

    elif action == "queue":
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("""
                SELECT category,summary,source
                FROM posts
                WHERE published=0
                ORDER BY created_at DESC
                LIMIT 8
            """)
            rows = await cur.fetchall()

        if rows:
            text = "📰 <b>اخبار در صف انتظار</b>\n\n"

            for i, row in enumerate(rows, 1):
                text += (
                    f"{i}. <b>{escape(row[0])}</b>\n"
                    f"{escape(row[1][:250])}\n"
                    f"<code>{escape(row[2])}</code>\n\n"
                )
        else:
            text = "📭 خبر جدیدی در صف انتظار وجود ندارد."

        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(
                        "🔙 بازگشت",
                        callback_data="home",
                    )
                ]
            ]),
        )

    elif action == "publish":
        await query.edit_message_text(
            "⏳ در حال آماده‌سازی و انتشار بولتن..."
        )

        try:
            result = await publish_digest(context.bot)

            message = (
                "✅ بولتن با موفقیت منتشر شد."
                if result
                else "📭 خبری برای انتشار وجود ندارد."
            )

        except Exception:
            log.exception("Manual publishing failed")
            message = "❌ انتشار ناموفق بود. گزارش خطا ثبت شد."

        await context.bot.send_message(
            chat_id=OWNER_ID,
            text=message,
            reply_markup=admin_keyboard(),
        )

    elif action == "sources":
        sources = await get_sources()

        text = "📡 <b>منابع خبری فعال</b>\n\n"

        for i, source in enumerate(sources, 1):
            text += f"{i}. <code>{escape(source)}</code>\n"

        text += (
            "\nبرای افزودن منبع:\n"
            "<code>/addsource @channel</code>\n\n"
            "برای حذف منبع:\n"
            "<code>/removesource @channel</code>"
        )

        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(
                        "🔙 بازگشت",
                        callback_data="home",
                    )
                ]
            ]),
        )

    elif action == "ai":
        await query.edit_message_text(
            "🧠 <b>تنظیمات هوش مصنوعی</b>\n\n"
            f"مدل فعال: <code>{escape(OPENAI_MODEL)}</code>\n"
            "وظیفه: ترجمه و خلاصه‌سازی فارسی\n"
            "ارائه‌دهنده: OpenAI\n\n"
            "کلید API از متغیر محیطی خوانده می‌شود.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(
                        "🔙 بازگشت",
                        callback_data="home",
                    )
                ]
            ]),
        )

    elif action == "schedule":
        interval = await get_setting(
            "publish_interval",
            PUBLISH_INTERVAL_HOURS,
        )

        await query.edit_message_text(
            f"⏰ <b>فاصله انتشار فعلی: {interval} ساعت</b>\n\n"
            "فاصله جدید را انتخاب کن:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("۱ ساعت", callback_data="interval:1"),
                    InlineKeyboardButton("۳ ساعت", callback_data="interval:3"),
                ],
                [
                    InlineKeyboardButton("۶ ساعت", callback_data="interval:6"),
                    InlineKeyboardButton("۱۲ ساعت", callback_data="interval:12"),
                ],
                [
                    InlineKeyboardButton(
                        "🔙 بازگشت",
                        callback_data="home",
                    )
                ],
            ]),
        )

    elif action.startswith("interval:"):
        value = int(action.split(":")[1])

        await set_setting("publish_interval", value)

        await query.edit_message_text(
            f"✅ فاصله انتشار روی {value} ساعت ذخیره شد.\n"
            "این تنظیم در چرخه زمان‌بندی بعدی اعمال می‌شود.",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(
                        "🔙 بازگشت",
                        callback_data="home",
                    )
                ]
            ]),
        )


# =========================================================
# SOURCE MANAGEMENT COMMANDS
# =========================================================

async def add_source(update, context):
    if not is_owner(update):
        await deny(update)
        return

    if not context.args:
        await update.message.reply_text(
            "فرمت صحیح:\n/addsource @channel"
        )
        return

    source = context.args[0].strip()

    if not re.fullmatch(r"@[A-Za-z0-9_]{5,}", source):
        await update.message.reply_text(
            "❌ نام کاربری کانال معتبر نیست."
        )
        return

    sources = await get_sources()

    if source in sources:
        await update.message.reply_text(
            "این منبع قبلاً ثبت شده است."
        )
        return

    sources.append(source)
    await save_sources(sources)

    await update.message.reply_text(
        f"✅ منبع {source} اضافه شد.\n"
        "دسترسی حساب Telethon به این کانال باید برقرار باشد."
    )


async def remove_source(update, context):
    if not is_owner(update):
        await deny(update)
        return

    if not context.args:
        await update.message.reply_text(
            "فرمت صحیح:\n/removesource @channel"
        )
        return

    source = context.args[0].strip()
    sources = await get_sources()

    if source not in sources:
        await update.message.reply_text(
            "این منبع در فهرست وجود ندارد."
        )
        return

    sources.remove(source)
    await save_sources(sources)

    await update.message.reply_text(
        f"✅ منبع {source} حذف شد."
    )


async def start_command(update, context):
    if is_owner(update):
        await admin_panel(update, context)
    else:
        await update.message.reply_text(
            "سلام! من FitPulse Pro هستم."
        )


# =========================================================
# APPLICATION
# =========================================================

async def main():
    await init_db()

    client = TelegramClient(
        "fitpulse_user",
        API_ID,
        API_HASH,
    )

    await client.start()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .build()
    )

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("admin", admin_panel))
    app.add_handler(CommandHandler("addsource", add_source))
    app.add_handler(CommandHandler("removesource", remove_source))

    app.add_handler(
        CallbackQueryHandler(admin_callback)
    )

    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    log.info("FITPULSE PRO v2.0 STARTED")
    log.info("Owner ID: %s", OWNER_ID)

    try:
        while True:
            try:
                await collect(client)

                now = datetime.now(TIMEZONE)

                interval = int(
                    await get_setting(
                        "publish_interval",
                        PUBLISH_INTERVAL_HOURS,
                    )
                )

                slot_hour = (
                    now.hour // interval
                ) * interval

                slot = now.replace(
                    hour=slot_hour,
                    minute=0,
                    second=0,
                    microsecond=0,
                )

                slot_key = slot.isoformat()

                last_slot = await get_setting(
                    "last_publish_slot"
                )

                if last_slot != slot_key:
                    result = await publish_digest(app.bot)

                    if result:
                        await set_setting(
                            "last_publish_slot",
                            slot_key,
                        )

            except Exception:
                log.exception("Main loop error")

            await asyncio.sleep(POLL_MINUTES * 60)

    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
