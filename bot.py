import os, re, json, asyncio, hashlib, logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import aiosqlite
from telethon import TelegramClient
from telegram import Bot
from telegram.constants import ParseMode

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fitpulse")

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
BOT_TOKEN = os.environ["TG_BOT_TOKEN"]
TARGET = os.environ["TARGET_CHANNEL"]
TIMEZONE = ZoneInfo(os.getenv("TIMEZONE", "Europe/Paris"))
POLL_MINUTES = int(os.getenv("POLL_MINUTES", "5"))
PUBLISH_INTERVAL_HOURS = int(os.getenv("PUBLISH_INTERVAL_HOURS", "6"))
DB_PATH = os.getenv("DB_PATH", "fitpulse.db")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

SOURCES = [x.strip() for x in os.getenv("SOURCE_CHANNELS", "").split(",") if x.strip()]
if not SOURCES:
    raise RuntimeError("SOURCE_CHANNELS is empty. Add public channel usernames/links to .env")

CATEGORIES = {
    "🏆 اخبار ورزشی": ["فوتبال", "بسکتبال", "والیبال", "بدنسازی", "فیتنس", "ورزش", "لیگ", "مسابقه", "قهرمان", "تمرین", "athlete", "football", "basketball", "volleyball", "fitness", "gym", "sport", "match", "league", "championship", "transfer"],
    "🥗 سلامت و تغذیه": ["سلامت", "تغذیه", "پروتئین", "ویتامین", "مکمل", "کالری", "رژیم", "خواب", "ریکاوری", "پزشکی", "health", "nutrition", "protein", "supplement", "recovery", "diet", "sleep", "medical", "wellbeing", "wellness"],
    "🌱 سبک زندگی سالم": ["سبک زندگی", "عادت", "پیاده‌روی", "مدیتیشن", "استرس", "آب", "فعالیت روزانه", "lifestyle", "habit", "walking", "mindfulness", "healthy", "stress", "hydration", "daily routine"]
}

def categorize(text):
    low = text.lower()
    scores = {cat: sum(1 for kw in kws if kw.lower() in low) for cat, kws in CATEGORIES.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] else "🌱 سبک زندگی سالم"

def clean(text):
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"@\w+", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

async def summarize(text):
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is required for English-to-Persian translation. Set it in .env before running.")
    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        response = await client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.3,
            messages=[
                {"role": "system", "content": "متن ورودی ممکن است انگلیسی باشد. آن را به فارسی روان، دقیق و بی‌طرف ترجمه و در ۲ تا ۳ جمله خلاصه کن. نام اشخاص، تیم‌ها، اعداد، تاریخ‌ها و نتایج را حفظ کن. هیچ ادعای تازه، تشخیص یا توصیه پزشکی شخصی نساز. اگر متن علمی/سلامتی قطعی نیست، عدم قطعیت و محدودیت شواهد را صریح حفظ کن. خروجی فقط فارسی باشد."},
                {"role": "user", "content": text[:12000]}
            ]
        )
        return response.choices[0].message.content.strip()
    except Exception:
        log.exception("AI summary failed; using excerpt")
        return clean(text)[:650]

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL, source_msg_id INTEGER NOT NULL,
            digest TEXT UNIQUE NOT NULL, category TEXT NOT NULL,
            summary TEXT NOT NULL, url TEXT, created_at TEXT NOT NULL,
            published INTEGER DEFAULT 0,
            UNIQUE(source, source_msg_id)
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS state (
            source TEXT PRIMARY KEY, last_id INTEGER DEFAULT 0
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY, value TEXT NOT NULL
        )""")
        await db.commit()

async def add_post(source, msg_id, body, url):
    digest = hashlib.sha256(clean(body).lower().encode()).hexdigest()
    category = categorize(body)
    summary = await summarize(body)
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO posts(source,source_msg_id,digest,category,summary,url,created_at) VALUES(?,?,?,?,?,?,?)",
                (source, msg_id, digest, category, summary, url, datetime.now(timezone.utc).isoformat())
            )
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

async def collect(client):
    for source in SOURCES:
        try:
            entity = await client.get_entity(source)
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute("SELECT last_id FROM state WHERE source=?", (source,))
                row = await cur.fetchone()
                last_id = row[0] if row else 0
            newest = last_id
            async for msg in client.iter_messages(entity, min_id=last_id, reverse=True, limit=100):
                newest = max(newest, msg.id)
                body = msg.message or ""
                if len(clean(body)) < 45:
                    continue
                url = f"https://t.me/{source.lstrip('@')}/{msg.id}" if source.startswith("@") else ""
                inserted = await add_post(source, msg.id, body, url)
                if inserted:
                    log.info("Queued: %s / %s", source, msg.id)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("INSERT INTO state(source,last_id) VALUES(?,?) ON CONFLICT(source) DO UPDATE SET last_id=excluded.last_id", (source, newest))
                await db.commit()
        except Exception:
            log.exception("Could not collect from %s", source)

async def publish_digest(bot):
    today = datetime.now(TIMEZONE).date().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id,category,summary,url,created_at FROM posts WHERE published=0 ORDER BY created_at ASC LIMIT 60")
        rows = await cur.fetchall()
    if not rows:
        log.info("No new posts to publish")
        return
    grouped = {}
    for row in rows:
        grouped.setdefault(row[1], []).append(row)
    parts = [f"🔥 <b>FITPULSE | اخبار روز</b>\n<code>{today}</code>"]
    for category, items in grouped.items():
        parts.append(f"\n<b>{category}</b>")
        for i, item in enumerate(items, 1):
            summary = item[2].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            line = f"{i}. {summary}"
            if item[3]:
                line += f'\n   <a href="{item[3]}">🔗 منبع خبر</a>'
            parts.append(line)
    parts.append("\n<i>مطالب گردآوری و خلاصه شده‌اند؛ برای جزئیات به منبع اصلی مراجعه کنید.</i>")
    message = "\n".join(parts)
    # Telegram message limit is 4096 characters.
    chunks, current = [], ""
    for line in message.split("\n"):
        if len(current) + len(line) + 1 > 3800:
            chunks.append(current); current = ""
        current += ("\n" if current else "") + line
    if current: chunks.append(current)
    for chunk in chunks:
        await bot.send_message(chat_id=TARGET, text=chunk, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executemany("UPDATE posts SET published=1 WHERE id=?", [(r[0],) for r in rows])
        await db.commit()
    log.info("Published %d items", len(rows))

async def main():
    if not OPENAI_API_KEY:
        raise RuntimeError("برای ترجمه انگلیسی به فارسی، OPENAI_API_KEY را در فایل .env تنظیم کنید.")
    await init_db()
    client = TelegramClient("fitpulse_user", API_ID, API_HASH)
    await client.start()
    bot = Bot(BOT_TOKEN)
    log.info("FitPulse started. Sources: %s", SOURCES)
    if PUBLISH_INTERVAL_HOURS < 1 or 24 % PUBLISH_INTERVAL_HOURS != 0:
        raise RuntimeError("PUBLISH_INTERVAL_HOURS must be a positive divisor of 24, e.g. 6")
    log.info("Publishing automatically every %s hours (%s local time)", PUBLISH_INTERVAL_HOURS, TIMEZONE)
    while True:
        await collect(client)
        now = datetime.now(TIMEZONE)
        slot_hour = (now.hour // PUBLISH_INTERVAL_HOURS) * PUBLISH_INTERVAL_HOURS
        slot = now.replace(hour=slot_hour, minute=0, second=0, microsecond=0)
        slot_key = slot.isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("SELECT value FROM settings WHERE key='last_publish_slot'")
            row = await cur.fetchone()
        last_slot = row[0] if row else None
        if last_slot != slot_key:
            await publish_digest(bot)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("INSERT INTO settings(key,value) VALUES('last_publish_slot',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (slot_key,))
                await db.commit()
        await asyncio.sleep(POLL_MINUTES * 60)

if __name__ == "__main__":
    asyncio.run(main())
