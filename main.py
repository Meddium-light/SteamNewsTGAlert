import os
import asyncio
import random
from datetime import datetime, timezone
from typing import List, Optional, Set, Tuple

import httpx
import aiosqlite
import feedparser

from telegram import Update
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import Application, CommandHandler, ContextTypes


# ================= ENV =================

BOT_TOKEN = os.getenv("BOT_TOKEN")
ALLOWED_CHAT_IDS_RAW = os.getenv("ALLOWED_CHAT_IDS")

if not BOT_TOKEN:
    raise RuntimeError("ENV BOT_TOKEN не задан")

if not ALLOWED_CHAT_IDS_RAW:
    raise RuntimeError("ENV ALLOWED_CHAT_IDS не задан (формат: 123,456,789)")


def parse_allowed_chat_ids(raw: str) -> Set[int]:
    out = set()
    for x in raw.split(","):
        x = x.strip()
        if not x:
            continue
        try:
            out.add(int(x))
        except ValueError:
            raise RuntimeError(f"ALLOWED_CHAT_IDS содержит не число: {x}")
    if not out:
        raise RuntimeError("ALLOWED_CHAT_IDS пуст после парсинга")
    return out


ALLOWED_CHAT_IDS = parse_allowed_chat_ids(ALLOWED_CHAT_IDS_RAW)

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))
DB_PATH = os.getenv("DB_PATH", "state.db")
HEARTBEAT_MINUTES = int(os.getenv("HEARTBEAT_MINUTES", "30"))

APPID = 570
STEAM_NEWS_API = "https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/"
STEAM_NEWS_RSS = f"https://store.steampowered.com/feeds/news/app/{APPID}/?l=english"
FETCH_COUNT = 20


# ================= UTILS =================

def ts_to_str(unix_ts: int) -> str:
    dt = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M UTC")


async def db_init():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS kv (
                k TEXT PRIMARY KEY,
                v TEXT NOT NULL
            )
        """)
        await db.commit()


async def db_get(key: str) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT v FROM kv WHERE k = ?", (key,))
        row = await cur.fetchone()
        return row[0] if row else None


async def db_set(key: str, val: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (key, val),
        )
        await db.commit()


# ================= TELEGRAM SEND =================

async def send_with_retry(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str, html: bool = True) -> bool:
    for attempt in range(1, 6):
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=ParseMode.HTML if html else None,
                disable_web_page_preview=False,
            )
            return True
        except TelegramError:
            if attempt == 5:
                return False
            await asyncio.sleep(min(30, 2 ** attempt) + random.random())
    return False


async def alert_all(context: ContextTypes.DEFAULT_TYPE, text: str):
    msg = f"⚠️ <b>ALERT</b>\n{text}"
    for cid in ALLOWED_CHAT_IDS:
        await send_with_retry(context, cid, msg, html=True)


# ================= FETCH =================

async def fetch_api(client: httpx.AsyncClient) -> List[dict]:
    params = {
        "appid": APPID,
        "count": FETCH_COUNT,
        "maxlength": 0,
        "format": "json",
        "feeds": "steam_community_announcements",
    }
    r = await client.get(STEAM_NEWS_API, params=params, timeout=15)
    if r.status_code == 429:
        raise Exception("Steam rate limit 429")
    r.raise_for_status()
    return r.json()["appnews"]["newsitems"]


def fetch_rss() -> List[Tuple[str, str, str, int]]:
    """
    Returns list of tuples: (key, title, url, ts), newest -> older.
    key for RSS is link.
    """
    feed = feedparser.parse(STEAM_NEWS_RSS)
    out: List[Tuple[str, str, str, int]] = []
    for e in feed.entries[:FETCH_COUNT]:
        link = getattr(e, "link", "") or ""
        title = getattr(e, "title", "Dota 2 News") or "Dota 2 News"
        ts = 0
        if getattr(e, "published_parsed", None):
            ts = int(datetime(*e.published_parsed[:6], tzinfo=timezone.utc).timestamp())
        if link:
            out.append((link, title, link, ts))
    return out


# ================= CORE =================

def format_news(title: str, url: str, ts: int) -> str:
    when = ts_to_str(ts) if ts else ""
    # when может быть пустым — ок
    return f"📰 <b>{title}</b>\n{when}\n{url}".strip()


async def process(context: ContextTypes.DEFAULT_TYPE, items: List[Tuple[str, str, str, int]], keyname: str):
    last = await db_get(keyname)

    if not items:
        return

    newest = items[0][0]

    if last is None:
        # baseline на первый запуск
        await db_set(keyname, newest)
        return

    if newest == last:
        return

    send_list: List[Tuple[str, str, str, int]] = []
    for item in items:
        if item[0] == last:
            break
        send_list.append(item)

    for key, title, url, ts in reversed(send_list):
        msg = format_news(title, url, ts)
        for cid in ALLOWED_CHAT_IDS:
            ok = await send_with_retry(context, cid, msg, html=True)
            if not ok:
                await alert_all(context, f"Не доставилось сообщение (Telegram) для chat_id={cid}\n{url}")

    await db_set(keyname, newest)


async def poll(context: ContextTypes.DEFAULT_TYPE):
    # отметка "успешный заход в poll" (не означает успешность обоих источников)
    await db_set("last_success", str(int(datetime.now(tz=timezone.utc).timestamp())))

    # Steam API (основной)
    try:
        async with httpx.AsyncClient(headers={"User-Agent": "dota2-watcher"}) as client:
            api_items = await fetch_api(client)
        api_norm = []
        for x in api_items:
            gid = x.get("gid")
            title = x.get("title", "Dota 2 News")
            url = x.get("url", "")
            ts = int(x.get("date") or 0)
            if gid and url:
                api_norm.append((gid, title, url, ts))
        await process(context, api_norm, "last_gid")
    except Exception as e:
        await alert_all(context, f"API ошибка: {repr(e)}")

    # RSS (резервный)
    try:
        await process(context, fetch_rss(), "last_rss")
    except Exception as e:
        await alert_all(context, f"RSS ошибка: {repr(e)}")


async def heartbeat(context: ContextTypes.DEFAULT_TYPE):
    last = await db_get("last_success")
    if not last:
        return

    diff = int(datetime.now(tz=timezone.utc).timestamp()) - int(last)
    if diff > HEARTBEAT_MINUTES * 60:
        await alert_all(context, f"Нет успешных проверок уже {diff} сек")


# ================= COMMANDS =================

def is_allowed_chat(chat_id: int) -> bool:
    return chat_id in ALLOWED_CHAT_IDS


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed_chat(update.effective_chat.id):
        return
    await update.message.reply_text("Бот активен")


async def test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed_chat(update.effective_chat.id):
        return
    await update.message.reply_text("TEST OK")


async def force(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed_chat(update.effective_chat.id):
        return
    await poll(context)
    await update.message.reply_text("checked")


async def last(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправить последнюю (самую свежую) новость в текущий чат."""
    chat_id = update.effective_chat.id
    if not is_allowed_chat(chat_id):
        return

    # 1) Пробуем Steam API
    try:
        async with httpx.AsyncClient(headers={"User-Agent": "dota2-watcher"}) as client:
            api_items = await fetch_api(client)
        if api_items:
            x = api_items[0]
            title = x.get("title", "Dota 2 News")
            url = x.get("url", "")
            ts = int(x.get("date") or 0)
            if url:
                await update.message.reply_text(format_news(title, url, ts), parse_mode=ParseMode.HTML)
                return
    except Exception as e:
        # Не алертим всех из-за /last, чтобы не шуметь, просто сообщаем в чат
        await update.message.reply_text(f"Не получилось получить последнюю новость через API: {repr(e)}")

    # 2) Фоллбек RSS
    try:
        rss_items = fetch_rss()
        if rss_items:
            _, title, url, ts = rss_items[0]
            await update.message.reply_text(format_news(title, url, ts), parse_mode=ParseMode.HTML)
            return
        await update.message.reply_text("RSS не вернул новостей.")
    except Exception as e:
        await update.message.reply_text(f"Не получилось получить последнюю новость через RSS: {repr(e)}")


# ================= MAIN =================

async def main():
    await db_init()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("test", test))
    app.add_handler(CommandHandler("force", force))
    app.add_handler(CommandHandler("last", last))

    app.job_queue.run_repeating(poll, interval=POLL_SECONDS, first=3)
    app.job_queue.run_repeating(heartbeat, interval=60, first=60)

    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
