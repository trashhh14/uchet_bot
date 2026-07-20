"""Count approved Telegram videos by category in monthly Google Sheets tabs."""

import asyncio
import json
import logging
import os
import re
import sqlite3
from contextlib import closing
from datetime import datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    MessageReactionHandler,
    filters,
)

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

BOT_TOKEN = os.environ["BOT_TOKEN"]
APPROVER_USER_ID = int(os.environ["APPROVER_USER_ID"])
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SHEET_PREFIX = os.getenv("SHEET_PREFIX", "")
TIMEZONE = ZoneInfo(os.getenv("TIMEZONE", "Europe/Moscow"))
USERNAME_COLUMN = os.getenv("USERNAME_COLUMN", "A")
UZ_COLUMN = os.getenv("UZ_COLUMN", "B")
RP_COLUMN = os.getenv("RP_COLUMN", "C")
TOTAL_COLUMN = os.getenv("TOTAL_COLUMN", "D")
# Set after running /topicid in the required Telegram forum topic.
TARGET_THREAD_ID = int(os.getenv("TARGET_THREAD_ID", "0")) or None
TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", "0")) or None
GOOGLE_SERVICE_ACCOUNT_FILE = BASE_DIR / os.getenv(
    "GOOGLE_SERVICE_ACCOUNT_FILE", "google-service-account.json"
)
# On Railway set DATA_DIR=/data and attach a volume at /data.
DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE_DIR)))
DB_PATH = DATA_DIR / "counter.sqlite3"
GOOGLE_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
sheet_lock = asyncio.Lock()
# Telegram sends every video of an album as a separate message. The album's
# caption normally exists only on its first item, so we collect it briefly.
album_buffers: dict[tuple[int, str], list[tuple[int, str, list[str], str]]] = {}
album_tasks: dict[tuple[int, str], asyncio.Task] = {}
target_thread_id = TARGET_THREAD_ID
target_chat_id = TARGET_CHAT_ID

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


def normalized_username(value: str | None) -> str:
    return (value or "").strip().lstrip("@").casefold()


def quote_sheet_name(name: str) -> str:
    return "'" + name.replace("'", "''") + "'"


def monthly_sheet_name(month: str) -> str:
    return f"{SHEET_PREFIX}{month}"  # e.g. 2026-07 or РСЋР»СЊ 2026


def month_from_timestamp(timestamp: datetime) -> str:
    return timestamp.astimezone(TIMEZONE).strftime("%Y-%m")


def init_db() -> None:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS videos (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                sender_username TEXT NOT NULL,
                category TEXT NOT NULL CHECK(category IN ('UZ', 'RP')),
                month TEXT NOT NULL,
                album_id TEXT,
                PRIMARY KEY (chat_id, message_id)
            )
            """
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(videos)")}
        if "album_id" not in columns:
            conn.execute("ALTER TABLE videos ADD COLUMN album_id TEXT")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS counted_videos (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                PRIMARY KEY (chat_id, message_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sent_reports (
                month TEXT PRIMARY KEY
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.commit()


def load_topic_scope() -> None:
    """Load a topic selected with /settopic, if one was selected."""
    global target_chat_id, target_thread_id
    with closing(sqlite3.connect(DB_PATH)) as conn:
        rows = dict(conn.execute("SELECT key, value FROM settings WHERE key IN ('target_chat_id', 'target_thread_id')"))
    if "target_chat_id" in rows:
        target_chat_id = int(rows["target_chat_id"])
    if "target_thread_id" in rows:
        target_thread_id = int(rows["target_thread_id"])


def save_topic_scope(chat_id: int, thread_id: int) -> None:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.executemany(
            "INSERT OR REPLACE INTO settings(key, value) VALUES (?, ?)",
            [("target_chat_id", str(chat_id)), ("target_thread_id", str(thread_id))],
        )
        conn.commit()


def is_video_message(message) -> bool:
    return bool(
        message.video
        or (message.document and (message.document.mime_type or "").startswith("video/"))
    )


def categories_from_caption(caption: str | None) -> list[str]:
    """Return every explicit РЈР—/Р Рџ marker in the order it appears."""
    text = (caption or "").casefold()
    return ["UZ" if match.group() == "СѓР·" else "RP" for match in re.finditer(
        r"(?<![\wР°-СЏС‘])(СѓР·|СЂРї)(?![\wР°-СЏС‘])", text
    )]


def category_from_caption(caption: str | None) -> str | None:
    """Return one category when a caption describes one kind of video."""
    categories = categories_from_caption(caption)
    if len(set(categories)) != 1:
        return None
    return categories[0] if categories else None


def save_video(
    chat_id: int,
    message_id: int,
    username: str,
    category: str,
    month: str,
    album_id: str | None = None,
) -> None:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO videos(chat_id, message_id, sender_username, category, month, album_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (chat_id, message_id, username, category, month, album_id),
        )
        conn.commit()


def get_video(chat_id: int, message_id: int) -> tuple[str, str, str, str | None] | None:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        return conn.execute(
            """
            SELECT sender_username, category, month, album_id FROM videos
            WHERE chat_id = ? AND message_id = ?
            """,
            (chat_id, message_id),
        ).fetchone()


def get_album_videos(chat_id: int, album_id: str) -> list[tuple[int, str, str, str]]:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        return conn.execute(
            """
            SELECT message_id, sender_username, category, month FROM videos
            WHERE chat_id = ? AND album_id = ? ORDER BY message_id
            """,
            (chat_id, album_id),
        ).fetchall()


def was_counted(chat_id: int, message_id: int) -> bool:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        return conn.execute(
            "SELECT 1 FROM counted_videos WHERE chat_id = ? AND message_id = ?",
            (chat_id, message_id),
        ).fetchone() is not None


def mark_counted(chat_id: int, message_id: int, counted: bool) -> None:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        if counted:
            conn.execute(
                "INSERT OR IGNORE INTO counted_videos(chat_id, message_id) VALUES (?, ?)",
                (chat_id, message_id),
            )
        else:
            conn.execute(
                "DELETE FROM counted_videos WHERE chat_id = ? AND message_id = ?",
                (chat_id, message_id),
            )
        conn.commit()


def sheets_service():
    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if service_account_json:
        credentials = Credentials.from_service_account_info(
            json.loads(service_account_json), scopes=GOOGLE_SCOPES
        )
    else:
        credentials = Credentials.from_service_account_file(
            GOOGLE_SERVICE_ACCOUNT_FILE, scopes=GOOGLE_SCOPES
        )
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def get_month_rows(month: str) -> list[list[str]]:
    tab = quote_sheet_name(monthly_sheet_name(month))
    response = sheets_service().spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{tab}!{USERNAME_COLUMN}:{TOTAL_COLUMN}",
    ).execute()
    return response.get("values", [])


def update_creator_count(username: str, category: str, month: str, delta: int) -> tuple[int, int, int]:
    """Update UZ/RP/total in an existing monthly tab and return the new counts."""
    tab = quote_sheet_name(monthly_sheet_name(month))
    rows = get_month_rows(month)
    target = normalized_username(username)
    for row_number, row in enumerate(rows, start=1):
        if not row or normalized_username(row[0]) != target:
            continue
        try:
            uz = int(row[1]) if len(row) > 1 and row[1] else 0
            rp = int(row[2]) if len(row) > 2 and row[2] else 0
        except ValueError as exc:
            raise ValueError(f"Р’ СЃС‚СЂРѕРєРµ {row_number} РЈР— РёР»Рё Р Рџ РЅРµ СЏРІР»СЏРµС‚СЃСЏ С‡РёСЃР»РѕРј") from exc
        if category == "UZ":
            uz += delta
        else:
            rp += delta
        if uz < 0 or rp < 0:
            raise ValueError(f"РќРµР»СЊР·СЏ СѓРјРµРЅСЊС€РёС‚СЊ СЃС‡С‘С‚С‡РёРє @${username} РЅРёР¶Рµ РЅСѓР»СЏ")
        total = uz + rp
        sheets_service().spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{tab}!{UZ_COLUMN}{row_number}:{TOTAL_COLUMN}{row_number}",
            valueInputOption="USER_ENTERED",
            body={"values": [[uz, rp, total]]},
        ).execute()
        return uz, rp, total
    raise LookupError(
        f"РљСЂРµР°С‚РѕСЂ @{username} РЅРµ РЅР°Р№РґРµРЅ РІ СЃС‚РѕР»Р±С†Рµ {USERNAME_COLUMN} РІРєР»Р°РґРєРё {monthly_sheet_name(month)}"
    )


def remember_single_video(message) -> None:
    """Store one video when its category is known."""
    username = normalized_username(message.from_user.username)
    category = category_from_caption(message.caption)
    if not username:
        logger.info("Р’РёРґРµРѕ %s РїСЂРѕРїСѓС‰РµРЅРѕ: РЅРµС‚ @username", message.message_id)
    elif not category:
        logger.info("Р’РёРґРµРѕ @%s РїСЂРѕРїСѓС‰РµРЅРѕ: РІ РїРѕРґРїРёСЃРё РЅРµС‚ СЂРѕРІРЅРѕ РѕРґРЅРѕРіРѕ РЈР— РёР»Рё Р Рџ", username)
    else:
        month = month_from_timestamp(message.date)
        save_video(message.chat_id, message.message_id, username, category, month)
        logger.info("Р—Р°РїРѕРјРЅРёР» %s-РІРёРґРµРѕ РѕС‚ @%s РґР»СЏ %s", category, username, month)


async def finish_album(key: tuple[int, str]) -> None:
    """Apply the one album caption to every video in that album."""
    await asyncio.sleep(2)
    items = album_buffers.pop(key, [])
    album_tasks.pop(key, None)
    items.sort(key=lambda item: item[0])
    caption_categories = [category for _, _, categories, _ in items for category in categories]
    if len(caption_categories) == 1:
        categories_to_apply = caption_categories * len(items)
    elif len(caption_categories) == len(items):
        # E.g. "4 СЂРѕР»РёРє (Р Рџ) ... 5 СЂРѕР»РёРє (РЈР—) ..." for a two-video album.
        categories_to_apply = caption_categories
    else:
        logger.info("РђР»СЊР±РѕРј %s РїСЂРѕРїСѓС‰РµРЅ: РЈР—/Р Рџ РЅРµ СЃРѕРІРїР°РґР°СЋС‚ СЃ С‡РёСЃР»РѕРј РІРёРґРµРѕ", key[1])
        return
    for (message_id, username, _, month), category in zip(items, categories_to_apply):
        if username:
            save_video(key[0], message_id, username, category, month, key[1])
    logger.info("Р—Р°РїРѕРјРЅРёР» Р°Р»СЊР±РѕРј РёР· %s РІРёРґРµРѕ: %s", len(items), ", ".join(categories_to_apply))


async def remember_video(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message or not is_video_message(message) or not message.from_user:
        return
    if target_chat_id and message.chat_id != target_chat_id:
        return
    if target_thread_id and message.message_thread_id != target_thread_id:
        return
    if not message.media_group_id:
        remember_single_video(message)
        return

    key = (message.chat_id, message.media_group_id)
    item = (
        message.message_id,
        normalized_username(message.from_user.username),
        categories_from_caption(message.caption),
        month_from_timestamp(message.date),
    )
    album_buffers.setdefault(key, []).append(item)
    previous_task = album_tasks.get(key)
    if previous_task:
        previous_task.cancel()
    album_tasks[key] = asyncio.create_task(finish_album(key))


def contains_thumb_up(reactions) -> bool:
    return any(getattr(reaction, "emoji", None) == "рџ‘Ќ" for reaction in reactions)


async def process_reaction(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reaction = update.message_reaction
    if not reaction or not reaction.user or reaction.user.id != APPROVER_USER_ID:
        return
    old_has_thumb = contains_thumb_up(reaction.old_reaction)
    new_has_thumb = contains_thumb_up(reaction.new_reaction)
    if old_has_thumb == new_has_thumb:
        return

    chat_id, message_id = reaction.chat.id, reaction.message_id
    video = get_video(chat_id, message_id)
    if not video:
        return
    username, category, month, album_id = video
    approving = new_has_thumb
    videos_to_process = (
        get_album_videos(chat_id, album_id)
        if album_id
        else [(message_id, username, category, month)]
    )
    try:
        async with sheet_lock:
            for target_id, target_username, target_category, target_month in videos_to_process:
                if approving == was_counted(chat_id, target_id):
                    continue
                counts = await asyncio.to_thread(
                    update_creator_count,
                    target_username,
                    target_category,
                    target_month,
                    1 if approving else -1,
                )
                mark_counted(chat_id, target_id, approving)
                logger.info(
                    "@%s %s: РЈР—=%s, Р Рџ=%s, РІСЃРµРіРѕ=%s",
                    target_username,
                    target_category,
                    *counts,
                )
    except Exception:
        logger.exception("РќРµ СѓРґР°Р»РѕСЃСЊ РѕР±РЅРѕРІРёС‚СЊ СЃС‡С‘С‚С‡РёРє РґР»СЏ @%s", username)


async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user and update.effective_message:
        await update.effective_message.reply_text(
            f"РўРІРѕР№ Telegram ID: {update.effective_user.id}"
        )


async def topicid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the forum-topic ID so it can be placed in .env."""
    if not update.effective_user or update.effective_user.id != APPROVER_USER_ID:
        return
    message = update.effective_message
    if message:
        if message.message_thread_id:
            await message.reply_text(
                f"ID СЌС‚РѕРіРѕ С‚РѕРїРёРєР°: {message.message_thread_id}\n"
                "Р§С‚РѕР±С‹ РІРєР»СЋС‡РёС‚СЊ СѓС‡С‘С‚ С‚РѕР»СЊРєРѕ Р·РґРµСЃСЊ, РЅР°РїРёС€Рё /settopic."
            )
        else:
            await message.reply_text("Р­С‚Р° РєРѕРјР°РЅРґР° РѕС‚РїСЂР°РІР»РµРЅР° РЅРµ РІРЅСѓС‚СЂРё С‚РѕРїРёРєР° С„РѕСЂСѓРјР°.")


async def settopic(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Restrict counting to the current forum topic, persistently."""
    global target_chat_id, target_thread_id
    if not update.effective_user or update.effective_user.id != APPROVER_USER_ID:
        return
    message = update.effective_message
    if not message or not message.message_thread_id:
        if message:
            await message.reply_text("РћС‚РєСЂРѕР№ РЅСѓР¶РЅС‹Р№ С‚РѕРїРёРє Рё РѕС‚РїСЂР°РІСЊ /settopic РїСЂСЏРјРѕ РІ РЅС‘Рј.")
        return
    target_chat_id, target_thread_id = message.chat_id, message.message_thread_id
    save_topic_scope(target_chat_id, target_thread_id)
    await message.reply_text("Р“РѕС‚РѕРІРѕ. РўРµРїРµСЂСЊ СЃС‡РёС‚Р°СЋ СЂРѕР»РёРєРё С‚РѕР»СЊРєРѕ РІ СЌС‚РѕРј С‚РѕРїРёРєРµ.")


async def send_monthly_report(month: str, context: ContextTypes.DEFAULT_TYPE) -> None:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        if conn.execute("SELECT 1 FROM sent_reports WHERE month = ?", (month,)).fetchone():
            return
    try:
        rows = await asyncio.to_thread(get_month_rows, month)
        body = []
        for row in rows:
            if not row or not normalized_username(row[0]):
                continue
            try:
                uz = int(row[1]) if len(row) > 1 and row[1] else 0
                rp = int(row[2]) if len(row) > 2 and row[2] else 0
            except ValueError:
                continue  # Allows a header row.
            body.append(f"@{normalized_username(row[0])}: РЈР— вЂ” {uz}, Р Рџ вЂ” {rp}, РІСЃРµРіРѕ вЂ” {uz + rp}")
        text = f"РћС‚С‡С‘С‚ Р·Р° {month}:\n" + ("\n".join(body) if body else "РќРµС‚ РґР°РЅРЅС‹С….")
        text += f"\n\nРўР°Р±Р»РёС†Р°: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
        await context.bot.send_message(chat_id=APPROVER_USER_ID, text=text)
        with closing(sqlite3.connect(DB_PATH)) as conn:
            conn.execute("INSERT INTO sent_reports(month) VALUES (?)", (month,))
            conn.commit()
    except Exception:
        logger.exception("РќРµ СѓРґР°Р»РѕСЃСЊ РѕС‚РїСЂР°РІРёС‚СЊ РѕС‚С‡С‘С‚ Р·Р° %s", month)


async def scheduled_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    now = datetime.now(TIMEZONE)
    if now.day != 1:
        return
    previous_month = (now.replace(day=1) - __import__("datetime").timedelta(days=1)).strftime("%Y-%m")
    await send_monthly_report(previous_month, context)


async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or update.effective_user.id != APPROVER_USER_ID:
        return
    month = context.args[0] if context.args else datetime.now(TIMEZONE).strftime("%Y-%m")
    await send_monthly_report(month, context)


def main() -> None:
    if not os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") and not GOOGLE_SERVICE_ACCOUNT_FILE.exists():
        raise FileNotFoundError(f"РќРµ РЅР°Р№РґРµРЅ Google key: {GOOGLE_SERVICE_ACCOUNT_FILE}")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    init_db()
    load_topic_scope()
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("topicid", topicid))
    app.add_handler(CommandHandler("settopic", settopic))
    app.add_handler(CommandHandler("report", report_command))
    app.add_handler(MessageHandler(filters.VIDEO | filters.Document.VIDEO, remember_video))
    app.add_handler(MessageReactionHandler(process_reaction))
    app.job_queue.run_daily(scheduled_report, time=time(hour=9, tzinfo=TIMEZONE))
    app.run_polling(allowed_updates=["message", "message_reaction"])


if __name__ == "__main__":
    main()
