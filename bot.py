"""
Telegram Movie Card Bot — Render-ready (Webhook mode) + MongoDB dedup
======================================================================
Uses webhooks instead of polling so Render's free tier
doesn't spin down due to inactivity. A tiny Flask web
server handles the webhook and also acts as a health-check
endpoint so Render keeps the service alive.

MongoDB is used to track every movie that has been posted.
Before posting, the bot checks by TMDB movie ID — if it's
already in the DB it skips silently, preventing duplicate cards.

Install:
    pip install -r requirements.txt

Environment variables (set in Render dashboard):
    BOT_TOKEN        – from @BotFather
    FILE_CHANNEL_ID  – channel where you upload files  (e.g. -1001234567890)
    POST_CHANNEL_ID  – channel where cards are posted
    TMDB_API_KEY     – from https://www.themoviedb.org/settings/api
    MONGO_URI        – from MongoDB Atlas (free tier)  e.g. mongodb+srv://...
    WEBHOOK_URL      – your Render public URL  (e.g. https://my-bot.onrender.com)
    PORT             – leave blank; Render sets this automatically
"""

import asyncio
import logging
import os
import re
from io import BytesIO
from threading import Thread

import aiohttp
import requests
from flask import Flask, Response, request
from pymongo import MongoClient
from telegram import Bot, Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BOT_TOKEN       = os.environ["BOT_TOKEN"]
FILE_CHANNEL_ID = int(os.environ["FILE_CHANNEL_ID"])
POST_CHANNEL_ID = int(os.environ["POST_CHANNEL_ID"])
TMDB_API_KEY    = os.environ["TMDB_API_KEY"]
MONGO_URI       = os.environ["MONGO_URI"]
WEBHOOK_URL     = os.environ["WEBHOOK_URL"].rstrip("/")   # no trailing slash
PORT            = int(os.environ.get("PORT", 8080))

TMDB_BASE     = "https://api.themoviedb.org/3"
TMDB_IMG_BASE = "https://image.tmdb.org/t/p/w780"

# ── MongoDB setup ─────────────────────────────────────────────────────────────
_mongo_client = MongoClient(MONGO_URI)
_db           = _mongo_client["moviebot"]
posted_col    = _db["posted_movies"]   # collection that tracks posted TMDB IDs
# Create a unique index on tmdb_id so duplicate inserts are rejected cleanly
posted_col.create_index("tmdb_id", unique=True)
log.info("MongoDB connected ✅")

# ── Flask app (health-check + webhook receiver) ───────────────────────────────
flask_app = Flask(__name__)

# Will be set after the Application is built
tg_app: Application = None   # type: ignore


@flask_app.get("/")
def health():
    return Response("✅ Bot is running", status=200)


@flask_app.post("/webhook")
def webhook():
    json_data = request.get_json(force=True)
    update = Update.de_json(json_data, tg_app.bot)

    asyncio.run(tg_app.process_update(update))  # ✅ FIX

    return Response("ok", status=200)


# ── TMDB helpers ──────────────────────────────────────────────────────────────

def extract_title_year(text: str) -> tuple[str, str | None]:
    """
    Extract movie title and year from a filename or caption.
      'Tekken.2010.1080p.BluRay.mkv'   →  ('Tekken', '2010')
      'The Deer Hunter 1978 720p brrip' →  ('The Deer Hunter', '1978')
    """
    text = re.sub(r'\.\w{2,4}$', '', text)          # strip extension
    match = re.search(r'\b(19|20)\d{2}\b', text)
    year  = match.group() if match else None
    title = text[:match.start()].strip() if year else text
    title = re.sub(r'[._\-]+', ' ', title).strip()
    title = re.split(
        r'\b(1080p|720p|480p|BluRay|BRRip|WEB|HDTV|x264|x265|AAC|DTS)\b',
        title, flags=re.IGNORECASE
    )[0].strip()
    return title, year


def detect_quality(text: str) -> str:
    quality = "N/A"
    for q in ("2160p", "1080p", "720p", "480p"):
        if q.lower() in text.lower():
            quality = q
            break
    for enc in ("bluray", "brrip", "web-dl", "webrip", "hdrip"):
        if enc.lower() in text.lower():
            quality += f", {enc}"
            break
    return quality


async def tmdb_search(session: aiohttp.ClientSession, title: str, year: str | None) -> dict | None:
    params = {"api_key": TMDB_API_KEY, "query": title, "language": "en-US"}
    if year:
        params["year"] = year
    async with session.get(f"{TMDB_BASE}/search/movie", params=params) as r:
        data = await r.json()
    results = data.get("results", [])
    return results[0] if results else None


async def tmdb_details(session: aiohttp.ClientSession, movie_id: int) -> dict:
    params = {"api_key": TMDB_API_KEY, "language": "en-US"}
    async with session.get(f"{TMDB_BASE}/movie/{movie_id}", params=params) as r:
        return await r.json()


async def fetch_poster(session: aiohttp.ClientSession, poster_path: str) -> bytes | None:
    if not poster_path:
        return None
    async with session.get(f"{TMDB_IMG_BASE}{poster_path}") as r:
        return await r.read() if r.status == 200 else None


def build_caption(details: dict, quality: str, post_channel_username: str) -> str:
    title   = details.get("title", "Unknown")
    year    = (details.get("release_date") or "")[:4]
    genres  = ", ".join(g["name"] for g in details.get("genres", []))
    rating  = round(details.get("vote_average", 0), 1)
    lang    = (details.get("original_language") or "en").upper()

    return (
        f"📥 <b>New #MOVIE Added</b>\n\n"
        f"✨ <b>TITLE</b>   : {title} {year}\n\n"
        f"🎭 <b>GENRES</b>  : {genres}\n"
        f"📺 <b>OTT</b>     : N/A\n"
        f"🎞 <b>QUALITY</b> : {quality}\n"
        f"🎧 <b>AUDIO</b>   : {lang}\n"
        f"🔥 <b>RATING</b>  : {rating}\n\n"
        f"🔍 <b>Search</b> → @{post_channel_username}"
    )


# ── Telegram update handler ───────────────────────────────────────────────────

async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post
    if not msg or msg.chat_id != FILE_CHANNEL_ID:
        return

    # Grab raw text (filename or caption)
    raw = ""
    if msg.document:
        raw = msg.document.file_name or msg.caption or ""
    elif msg.video:
        raw = msg.video.file_name or msg.caption or ""
    elif msg.caption:
        raw = msg.caption
    elif msg.text:
        raw = msg.text

    if not raw:
        return

    quality = detect_quality(raw)
    title, year = extract_title_year(raw)
    log.info(f"Parsed  title={title!r}  year={year!r}  quality={quality!r}")

    if not title:
        return

    bot: Bot = context.bot

    # Fetch the post-channel username for the Search link
    try:
        post_chat = await bot.get_chat(POST_CHANNEL_ID)
        post_username = post_chat.username or str(POST_CHANNEL_ID)
    except Exception:
        post_username = str(POST_CHANNEL_ID)

    async with aiohttp.ClientSession() as session:
        result = await tmdb_search(session, title, year)

        if not result:
            log.warning(f"TMDB returned no results for '{title}'")
            await bot.send_message(
                chat_id=POST_CHANNEL_ID,
                text=f"📥 New #MOVIE Added\n\n✨ TITLE : {title} {year or ''}\n🔥 RATING : N/A",
            )
            return

        tmdb_id = result["id"]

        # ── Duplicate check ───────────────────────────────────────────────────
        if posted_col.find_one({"tmdb_id": tmdb_id}):
            log.info(f"⏭  Skipping '{title}' (tmdb_id={tmdb_id}) — already posted")
            return

        details      = await tmdb_details(session, tmdb_id)
        poster_bytes = await fetch_poster(session, result.get("poster_path"))

    caption = build_caption(details, quality, post_username)

    if poster_bytes:
        await bot.send_photo(
            chat_id=POST_CHANNEL_ID,
            photo=BytesIO(poster_bytes),
            caption=caption,
            parse_mode="HTML",
        )
    else:
        await bot.send_message(
            chat_id=POST_CHANNEL_ID,
            text=caption,
            parse_mode="HTML",
        )

    # ── Save to MongoDB so we never post this movie again ─────────────────────
    posted_col.insert_one({
        "tmdb_id":    tmdb_id,
        "title":      details.get("title"),
        "year":       (details.get("release_date") or "")[:4],
        "quality":    quality,
        "posted_at":  __import__("datetime").datetime.utcnow(),
    })

    log.info(f"✅ Posted card for '{title}' ({year}) to {POST_CHANNEL_ID}")


# ── Bot setup + webhook registration ─────────────────────────────────────────

async def setup_webhook(app: Application):
    url = f"{WEBHOOK_URL}/webhook"
    await app.bot.set_webhook(url=url)
    log.info(f"Webhook set → {url}")


def run_bot_thread(app: Application):
    """Run the telegram Application in its own thread with its own event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def _run():
        await app.initialize()
        await setup_webhook(app)
        await app.start()
        log.info("Bot application started (webhook mode)")
        # Keep alive — Flask thread handles the actual webhook POSTs
        while True:
            await asyncio.sleep(3600)

    loop.run_until_complete(_run())


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    global tg_app

    tg_app = (
        Application.builder()
        .token(BOT_TOKEN)
        .updater(None)          # disable polling; we use webhooks
        .build()
    )

    tg_app.add_handler(
        MessageHandler(
            filters.ChatType.CHANNEL & (
                filters.Document.ALL | filters.VIDEO | filters.PHOTO | filters.TEXT
            ),
            handle_file,
        )
    )

    # Run the bot in a background thread
    bot_thread = Thread(target=run_bot_thread, args=(tg_app,), daemon=True)
    bot_thread.start()

    # Flask handles HTTP — Render needs a web process
    log.info(f"Flask listening on 0.0.0.0:{PORT}")
    


if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
    
# Auto-start for Gunicorn
else:
    main()

