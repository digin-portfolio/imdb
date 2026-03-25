import asyncio
import logging
import os
import re
from io import BytesIO

import aiohttp
from flask import Flask, request, Response
from pymongo import MongoClient, errors
from telegram import Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters

# ── Logging ─────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ── ENV ─────────────────────────────────────────────
BOT_TOKEN       = os.environ["BOT_TOKEN"]
FILE_CHANNEL_ID = int(os.environ["FILE_CHANNEL_ID"])
POST_CHANNEL_ID = int(os.environ["POST_CHANNEL_ID"])
TMDB_API_KEY    = os.environ["TMDB_API_KEY"]
MONGO_URI       = os.environ["MONGO_URI"]
WEBHOOK_URL     = os.environ["WEBHOOK_URL"].rstrip("/")

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMG  = "https://image.tmdb.org/t/p/w1280"

# ── MongoDB ─────────────────────────────────────────
mongo = MongoClient(MONGO_URI)
db = mongo["moviebot"]
posted_col = db["posted_movies"]
posted_col.create_index("tmdb_id", unique=True)

log.info("MongoDB connected ✅")

# ── Flask ───────────────────────────────────────────
app = Flask(__name__)

# ── Telegram App ─────────────────────────────────────
tg_app = Application.builder().token(BOT_TOKEN).build()

# ── Helpers ─────────────────────────────────────────

def extract_title_year(text: str):
    text = re.sub(r'\.\w{2,4}$', '', text)
    match = re.search(r'\b(19|20)\d{2}\b', text)
    year = match.group() if match else None
    title = text[:match.start()].strip() if year else text
    title = re.sub(r'[._\-]+', ' ', title).strip()
    title = re.split(
        r'\b(1080p|720p|480p|BluRay|BRRip|WEB|HDTV|x264|x265|AAC|DTS)\b',
        title, flags=re.IGNORECASE
    )[0].strip()
    return title, year

def detect_quality(text: str):
    text = text.lower()
    for q in ("2160p", "1080p", "720p", "480p"):
        if q in text:
            return q
    return "N/A"

async def tmdb_search(session, title, year):
    params = {"api_key": TMDB_API_KEY, "query": title}
    if year:
        params["year"] = year
    async with session.get(f"{TMDB_BASE}/search/movie", params=params) as r:
        data = await r.json()
    return data.get("results", [None])[0]

async def tmdb_details(session, movie_id):
    params = {"api_key": TMDB_API_KEY}
    async with session.get(f"{TMDB_BASE}/movie/{movie_id}", params=params) as r:
        return await r.json()

async def fetch_image(session, poster, backdrop):
    path = backdrop or poster  # prefer 16:9
    if not path:
        return None
    async with session.get(f"{TMDB_IMG}{path}") as r:
        return await r.read()

def build_caption(details, quality):
    title = details.get("title")
    year = (details.get("release_date") or "")[:4]
    genres = ", ".join(g["name"] for g in details.get("genres", []))
    rating = round(details.get("vote_average", 0), 1)
    lang = details.get("original_language", "").upper()

    return f"""
📥 <b>New #MOVIE Added</b>

<blockquote>
✨ <b>TITLE :</b> {title} {year}
🎭 <b>GENRES :</b> {genres}
📺 <b>QUALITY :</b> {quality}
🌐 <b>LANG :</b> {lang}
🔥 <b>RATING :</b> {rating}
</blockquote>
"""

# ── Handler ─────────────────────────────────────────

async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post
    if not msg or msg.chat_id != FILE_CHANNEL_ID:
        return

    raw = msg.document.file_name if msg.document else (msg.caption or "")
    if not raw:
        return

    quality = detect_quality(raw)
    title, year = extract_title_year(raw)

    async with aiohttp.ClientSession() as session:
        result = await tmdb_search(session, title, year)
        if not result:
            return

        tmdb_id = result["id"]

        # 🔴 FIX: prevent duplicate (race-safe)
        try:
            posted_col.insert_one({"tmdb_id": tmdb_id})
        except errors.DuplicateKeyError:
            log.info(f"⏭ Duplicate skipped ({title})")
            return

        details = await tmdb_details(session, tmdb_id)
        image = await fetch_image(
            session,
            result.get("poster_path"),
            result.get("backdrop_path")
        )

    caption = build_caption(details, quality)

    if image:
        await context.bot.send_photo(
            chat_id=POST_CHANNEL_ID,
            photo=BytesIO(image),
            caption=caption,
            parse_mode="HTML"
        )
    else:
        await context.bot.send_message(
            chat_id=POST_CHANNEL_ID,
            text=caption,
            parse_mode="HTML"
        )

    log.info(f"Posted {title}")

# ── Register Handler ────────────────────────────────
tg_app.add_handler(
    MessageHandler(
        filters.ChatType.CHANNEL &
        (filters.Document.ALL | filters.VIDEO | filters.TEXT),
        handle_file
    )
)

# ── Flask routes ────────────────────────────────────

@app.get("/")
def home():
    return "Bot running ✅"

@app.post("/webhook")
def webhook():
    update = Update.de_json(request.get_json(force=True), tg_app.bot)
    asyncio.run(tg_app.process_update(update))
    return Response("ok")

# ── Startup ─────────────────────────────────────────

async def start():
    await tg_app.initialize()
    await tg_app.start()
    await tg_app.bot.set_webhook(f"{WEBHOOK_URL}/webhook")
    log.info("Webhook set ✅")

asyncio.run(start())
