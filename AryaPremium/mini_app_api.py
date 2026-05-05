"""
Arya Premium Mini App API
Run from AryaPremium/ directory:
    BOT_USERNAME=UseAryaBot python3 mini_app_api.py
"""
import os
import uuid
import random
import string
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, Response

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

from config import Config
from database import db as arya_db

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

BOT_USERNAME = os.environ.get("BOT_USERNAME", "UseAryaBot")

# ── Lifespan ─────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await arya_db.connect()
    logger.info("✅ MongoDB connected")
    yield
    if arya_db.client:
        arya_db.client.close()

app = FastAPI(title="Arya Premium Mini App API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
)


# ── Image helper: is this a real HTTP URL? ────────────────────────
def _is_url(v: str | None) -> bool:
    return bool(v and (v.startswith("http://") or v.startswith("https://")))


# ── Story formatter ───────────────────────────────────────────────
def _format(s: dict) -> dict | None:
    story_id = str(s["_id"]) if s.get("_id") else None
    if not story_id:
        return None

    title = (
        s.get("story_name_en") or s.get("story_name_hi")
        or s.get("story_name") or s.get("name") or s.get("title")
    )
    if title:
        title = str(title).strip()
    if not title:
        return None

    description = (s.get("description") or s.get("description_hi") or "").strip()

    # Poster: use HTTP URL if available, else proxy via /api/image/{id}
    poster_url = s.get("poster_url") or s.get("cover") or s.get("image_url")
    if _is_url(poster_url):
        poster = poster_url
    elif s.get("image"):
        # Telegram file_id → serve via API image proxy
        poster = f"/api/image/{story_id}"
    else:
        poster = None  # frontend will use genre gradient

    return {
        "id":            story_id,
        "title":         title,
        "description":   description,
        "poster":        poster,
        "banner":        poster,
        "cover":         poster,
        "price":         float(s.get("price") or 0),
        "language":      s.get("language") or "Hindi",
        "platform":      s.get("platform") or "Pocket FM",
        "genre":         s.get("genre") or "Drama",
        "status":        "available",
        "episodes":      s.get("episodes") or s.get("ep_count") or s.get("total_eps") or "?",
        "totalEpisodes": s.get("episodes") or s.get("total_eps") or s.get("ep_count") or "?",
        "size":          s.get("total_size") or s.get("size") or None,
        "isCompleted":   bool(
            s.get("is_completed") or s.get("completed")
            or str(s.get("status", "")).lower() == "completed"
        ),
    }


# ── GET /api/health ───────────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {"status": "ok", "bot": BOT_USERNAME}


# ── GET /api/stories ──────────────────────────────────────────────
@app.get("/api/stories")
async def get_stories():
    try:
        stories = await arya_db.get_all_stories()
        result = [r for r in (_format(s) for s in stories) if r]
        logger.info(f"Returning {len(result)} stories")
        return {"success": True, "data": result}
    except Exception as e:
        logger.error(f"/api/stories error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ── GET /api/image/{story_id} — Telegram image proxy ─────────────
@app.get("/api/image/{story_id}")
async def get_story_image(story_id: str):
    """Proxy Telegram file_id images so browser can display them."""
    import httpx
    from bson.objectid import ObjectId
    try:
        story = await arya_db.db.premium_stories.find_one({"_id": ObjectId(story_id)})
    except Exception:
        raise HTTPException(404, "Invalid story id")

    if not story:
        raise HTTPException(404, "Story not found")

    file_id = story.get("image")
    if not file_id or _is_url(file_id):
        raise HTTPException(404, "No Telegram image for this story")

    # Get bot token from the story's assigned store bot, fallback to mgmt token
    bot_token = None
    bot_id = story.get("bot_id")
    if bot_id:
        bot_doc = await arya_db.db.premium_bots.find_one({"id": bot_id})
        if bot_doc:
            bot_token = bot_doc.get("token")
    if not bot_token:
        bot_token = os.environ.get("MGMT_BOT_TOKEN") or Config.MGMT_BOT_TOKEN

    if not bot_token:
        raise HTTPException(500, "No bot token available for image proxy")

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            # Step 1: get file path from Telegram
            r = await client.get(
                f"https://api.telegram.org/bot{bot_token}/getFile",
                params={"file_id": file_id}
            )
            data = r.json()
            if not data.get("ok"):
                raise HTTPException(404, "Telegram getFile failed")
            file_path = data["result"]["file_path"]

            # Step 2: download the file
            img_r = await client.get(
                f"https://api.telegram.org/file/bot{bot_token}/{file_path}"
            )
            content_type = img_r.headers.get("content-type", "image/jpeg")
            return Response(content=img_r.content, media_type=content_type)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Image proxy error for {story_id}: {e}")
        raise HTTPException(500, "Image fetch failed")


# ── POST /api/checkout ────────────────────────────────────────────
@app.post("/api/checkout")
async def checkout(payload: dict):
    """Create order and return deep-link to bot for payment."""
    telegram_id = payload.get("telegram_id")
    story_ids = payload.get("story_ids", [])
    username = payload.get("username", "")

    if not story_ids:
        raise HTTPException(status_code=400, detail="Cart is empty")

    # Allow null telegram_id (testing outside Telegram) — use 0 as placeholder
    if not telegram_id:
        telegram_id = 0

    from bson.objectid import ObjectId
    valid_stories = []
    for sid in story_ids:
        try:
            doc = await arya_db.db.premium_stories.find_one({"_id": ObjectId(str(sid))})
            if doc:
                valid_stories.append(doc)
        except Exception:
            pass

    if not valid_stories:
        raise HTTPException(status_code=400, detail="No valid stories found")

    total_price = sum(float(s.get("price") or 0) for s in valid_stories)

    # Order ID matching Arya Premium bot format: OD-{user_id}-{RANDOM6}
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
    order_id = f"OD-{telegram_id}-{suffix}"

    await arya_db.db.orders.insert_one({
        "order_id":     order_id,
        "user_id":      telegram_id,
        "username":     username,
        "story_ids":    story_ids,
        "story_names":  [s.get("story_name_en", "") for s in valid_stories],
        "total_amount": total_price,
        "status":       "pending",
        "source":       "mini_app",
        "created_at":   datetime.now(timezone.utc),
    })
    logger.info(f"Order {order_id} created for user {telegram_id}, ₹{total_price}")

    bot_un = os.environ.get("BOT_USERNAME", "UseAryaBot")
    return {
        "success":      True,
        "checkout_url": f"https://t.me/{bot_un}?start=order_{order_id}",
        "order_id":     order_id,
        "total":        total_price,
    }


# ── POST /api/razorpay/order ──────────────────────────────────────
@app.post("/api/razorpay/order")
async def create_razorpay_order(payload: dict):
    """Create a Razorpay order for in-app payment."""
    story_ids = payload.get("story_ids", [])
    telegram_id = payload.get("telegram_id") or 0

    if not story_ids:
        raise HTTPException(400, "Cart is empty")

    from bson.objectid import ObjectId
    valid_stories = []
    for sid in story_ids:
        try:
            doc = await arya_db.db.premium_stories.find_one({"_id": ObjectId(str(sid))})
            if doc:
                valid_stories.append(doc)
        except Exception:
            pass

    if not valid_stories:
        raise HTTPException(400, "No valid stories")

    total_paise = int(sum(float(s.get("price") or 0) for s in valid_stories) * 100)

    rzp_key = Config.RAZORPAY_KEY
    rzp_secret = Config.RAZORPAY_SECRET
    if not rzp_key or not rzp_secret:
        raise HTTPException(500, "Razorpay not configured")

    import httpx, base64
    auth = base64.b64encode(f"{rzp_key}:{rzp_secret}".encode()).decode()

    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
    receipt = f"OD-{telegram_id}-{suffix}"

    async with httpx.AsyncClient() as client:
        r = await client.post(
            "https://api.razorpay.com/v1/orders",
            json={"amount": total_paise, "currency": "INR", "receipt": receipt},
            headers={"Authorization": f"Basic {auth}", "Content-Type": "application/json"}
        )
        if r.status_code != 200:
            logger.error(f"Razorpay error: {r.text}")
            raise HTTPException(500, "Razorpay order creation failed")
        rzp_order = r.json()

    return {
        "success":        True,
        "razorpay_order_id": rzp_order["id"],
        "amount":         total_paise,
        "currency":       "INR",
        "key":            rzp_key,
        "receipt":        receipt,
        "story_names":    [s.get("story_name_en", "") for s in valid_stories],
    }


# ── POST /api/razorpay/verify ─────────────────────────────────────
@app.post("/api/razorpay/verify")
async def verify_razorpay(payload: dict):
    """Verify Razorpay payment signature and create order."""
    import hmac, hashlib
    rzp_order_id  = payload.get("razorpay_order_id", "")
    rzp_payment_id = payload.get("razorpay_payment_id", "")
    rzp_signature  = payload.get("razorpay_signature", "")
    story_ids      = payload.get("story_ids", [])
    telegram_id    = payload.get("telegram_id") or 0
    username       = payload.get("username", "")

    secret = Config.RAZORPAY_SECRET
    generated = hmac.new(
        secret.encode(), f"{rzp_order_id}|{rzp_payment_id}".encode(), hashlib.sha256
    ).hexdigest()

    if generated != rzp_signature:
        raise HTTPException(400, "Invalid payment signature")

    # Create order record
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
    order_id = f"OD-{telegram_id}-{suffix}"

    from bson.objectid import ObjectId
    valid_stories = []
    for sid in story_ids:
        try:
            doc = await arya_db.db.premium_stories.find_one({"_id": ObjectId(str(sid))})
            if doc:
                valid_stories.append(doc)
        except Exception:
            pass

    total_price = sum(float(s.get("price") or 0) for s in valid_stories)

    await arya_db.db.orders.insert_one({
        "order_id":          order_id,
        "user_id":           telegram_id,
        "username":          username,
        "story_ids":         story_ids,
        "total_amount":      total_price,
        "status":            "paid",
        "source":            "razorpay",
        "razorpay_order_id": rzp_order_id,
        "razorpay_payment_id": rzp_payment_id,
        "created_at":        datetime.now(timezone.utc),
    })
    logger.info(f"Razorpay order {order_id} verified for {telegram_id}")

    bot_un = os.environ.get("BOT_USERNAME", "UseAryaBot")
    return {
        "success":      True,
        "order_id":     order_id,
        "checkout_url": f"https://t.me/{bot_un}?start=order_{order_id}",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("mini_app_api:app", host="0.0.0.0", port=8000, reload=True)
