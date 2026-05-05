"""
Arya Premium Mini App API — Production Grade
Run: BOT_USERNAME=UseAryaBot python3 mini_app_api.py
"""
import os, random, string, hmac, hashlib, logging, base64
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

from config import Config
from database import db as arya_db

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("arya_api")

from bson.objectid import ObjectId

BOT_USERNAME = os.environ.get("BOT_USERNAME", "UseAryaBot")
RZP_KEY      = Config.RAZORPAY_KEY
RZP_SECRET   = Config.RAZORPAY_SECRET
BANNER_SIZE  = (1184, 556)  # enforced by mgmt bot

# ── Lifespan ──────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await arya_db.connect()
    logger.info(f"✅ MongoDB connected | bot={BOT_USERNAME}")
    yield
    if arya_db.client:
        arya_db.client.close()

app = FastAPI(title="Arya Premium API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


# ═══════════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════════

def _rand(n=6): return "".join(random.choices(string.ascii_uppercase + string.digits, k=n))

def _is_url(v): return bool(v and (str(v).startswith("http://") or str(v).startswith("https://")))

def _make_order_id(uid): return f"OD-{uid}-{_rand(6)}"

def _format_story(s: dict) -> dict | None:
    story_id = str(s["_id"]) if s.get("_id") else None
    if not story_id: return None

    title = (
        s.get("story_name_en") or s.get("story_name_hi") or
        s.get("story_name") or s.get("name") or s.get("title")
    )
    if not title or not str(title).strip(): return None
    title = str(title).strip()

    description = (s.get("description") or s.get("description_hi") or "").strip()

    # Poster URL: prefer HTTP URL; else proxy via /api/image/{id}
    raw_poster = s.get("poster_url") or s.get("cover") or s.get("image_url")
    if _is_url(raw_poster):
        poster = raw_poster
    elif s.get("image"):
        poster = f"/api/image/{story_id}"
    else:
        poster = None

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
        "isCompleted":   bool(s.get("is_completed") or s.get("completed")),
        "bot_id":        s.get("bot_id"),
    }


async def _get_bot_token(bot_id) -> str | None:
    """Get the Telegram bot token for image proxy."""
    if bot_id:
        try:
            bot = await arya_db.db.premium_bots.find_one({"id": int(bot_id)})
            if bot and bot.get("token"):
                return bot["token"]
        except Exception:
            pass
    # Fallback to mgmt bot token
    return Config.MGMT_BOT_TOKEN or os.environ.get("MGMT_BOT_TOKEN")


async def _get_stories_from_ids(story_ids: list) -> list:
    """Fetch story documents from DB given a list of string ObjectIds."""
    from bson.objectid import ObjectId
    result = []
    for sid in story_ids:
        try:
            doc = await arya_db.db.premium_stories.find_one({"_id": ObjectId(str(sid))})
            if doc:
                result.append(doc)
        except Exception:
            pass
    return result


# ═══════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════

@app.get("/api/health")
async def health():
    return {"status": "ok", "bot": BOT_USERNAME, "razorpay": bool(RZP_KEY)}


# ── Banners ───────────────────────────────────────────────────────
@app.get("/api/banners")
async def get_banners():
    """
    Returns up to 10 hero banners:
     - 1 auto: most-purchased story (trending)
     - 1 auto: newest story added
     - up to 8 manual: from mini_app_banners collection
    """
    try:
        result = []

        # Auto: Trending (most purchased story)
        try:
            top_order = await arya_db.db.orders.find_one(
                {"status": {"$in": ["paid", "delivered"]}},
                sort=[("created_at", -1)]
            )
            if top_order:
                # find most common story across all paid orders
                pipeline = [
                    {"$match": {"status": {"$in": ["paid", "delivered"]}}},
                    {"$unwind": "$story_ids"},
                    {"$group": {"_id": "$story_ids", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 1}
                ]
                agg = await arya_db.db.orders.aggregate(pipeline).to_list(1)
                if agg:
                    trend_story = await arya_db.db.premium_stories.find_one(
                        {"_id": ObjectId(str(agg[0]["_id"]))}
                    )
                    if trend_story:
                        fmt = _format_story(trend_story)
                        if fmt:
                            result.append({
                                "id": f"trending_{fmt['id']}",
                                "type": "trending",
                                "story_id": fmt["id"],
                                "image": fmt["poster"] or fmt["banner"],
                                "title": fmt["title"],
                                "subtitle": "🔥 Trending Now",
                                "badge": "TRENDING",
                            })
        except Exception as e:
            logger.warning(f"Trending banner error: {e}")

        # Auto: Newest story
        try:
            newest = await arya_db.db.premium_stories.find_one(
                {}, sort=[("_id", -1)]
            )
            if newest:
                fmt = _format_story(newest)
                if fmt:
                    result.append({
                        "id": f"new_{fmt['id']}",
                        "type": "new",
                        "story_id": fmt["id"],
                        "image": fmt["poster"] or fmt["banner"],
                        "title": fmt["title"],
                        "subtitle": "✨ New Release",
                        "badge": "NEW",
                    })
        except Exception as e:
            logger.warning(f"Newest banner error: {e}")

        # Manual banners from DB (up to 8)
        try:
            cursor = arya_db.db.mini_app_banners.find({}).sort("order", 1).limit(8)
            manual = await cursor.to_list(length=8)
            for b in manual:
                bid = str(b["_id"])
                result.append({
                    "id": bid,
                    "type": "manual",
                    "story_id": b.get("story_id"),
                    "image": f"/api/image/banner/{bid}",
                    "title": b.get("title", ""),
                    "subtitle": b.get("subtitle", ""),
                    "badge": b.get("badge", ""),
                })
        except Exception as e:
            logger.warning(f"Manual banners error: {e}")

        return {"success": True, "data": result[:10]}
    except Exception as e:
        logger.error(f"/api/banners error: {e}", exc_info=True)
        raise HTTPException(500, str(e))


# ── Banner Image Proxy ────────────────────────────────────────────
@app.get("/api/image/banner/{banner_id}")
async def banner_image_proxy(banner_id: str):
    """Proxy banner image from Telegram file_id."""
    import httpx
    try:
        banner = await arya_db.db.mini_app_banners.find_one({"_id": ObjectId(banner_id)})
    except Exception:
        raise HTTPException(404, "Invalid banner id")
    if not banner:
        raise HTTPException(404, "Banner not found")

    file_id = banner.get("file_id")
    if not file_id:
        raise HTTPException(404, "No image")
    if _is_url(file_id):
        from fastapi.responses import RedirectResponse
        return RedirectResponse(file_id)

    token = await _get_bot_token(None)  # use mgmt bot token
    if not token:
        raise HTTPException(500, "No bot token")

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"https://api.telegram.org/bot{token}/getFile",
                params={"file_id": file_id}
            )
            data = r.json()
            if not data.get("ok"):
                raise HTTPException(404, data.get("description", "getFile failed"))
            file_path = data["result"]["file_path"]
            img = await client.get(
                f"https://api.telegram.org/file/bot{token}/{file_path}"
            )
            return Response(
                content=img.content,
                media_type=img.headers.get("content-type", "image/jpeg"),
                headers={"Cache-Control": "public, max-age=86400"}
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Banner image fetch failed: {e}")


# ── Stories ───────────────────────────────────────────────────────
@app.get("/api/stories")
async def get_stories():
    try:
        raw = await arya_db.get_all_stories()
        stories = [r for r in (_format_story(s) for s in raw) if r]
        logger.info(f"Serving {len(stories)} stories")
        return {"success": True, "data": stories}
    except Exception as e:
        logger.error(f"stories error: {e}", exc_info=True)
        raise HTTPException(500, str(e))


# ── Image Proxy ───────────────────────────────────────────────────
@app.get("/api/image/{story_id}")
async def image_proxy(story_id: str):
    """
    Proxy Telegram file_id images so browser can render them.
    Flow: getFile → file_path → download → stream bytes.
    """
    import httpx
    from bson.objectid import ObjectId

    # 1. Load story
    try:
        story = await arya_db.db.premium_stories.find_one({"_id": ObjectId(story_id)})
    except Exception:
        raise HTTPException(404, "Invalid story id")
    if not story:
        raise HTTPException(404, "Story not found")

    file_id = story.get("image")
    if not file_id:
        raise HTTPException(404, "No image for this story")
    if _is_url(file_id):
        # It's already a URL — redirect
        from fastapi.responses import RedirectResponse
        return RedirectResponse(file_id)

    # 2. Get bot token
    token = await _get_bot_token(story.get("bot_id"))
    if not token:
        raise HTTPException(500, "No bot token configured")

    # 3. Fetch via Telegram Bot API
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # Step A: getFile
            r = await client.get(
                f"https://api.telegram.org/bot{token}/getFile",
                params={"file_id": file_id},
            )
            data = r.json()
            if not data.get("ok"):
                err = data.get("description", "getFile failed")
                logger.error(f"Telegram getFile error: {err} | file_id={file_id}")
                raise HTTPException(404, f"Telegram error: {err}")

            file_path = data["result"]["file_path"]

            # Step B: download
            img = await client.get(
                f"https://api.telegram.org/file/bot{token}/{file_path}"
            )
            if img.status_code != 200:
                raise HTTPException(502, "Image download failed")

            ct = img.headers.get("content-type", "image/jpeg")
            return Response(
                content=img.content,
                media_type=ct,
                headers={"Cache-Control": "public, max-age=86400"},
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Image proxy error [{story_id}]: {e}", exc_info=True)
        raise HTTPException(500, "Image fetch failed")


# ── Checkout (Bot UPI flow) ───────────────────────────────────────
@app.post("/api/checkout")
async def checkout(payload: dict):
    """Create pending order → return Telegram bot deep-link for UPI payment."""
    story_ids  = payload.get("story_ids", [])
    tg_id      = payload.get("telegram_id") or 0
    username   = payload.get("username", "")

    if not story_ids:
        raise HTTPException(400, "Cart is empty")

    stories = await _get_stories_from_ids(story_ids)
    if not stories:
        raise HTTPException(400, "No valid stories")

    total  = sum(float(s.get("price") or 0) for s in stories)
    oid    = _make_order_id(tg_id)

    await arya_db.db.orders.insert_one({
        "order_id":    oid,
        "user_id":     tg_id,
        "username":    username,
        "story_ids":   story_ids,
        "story_names": [s.get("story_name_en", "") for s in stories],
        "total":       total,
        "status":      "pending",
        "source":      "bot_upi",
        "created_at":  datetime.now(timezone.utc),
    })
    logger.info(f"Order {oid} created (UPI/bot) for {tg_id}, ₹{total}")

    bot = os.environ.get("BOT_USERNAME", BOT_USERNAME)
    return {
        "success":      True,
        "order_id":     oid,
        "total":        total,
        "checkout_url": f"https://t.me/{bot}?start=order_{oid}",
    }


# ── Razorpay: Create Order ────────────────────────────────────────
@app.post("/api/create-order")
async def create_razorpay_order(payload: dict):
    """Create a Razorpay order. Returns order_id, key, amount for frontend."""
    story_ids = payload.get("story_ids", [])
    tg_id     = payload.get("telegram_id") or 0

    if not story_ids:
        raise HTTPException(400, "Cart is empty")
    if not RZP_KEY or not RZP_SECRET:
        raise HTTPException(500, "Razorpay not configured on server")

    stories = await _get_stories_from_ids(story_ids)
    if not stories:
        raise HTTPException(400, "No valid stories")

    total_paise = int(sum(float(s.get("price") or 0) for s in stories) * 100)
    receipt     = _make_order_id(tg_id)

    import httpx
    auth_header = "Basic " + base64.b64encode(f"{RZP_KEY}:{RZP_SECRET}".encode()).decode()

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                "https://api.razorpay.com/v1/orders",
                json={
                    "amount":   total_paise,
                    "currency": "INR",
                    "receipt":  receipt,
                    "notes":    {"telegram_id": str(tg_id), "story_ids": ",".join(story_ids)},
                },
                headers={"Authorization": auth_header, "Content-Type": "application/json"},
            )
            if r.status_code != 200:
                logger.error(f"Razorpay create-order failed: {r.text}")
                raise HTTPException(502, "Razorpay order creation failed")
            rzp = r.json()

        return {
            "success":          True,
            "razorpay_order_id": rzp["id"],
            "amount":           total_paise,
            "currency":         "INR",
            "key":              RZP_KEY,
            "receipt":          receipt,
            "story_names":      [s.get("story_name_en", "") for s in stories],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"create-order error: {e}", exc_info=True)
        raise HTTPException(500, str(e))


# ── Razorpay: Verify Payment ──────────────────────────────────────
@app.post("/api/verify-payment")
async def verify_payment(payload: dict):
    """
    Verify Razorpay HMAC signature.
    On success: store order in DB (status=paid) + return bot deep-link for delivery.
    """
    rzp_order_id   = payload.get("razorpay_order_id", "")
    rzp_payment_id = payload.get("razorpay_payment_id", "")
    rzp_signature  = payload.get("razorpay_signature", "")
    story_ids      = payload.get("story_ids", [])
    tg_id          = payload.get("telegram_id") or 0
    username       = payload.get("username", "")

    if not all([rzp_order_id, rzp_payment_id, rzp_signature]):
        raise HTTPException(400, "Missing payment fields")

    # HMAC-SHA256 verification
    expected = hmac.new(
        RZP_SECRET.encode("utf-8"),
        f"{rzp_order_id}|{rzp_payment_id}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(expected, rzp_signature):
        logger.warning(f"Invalid Razorpay signature for {rzp_payment_id}")
        raise HTTPException(400, "Payment verification failed — invalid signature")

    # Store verified order
    stories = await _get_stories_from_ids(story_ids)
    total   = sum(float(s.get("price") or 0) for s in stories)
    oid     = _make_order_id(tg_id)

    await arya_db.db.orders.insert_one({
        "order_id":            oid,
        "user_id":             tg_id,
        "username":            username,
        "story_ids":           story_ids,
        "story_names":         [s.get("story_name_en", "") for s in stories],
        "total":               total,
        "status":              "paid",
        "source":              "razorpay",
        "razorpay_order_id":   rzp_order_id,
        "razorpay_payment_id": rzp_payment_id,
        "created_at":          datetime.now(timezone.utc),
    })

    # Mark purchases in user record
    if tg_id:
        for sid in story_ids:
            await arya_db.add_purchase(int(tg_id), sid)

    logger.info(f"Payment verified: {oid} | {rzp_payment_id} | user={tg_id} | ₹{total}")

    bot = os.environ.get("BOT_USERNAME", BOT_USERNAME)
    return {
        "success":      True,
        "order_id":     oid,
        "total":        total,
        "checkout_url": f"https://t.me/{bot}?start=order_{oid}",
    }


# ── Delete Account (30-day grace) ────────────────────────────────
@app.post("/api/delete-account")
async def delete_account(payload: dict):
    """
    Schedule account deletion with 30-day grace period.
    User data is NOT deleted immediately — a flag is set.
    A background job / admin can purge after 30 days.
    """
    from datetime import timedelta
    tg_id = payload.get("telegram_id")
    if not tg_id:
        raise HTTPException(400, "telegram_id required")
    try:
        tg_id = int(tg_id)
    except Exception:
        raise HTTPException(400, "Invalid telegram_id")

    scheduled_at = datetime.now(timezone.utc)
    delete_at    = scheduled_at + timedelta(days=30)

    await arya_db.db.users.update_one(
        {"user_id": tg_id},
        {"$set": {
            "deletion_scheduled":   True,
            "deletion_scheduled_at": scheduled_at,
            "scheduled_delete_at":  delete_at,
        }},
        upsert=True
    )
    logger.info(f"Account deletion scheduled: user={tg_id}, delete_at={delete_at.isoformat()}")
    return {
        "success":     True,
        "message":     "Account scheduled for deletion in 30 days",
        "delete_at":   delete_at.isoformat(),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("mini_app_api:app", host="0.0.0.0", port=8000, reload=True)
