import os
import uuid
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ─────────────────────────────────────────────────────────────────
# Use AryaPremium's own database module (already tested, working)
# ─────────────────────────────────────────────────────────────────
import sys
import importlib

# Add AryaPremium to path so we can import its database
_arya_path = os.path.join(os.path.dirname(__file__), "AryaPremium")
if _arya_path not in sys.path:
    sys.path.insert(0, _arya_path)

# ─────────────────────────────────────────────────────────────────
# Lifespan: connect/disconnect
# ─────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        from AryaPremium.database import db as arya_db
        await arya_db.connect()
        app.state.db = arya_db
        logger.info("✅ Connected to MongoDB via AryaPremium DB module")
    except Exception as e:
        logger.error(f"DB connect failed: {e}")
        raise
    yield
    logger.info("Disconnected from MongoDB")


app = FastAPI(title="Arya Premium Mini App API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────────────────────────
# Helper: format a single MongoDB story doc → frontend Story shape
# ─────────────────────────────────────────────────────────────────
def _format_story(s: dict) -> dict | None:
    # ID — never null
    story_id = str(s["_id"]) if s.get("_id") else None
    if not story_id:
        return None

    # TITLE — real value, never "Unknown"
    title = (
        s.get("story_name_en")
        or s.get("story_name_hi")
        or s.get("story_name")
        or s.get("name")
        or s.get("title")
    )
    if title:
        title = title.strip()
    if not title:
        return None  # skip stories with no title

    # DESCRIPTION — clean UTF-8
    description = s.get("description") or s.get("description_hi") or ""
    try:
        description = description.encode("utf-8", errors="ignore").decode("utf-8").strip()
    except Exception:
        description = ""

    # COVER — prefer HTTP URL, fallback to Telegram file_id, then placeholder
    cover = (
        s.get("poster_url")
        or s.get("cover")
        or s.get("image_url")
        or s.get("image")       # Telegram file_id (mgmt bot saves this)
        or "https://images.unsplash.com/photo-1614729939124-032f0b56c9ce?w=400"
    )

    return {
        "id":           story_id,
        "title":        title,
        "description":  description,
        "poster":       cover,
        "banner":       cover,
        "cover":        cover,
        "price":        float(s.get("price") or 0),
        "language":     s.get("language") or "Hindi",
        "platform":     s.get("platform") or "Pocket FM",
        "genre":        s.get("genre") or "Drama",
        "status":       "available",
        "episodes":     s.get("episodes") or s.get("ep_count") or s.get("total_eps") or "?",
        "totalEpisodes":s.get("episodes") or s.get("total_eps") or s.get("ep_count") or "?",
        "size":         s.get("total_size") or s.get("size") or None,
        "isCompleted":  bool(s.get("is_completed") or s.get("completed") or
                            (s.get("status", "") == "Completed")),
    }


# ─────────────────────────────────────────────────────────────────
# GET /stories
# ─────────────────────────────────────────────────────────────────
@app.get("/stories")
async def get_stories():
    """Fetch all premium stories using AryaPremium's db.get_all_stories()"""
    try:
        arya_db = app.state.db
        # Use the existing, tested method from AryaPremium/database.py
        stories = await arya_db.get_all_stories()

        formatted = []
        for s in stories:
            item = _format_story(s)
            if item:
                formatted.append(item)

        logger.info(f"Returning {len(formatted)} stories")
        return {"success": True, "data": formatted}

    except Exception as e:
        logger.error(f"Error in /stories: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────────
# POST /checkout
# ─────────────────────────────────────────────────────────────────
@app.post("/checkout")
async def checkout(payload: dict):
    """Secure Order-based Checkout System"""
    telegram_id = payload.get("telegram_id")
    story_ids   = payload.get("story_ids", [])
    username    = payload.get("username", "")

    if not telegram_id or not story_ids:
        raise HTTPException(status_code=400, detail="Missing telegram_id or story_ids")

    arya_db = app.state.db

    # Validate stories exist
    from bson.objectid import ObjectId
    valid_stories = []
    for sid in story_ids:
        try:
            doc = await arya_db.db.premium_stories.find_one({"_id": ObjectId(sid)})
            if doc:
                valid_stories.append(doc)
        except Exception:
            pass

    if not valid_stories:
        raise HTTPException(status_code=400, detail="Invalid stories requested")

    total_price = sum(float(s.get("price", 0) or 0) for s in valid_stories)

    # Create order
    order_id = f"OD_{uuid.uuid4().hex[:8].upper()}"
    order_doc = {
        "order_id":    order_id,
        "user_id":     telegram_id,
        "username":    username,
        "story_ids":   story_ids,
        "total_amount":total_price,
        "status":      "pending",
        "created_at":  datetime.now(timezone.utc),
    }
    await arya_db.db.orders.insert_one(order_doc)
    logger.info(f"Order {order_id} created for user {telegram_id}")

    bot_username = os.environ.get("BOT_USERNAME", "AryaPremiumBot")
    return {
        "success":      True,
        "checkout_url": f"https://t.me/{bot_username}?start=order_{order_id}",
        "order_id":     order_id,
        "message":      "Redirecting to bot for payment",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("mini_app_api:app", host="0.0.0.0", port=8000, reload=True)
