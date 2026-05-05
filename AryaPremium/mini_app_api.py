"""
Arya Premium Mini App API
Run from AryaPremium/ directory:
    BOT_USERNAME=AryaPremiumBot python3 mini_app_api.py
"""
import os
import uuid
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# ── Load .env first ──────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

# ── Local imports (run from AryaPremium/ directory) ──────────────
from config import Config          # AryaPremium/config.py
from database import db as arya_db # AryaPremium/database.py  (db.get_all_stories etc.)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ── Lifespan ─────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await arya_db.connect()
    logger.info("✅ MongoDB connected")
    yield
    if arya_db.client:
        arya_db.client.close()
    logger.info("MongoDB disconnected")


app = FastAPI(title="Arya Premium Mini App API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Story formatter ───────────────────────────────────────────────
def _format(s: dict) -> dict | None:
    # ID — never null
    story_id = str(s["_id"]) if s.get("_id") else None
    if not story_id:
        return None

    # TITLE — real value only
    title = (
        s.get("story_name_en")
        or s.get("story_name_hi")
        or s.get("story_name")
        or s.get("name")
        or s.get("title")
    )
    if title:
        title = str(title).strip()
    if not title:
        return None   # skip untitled stories

    # DESCRIPTION
    description = (s.get("description") or s.get("description_hi") or "").strip()

    # COVER — prefer URL, fallback to Telegram file_id
    cover = (
        s.get("poster_url")
        or s.get("cover")
        or s.get("image_url")
        or s.get("image")
        or "https://images.unsplash.com/photo-1614729939124-032f0b56c9ce?w=400"
    )

    return {
        "id":            story_id,
        "title":         title,
        "description":   description,
        "poster":        cover,
        "banner":        cover,
        "cover":         cover,
        "price":         float(s.get("price") or 0),
        "language":      s.get("language") or "Hindi",
        "platform":      s.get("platform") or "Pocket FM",
        "genre":         s.get("genre") or "Drama",
        "status":        "available",
        "episodes":      s.get("episodes") or s.get("ep_count") or s.get("total_eps") or "?",
        "totalEpisodes": s.get("episodes") or s.get("total_eps") or s.get("ep_count") or "?",
        "size":          s.get("total_size") or s.get("size") or None,
        "isCompleted":   bool(
            s.get("is_completed")
            or s.get("completed")
            or str(s.get("status", "")).lower() == "completed"
        ),
    }


# ── GET /api/stories ──────────────────────────────────────────────
@app.get("/api/stories")
async def get_stories():
    try:
        stories = await arya_db.get_all_stories()   # ← correct method
        result  = [_format(s) for s in stories]
        result  = [r for r in result if r]          # drop None (no title/id)
        logger.info(f"Returning {len(result)} stories")
        return {"success": True, "data": result}
    except Exception as e:
        logger.error(f"/api/stories error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ── POST /api/checkout ────────────────────────────────────────────
@app.post("/api/checkout")
async def checkout(payload: dict):
    telegram_id = payload.get("telegram_id")
    story_ids   = payload.get("story_ids", [])
    username    = payload.get("username", "")

    if not telegram_id or not story_ids:
        raise HTTPException(status_code=400, detail="Missing telegram_id or story_ids")

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
    order_id    = f"OD_{uuid.uuid4().hex[:8].upper()}"

    await arya_db.db.orders.insert_one({
        "order_id":     order_id,
        "user_id":      telegram_id,
        "username":     username,
        "story_ids":    story_ids,
        "total_amount": total_price,
        "status":       "pending",
        "created_at":   datetime.now(timezone.utc),
    })
    logger.info(f"Order {order_id} created for {telegram_id}")

    bot_username = os.environ.get("BOT_USERNAME", "AryaPremiumBot")
    return {
        "success":      True,
        "checkout_url": f"https://t.me/{bot_username}?start=order_{order_id}",
        "order_id":     order_id,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("mini_app_api:app", host="0.0.0.0", port=8000, reload=True)
