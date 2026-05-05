import os
import uuid
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from config import Config

logger = logging.getLogger(__name__)

# MongoDB Client
client = None
db = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, db
    mongo_uri = Config.DATABASE_URI
    if not mongo_uri:
        raise ValueError("DATABASE_URI not set in config/env!")
    
    client = AsyncIOMotorClient(mongo_uri)
    db = client[Config.DATABASE_NAME]
    logger.info("Connected to MongoDB for Mini App API")
    yield
    client.close()
    logger.info("Disconnected from MongoDB")

app = FastAPI(title="Arya Premium Mini App API", lifespan=lifespan)

# Allow requests from the React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, restrict this to your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/stories")
async def get_stories():
    """Fetch all premium stories from the database"""
    try:
        cursor = db.premium_stories.find({})
        stories = await cursor.to_list(length=None)

        formatted_stories = []
        for s in stories:
            # --- ID: always use string of _id ---
            story_id = str(s.get("_id", ""))
            if not story_id:
                continue

            # --- TITLE: use story_name_en, fallback to story_name_hi ---
            title = (
                s.get("story_name_en")
                or s.get("story_name_hi")
                or s.get("name_en")
                or s.get("name")
                or s.get("title")
                or ""
            ).strip()
            if not title:
                continue  # Skip entries with no valid title

            # --- DESCRIPTION: clean and safe ---
            description = (
                s.get("description")
                or s.get("description_hi")
                or "No description available."
            )
            try:
                description = description.encode("utf-8", errors="ignore").decode("utf-8").strip()
            except Exception:
                description = "No description available."

            # --- COVER IMAGE: 'image' is what mgmt bot saves (Telegram file_id), poster_url is URL ---
            cover = (
                s.get("poster_url")     # URL form (if set manually)
                or s.get("image")       # Telegram file_id (saved by mgmt bot)
                or s.get("cover")
                or s.get("image_url")
                or "https://images.unsplash.com/photo-1614729939124-032f0b56c9ce"
            )

            # --- STATUS ---
            is_completed = s.get("is_completed") or s.get("completed") or False
            status = "Completed" if is_completed else "Ongoing"

            formatted_stories.append({
                "id": story_id,
                "title": title,
                "description": description,
                "poster": cover,      # frontend Story type uses 'poster'
                "banner": cover,      # frontend Story type uses 'banner'
                "cover": cover,       # keep for backward compat
                "price": float(s.get("price", 0) or 0),
                "language": s.get("language", "Hindi"),
                "platform": s.get("platform", "Pocket FM"),
                "genre": s.get("genre", "Romance"),
                "status": "available",  # frontend expects: "available" | "coming_soon"
                "episodes": s.get("episodes") or s.get("ep_count") or s.get("total_eps") or 0,
                "totalEpisodes": s.get("episodes") or s.get("total_eps") or s.get("ep_count") or "?",
                "size": s.get("total_size") or s.get("size") or "Unknown",
                "isCompleted": bool(s.get("is_completed") or s.get("completed")),
            })

        return {"success": True, "data": formatted_stories}
    except Exception as e:
        logger.error(f"Error fetching stories: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.post("/checkout")
async def checkout(payload: dict):
    """Secure Order-based Checkout System"""
    telegram_id = payload.get("telegram_id")
    story_ids = payload.get("story_ids", [])
    username = payload.get("username", "Unknown")
    
    if not telegram_id or not story_ids:
        raise HTTPException(status_code=400, detail="Missing telegram_id or story_ids")

    # 1. VALIDATE STORIES FROM DATABASE (Security check)
    cursor = db.premium_stories.find({"story_id": {"$in": story_ids}})
    valid_stories = await cursor.to_list(length=None)
    valid_story_ids = [s.get("story_id") for s in valid_stories]

    if not valid_story_ids:
        raise HTTPException(status_code=400, detail="Invalid stories requested")

    # Calculate total price (optional, but good for record keeping)
    total_price = sum(float(s.get("price", 0)) for s in valid_stories)

    # 2. CREATE SECURE ORDER SYSTEM
    order_id = f"OD_{uuid.uuid4().hex[:8].upper()}"
    
    order_doc = {
        "order_id": order_id,
        "user_id": telegram_id,
        "username": username,
        "story_ids": valid_story_ids,
        "total_amount": total_price,
        "status": "pending",
        "created_at": datetime.now(timezone.utc)
    }
    
    # Store order in MongoDB
    await db.orders.insert_one(order_doc)
    logger.info(f"Order {order_id} created for user {telegram_id}")

    # 3. RETURN SECURE BOT LINK
    import os
    bot_username = os.environ.get("BOT_USERNAME", "AryaPremiumBot")
    bot_deep_link = f"https://t.me/{bot_username}?start=order_{order_id}"
    
    return {
        "success": True,
        "checkout_url": bot_deep_link,
        "message": "Redirecting to bot for payment",
        "order_id": order_id
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("mini_app_api:app", host="0.0.0.0", port=8000, reload=True)
