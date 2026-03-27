import os
import json
import logging
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from pyrogram import Client, filters

logger = logging.getLogger(__name__)

YOUTUBE_SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube"
]
TOKEN_FILE = "youtube_token.json"
CLIENT_SECRET_FILE = "client_secret.json"

def get_youtube_auth_url():
    if not os.path.exists(CLIENT_SECRET_FILE):
        return None, "client_secret.json not found! Please download it from Google Cloud Console and place it in the bot's root directory."
    from google_auth_oauthlib.flow import InstalledAppFlow
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, YOUTUBE_SCOPES)
    # Redirect URI MUST be ur-encoded or 'urn:ietf:wg:oauth:2.0:oob'
    flow.redirect_uri = 'urn:ietf:wg:oauth:2.0:oob'
    auth_url, _ = flow.authorization_url(prompt='consent')
    return auth_url, flow

def save_youtube_credentials(flow, code):
    try:
        flow.fetch_token(code=code)
        creds = flow.credentials
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
        return True, "Successfully authorized and saved token!"
    except Exception as e:
        return False, str(e)

def get_authenticated_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, YOUTUBE_SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(TOKEN_FILE, 'w') as token:
                token.write(creds.to_json())
        else:
            return None
    return build('youtube', 'v3', credentials=creds)

async def upload_video_to_youtube(video_path, title, description="", tags=None, category_id="22", privacy_status="private", thumbnail_path=None):
    try:
        import asyncio
        youtube = get_authenticated_service()
        if not youtube:
            return False, "YouTube is not authorized. Please configure OAuth first."

        body = {
            'snippet': {
                'title': title,
                'description': description,
                'tags': tags or ["Auto-Forward-Bot"],
                'categoryId': category_id
            },
            'status': {
                'privacyStatus': privacy_status,
                'selfDeclaredMadeForKids': False, 
            }
        }

        media = MediaFileUpload(video_path, chunksize=-1, resumable=True)
        request = youtube.videos().insert(
            part=",".join(body.keys()),
            body=body,
            media_body=media
        )

        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, request.execute)
        
        video_id = response['id']

        if thumbnail_path and os.path.exists(thumbnail_path):
            try:
                thumb_media = MediaFileUpload(thumbnail_path, mimetype='image/jpeg')
                thumb_request = youtube.thumbnails().set(videoId=video_id, media_body=thumb_media)
                await loop.run_in_executor(None, thumb_request.execute)
            except Exception as e:
                logger.warning(f"YouTube Thumbnail Upload Failed: {e}")

        return True, f"https://youtu.be/{video_id}"
    except Exception as e:
        logger.error(f"YouTube Upload Failed: {e}")
        return False, str(e)


async def update_youtube_video(video_id: str, title: str, description: str = "") -> tuple:
    """Update the title and description of an existing YouTube video."""
    try:
        import asyncio
        youtube = get_authenticated_service()
        if not youtube:
            return False, "YouTube is not authorized. Please configure OAuth first."

        body = {
            'id': video_id,
            'snippet': {
                'title': title,
                'description': description,
                'categoryId': '22'  # People & Blogs
            }
        }

        loop = asyncio.get_event_loop()
        request = youtube.videos().update(part="snippet", body=body)
        response = await loop.run_in_executor(None, request.execute)
        updated_id = response.get('id', video_id)
        return True, f"Updated: https://youtu.be/{updated_id}"
    except Exception as e:
        logger.error(f"YouTube Update Failed: {e}")
        return False, str(e)


# Simple Auth Command (Requires bot admin to do it usually, so we'll check it's admin/owner)
_flows_cache = {}

@Client.on_message(filters.command("ytauth") & filters.private)
async def yt_auth_cmd(bot, message):
    user_id = message.from_user.id
    if len(message.command) > 1:
        # Processing code
        code = message.text.split(None, 1)[1].strip()
        if user_id not in _flows_cache:
            return await message.reply("No auth flow started. Use /ytauth first.")
        
        m = await message.reply("Verifying code...")
        success, res = save_youtube_credentials(_flows_cache[user_id], code)
        del _flows_cache[user_id]
        if success:
            await m.edit("✅ **YouTube Authentication Successful!** The bot can now upload directly to your channel.")
        else:
            await m.edit(f"❌ **Failed:** `{res}`")
        return

    # Check if we already have a valid token
    svc = get_authenticated_service()
    if svc:
        return await message.reply("✅ **YouTube API is already authorized.**\nYou can start uploading videos. (Run `/ytauth reset` to re-authorize if needed)")

    url, flow_or_err = get_youtube_auth_url()
    if not url:
        return await message.reply(f"❌ **Error:** {flow_or_err}")
    
    _flows_cache[user_id] = flow_or_err
    await message.reply(f"**🔗 YouTube Authentication Required**\n\n"
                        f"1. **[Click Here to Authorize]({url})**\n"
                        f"2. Log in with your YouTube account and grant permission.\n"
                        f"3. Copy the authorization code provided by Google.\n"
                        f"4. Send it back to me like this:\n"
                        f"`/ytauth PasteYourCodeHere`", disable_web_page_preview=True)
