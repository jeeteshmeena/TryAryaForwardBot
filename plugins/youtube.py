import os
import re
import asyncio
import logging
from pyrogram import Client, filters, ContinuePropagation

logger = logging.getLogger(__name__)

# ── Lazy-import Google libraries so the plugin always loads ──────────────────
# If these packages are missing the /ytauth command will report the error
# clearly instead of crashing the entire plugin at startup.
try:
    from googleapiclient.discovery import build as _yt_build
    from googleapiclient.http import MediaFileUpload as _MediaFileUpload
    from google.auth.transport.requests import Request as _Request
    from google.oauth2.credentials import Credentials as _Credentials
    _GOOGLE_LIBS_OK = True
except ImportError as _e:
    _GOOGLE_LIBS_OK = False
    _GOOGLE_IMPORT_ERR = str(_e)
    logger.warning(f"[youtube.py] Google API libs not available: {_e}. /ytauth will show install instructions.")

YOUTUBE_SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube"
]
TOKEN_FILE = "youtube_token.json"
CLIENT_SECRET_FILE = "client_secret.json"


def _check_libs():
    """Return (ok, error_msg). Call before any Google API usage."""
    if not _GOOGLE_LIBS_OK:
        return False, (
            "❌ Google API libraries are not installed.\n\n"
            f"Missing: `{_GOOGLE_IMPORT_ERR}`\n\n"
            "Please run on the VPS:\n"
            "```\npip install google-api-python-client google-auth-httplib2 google-auth-oauthlib\n```"
        )
    return True, None


def get_youtube_auth_url():
    ok, err = _check_libs()
    if not ok:
        return None, err
    if not os.path.exists(CLIENT_SECRET_FILE):
        return None, (
            "`client_secret.json` not found!\n\n"
            "Download it from Google Cloud Console → APIs & Services → Credentials "
            "and place it in the bot's root directory."
        )
    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, YOUTUBE_SCOPES)
        flow.redirect_uri = 'urn:ietf:wg:oauth:2.0:oob'
        auth_url, _ = flow.authorization_url(prompt='consent')
        return auth_url, flow
    except Exception as e:
        logger.error(f"[ytauth] get_youtube_auth_url error: {e}")
        return None, str(e)


def save_youtube_credentials(flow, code):
    try:
        flow.fetch_token(code=code)
        creds = flow.credentials
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
        return True, "Successfully authorized and saved token!"
    except Exception as e:
        logger.error(f"[ytauth] save_youtube_credentials error: {e}")
        return False, str(e)


def get_authenticated_service():
    ok, _ = _check_libs()
    if not ok:
        return None
    try:
        creds = None
        if os.path.exists(TOKEN_FILE):
            creds = _Credentials.from_authorized_user_file(TOKEN_FILE, YOUTUBE_SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(_Request())
                    with open(TOKEN_FILE, 'w') as token:
                        token.write(creds.to_json())
                except Exception as refresh_err:
                    err_str = str(refresh_err).lower()
                    # Stale token with wrong scopes — delete it and force re-auth
                    if "invalid_scope" in err_str or "invalid_grant" in err_str or "bad request" in err_str:
                        logger.warning(f"[ytauth] Stale/invalid token detected ({refresh_err}). Deleting token file — re-auth required.")
                        try:
                            os.remove(TOKEN_FILE)
                        except Exception:
                            pass
                    return None
            else:
                return None
        return _yt_build('youtube', 'v3', credentials=creds)
    except Exception as e:
        err_str = str(e).lower()
        if "invalid_scope" in err_str or "invalid_grant" in err_str or "bad request" in err_str:
            logger.warning(f"[ytauth] Stale/invalid token detected ({e}). Deleting token — re-auth required.")
            try:
                os.remove(TOKEN_FILE)
            except Exception:
                pass
        else:
            logger.error(f"[ytauth] get_authenticated_service error: {e}")
        return None


async def upload_video_to_youtube(video_path, title, description="", tags=None,
                                   category_id="22", privacy_status="private",
                                   thumbnail_path=None):
    try:
        import asyncio
        youtube = get_authenticated_service()
        if not youtube:
            return False, "YouTube is not authorized. Please run /ytauth first."

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

        # Use 10MB chunks for resumable upload — chunksize=-1 (single-shot) fails for large files
        media = _MediaFileUpload(video_path, mimetype='video/mp4', chunksize=10 * 1024 * 1024, resumable=True)
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
                thumb_media = _MediaFileUpload(thumbnail_path, mimetype='image/jpeg')
                thumb_req = youtube.thumbnails().set(videoId=video_id, media_body=thumb_media)
                await loop.run_in_executor(None, thumb_req.execute)
            except Exception as e:
                logger.warning(f"[ytauth] Thumbnail upload failed: {e}")

        return True, f"https://youtu.be/{video_id}"
    except Exception as e:
        logger.error(f"[ytauth] upload_video_to_youtube error: {e}")
        return False, str(e)


async def update_youtube_video(video_id: str, title: str, description: str = "") -> tuple:
    """Update the title and description of an existing YouTube video."""
    try:
        import asyncio
        youtube = get_authenticated_service()
        if not youtube:
            return False, "YouTube is not authorized. Please run /ytauth first."

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
        logger.error(f"[ytauth] update_youtube_video error: {e}")
        return False, str(e)


# ── /ytauth command ───────────────────────────────────────────────────────────
_flows_cache = {}

@Client.on_message(filters.command("ytauth") & filters.private)
async def yt_auth_cmd(bot, message):
    # Owner-only guard
    try:
        from config import Config
        owner_ids = Config.BOT_OWNER_ID
        if owner_ids and message.from_user.id not in owner_ids:
            return await message.reply("⛔ This command is only available to the bot owner.")
    except Exception:
        pass  # If config unavailable, allow anyway

    user_id = message.from_user.id

    # ── Handle code submission: /ytauth <code> ────────────────────────────
    if len(message.command) > 1:
        code = message.text.split(None, 1)[1].strip()
        if code.lower() == "reset":
            try:
                os.remove(TOKEN_FILE)
                _flows_cache.pop(user_id, None)
            except Exception:
                pass
            return await message.reply("♻️ YouTube token cleared. Send /ytauth to re-authorize.")
        if user_id not in _flows_cache:
            return await message.reply(
                "⚠️ No auth flow found.\n\n"
                "Please send /ytauth (without a code) first to get the authorization link, "
                "then paste your code."
            )
        m = await message.reply("⏳ Verifying code...")
        success, res = save_youtube_credentials(_flows_cache[user_id], code)
        _flows_cache.pop(user_id, None)
        if success:
            await m.edit(
                "✅ **YouTube Authentication Successful!**\n\n"
                "The bot can now upload videos directly to your channel.\n"
                "Run `/ytauth reset` to revoke access if needed."
            )
        else:
            await m.edit(f"❌ **Failed:** `{res}`")
        return

    # ── Check if Google libs are available ───────────────────────────────
    ok, libs_err = _check_libs()
    if not ok:
        return await message.reply(libs_err)

    # ── Already have a valid token? ───────────────────────────────────────
    svc = get_authenticated_service()
    if svc:
        return await message.reply(
            "✅ **YouTube API is already authorized.**\n\n"
            "You can start uploading videos via the Merger.\n"
            "Run `/ytauth reset` to re-authorize with a different account."
        )

    # ── Start auth flow ───────────────────────────────────────────────────
    url, flow_or_err = get_youtube_auth_url()
    if not url:
        return await message.reply(f"❌ **Setup Error:**\n\n{flow_or_err}")

    _flows_cache[user_id] = flow_or_err
    await message.reply(
        "**🔗 YouTube Authentication Required**\n\n"
        "**Step 1:** [Click here to authorize]({url})\n"
        "**Step 2:** Log in with your YouTube channel account and grant permission.\n"
        "**Step 3:** Copy the authorization code shown by Google.\n"
        "**Step 4:** Send it back here:\n"
        "`/ytauth YOUR_CODE_HERE`\n\n"
        "⚠️ The code expires in a few minutes — act quickly!".format(url=url),
        disable_web_page_preview=True
    )


# ── /ytedit command ───────────────────────────────────────────────────────────
_ytedit_waiter: dict = {}

@Client.on_message(filters.command("ytedit") & filters.private)
async def yt_edit_cmd(bot, message):
    """Edit the title and description of any YouTube video by URL or ID.
    Usage: /ytedit  — then follow the prompts.
    """
    try:
        from config import Config
        owner_ids = Config.BOT_OWNER_ID
        if owner_ids and message.from_user.id not in owner_ids:
            return await message.reply("⛔ This command is only available to the bot owner.")
    except Exception:
        pass

    ok, libs_err = _check_libs()
    if not ok:
        return await message.reply(libs_err)

    svc = get_authenticated_service()
    if not svc:
        return await message.reply(
            "❌ YouTube is not authorized.\n\n"
            "Please run /ytauth first to connect your YouTube account."
        )

    uid = message.from_user.id
    loop = asyncio.get_event_loop()

    # ── Step 1: ask for video URL or ID ─────────────────────────────────────
    ask1 = await message.reply(
        "✏️ **Edit YouTube Video**\n\n"
        "Send the **YouTube video URL or Video ID** of the video you want to edit.\n"
        "_Example: `https://youtu.be/dQw4w9WgXcQ` or just `dQw4w9WgXcQ`_"
    )

    fut1 = loop.create_future()
    _ytedit_waiter[uid] = fut1
    try:
        resp1 = await asyncio.wait_for(fut1, timeout=120)
    except asyncio.TimeoutError:
        _ytedit_waiter.pop(uid, None)
        return await ask1.reply("⏰ Timed out. Please run /ytedit again.")
    finally:
        _ytedit_waiter.pop(uid, None)

    raw = (resp1.text or "").strip()
    # Extract video ID from various URL formats
    vid_id = None
    import re
    patterns = [
        r'youtu\.be/([A-Za-z0-9_-]{11})',
        r'youtube\.com/watch\?.*v=([A-Za-z0-9_-]{11})',
        r'youtube\.com/shorts/([A-Za-z0-9_-]{11})',
        r'^([A-Za-z0-9_-]{11})$',
    ]
    for pat in patterns:
        m = re.search(pat, raw)
        if m:
            vid_id = m.group(1)
            break
    if not vid_id:
        return await resp1.reply("❌ Could not extract a valid YouTube video ID. Please try again with /ytedit.")

    # ── Step 2: ask for the new title ───────────────────────────────────────
    ask2 = await resp1.reply(
        f"✅ Video ID detected: `{vid_id}`\n\n"
        "Now send the **new title** for this video (max 100 characters).\n"
        "_Send /skip to keep the existing title._"
    )
    fut2 = loop.create_future()
    _ytedit_waiter[uid] = fut2
    try:
        resp2 = await asyncio.wait_for(fut2, timeout=180)
    except asyncio.TimeoutError:
        _ytedit_waiter.pop(uid, None)
        return await ask2.reply("⏰ Timed out.")
    finally:
        _ytedit_waiter.pop(uid, None)

    new_title = (resp2.text or "").strip()
    skip_title = new_title.lower() == "/skip"

    # ── Step 3: ask for custom description or use auto ──────────────────────
    ask3 = await resp2.reply(
        "📝 Send the **new description** for this video.\n"
        "_Send /auto to use the standard bot description (with timestamps)._\n"
        "_Send /skip to keep the existing description._"
    )
    fut3 = loop.create_future()
    _ytedit_waiter[uid] = fut3
    try:
        resp3 = await asyncio.wait_for(fut3, timeout=300)
    except asyncio.TimeoutError:
        _ytedit_waiter.pop(uid, None)
        return await ask3.reply("⏰ Timed out.")
    finally:
        _ytedit_waiter.pop(uid, None)

    new_desc_raw = (resp3.text or "").strip()
    skip_desc = new_desc_raw.lower() == "/skip"
    use_auto = new_desc_raw.lower() == "/auto"

    # ── Fetch current snippet if skipping anything ──────────────────────────
    proc_msg = await resp3.reply("⏳ Fetching current video details from YouTube…")
    try:
        curr_resp = await loop.run_in_executor(
            None,
            lambda: svc.videos().list(part="snippet", id=vid_id).execute()
        )
        items = curr_resp.get("items", [])
        if not items:
            return await proc_msg.edit_text(f"❌ No video found with ID `{vid_id}`. Make sure the video belongs to the authenticated channel.")
        curr_snippet = items[0]["snippet"]
    except Exception as e:
        return await proc_msg.edit_text(f"❌ Could not fetch video: `{e}`")

    title_to_use = curr_snippet.get("title", "") if skip_title else new_title[:100]

    if skip_desc:
        desc_to_use = curr_snippet.get("description", "")
    elif use_auto:
        desc_to_use = (
            f"हे अजनबियों, मैं आर्य बॉट [आपका दोस्त] हूँ। मैंने सफलतापूर्वक '{title_to_use}' को 'The Last Broadcast' पर मर्ज और अपलोड कर दिया है।\n\n"
            "चूंकि यह एक स्वचालित प्रक्रिया है, इसलिए आपको कुछ समस्याएं मिल सकती हैं। यदि आपको कोई समस्या आती है, तो आप टिप्पणियों में रिपोर्ट कर सकते हैं।\n\n"
            f"यदि मेरे कार्य से आपको सहायता मिली है, तो आप इस लिंक पर जाकर मुझे समर्थन दे सकते हैं: https://razorpay.me/@SusJeetX\n\n"
            "चेतावनी: किसी भी समय कॉपीराइट की समस्या आ सकती है। मेरे टेलीग्राम चैनल से जुड़ें: https://t.me/StoriesByJeetXNew\n\n"
            "───────────────────────────\n\n"
            f"Hey Strangers, I'm Arya Bot [Your Friend]. I successfully merged and uploaded '{title_to_use}' on The Last Broadcast.\n\n"
            "You may notice some issues such as episode order mismatches or missing episodes. For better navigation, timestamps are provided below.\n\n"
            "If my work has helped you, support me at: https://razorpay.me/@SusJeetX\n\n"
            "Warning: Copyright issues may occur. Join my channel: https://t.me/StoriesByJeetXNew"
        )
    else:
        desc_to_use = new_desc_raw

    # ── Apply the update ─────────────────────────────────────────────────────
    await proc_msg.edit_text("⏳ Updating video on YouTube…")
    try:
        update_body = {
            'id': vid_id,
            'snippet': {
                'title': title_to_use,
                'description': desc_to_use,
                'categoryId': curr_snippet.get('categoryId', '22'),
                'tags': curr_snippet.get('tags', []),
                'defaultLanguage': curr_snippet.get('defaultLanguage', 'en'),
            }
        }
        upd_resp = await loop.run_in_executor(
            None,
            lambda: svc.videos().update(part="snippet", body=update_body).execute()
        )
        updated_id = upd_resp.get('id', vid_id)
        await proc_msg.edit_text(
            f"✅ **Video Updated Successfully!**\n\n"
            f"🎬 Title: `{title_to_use}`\n"
            f"🔗 https://youtu.be/{updated_id}"
        )
    except Exception as e:
        await proc_msg.edit_text(f"❌ Update failed: `{e}`")


@Client.on_message(filters.private, group=-15)
async def _ytedit_input_router(bot, message):
    """Route replies from /ytedit flow."""
    uid = message.from_user.id if message.from_user else None
    if uid and uid in _ytedit_waiter:
        fut = _ytedit_waiter.pop(uid)
        if not fut.done():
            fut.set_result(message)
    raise ContinuePropagation
