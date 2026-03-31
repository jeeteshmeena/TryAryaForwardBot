"""
Merger Plugin
=============
Merges media files (MP3/MP4) from a source channel range into one combined file.
Uses FFmpeg's concat demuxer for lossless merging (no re-encoding when codecs match).

Commands:
  /merge  — Open the Merger manager UI

Flow:
  /merge → Select Account → Select Source Channel → Send Start Link
         → Send End Link → Set Output Filename → Job starts
         → Downloads all files → Merges via FFmpeg → Uploads result
"""
import os
import re
import time
import uuid
import asyncio
import logging
import shutil
import subprocess
from database import db
from .test import CLIENT, start_clone_bot
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, ReplyKeyboardRemove
)

logger = logging.getLogger(__name__)
_CLIENT = CLIENT()

# ─── In-memory task registry ─────────────────────────────────────────────────
_merge_tasks: dict[str, asyncio.Task] = {}

# ─── Future-based ask() ──────────────────────────────────────────────────────
_merge_waiting: dict[int, asyncio.Future] = {}


@Client.on_message(filters.private, group=-14)
async def _merge_input_router(bot, message):
    uid = message.from_user.id if message.from_user else None
    if uid and uid in _merge_waiting:
        fut = _merge_waiting.pop(uid)
        if not fut.done():
            fut.set_result(message)


async def _merge_ask(bot, user_id: int, text: str, reply_markup=None, timeout: int = 300):
    loop = asyncio.get_event_loop()
    fut: asyncio.Future = loop.create_future()
    old = _merge_waiting.pop(user_id, None)
    if old and not old.done():
        old.cancel()
    _merge_waiting[user_id] = fut
    await bot.send_message(user_id, text, reply_markup=reply_markup)
    try:
        return await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        _merge_waiting.pop(user_id, None)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# DB helpers
# ══════════════════════════════════════════════════════════════════════════════

COLL = "mergejobs"


async def _mg_save(job: dict):
    await db.db[COLL].replace_one({"job_id": job["job_id"]}, job, upsert=True)


async def _mg_get(job_id: str) -> dict | None:
    return await db.db[COLL].find_one({"job_id": job_id})


async def _mg_list(user_id: int) -> list[dict]:
    return [j async for j in db.db[COLL].find({"user_id": user_id})]


async def _mg_delete(job_id: str):
    await db.db[COLL].delete_one({"job_id": job_id})


async def _mg_update(job_id: str, **kwargs):
    await db.db[COLL].update_one({"job_id": job_id}, {"$set": kwargs})


# ══════════════════════════════════════════════════════════════════════════════
# FFmpeg helpers
# ══════════════════════════════════════════════════════════════════════════════

def _check_ffmpeg() -> bool:
    """Return True if ffmpeg is available on the system."""
    return shutil.which("ffmpeg") is not None


def _get_media_type(file_path: str) -> str:
    """Detect if a file is audio or video using ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-show_entries",
             "stream=codec_type", "-of", "csv=p=0", file_path],
            capture_output=True, text=True, timeout=30
        )
        output = result.stdout.strip()
        if "video" in output:
            return "video"
        elif "audio" in output:
            return "audio"
    except Exception:
        pass
    # Fallback: use extension
    ext = os.path.splitext(file_path)[1].lower()
    if ext in (".mp4", ".mkv", ".avi", ".webm", ".mov"):
        return "video"
    return "audio"


def _merge_files_ffmpeg(file_list: list[str], output_path: str,
                        metadata: dict = None) -> tuple[bool, str]:
    """
    Merge files using FFmpeg concat demuxer (lossless when codecs match).
    Returns (success: bool, error_message: str).
    """
    # Create concat list file
    list_path = output_path + ".list.txt"
    try:
        with open(list_path, "w", encoding="utf-8") as f:
            for fp in file_list:
                # FFmpeg wants single-quoted paths with escaping
                safe = fp.replace("'", "'\\''")
                f.write(f"file '{safe}'\n")

        cmd = [
            "ffmpeg", "-y",
            "-f", "concat",
            "-safe", "0",
            "-i", list_path,
            "-c", "copy",  # No re-encoding → lossless
        ]

        # Add metadata if provided
        if metadata:
            for key, val in metadata.items():
                cmd.extend(["-metadata", f"{key}={val}"])

        cmd.append(output_path)

        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=3600  # 1 hour max
        )

        if result.returncode != 0:
            # If concat copy fails (codec mismatch), try re-encode
            logger.warning(f"Concat copy failed, trying re-encode: {result.stderr[-200:]}")
            cmd_re = [
                "ffmpeg", "-y",
                "-f", "concat",
                "-safe", "0",
                "-i", list_path,
            ]
            # Detect type for appropriate re-encode settings
            media_type = _get_media_type(file_list[0])
            if media_type == "video":
                cmd_re.extend(["-c:v", "libx264", "-preset", "fast",
                               "-crf", "18", "-c:a", "aac", "-b:a", "192k"])
            else:
                cmd_re.extend(["-c:a", "libmp3lame", "-b:a", "320k"])

            if metadata:
                for key, val in metadata.items():
                    cmd_re.extend(["-metadata", f"{key}={val}"])
            cmd_re.append(output_path)

            result2 = subprocess.run(
                cmd_re, capture_output=True, text=True, timeout=7200
            )
            if result2.returncode != 0:
                return False, result2.stderr[-500:]

        return True, ""
    except subprocess.TimeoutExpired:
        return False, "FFmpeg timed out (exceeded max duration)"
    except Exception as e:
        return False, str(e)
    finally:
        if os.path.exists(list_path):
            os.remove(list_path)


# ══════════════════════════════════════════════════════════════════════════════
# Core merge runner
# ══════════════════════════════════════════════════════════════════════════════

BATCH_SIZE = 200


async def _run_merge_job(job_id: str, user_id: int, bot):
    """
    Main coroutine for a Merge Job.
    1. Downloads all media files in range from source channel
    2. Merges them with FFmpeg
    3. Uploads the result to the user
    """
    job = await _mg_get(job_id)
    if not job:
        return

    client = None
    work_dir = f"merge_tmp/{job_id}"
    os.makedirs(work_dir, exist_ok=True)

    try:
        acc = await db.get_bot(user_id, job["account_id"])
        if not acc:
            await _mg_update(job_id, status="error", error="Account not found")
            return

        client = await start_clone_bot(_CLIENT.client(acc))

        from_chat = job["from_chat"]
        start_id  = job["start_id"]
        end_id    = job["end_id"]
        out_name  = job.get("output_name", "merged")
        metadata  = job.get("metadata", {})

        await _mg_update(job_id, status="downloading", error="")

        # ── Phase 1: Download all media files ─────────────────────────────
        downloaded_files = []
        current = start_id
        total_expected = end_id - start_id + 1
        downloaded_count = 0
        skipped = 0

        start_time = time.time()

        try:
            await bot.send_message(
                user_id,
                f"<b>⬇️ Merge Job Started</b>\n\n"
                f"<b>Downloading files {start_id} → {end_id}...</b>\n"
                f"<b>Total range:</b> {total_expected} messages\n\n"
                f"<i>This may take a while for large ranges.</i>"
            )
        except Exception:
            pass

        while current <= end_id:
            # Check if stopped
            fresh = await _mg_get(job_id)
            if not fresh or fresh.get("status") == "stopped":
                return

            batch_end = min(current + BATCH_SIZE - 1, end_id)
            batch_ids = list(range(current, batch_end + 1))

            try:
                msgs = await client.get_messages(from_chat, batch_ids)
                if not isinstance(msgs, list):
                    msgs = [msgs]
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2)
                continue
            except Exception as e:
                logger.warning(f"[Merge {job_id}] Fetch error at {current}: {e}")
                current += BATCH_SIZE
                continue

            valid = [m for m in msgs if m and not m.empty and not m.service]
            valid.sort(key=lambda m: m.id)

            for msg in valid:
                if not msg.media:
                    skipped += 1
                    continue

                # Only process audio/video/document (media files)
                media_obj = None
                for attr in ('audio', 'video', 'document', 'voice'):
                    media_obj = getattr(msg, attr, None)
                    if media_obj:
                        break

                if not media_obj:
                    skipped += 1
                    continue

                # Get original filename
                original_name = getattr(media_obj, 'file_name', None)
                ext = ""
                if original_name:
                    ext = os.path.splitext(original_name)[1]
                elif getattr(msg, 'audio', None) or getattr(msg, 'voice', None):
                    ext = ".mp3"
                elif getattr(msg, 'video', None):
                    ext = ".mp4"

                # Sequential naming to preserve order
                seq_name = f"{downloaded_count:05d}{ext}"
                dl_path = os.path.join(work_dir, seq_name)

                # Download with retry
                fp = None
                for dl_attempt in range(5):
                    try:
                        fp = await client.download_media(msg, file_name=dl_path)
                        if fp:
                            break
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 2)
                    except Exception as dl_e:
                        err_str = str(dl_e).upper()
                        if "TIMEOUT" in err_str or "CONNECTION" in err_str:
                            await asyncio.sleep(5)
                            continue
                        if dl_attempt < 4:
                            await asyncio.sleep(3)
                            continue
                        logger.warning(f"[Merge {job_id}] Download failed for {msg.id}: {dl_e}")
                        break

                if fp and os.path.exists(fp):
                    downloaded_files.append(fp)
                    downloaded_count += 1
                    await _mg_update(job_id, downloaded=downloaded_count)

                    # Progress update every 10 files
                    if downloaded_count % 10 == 0:
                        elapsed = time.time() - start_time
                        avg_per_file = elapsed / downloaded_count if downloaded_count else 1
                        remaining = (total_expected - downloaded_count - skipped) * avg_per_file
                        eta_str = _format_time(remaining)
                        try:
                            await bot.send_message(
                                user_id,
                                f"<b>⬇️ Downloading...</b> {downloaded_count}/{total_expected}\n"
                                f"<b>⏱ ETA:</b> ~{eta_str}"
                            )
                        except Exception:
                            pass
                else:
                    skipped += 1

            current = batch_end + 1

        if not downloaded_files:
            await _mg_update(job_id, status="error", error="No media files found in range")
            try:
                await bot.send_message(user_id, "<b>❌ Merge failed: No media files found in the specified range.</b>")
            except Exception:
                pass
            return

        # ── Phase 2: Merge with FFmpeg ────────────────────────────────────
        await _mg_update(job_id, status="merging", downloaded=downloaded_count)

        # Detect output format from first file
        first_type = _get_media_type(downloaded_files[0])
        out_ext = ".mp4" if first_type == "video" else ".mp3"
        output_path = os.path.join(work_dir, f"{out_name}{out_ext}")

        try:
            await bot.send_message(
                user_id,
                f"<b>🔀 Merging {downloaded_count} files...</b>\n"
                f"<b>Output:</b> <code>{out_name}{out_ext}</code>\n\n"
                f"<i>Using lossless concat (no quality loss).\n"
                f"This may take several minutes for large files.</i>"
            )
        except Exception:
            pass

        # Run FFmpeg in a thread to avoid blocking the event loop
        loop = asyncio.get_event_loop()
        success, error_msg = await loop.run_in_executor(
            None, _merge_files_ffmpeg, downloaded_files, output_path, metadata
        )

        if not success:
            await _mg_update(job_id, status="error", error=error_msg[:500])
            try:
                await bot.send_message(
                    user_id,
                    f"<b>❌ Merge failed!</b>\n\n<code>{error_msg[:500]}</code>"
                )
            except Exception:
                pass
            return

        # Check file size (Telegram limit)
        file_size = os.path.getsize(output_path)
        file_size_mb = file_size / (1024 * 1024)

        if file_size > 2 * 1024 * 1024 * 1024:  # 2GB
            await _mg_update(job_id, status="error",
                             error=f"Merged file too large: {file_size_mb:.0f}MB (Telegram limit: 2GB)")
            try:
                await bot.send_message(
                    user_id,
                    f"<b>❌ Merged file is {file_size_mb:.0f}MB — exceeds Telegram's 2GB upload limit.</b>\n"
                    f"<i>Try merging fewer files.</i>"
                )
            except Exception:
                pass
            return

        # ── Phase 3: Upload merged file ───────────────────────────────────
        await _mg_update(job_id, status="uploading")

        try:
            await bot.send_message(
                user_id,
                f"<b>⬆️ Uploading merged file...</b>\n"
                f"<b>Size:</b> {file_size_mb:.1f} MB"
            )
        except Exception:
            pass

        for up_attempt in range(3):
            try:
                if first_type == "video":
                    await client.send_video(
                        chat_id=user_id,
                        video=output_path,
                        caption=f"<b>✅ Merged: {out_name}{out_ext}</b>\n"
                                f"<b>Files:</b> {downloaded_count}\n"
                                f"<b>Size:</b> {file_size_mb:.1f} MB",
                        file_name=f"{out_name}{out_ext}",
                        supports_streaming=True
                    )
                else:
                    await client.send_audio(
                        chat_id=user_id,
                        audio=output_path,
                        caption=f"<b>✅ Merged: {out_name}{out_ext}</b>\n"
                                f"<b>Files:</b> {downloaded_count}\n"
                                f"<b>Size:</b> {file_size_mb:.1f} MB",
                        file_name=f"{out_name}{out_ext}",
                        title=metadata.get("title", out_name),
                        performer=metadata.get("artist", "")
                    )
                break
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2)
            except Exception as up_e:
                if up_attempt < 2:
                    await asyncio.sleep(5)
                    continue
                raise up_e

        # ── Done ──────────────────────────────────────────────────────────
        elapsed_total = time.time() - start_time
        await _mg_update(job_id, status="done")

        try:
            await bot.send_message(
                user_id,
                f"<b>✅ Merge Job Complete!</b>\n\n"
                f"<b>📊 Summary:</b>\n"
                f" ┣ <b>Downloaded:</b> {downloaded_count} files\n"
                f" ┣ <b>Skipped:</b> {skipped} (non-media)\n"
                f" ┣ <b>Output:</b> <code>{out_name}{out_ext}</code>\n"
                f" ┣ <b>Size:</b> {file_size_mb:.1f} MB\n"
                f" ┗ <b>Time:</b> {_format_time(elapsed_total)}"
            )
        except Exception:
            pass

    except asyncio.CancelledError:
        logger.info(f"[Merge {job_id}] Cancelled")
        await _mg_update(job_id, status="stopped")
    except Exception as e:
        logger.error(f"[Merge {job_id}] Fatal: {e}")
        await _mg_update(job_id, status="error", error=str(e)[:500])
        try:
            await bot.send_message(user_id, f"<b>❌ Merge error:</b>\n<code>{str(e)[:500]}</code>")
        except Exception:
            pass
    finally:
        _merge_tasks.pop(job_id, None)
        # Cleanup temp files
        try:
            if os.path.exists(work_dir):
                shutil.rmtree(work_dir, ignore_errors=True)
        except Exception:
            pass
        if client:
            try:
                await client.stop()
            except Exception:
                pass


def _format_time(seconds: float) -> str:
    """Format seconds into human-readable duration."""
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    else:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h}h {m}m"


# ══════════════════════════════════════════════════════════════════════════════
# Extract message ID from link
# ══════════════════════════════════════════════════════════════════════════════

def _parse_msg_link(text: str) -> tuple[int | None, int | None]:
    """
    Parse a Telegram message link and return (chat_id, message_id).
    Supports: https://t.me/channel/123, https://t.me/c/123456/789
    Also accepts raw message IDs.
    """
    text = text.strip()

    # Raw number
    if text.isdigit():
        return None, int(text)

    # https://t.me/c/<chat_id>/<msg_id> (private channel)
    m = re.match(r'https?://t\.me/c/(\d+)/(\d+)', text)
    if m:
        chat_id = -1000000000000 - int(m.group(1))  # Convert to full chat ID
        return int(m.group(1)), int(m.group(2))

    # https://t.me/<username>/<msg_id> (public channel)
    m = re.match(r'https?://t\.me/([^/]+)/(\d+)', text)
    if m:
        return m.group(1), int(m.group(2))

    return None, None


# ══════════════════════════════════════════════════════════════════════════════
# Command handler
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.command("merge") & filters.private)
async def merge_cmd(bot, message):
    user_id = message.from_user.id

    # Check FFmpeg
    if not _check_ffmpeg():
        return await message.reply(
            "<b>❌ FFmpeg is not installed on this server.</b>\n\n"
            "<i>Ask the admin to install it:\n"
            "<code>sudo apt install ffmpeg</code></i>"
        )

    jobs = await _mg_list(user_id)
    active = [j for j in jobs if j.get("status") in ("downloading", "merging", "uploading")]

    buttons = []

    if active:
        buttons.append([InlineKeyboardButton("━━━ Active Merges ━━━", callback_data="merge#noop")])
        for j in active:
            st = {"downloading": "⬇️", "merging": "🔀", "uploading": "⬆️"}.get(j["status"], "❓")
            label = f"{st} {j.get('output_name', j['job_id'][-6:])}"
            buttons.append([InlineKeyboardButton(label, callback_data=f"merge#view_{j['job_id']}")])

    recent = [j for j in jobs if j.get("status") in ("done", "error", "stopped")][:5]
    if recent:
        buttons.append([InlineKeyboardButton("━━━ Recent ━━━", callback_data="merge#noop")])
        for j in recent:
            st = {"done": "✅", "error": "⚠️", "stopped": "🔴"}.get(j["status"], "❓")
            label = f"{st} {j.get('output_name', j['job_id'][-6:])}"
            buttons.append([InlineKeyboardButton(label, callback_data=f"merge#view_{j['job_id']}")])

    buttons.append([InlineKeyboardButton("➕ New Merge Job", callback_data="merge#create")])
    buttons.append([InlineKeyboardButton("⫷ Close", callback_data="merge#close")])

    await message.reply(
        "<b>🔀 Merger</b>\n\n"
        "<i>Merge MP3/MP4 files from a channel range into one file.\n"
        "No quality loss • Maintains order • Auto cleanup</i>",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


@Client.on_callback_query(filters.regex(r'^merge#'))
async def merge_callback(bot, query):
    user_id = query.from_user.id
    action = query.data.split("#", 1)[1]

    if action == "noop":
        return await query.answer()

    elif action == "close":
        return await query.message.delete()

    elif action.startswith("view_"):
        job_id = action.split("_", 1)[1]
        job = await _mg_get(job_id)
        if not job:
            return await query.answer("Job not found!", show_alert=True)

        st_emoji = {"downloading": "⬇️", "merging": "🔀", "uploading": "⬆️",
                     "done": "✅", "error": "⚠️", "stopped": "🔴"}.get(job["status"], "❓")

        text = (
            f"<b>{st_emoji} Merge Job</b>\n\n"
            f"<b>Output:</b> <code>{job.get('output_name', '?')}</code>\n"
            f"<b>Range:</b> {job.get('start_id')} → {job.get('end_id')}\n"
            f"<b>Downloaded:</b> {job.get('downloaded', 0)} files\n"
            f"<b>Status:</b> {job['status']}\n"
        )
        if job.get("error"):
            text += f"\n<b>Error:</b> <code>{job['error'][:200]}</code>"

        buttons = []
        if job["status"] in ("downloading", "merging", "uploading"):
            buttons.append([InlineKeyboardButton("🛑 Stop", callback_data=f"merge#stop_{job_id}")])
        buttons.append([InlineKeyboardButton("🗑 Delete", callback_data=f"merge#del_{job_id}")])
        buttons.append([InlineKeyboardButton("↩ Back", callback_data="merge#back")])

        await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

    elif action.startswith("stop_"):
        job_id = action.split("_", 1)[1]
        await _mg_update(job_id, status="stopped")
        task = _merge_tasks.get(job_id)
        if task:
            task.cancel()
        await query.answer("Merge job stopped!", show_alert=True)

    elif action.startswith("del_"):
        job_id = action.split("_", 1)[1]
        task = _merge_tasks.get(job_id)
        if task:
            task.cancel()
        await _mg_delete(job_id)
        # Cleanup work dir
        work_dir = f"merge_tmp/{job_id}"
        if os.path.exists(work_dir):
            shutil.rmtree(work_dir, ignore_errors=True)
        await query.answer("Job deleted!", show_alert=True)
        # Refresh list
        await merge_cmd(bot, query.message)

    elif action == "back":
        await merge_cmd(bot, query.message)

    elif action == "create":
        await query.message.delete()
        await _create_merge_flow(bot, user_id)


# ══════════════════════════════════════════════════════════════════════════════
# Creation flow
# ══════════════════════════════════════════════════════════════════════════════

async def _create_merge_flow(bot, user_id: int):
    """Interactive flow to create a new merge job."""
    try:
        # ── Step 1: Select Account ────────────────────────────────────────
        accounts = await db.get_bots(user_id)
        if not accounts:
            return await bot.send_message(
                user_id,
                "<b>❌ No accounts found. Add one in /settings → Accounts first.</b>"
            )

        kb = []
        for acc in accounts:
            icon = "🤖" if acc.get("is_bot", True) else "👤"
            kb.append([f"{icon} {acc['name']}"])
        kb.append(["❌ Cancel"])

        msg = await _merge_ask(
            bot, user_id,
            "<b>🔀 New Merge Job</b>\n\n"
            "<b>Step 1/5:</b> Select an account to use:",
            reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=True)
        )

        if msg.text == "❌ Cancel":
            return await bot.send_message(user_id, "<b>Cancelled.</b>",
                                          reply_markup=ReplyKeyboardRemove())

        sel_name = msg.text.split(" ", 1)[1] if " " in msg.text else msg.text
        sel_acc = next((a for a in accounts if a["name"] == sel_name), None)
        if not sel_acc:
            return await bot.send_message(user_id, "<b>❌ Account not found.</b>",
                                          reply_markup=ReplyKeyboardRemove())

        # ── Step 2: Source channel link ───────────────────────────────────
        msg = await _merge_ask(
            bot, user_id,
            "<b>Step 2/5:</b> Send the <b>start file link</b> from the source channel.\n\n"
            "<i>Example: https://t.me/c/123456/100\n"
            "Or just the message ID if the channel is already known.</i>",
            reply_markup=ReplyKeyboardRemove()
        )

        if msg.text and msg.text.lower() == "/cancel":
            return await bot.send_message(user_id, "<b>Cancelled.</b>")

        chat_ref_start, start_id = _parse_msg_link(msg.text or "")
        if start_id is None:
            return await bot.send_message(user_id, "<b>❌ Could not parse message link. Cancelled.</b>")

        # Resolve chat from the link
        from_chat = None
        if chat_ref_start:
            if isinstance(chat_ref_start, int):
                from_chat = -1000000000000 - chat_ref_start
            else:
                from_chat = chat_ref_start

        # ── Step 3: End file link ─────────────────────────────────────────
        msg = await _merge_ask(
            bot, user_id,
            "<b>Step 3/5:</b> Send the <b>end file link</b> (last file to include).\n\n"
            "<i>All files between start → end will be merged in order.</i>"
        )

        if msg.text and msg.text.lower() == "/cancel":
            return await bot.send_message(user_id, "<b>Cancelled.</b>")

        chat_ref_end, end_id = _parse_msg_link(msg.text or "")
        if end_id is None:
            return await bot.send_message(user_id, "<b>❌ Could not parse end link. Cancelled.</b>")

        # If from_chat not from start link, try from end link
        if from_chat is None and chat_ref_end:
            if isinstance(chat_ref_end, int):
                from_chat = -1000000000000 - chat_ref_end
            else:
                from_chat = chat_ref_end

        if from_chat is None:
            return await bot.send_message(
                user_id,
                "<b>❌ Could not determine source channel. Use full links.</b>"
            )

        # Ensure start < end
        if start_id > end_id:
            start_id, end_id = end_id, start_id

        total = end_id - start_id + 1

        # ── Step 4: Output filename ───────────────────────────────────────
        msg = await _merge_ask(
            bot, user_id,
            f"<b>Step 4/5:</b> Send the <b>output filename</b> (without extension).\n\n"
            f"<b>Range:</b> {start_id} → {end_id} ({total} messages)\n\n"
            f"<i>Example: My_Merged_Video</i>"
        )

        if msg.text and msg.text.lower() == "/cancel":
            return await bot.send_message(user_id, "<b>Cancelled.</b>")

        output_name = (msg.text or "merged").strip()
        # Sanitize filename
        output_name = re.sub(r'[<>:"/\\|?*]', '_', output_name)

        # ── Step 5: Metadata (optional) ───────────────────────────────────
        msg = await _merge_ask(
            bot, user_id,
            "<b>Step 5/5:</b> Send metadata (optional).\n\n"
            "<i>Format: <code>title | artist | album</code>\n"
            "Or send <code>skip</code> to use defaults.</i>"
        )

        metadata = {}
        if msg.text and msg.text.lower() != "skip" and msg.text.lower() != "/cancel":
            parts = [p.strip() for p in msg.text.split("|")]
            if len(parts) >= 1 and parts[0]:
                metadata["title"] = parts[0]
            if len(parts) >= 2 and parts[1]:
                metadata["artist"] = parts[1]
            if len(parts) >= 3 and parts[2]:
                metadata["album"] = parts[2]

        # ── Create job ────────────────────────────────────────────────────
        job_id = str(uuid.uuid4())
        job = {
            "job_id": job_id,
            "user_id": user_id,
            "account_id": sel_acc["id"],
            "from_chat": from_chat,
            "start_id": start_id,
            "end_id": end_id,
            "output_name": output_name,
            "metadata": metadata,
            "status": "downloading",
            "downloaded": 0,
            "error": "",
            "created_at": time.time(),
        }

        await _mg_save(job)

        # Start the task
        task = asyncio.create_task(_run_merge_job(job_id, user_id, bot))
        _merge_tasks[job_id] = task

        await bot.send_message(
            user_id,
            f"<b>✅ Merge Job Created & Started!</b>\n\n"
            f"<b>Source:</b> <code>{from_chat}</code>\n"
            f"<b>Range:</b> {start_id} → {end_id} ({total} msgs)\n"
            f"<b>Output:</b> <code>{output_name}</code>\n"
            f"<b>Job ID:</b> <code>{job_id[-6:]}</code>\n\n"
            f"<i>Use /merge to monitor progress.</i>",
            reply_markup=ReplyKeyboardRemove()
        )

    except asyncio.TimeoutError:
        await bot.send_message(user_id, "<b>⏱ Timed out. Try again with /merge.</b>",
                               reply_markup=ReplyKeyboardRemove())
    except Exception as e:
        logger.error(f"[Merge create] Error: {e}")
        await bot.send_message(user_id, f"<b>❌ Error:</b> <code>{e}</code>",
                               reply_markup=ReplyKeyboardRemove())
