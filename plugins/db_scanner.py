"""
Database Channel Index Scanner
================================
Scans and indexes every file in a database channel.
Provides:
  • Full scan with flood-safe chunked iteration
  • Incremental update (new messages only)
  • Downloadable TXT report with all file metadata
  • Auto-index update when new messages arrive in DB channel
  • Integrated with the Share Link flow (used for gap-fill inference)
"""
import os
import re
import time
import asyncio
import logging
import tempfile
import datetime
from pyrogram import Client, filters
from pyrogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from database import db
from plugins.test import CLIENT
from plugins.jobs import _ask

logger = logging.getLogger(__name__)
_CLIENT = CLIENT()

# Active scan sessions: {user_id: True}
_active_scans: dict = {}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_file_info(msg) -> dict | None:
    """Extract all file metadata from a message into a flat dict."""
    entry = {
        'msg_id':   msg.id,
        'date':     int(msg.date.timestamp()) if msg.date else 0,
        'file_name': None,
        'file_name_base': None,
        'title':    None,
        'performer': None,
        'caption':  msg.caption or msg.text or None,
        'size':     None,
        'mime':     None,
        'media_type': None,
    }

    media = None
    for attr in ('audio', 'voice', 'document', 'video'):
        m = getattr(msg, attr, None)
        if m:
            media = m
            entry['media_type'] = attr
            break

    if media is None:
        # no file — skip
        return None

    fname = getattr(media, 'file_name', None)
    if fname:
        entry['file_name'] = str(fname)
        base, _ = os.path.splitext(str(fname))
        entry['file_name_base'] = base

    entry['title']     = getattr(media, 'title',     None)
    entry['performer'] = getattr(media, 'performer', None)
    entry['size']      = getattr(media, 'file_size', None)
    entry['mime']      = getattr(media, 'mime_type', None)

    return entry


async def _scan_channel(bot, chat_id: int, start_id: int, end_id: int,
                        progress_msg=None, chunk: int = 200):
    """
    Scan messages from start_id to end_id in chat_id.
    Returns list of file-entry dicts.
    """
    entries = []
    current = start_id
    total_range = max(1, end_id - start_id + 1)
    processed = 0
    last_update = time.time()

    while current <= end_id:
        chunk_end = min(current + chunk - 1, end_id)
        ids = list(range(current, chunk_end + 1))

        try:
            msgs = await bot.get_messages(chat_id, ids)
        except Exception as e:
            logger.warning(f"[scanner] get_messages error: {e}")
            await asyncio.sleep(5)
            continue

        for msg in msgs:
            entry = _get_file_info(msg)
            if entry:
                entries.append(entry)

        processed += len(ids)
        current = chunk_end + 1

        # Progress update every 5 seconds
        if progress_msg and time.time() - last_update > 5:
            pct = min(100, int(processed / total_range * 100))
            try:
                await progress_msg.edit_text(
                    f"<b>📡 Scanning...</b>\n\n"
                    f"<code>[{'█' * (pct//5):<20}] {pct}%</code>\n"
                    f"Messages: <b>{processed:,}/{total_range:,}</b>\n"
                    f"Files found: <b>{len(entries):,}</b>"
                )
            except Exception:
                pass
            last_update = time.time()

        # Flood control
        await asyncio.sleep(0.1)

    return entries


def _build_report(entries: list, chat_title: str, chat_id: int,
                  start_id: int, end_id: int) -> str:
    """Build a structured text report from the index entries."""
    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
    lines = []
    lines.append("=" * 60)
    lines.append("  ARYA BOT — Database Channel Index Report")
    lines.append("=" * 60)
    lines.append(f"  Channel : {chat_title}  (ID: {chat_id})")
    lines.append(f"  Range   : {start_id} – {end_id}")
    lines.append(f"  Files   : {len(entries)}")
    lines.append(f"  Generated: {now.strftime('%Y-%m-%d %H:%M:%S IST')}")
    lines.append("=" * 60)
    lines.append("")

    # Sort by msg_id (chronological)
    sorted_entries = sorted(entries, key=lambda e: e['msg_id'])

    lines.append(f"{'#':>5}  {'MsgID':>8}  {'Type':>8}  {'FileName / Title'}")
    lines.append("-" * 80)

    for i, e in enumerate(sorted_entries, 1):
        fname = e.get('file_name') or e.get('title') or e.get('caption') or '(no name)'
        mtype = e.get('media_type', '?')
        size  = e.get('size')
        size_s = f"{size/1024/1024:.1f}MB" if size else ""
        lines.append(
            f"{i:>5}  {e['msg_id']:>8}  {mtype:>8}  {fname}"
            + (f"  [{size_s}]" if size_s else "")
        )
        if e.get('title') and e.get('file_name'):
            lines.append(f"{'':>5}  {'':>8}  {'':>8}  ↳ Title: {e['title']}")
        if e.get('caption'):
            cap = e['caption'][:80].replace('\n', ' ')
            lines.append(f"{'':>5}  {'':>8}  {'':>8}  ↳ Caption: {cap}")

    lines.append("")
    lines.append("=" * 60)
    lines.append("  End of Report  •  Arya Bot")
    lines.append("=" * 60)
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Main scan flow (called from share_jobs or directly)
# ─────────────────────────────────────────────────────────────────────────────

async def run_channel_scan(bot, user_id: int, chat_id: int,
                           start_id: int, end_id: int, chat_title: str = ""):
    """
    Full scan: scans chat_id[start_id..end_id], saves to DB, sends report file.
    Returns the entries list.
    """
    if user_id in _active_scans:
        await bot.send_message(user_id, "<b>⚠️ A scan is already in progress. Please wait.</b>")
        return []

    _active_scans[user_id] = True
    progress = await bot.send_message(
        user_id,
        "<b>📡 Starting channel scan...</b>\n\nThis may take a while for large channels. "
        "I'll update progress every few seconds."
    )

    try:
        entries = await _scan_channel(
            bot, chat_id, start_id, end_id, progress_msg=progress
        )

        # Save to DB
        await db.save_channel_index(
            chat_id, entries,
            meta={'start': start_id, 'end': end_id, 'title': chat_title}
        )

        # Build report file
        report_text = _build_report(entries, chat_title, chat_id, start_id, end_id)
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.txt', prefix='arya_index_',
            delete=False, encoding='utf-8'
        ) as f:
            f.write(report_text)
            tmp_path = f.name

        # Edit progress → done
        try:
            await progress.edit_text(
                f"<b>✅ Scan Complete!</b>\n\n"
                f"📊 <b>Files indexed:</b> {len(entries):,}\n"
                f"🗂 <b>Channel:</b> {chat_title or chat_id}\n"
                f"📋 Downloading full report…"
            )
        except Exception:
            pass

        # Send the report file
        await bot.send_document(
            user_id,
            document=tmp_path,
            file_name=f"arya_index_{chat_title or chat_id}.txt",
            caption=(
                f"<b>📋 Channel Index Report</b>\n"
                f"<i>{chat_title}</i>  •  <b>{len(entries):,} files</b>\n\n"
                f"<i>This index is saved. Future link generations will use it "
                f"to fix gaps and ambiguous filenames automatically.</i>\n\n"
                f"<i>Note: If some files are still completely unparseable, you can forward this report using /deepscanbatch to auto-correct them!</i>"
            )
        )

        try:
            os.unlink(tmp_path)
        except Exception:
            pass

        return entries

    except Exception as e:
        logger.error(f"[scanner] run_channel_scan failed: {e}", exc_info=True)
        try:
            await progress.edit_text(f"<b>❌ Scan failed:</b> <code>{e}</code>")
        except Exception:
            pass
        return []

    finally:
        _active_scans.pop(user_id, None)


# ─────────────────────────────────────────────────────────────────────────────
# Standalone flow (called via button/command from share_jobs)
# ─────────────────────────────────────────────────────────────────────────────

async def _scan_flow(bot, user_id: int):
    """Interactive flow to select channel and scan it."""
    try:
        from plugins.utils import ask_channel_picker
        picked = await ask_channel_picker(
            bot, user_id,
            "<b>❪ DATABASE SCANNER ❫</b>\n\nSelect the <b>database channel</b> to scan and index:"
        )

        if not picked:
            return

        title = picked["title"]
        chat_id = picked["chat_id"]
        markup2 = ReplyKeyboardMarkup([["⛔ Cᴀɴᴄᴇʟ"]], resize_keyboard=True, one_time_keyboard=True)

        # Check if existing index
        existing = await db.get_channel_index(chat_id)
        if existing:
            n = existing.get('count', len(existing.get('entries', [])))
            ts = existing.get('scanned_at', 0)
            dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
            opt_kb = [
                ["🔄 Full Re-Scan (rebuild index)"],
                ["⟳ Update (scan new messages only)"],
                ["📋 Download Existing Report"],
                ["⛔ Cancel"],
            ]
            opt_msg = await _ask(
                bot, user_id,
                f"<b>📊 Existing index found</b>\n\n"
                f"Files indexed: <b>{n:,}</b>\n"
                f"Last scanned: <b>{dt.strftime('%d %b %Y %H:%M IST')}</b>\n\n"
                "What would you like to do?",
                reply_markup=ReplyKeyboardMarkup(opt_kb, resize_keyboard=True, one_time_keyboard=True)
            )
            choice = (opt_msg.text or "").strip()

            if "Cancel" in choice:
                return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())

            if "Download" in choice:
                # Re-generate and send existing report
                entries = existing.get('entries', [])
                report_text = _build_report(entries, title, chat_id,
                                            existing.get('meta', {}).get('start', 0),
                                            existing.get('meta', {}).get('end', 0))
                with tempfile.NamedTemporaryFile(mode='w', suffix='.txt',
                                                 prefix='arya_index_', delete=False,
                                                 encoding='utf-8') as f:
                    f.write(report_text)
                    tmp = f.name
                await bot.send_document(
                    user_id, tmp,
                    file_name=f"arya_index_{title}.txt",
                    caption=f"<b>📋 Channel Index</b>  •  {n:,} files\n<i>{title}</i>\n\n<i>Note: You can forward this report using /deepscanbatch to auto-correct unparseable files!</i>"
                )
                try:
                    os.unlink(tmp)
                except Exception:
                    pass
                return await bot.send_message(user_id, "✅ Done.", reply_markup=ReplyKeyboardRemove())

            if "Update" in choice:
                # Incremental — scan from last known msg_id + 1
                entries = existing.get('entries', [])
                last_id = max((e['msg_id'] for e in entries), default=0)
                msg_end = await _ask(
                    bot, user_id,
                    f"<b>⟳ Incremental Update</b>\n\n"
                    f"Last indexed message ID: <code>{last_id}</code>\n"
                    "Send the <b>latest message ID or link</b> to scan up to:",
                    reply_markup=markup2
                )
                if getattr(msg_end, 'text', None) and any(x in msg_end.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']):
                    return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
                try:
                    end_id = _parse_msg_id(msg_end)
                except ValueError as ve:
                    return await bot.send_message(user_id, f"<b>❌ {ve}</b>", reply_markup=ReplyKeyboardRemove())

                await bot.send_message(user_id, "✅ Starting incremental scan…", reply_markup=ReplyKeyboardRemove())
                new_entries = await _scan_channel(bot, chat_id, last_id + 1, end_id)

                # Merge
                existing_ids = {e['msg_id'] for e in entries}
                merged = entries + [e for e in new_entries if e['msg_id'] not in existing_ids]
                await db.save_channel_index(chat_id, merged, meta={
                    'start': existing.get('meta', {}).get('start', 0),
                    'end': end_id, 'title': title
                })

                report_text = _build_report(merged, title, chat_id,
                                             existing.get('meta', {}).get('start', 0), end_id)
                with tempfile.NamedTemporaryFile(mode='w', suffix='.txt',
                                                 prefix='arya_index_', delete=False,
                                                 encoding='utf-8') as f:
                    f.write(report_text)
                    tmp = f.name
                await bot.send_document(
                    user_id, tmp,
                    file_name=f"arya_index_{title}_updated.txt",
                    caption=f"<b>✅ Index Updated</b>\n{len(merged):,} files total  •  {len(new_entries)} new\n\n<i>Note: You can forward this report using /deepscanbatch to auto-correct unparseable files!</i>"
                )
                try:
                    os.unlink(tmp)
                except Exception:
                    pass
                return

        # Full scan — get range
        msg_start = await _ask(
            bot, user_id,
            "<b>❪ STEP 2: START ❫</b>\n\nForward the <b>first message</b> or send its ID/link:",
            reply_markup=markup2
        )
        if getattr(msg_start, 'text', None) and any(x in msg_start.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']):
            return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        try:
            start_id = _parse_msg_id(msg_start)
        except ValueError as ve:
            return await bot.send_message(user_id, f"<b>❌ {ve}</b>", reply_markup=ReplyKeyboardRemove())

        msg_end = await _ask(
            bot, user_id,
            "<b>❪ STEP 3: END ❫</b>\n\nForward the <b>last message</b> or send its ID/link:",
            reply_markup=markup2
        )
        if getattr(msg_end, 'text', None) and any(x in msg_end.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']):
            return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        try:
            end_id = _parse_msg_id(msg_end)
        except ValueError as ve:
            return await bot.send_message(user_id, f"<b>❌ {ve}</b>", reply_markup=ReplyKeyboardRemove())

        await bot.send_message(
            user_id,
            f"<b>🔍 Scanning {title}</b>\n"
            f"Range: <code>{start_id}</code> → <code>{end_id}</code>\n"
            f"Total messages: <b>~{end_id - start_id + 1:,}</b>",
            reply_markup=ReplyKeyboardRemove()
        )

        await run_channel_scan(bot, user_id, chat_id, start_id, end_id, title)

    except Exception as e:
        logger.error(f"[scanner] _scan_flow error: {e}", exc_info=True)
        await bot.send_message(user_id, f"<b>❌ Error:</b> <code>{e}</code>",
                               reply_markup=ReplyKeyboardRemove())


def _parse_msg_id(msg) -> int:
    if getattr(msg, 'forward_from_message_id', None):
        return msg.forward_from_message_id
    text = (msg.text or msg.caption or "").strip().rstrip('/')
    if text.isdigit():
        return int(text)
    if "t.me/" in text:
        parts = text.split('/')
        if parts[-1].isdigit():
            return int(parts[-1])
    raise ValueError("Invalid Message ID or Link (forward the message or send its ID)")


# ─────────────────────────────────────────────────────────────────────────────
# Auto-index: listen for new files arriving in any DB channel
# ─────────────────────────────────────────────────────────────────────────────

_indexed_channels: set = set()  # caches verified DB channels
_ignored_channels: set = set()  # negative cache for non-DB channels
_index_buffer: dict = {}      # chat_id -> {msg_id: entry}
_index_lock = asyncio.Lock()
_indexer_started = False
_verify_locks: dict = {}

def _get_verify_lock(chat_id):
    if chat_id not in _verify_locks:
        _verify_locks[chat_id] = asyncio.Lock()
    return _verify_locks[chat_id]

async def _batch_indexer_task():
    """Background task to batch-commit auto-indexed files to DB to prevent bot hanging."""
    while True:
        await asyncio.sleep(5)
        async with _index_lock:
            if not _index_buffer:
                continue
            to_process = _index_buffer.copy()
            _index_buffer.clear()
            
        for chat_id, entries_dict in to_process.items():
            try:
                # 1. Fetch full index once
                existing = await db.get_channel_index(chat_id)
                if not existing:
                    continue

                current_entries = existing.get('entries', [])
                
                # 2. Merge existing with new (overwriting dupes)
                current_dict = {e['msg_id']: e for e in current_entries}
                current_dict.update(entries_dict)
                
                # 3. Sort chronologically
                new_entries = sorted(list(current_dict.values()), key=lambda x: x['msg_id'])
                
                # 4. Save bulk update
                await db.save_channel_index(
                    chat_id, 
                    new_entries, 
                    meta=existing.get('meta', {})
                )
                logger.info(f"[scanner] Batch updated {len(entries_dict)} msgs for DB chat {chat_id}")
            except Exception as e:
                logger.error(f"[scanner] Batch auto-index failed for {chat_id}: {e}")

async def _try_auto_index(client, message):
    """Called for every new message. Batched in memory to prevent event-loop choking."""
    global _indexer_started
    if not _indexer_started:
        _indexer_started = True
        asyncio.create_task(_batch_indexer_task())

    chat_id = message.chat.id
    
    # 1. Verification cache bypasses DB spam
    if chat_id in _ignored_channels:
        return
        
    if chat_id not in _indexed_channels:
        async with _get_verify_lock(chat_id):
            if chat_id in _ignored_channels:
                return
            if chat_id not in _indexed_channels:
                # Check DB once
                existing = await db.get_channel_index(chat_id)
                if not existing:
                    _ignored_channels.add(chat_id)
                    # Clear negative cache after 60s in case user adds channel
                    asyncio.get_event_loop().call_later(60, lambda: _ignored_channels.discard(chat_id))
                    return  # not a tracked database
                _indexed_channels.add(chat_id)

    # 2. File Extractor
    entry = _get_file_info(message)
    if not entry:
        return  
        
    # 3. Buffer immediately
    async with _index_lock:
        if chat_id not in _index_buffer:
            _index_buffer[chat_id] = {}
        _index_buffer[chat_id][entry['msg_id']] = entry


# ─────────────────────────────────────────────────────────────────────────────
# Expose scan for use in share_jobs (gap fill using pre-built index)
# ─────────────────────────────────────────────────────────────────────────────

async def get_index_entries(chat_id: int) -> list:
    """Return the stored index entries for a channel, or []."""
    doc = await db.get_channel_index(chat_id)
    if not doc:
        return []
    return doc.get('entries', [])