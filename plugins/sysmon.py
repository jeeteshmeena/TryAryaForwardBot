"""
System Monitor & Resource Guard — AryaBot
==========================================
Monitors RAM / CPU / Disk in the background.
Automatically pauses jobs when the system is under stress.
Sends Telegram alerts and lets user resume from exact position.

Thresholds (adjustable at top of file):
  WARNING  — RAM > 75% or CPU > 80%   → warn user
  CRITICAL — RAM > 88% or CPU > 92%   → pause Multi Job + Live Job
                                          pause Merger only if > 1 running
  EMERGENCY— RAM > 95% or CPU > 97%   → pause ALL tasks including Merger

Commands (owner-only):
  /sysstat   — View current RAM / CPU / Disk / running tasks
  /cleanup   — Delete merge_tmp/* and downloads/* with confirmation
  /pauseall  — Force-pause all jobs immediately
  /resumeall — Resume all system-paused jobs

The monitor runs as a background asyncio task started from __init__.py or main.py.
"""

import os
import re
import shutil
import asyncio
import logging
import time
import psutil
from datetime import datetime

from pyrogram import Client, filters
from pyrogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    CallbackQuery, Message
)
from config import Config

logger = logging.getLogger(__name__)

# ── Thresholds ─────────────────────────────────────────────────────────────────
RAM_WARN      = 90   # %
RAM_CRITICAL  = 95   # %
RAM_EMERGENCY = 97   # %
CPU_WARN      = 95   # %
CPU_CRITICAL  = 99   # %
CPU_EMERGENCY = 100  # % (Essentially disable CPU auto-pause, CPU 100% is normal for FFmpeg)

MONITOR_INTERVAL  = 30   # seconds between each check
ALERT_COOLDOWN_WARN  = 1800  # 30 min cooldown for WARNING alerts (moderate load — don't spam)
ALERT_COOLDOWN_CRIT  = 300   # 5 min cooldown for CRITICAL/EMERGENCY (real problem — act fast)

# ── State ──────────────────────────────────────────────────────────────────────
_last_alert_ts: dict[str, float] = {}   # level → timestamp
_sys_paused_jobs: set[str] = set()      # job_ids paused by THIS monitor
_monitor_task: asyncio.Task | None = None

# ── Temp dirs the cleanup command will wipe ────────────────────────────────────
TEMP_DIRS = ["merge_tmp", "downloads"]

# ── Arya Small-Caps font helper (reuse from share_bot) ────────────────────────
def _sc(text: str) -> str:
    return text.translate(str.maketrans(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
        "ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀꜱᴛᴜᴠᴡxʏᴢᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀꜱᴛᴜᴠᴡxʏᴢ"
    ))


def _is_owner(user_id: int) -> bool:
    return user_id in Config.BOT_OWNER_ID


# ── Bar renderer ───────────────────────────────────────────────────────────────
def _bar(pct: float, width: int = 10) -> str:
    filled = int(pct / 100 * width)
    empty  = width - filled
    if pct >= 90: char = "█"
    elif pct >= 70: char = "▓"
    else: char = "▒"
    return char * filled + "░" * empty


def _level_emoji(pct: float) -> str:
    if pct >= 90: return "🔴"
    if pct >= 75: return "🟡"
    return "🟢"


# ── System snapshot ───────────────────────────────────────────────────────────
def _sys_snapshot() -> dict:
    ram    = psutil.virtual_memory()
    cpu    = psutil.cpu_percent(interval=1)
    disk   = psutil.disk_usage("/")
    proc   = psutil.Process(os.getpid())
    bot_ram_mb = proc.memory_info().rss / 1024 / 1024

    return {
        "ram_pct":     ram.percent,
        "ram_used_gb": ram.used / 1024**3,
        "ram_total_gb": ram.total / 1024**3,
        "ram_avail_gb": ram.available / 1024**3,
        "cpu_pct":     cpu,
        "disk_pct":    disk.percent,
        "disk_used_gb": disk.used / 1024**3,
        "disk_total_gb": disk.total / 1024**3,
        "disk_free_gb":  disk.free / 1024**3,
        "bot_ram_mb":   bot_ram_mb,
    }


def _temp_dir_sizes() -> dict[str, float]:
    """Return {dir_name: size_mb} for each temp directory."""
    result = {}
    for d in TEMP_DIRS:
        if os.path.exists(d):
            total = sum(
                f.stat().st_size
                for f in __import__("pathlib").Path(d).rglob("*")
                if f.is_file()
            )
            result[d] = total / 1024 / 1024
        else:
            result[d] = 0.0
    return result


# ── Running job counters ───────────────────────────────────────────────────────
def _count_running_jobs() -> dict:
    """Import lazily to avoid circular imports."""
    try:
        from plugins.multijob import _mj_tasks, _mj_paused
        mj_active = sum(1 for t in _mj_tasks.values() if not t.done())
        mj_paused = sum(1 for ev in _mj_paused.values() if not ev.is_set())
    except Exception:
        mj_active, mj_paused = 0, 0

    try:
        from plugins.jobs import _job_tasks
        lj_active = sum(1 for t in _job_tasks.values() if not t.done())
    except Exception:
        lj_active = 0

    try:
        from plugins.merger import _mg_tasks, _mg_paused
        mg_active = sum(1 for t in _mg_tasks.values() if not t.done())
        mg_paused = sum(1 for ev in _mg_paused.values() if not ev.is_set())
    except Exception:
        mg_active, mg_paused = 0, 0

    return {
        "mj_active": mj_active,
        "mj_paused": mj_paused,
        "lj_active": lj_active,
        "mg_active": mg_active,
        "mg_paused": mg_paused,
    }


# ── Pause helpers ─────────────────────────────────────────────────────────────
async def _pause_multijobs(reason: str) -> list[str]:
    """Pause all running Multi Jobs. Returns list of paused job_ids."""
    paused = []
    try:
        from plugins.multijob import _mj_tasks, _mj_paused
        from database import db
        for jid, task in list(_mj_tasks.items()):
            if task.done():
                continue
            ev = _mj_paused.get(jid)
            if ev and ev.is_set():  # currently running
                ev.clear()          # pause
                _sys_paused_jobs.add(f"mj:{jid}")
                paused.append(jid)
                try:
                    await db.db["multijobs"].update_one(
                        {"job_id": jid}, {"$set": {"status": "paused", "paused_reason": reason}}
                    )
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"[SysMonitor] pause_multijobs error: {e}")
    return paused


async def _pause_livejobs(reason: str) -> list[str]:
    """Stop (cancel) all Live Jobs — they have no pause, so we stop them
    and set status=paused so they can be restarted from last_seen_id."""
    stopped = []
    try:
        from plugins.jobs import _job_tasks
        from database import db
        for jid, task in list(_job_tasks.items()):
            if task.done():
                continue
            _sys_paused_jobs.add(f"lj:{jid}")
            stopped.append(jid)
            try:
                await db.db.jobs.update_one(
                    {"job_id": jid},
                    {"$set": {"status": "paused", "paused_reason": reason}}
                )
            except Exception:
                pass
            task.cancel()
    except Exception as e:
        logger.error(f"[SysMonitor] pause_livejobs error: {e}")
    return stopped


async def _pause_mergers(reason: str, force_all: bool = False) -> list[str]:
    """Pause merger jobs.
    If force_all=False: only pause if more than 1 merger is running.
    If force_all=True: pause ALL mergers regardless of count.
    """
    paused = []
    try:
        from plugins.merger import _mg_tasks, _mg_paused
        from database import db

        active_ids = [jid for jid, t in _mg_tasks.items() if not t.done()]
        if not force_all and len(active_ids) <= 1:
            return []  # Respect the "single merger can continue" rule

        for jid in active_ids:
            ev = _mg_paused.get(jid)
            if ev and ev.is_set():
                ev.clear()
                _sys_paused_jobs.add(f"mg:{jid}")
                paused.append(jid)
                try:
                    await db.db["merger_jobs"].update_one(
                        {"job_id": jid}, {"$set": {"status": "paused", "paused_reason": reason}}
                    )
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"[SysMonitor] pause_mergers error: {e}")
    return paused


async def _resume_sys_paused_jobs(bot) -> dict:
    """Resume all jobs that were paused by the system monitor."""
    resumed = {"mj": 0, "lj": 0, "mg": 0}

    to_remove = set()
    for key in list(_sys_paused_jobs):
        typ, jid = key.split(":", 1)

        if typ == "mj":
            try:
                from plugins.multijob import _mj_paused
                from database import db
                ev = _mj_paused.get(jid)
                if ev:
                    ev.set()
                    resumed["mj"] += 1
                await db.db["multijobs"].update_one(
                    {"job_id": jid}, {"$set": {"status": "running"}, "$unset": {"paused_reason": ""}}
                )
            except Exception:
                pass
            to_remove.add(key)

        elif typ == "lj":
            try:
                from plugins.jobs import _job_tasks, _start_job_task
                from database import db
                job = await db.db.jobs.find_one({"job_id": jid})
                if job:
                    await db.db.jobs.update_one(
                        {"job_id": jid},
                        {"$set": {"status": "running"}, "$unset": {"paused_reason": ""}}
                    )
                    if jid not in _job_tasks or _job_tasks[jid].done():
                        _start_job_task(jid, job["user_id"])
                    resumed["lj"] += 1
            except Exception:
                pass
            to_remove.add(key)

        elif typ == "mg":
            try:
                from plugins.merger import _mg_paused
                from database import db
                ev = _mg_paused.get(jid)
                if ev:
                    ev.set()
                    resumed["mg"] += 1
                await db.db["merger_jobs"].update_one(
                    {"job_id": jid}, {"$set": {"status": "running"}, "$unset": {"paused_reason": ""}}
                )
            except Exception:
                pass
            to_remove.add(key)

    _sys_paused_jobs -= to_remove
    return resumed


# ── Stat message builder ───────────────────────────────────────────────────────
def _build_stat_msg(snap: dict, jobs: dict, temps: dict, include_temps: bool = True) -> str:
    r = snap["ram_pct"]
    c = snap["cpu_pct"]
    d = snap["disk_pct"]

    lines = [
        f"<b>»  {_sc('System Status')}</b>\n",
        f"╔══════════════════════════\n",
        f"║  🧠 <b>RAM</b>",
        f"  {_level_emoji(r)} [{_bar(r)}] <code>{r:.1f}%</code>",
        f"  <code>{snap['ram_used_gb']:.1f} / {snap['ram_total_gb']:.1f} GB</code>  "
        f"(Free: <code>{snap['ram_avail_gb']:.1f} GB</code>)\n",
        f"║  ⚡ <b>CPU</b>",
        f"  {_level_emoji(c)} [{_bar(c)}] <code>{c:.1f}%</code>\n",
        f"║  💾 <b>Disk</b>",
        f"  {_level_emoji(d)} [{_bar(d)}] <code>{d:.1f}%</code>",
        f"  <code>{snap['disk_used_gb']:.1f} / {snap['disk_total_gb']:.1f} GB</code>  "
        f"(Free: <code>{snap['disk_free_gb']:.1f} GB</code>)\n",
        f"║  🤖 <b>Bot RAM:</b> <code>{snap['bot_ram_mb']:.1f} MB</code>\n",
        f"╠══════════════════════════\n",
        f"║  📋 <b>{_sc('Active Jobs')}</b>\n",
        f"  🔄 Live Jobs: <code>{jobs['lj_active']}</code>",
        f"  📦 Multi Jobs: <code>{jobs['mj_active']}</code>  "
        f"(Paused: <code>{jobs['mj_paused']}</code>)",
        f"  🎵 Mergers: <code>{jobs['mg_active']}</code>  "
        f"(Paused: <code>{jobs['mg_paused']}</code>)\n",
    ]

    if _sys_paused_jobs:
        lines.append(f"║  ⏸ <b>System-Paused:</b> <code>{len(_sys_paused_jobs)}</code> job(s)\n")

    if include_temps:
        lines.append(f"╠══════════════════════════\n")
        lines.append(f"║  🗑 <b>{_sc('Temp Files')}</b>\n")
        for dn, sz in temps.items():
            lines.append(f"  • <code>{dn}/</code> → <code>{sz:.1f} MB</code>")
        total_sz = sum(temps.values())
        lines.append(f"  Total: <code>{total_sz:.1f} MB</code>\n")

    lines.append(f"╚══════════════════════════")
    lines.append(f"\n<i>Updated: {datetime.now().strftime('%d %b %Y %H:%M:%S')}</i>")
    return "\n".join(lines)


# ── Monitor loop ──────────────────────────────────────────────────────────────
async def _monitor_loop(bot):
    """Background task — checks system health every MONITOR_INTERVAL seconds."""
    await asyncio.sleep(15)  # Give the bot time to fully start
    logger.info("[SysMonitor] Background monitor started.")

    while True:
        try:
            snap = _sys_snapshot()
            r, c = snap["ram_pct"], snap["cpu_pct"]
            now  = time.time()
            jobs = _count_running_jobs()
            total_active = jobs["mj_active"] + jobs["lj_active"] + jobs["mg_active"]

            avail_gb = snap["ram_avail_gb"]

            # Dynamic Resource Scaling: Only trigger emergency on RAM death, not CPU spikes
            is_ram_emer = (r >= RAM_EMERGENCY and avail_gb < 0.15)
            # We no longer trigger auto-pauses purely for CPU spikes. High CPU just means FFmpeg is working.
            # We only warn for CPU, but never EMERGENCY pause for it, otherwise every encoding job dies instantly.

            # Determine current level
            if is_ram_emer:
                level = "emergency"
            elif (r >= RAM_CRITICAL and avail_gb < 0.3):
                level = "critical"
            elif (r >= RAM_WARN and avail_gb < 0.5) or c >= CPU_WARN:
                level = "warning"
            else:
                level = "ok"

            # Only act if we have jobs and can notify
            if level != "ok":
                # When system is OK, reset warning cooldown so next spike is fresh
                pass
            else:
                # Level is OK — reset warning cooldown so next flare sends a fresh alert
                _last_alert_ts.pop("warning", None)

            if level != "ok" and total_active > 0:
                cooldown = ALERT_COOLDOWN_WARN if level == "warning" else ALERT_COOLDOWN_CRIT
                last = _last_alert_ts.get(level, 0)
                if now - last >= cooldown:
                    _last_alert_ts[level] = now

                    if level == "warning":
                        # Just warn, no pause — include Stats button so user doesn't need to type
                        txt = (
                            f"<b><u>System Load Warning</u></b>\n\n"
                            f"<b>RAM:</b> <code>{r:.1f}%</code> | <b>CPU:</b> <code>{c:.1f}%</code>\n\n"
                            f"<i>The system is currently under moderate load. "
                            f"Background jobs are still running, but please monitor the performance.</i>"
                        )
                        warn_btns = InlineKeyboardMarkup([[
                            InlineKeyboardButton("Sᴛᴀᴛs", callback_data="sysmon#stats"),
                            InlineKeyboardButton("Cʟᴇᴀɴᴜᴘ", callback_data="sysmon#cleanup"),
                        ]])
                        for uid in Config.BOT_OWNER_ID:
                            try: await bot.send_message(uid, txt, reply_markup=warn_btns)
                            except Exception: pass

                    elif level == "critical":
                        # Pause Multi Jobs + Live Jobs
                        # Pause Mergers only if > 1 running
                        reason = f"System critical: RAM {r:.0f}% CPU {c:.0f}%"
                        mj_p = await _pause_multijobs(reason)
                        lj_p = [] # Never pause Live Jobs, they are passive listeners
                        mg_p = await _pause_mergers(reason, force_all=False)

                        paused_count = len(mj_p) + len(mg_p)
                        merger_note = (
                            "Merger continuing (only 1 running — allowed)."
                            if jobs["mg_active"] <= 1
                            else f"Paused {len(mg_p)} merger(s)."
                        )

                        txt = (
                            f"<b><u>CRITICAL: Auto-Pause Triggered</u></b>\n\n"
                            f"<b>RAM:</b> <code>{r:.1f}%</code> | <b>CPU:</b> <code>{c:.1f}%</code>\n\n"
                            f"<i>Action automatically taken to stabilize system (Live Jobs protected):</i>\n"
                            f"• Paused <b>{len(mj_p)}</b> Multi Job(s)\n"
                            f"• {merger_note}\n\n"
                            f"<i>All paused jobs are safely bookmarked. "
                            f"Use <b>/resumeall</b> to seamlessly continue when the load drops.</i>"
                        )
                        btns = InlineKeyboardMarkup([[
                            InlineKeyboardButton("Resume All", callback_data="sysmon#resumeall"),
                            InlineKeyboardButton("Stats", callback_data="sysmon#stats"),
                        ]])
                        for uid in Config.BOT_OWNER_ID:
                            try: await bot.send_message(uid, txt, reply_markup=btns)
                            except Exception: pass

                    elif level == "emergency":
                        # Pause EVERYTHING including all mergers
                        reason = f"EMERGENCY: RAM {r:.0f}% CPU {c:.0f}%"
                        mj_p = await _pause_multijobs(reason)
                        lj_p = [] # Never pause Live Jobs, they are passive listeners
                        mg_p = await _pause_mergers(reason, force_all=True)

                        txt = (
                            f"<b><u>EMERGENCY: All Tasks Auto-Paused</u></b>\n\n"
                            f"<b>RAM:</b> <code>{r:.1f}%</code> | <b>CPU:</b> <code>{c:.1f}%</code>\n"
                            f"<i>System has reached an emergency threshold.</i>\n\n"
                            f"<i>Action immediately taken (Live Jobs protected):</i>\n"
                            f"• Paused <b>{len(mj_p)}</b> Multi Job(s)\n"
                            f"• Paused <b>{len(mg_p)}</b> Merger(s)\n\n"
                            f"<i>You should consider clearing temporary files to free memory. "
                            f"Use <b>/cleanup</b> to wipe cache, and <b>/resumeall</b> once recovered.</i>"
                        )
                        btns = InlineKeyboardMarkup([
                            [
                                InlineKeyboardButton("Resume All", callback_data="sysmon#resumeall"),
                                InlineKeyboardButton("Cleanup Now", callback_data="sysmon#cleanup"),
                            ],
                            [InlineKeyboardButton("Stats", callback_data="sysmon#stats")],
                        ])
                        for uid in Config.BOT_OWNER_ID:
                            try: await bot.send_message(uid, txt, reply_markup=btns)
                            except Exception: pass

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[SysMonitor] Monitor loop error: {e}")

        await asyncio.sleep(MONITOR_INTERVAL)


def start_monitor(bot):
    """Call this once from main/init to start the background monitor."""
    global _monitor_task
    if _monitor_task and not _monitor_task.done():
        return
    _monitor_task = asyncio.create_task(_monitor_loop(bot))
    logger.info("[SysMonitor] Monitor task created.")


# ══════════════════════════════════════════════════════════════════════════════
# /sysstat command
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.command("sysstat"))
async def cmd_sysstat(bot, message: Message):
    if not _is_owner(message.from_user.id):
        return await message.reply_text("⛔ Owner-only command.")

    await message.reply_text("<i>Fetching system info...</i>")
    snap  = _sys_snapshot()
    jobs  = _count_running_jobs()
    temps = _temp_dir_sizes()
    txt   = _build_stat_msg(snap, jobs, temps)

    btns = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔄 Rᴇꜰʀᴇsʜ", callback_data="sysmon#stats"),
            InlineKeyboardButton("🗑 Cʟᴇᴀɴᴜᴘ", callback_data="sysmon#cleanup"),
        ],
        [
            InlineKeyboardButton("⏸ Pᴀᴜsᴇ Aʟʟ", callback_data="sysmon#pauseall"),
            InlineKeyboardButton("▶️ Rᴇsᴜᴍᴇ Aʟʟ", callback_data="sysmon#resumeall"),
        ],
    ])
    await message.reply_text(txt, reply_markup=btns)


# ══════════════════════════════════════════════════════════════════════════════
# /cleanup command
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.command("cleanup"))
async def cmd_cleanup(bot, message: Message):
    if not _is_owner(message.from_user.id):
        return await message.reply_text("⛔ Owner-only command.")

    temps = _temp_dir_sizes()
    total = sum(temps.values())
    lines = [f"  • <code>{d}/</code> — <code>{sz:.1f} MB</code>" for d, sz in temps.items()]
    txt = (
        f"<b>🗑 Cleanup Confirmation</b>\n\n"
        f"The following temp folders will be <b>permanently deleted</b>:\n\n"
        + "\n".join(lines) +
        f"\n\n<b>Total:</b> <code>{total:.1f} MB</code>\n\n"
        f"⚠️ <i>Only empty folders or completed job folders will be deleted.\n"
        f"Active merge jobs won't be interrupted.</i>"
    )
    btns = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yᴇs, Cʟᴇᴀɴ!", callback_data="sysmon#do_cleanup"),
            InlineKeyboardButton("❌ Cᴀɴᴄᴇʟ", callback_data="sysmon#cancel"),
        ]
    ])
    await message.reply_text(txt, reply_markup=btns)


# ══════════════════════════════════════════════════════════════════════════════
# /pauseall / /resumeall commands
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.command("pauseall"))
async def cmd_pauseall(bot, message: Message):
    if not _is_owner(message.from_user.id):
        return await message.reply_text("⛔ Owner-only command.")
    m = await message.reply_text("<i>Pausing all jobs...</i>")
    reason = "Manual /pauseall by owner"
    mj_p = await _pause_multijobs(reason)
    lj_p = await _pause_livejobs(reason)
    mg_p = await _pause_mergers(reason, force_all=True)
    await m.edit_text(
        f"<b>⏸ All Tasks Paused</b>\n\n"
        f"• Multi Jobs paused: <code>{len(mj_p)}</code>\n"
        f"• Live Jobs stopped: <code>{len(lj_p)}</code>\n"
        f"• Mergers paused: <code>{len(mg_p)}</code>\n\n"
        f"Use /resumeall to restart them.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("▶️ Rᴇsᴜᴍᴇ Aʟʟ", callback_data="sysmon#resumeall")
        ]])
    )


@Client.on_message(filters.private & filters.command("resumeall"))
async def cmd_resumeall(bot, message: Message):
    if not _is_owner(message.from_user.id):
        return await message.reply_text("⛔ Owner-only command.")
    m = await message.reply_text("<i>Resuming system-paused jobs...</i>")
    res = await _resume_sys_paused_jobs(bot)
    await m.edit_text(
        f"<b>▶️ Jobs Resumed</b>\n\n"
        f"• Multi Jobs resumed: <code>{res['mj']}</code>\n"
        f"• Live Jobs restarted: <code>{res['lj']}</code>\n"
        f"• Mergers resumed: <code>{res['mg']}</code>\n\n"
        f"<i>Use /sysstat to check current status.</i>"
    )


# ══════════════════════════════════════════════════════════════════════════════
# Callback handler for inline buttons
# ══════════════════════════════════════════════════════════════════════════════

@Client.on_callback_query(filters.regex(r"^sysmon#"))
async def sysmon_cb(bot, query: CallbackQuery):
    uid = query.from_user.id
    if not _is_owner(uid):
        return await query.answer("⛔ Owner only!", show_alert=True)

    action = query.data.split("#", 1)[1]
    await query.answer()

    if action == "stats":
        snap  = _sys_snapshot()
        jobs  = _count_running_jobs()
        temps = _temp_dir_sizes()
        txt   = _build_stat_msg(snap, jobs, temps)
        btns  = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔄 Rᴇꜰʀᴇsʜ", callback_data="sysmon#stats"),
                InlineKeyboardButton("🗑 Cʟᴇᴀɴᴜᴘ", callback_data="sysmon#cleanup"),
            ],
            [
                InlineKeyboardButton("⏸ Pᴀᴜsᴇ Aʟʟ", callback_data="sysmon#pauseall"),
                InlineKeyboardButton("▶️ Rᴇsᴜᴍᴇ Aʟʟ", callback_data="sysmon#resumeall"),
            ],
        ])
        try:
            await query.message.edit_text(txt, reply_markup=btns)
        except Exception:
            await bot.send_message(uid, txt, reply_markup=btns)

    elif action == "cleanup":
        temps = _temp_dir_sizes()
        total = sum(temps.values())
        lines = [f"  • <code>{d}/</code> — <code>{sz:.1f} MB</code>" for d, sz in temps.items()]
        txt = (
            f"<b>🗑 Cleanup Confirmation</b>\n\n"
            f"Folders to clean:\n" + "\n".join(lines) +
            f"\n\n<b>Total space to free:</b> <code>{total:.1f} MB</code>\n\n"
            f"⚠️ <i>Active merge working dirs will be skipped.</i>"
        )
        btns = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yᴇs, Cʟᴇᴀɴ!", callback_data="sysmon#do_cleanup"),
                InlineKeyboardButton("❌ Cᴀɴᴄᴇʟ", callback_data="sysmon#cancel"),
            ]
        ])
        await query.message.edit_text(txt, reply_markup=btns)

    elif action == "do_cleanup":
        await query.message.edit_text("<i>Cleaning temp files...</i>")
        freed = 0.0
        skipped = []

        # Get active merger working dirs to skip
        active_wdirs = set()
        try:
            from plugins.merger import _mg_tasks
            for jid, task in _mg_tasks.items():
                if not task.done():
                    active_wdirs.add(f"merge_tmp/{jid}")
        except Exception:
            pass

        for d in TEMP_DIRS:
            if not os.path.exists(d):
                continue
            if d == "merge_tmp":
                # Only delete subdirs that are NOT active jobs
                for sub in os.listdir(d):
                    sub_path = os.path.join(d, sub)
                    if sub_path in active_wdirs or os.path.join(d, sub) in active_wdirs:
                        skipped.append(sub_path)
                        continue
                    try:
                        sub_size = sum(f.stat().st_size for f in __import__("pathlib").Path(sub_path).rglob("*") if f.is_file())
                        shutil.rmtree(sub_path, ignore_errors=True)
                        freed += sub_size / 1024 / 1024
                    except Exception:
                        pass
            else:
                # delete all files inside but keep the dir
                try:
                    dir_size = sum(f.stat().st_size for f in __import__("pathlib").Path(d).rglob("*") if f.is_file())
                    shutil.rmtree(d, ignore_errors=True)
                    os.makedirs(d, exist_ok=True)
                    freed += dir_size / 1024 / 1024
                except Exception:
                    pass

        skip_note = f"\n⚠️ Skipped {len(skipped)} active merger folder(s)." if skipped else ""
        snap = _sys_snapshot()
        txt = (
            f"<b>✅ Cleanup Complete!</b>\n\n"
            f"🗑 Freed: <code>{freed:.1f} MB</code>{skip_note}\n\n"
            f"<b>Current Disk Free:</b> <code>{snap['disk_free_gb']:.1f} GB</code>\n"
            f"<b>Current RAM Free:</b> <code>{snap['ram_avail_gb']:.1f} GB</code>"
        )
        btns = InlineKeyboardMarkup([[
            InlineKeyboardButton("📊 Sᴛᴀᴛs", callback_data="sysmon#stats"),
        ]])
        await query.message.edit_text(txt, reply_markup=btns)

    elif action == "cancel":
        await query.message.delete()

    elif action == "pauseall":
        await query.message.edit_text("<i>Pausing all jobs...</i>")
        reason = "Manual pause via /sysstat button"
        mj_p = await _pause_multijobs(reason)
        lj_p = await _pause_livejobs(reason)
        mg_p = await _pause_mergers(reason, force_all=True)
        txt = (
            f"<b>⏸ All Tasks Paused</b>\n\n"
            f"• Multi Jobs: <code>{len(mj_p)}</code>\n"
            f"• Live Jobs: <code>{len(lj_p)}</code>\n"
            f"• Mergers: <code>{len(mg_p)}</code>"
        )
        btns = InlineKeyboardMarkup([[
            InlineKeyboardButton("▶️ Rᴇsᴜᴍᴇ Aʟʟ", callback_data="sysmon#resumeall"),
            InlineKeyboardButton("📊 Sᴛᴀᴛs", callback_data="sysmon#stats"),
        ]])
        await query.message.edit_text(txt, reply_markup=btns)

    elif action == "resumeall":
        await query.message.edit_text("<i>Resuming jobs...</i>")
        res = await _resume_sys_paused_jobs(bot)
        txt = (
            f"<b>▶️ Jobs Resumed</b>\n\n"
            f"• Multi Jobs: <code>{res['mj']}</code>\n"
            f"• Live Jobs: <code>{res['lj']}</code>\n"
            f"• Mergers: <code>{res['mg']}</code>"
        )
        btns = InlineKeyboardMarkup([[
            InlineKeyboardButton("📊 Sᴛᴀᴛs", callback_data="sysmon#stats"),
        ]])
        await query.message.edit_text(txt, reply_markup=btns)
