"""
AryaBot Worker Node — v3
========================
Features:
  1. Registers itself in MongoDB — main bot shows it with /workers
  2. Heartbeat every 15s — if missed for >90s, watchdog on OTHER workers
     will detect the dead job and re-queue it automatically
  3. Atomic job claiming — two workers can never pick the same job
  4. WORKER_TASKS env var — pin a worker to specific task types
  5. Per-task crash isolation — one crash doesn't kill other tasks
  6. Watchdog loop — detects orphaned "running" jobs from dead workers
     and re-queues them so another live worker picks them up
  7. Manual shift support — main bot can set job status back to "queued"
     and clear worker_node; this worker will pick it up in next poll

Environment Variables:
  WORKER_NAME    — Display name e.g. "Worker-Node-1"   (default: Worker-Node)
  WORKER_TASKS   — Comma tasks to handle               (default: all)
                   e.g. "merger,cleaner" or "multijob,taskjob"
  POLL_INTERVAL  — Seconds between polls               (default: 8)
  DEAD_JOB_SEC   — Seconds of no heartbeat = job orphaned (default: 90)
"""

import os
import asyncio
import logging
import time
import platform
import socket

from bot import Bot
from database import db

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("arya_worker")

# ── Config ────────────────────────────────────────────────────────────────────
WORKER_NAME        = os.environ.get("WORKER_NAME", "Worker-Node")
POLL_INTERVAL      = int(os.environ.get("POLL_INTERVAL", "8"))
HEARTBEAT_INTERVAL = 15   # seconds between heartbeats
DEAD_JOB_SEC       = int(os.environ.get("DEAD_JOB_SEC", "90"))  # orphan threshold

_RAW_TASKS   = os.environ.get("WORKER_TASKS", "merger,cleaner,multijob,taskjob")
WORKER_TASKS = {t.strip().lower() for t in _RAW_TASKS.split(",") if t.strip()}

WORKER_COLL = "worker_registry"

# Tracks job_ids currently being run by THIS process (for watchdog to skip self)
_active_jobs: set = set()


# ── Worker Registry ───────────────────────────────────────────────────────────
async def _register_worker():
    doc = {
        "_id":            WORKER_NAME,
        "name":           WORKER_NAME,
        "host":           socket.gethostname(),
        "pid":            os.getpid(),
        "tasks":          sorted(WORKER_TASKS),
        "started_at":     time.time(),
        "last_heartbeat": time.time(),
        "status":         "online",
        "current_job":    None,
        "current_job_type": None,
        "python":         platform.python_version(),
        "platform":       platform.system(),
    }
    await db.db[WORKER_COLL].replace_one({"_id": WORKER_NAME}, doc, upsert=True)
    logger.info(f"[{WORKER_NAME}] Registered in MongoDB → collection: {WORKER_COLL}")


async def _set_worker_job(job_id: str, job_type: str):
    _active_jobs.add(job_id)
    await db.db[WORKER_COLL].update_one(
        {"_id": WORKER_NAME},
        {"$set": {
            "current_job":      job_id,
            "current_job_type": job_type,
            "last_heartbeat":   time.time(),
        }}
    )


async def _clear_worker_job(job_id: str):
    _active_jobs.discard(job_id)
    await db.db[WORKER_COLL].update_one(
        {"_id": WORKER_NAME},
        {"$set": {
            "current_job":      None,
            "current_job_type": None,
            "last_heartbeat":   time.time(),
        }}
    )


async def _unregister_worker():
    await db.db[WORKER_COLL].update_one(
        {"_id": WORKER_NAME},
        {"$set": {"status": "offline", "current_job": None, "current_job_type": None}}
    )
    logger.info(f"[{WORKER_NAME}] Marked offline.")


# ── Heartbeat Loop ────────────────────────────────────────────────────────────
async def _heartbeat_loop():
    while True:
        try:
            await db.db[WORKER_COLL].update_one(
                {"_id": WORKER_NAME},
                {"$set": {"last_heartbeat": time.time(), "status": "online"}}
            )
        except Exception as e:
            logger.warning(f"[{WORKER_NAME}] Heartbeat failed: {e}")
        await asyncio.sleep(HEARTBEAT_INTERVAL)


# ── Watchdog: Auto-Failover ───────────────────────────────────────────────────
# All worker collections and their job_id field
WATCHDOG_COLLS = {
    "merger":   "mergejobs",
    "cleaner":  "cleaner_jobs",
    "multijob": "multijobs",
    "taskjob":  "taskjobs",
}

async def _watchdog_loop():
    """
    Every 60s: scan all job collections for "running" jobs whose worker's
    heartbeat is older than DEAD_JOB_SEC. If found → re-queue the job so
    any live worker picks it up.

    Each worker runs this independently — atomic findOneAndUpdate ensures
    only one worker re-queues each orphan.
    """
    logger.info(f"[{WORKER_NAME}] Watchdog started (threshold: {DEAD_JOB_SEC}s)")
    await asyncio.sleep(30)   # give workers time to start before first scan
    while True:
        try:
            now = time.time()
            cutoff = now - DEAD_JOB_SEC

            # Get heartbeats of all registered workers
            workers = {w["_id"]: w async for w in db.db[WORKER_COLL].find({})}

            for task_type, coll_name in WATCHDOG_COLLS.items():
                try:
                    cursor = db.db[coll_name].find({"status": "running"})
                    async for job in cursor:
                        job_id      = job.get("job_id", "")
                        job_worker  = job.get("worker_node", "")
                        claimed_at  = job.get("claimed_at", 0)

                        # Skip jobs running on THIS worker, or running on the main bot
                        if job_worker in (WORKER_NAME, "main", "") or job_id in _active_jobs:
                            continue

                        # Check if the owning worker is still alive
                        owner = workers.get(job_worker)
                        if owner:
                            hb = owner.get("last_heartbeat", 0)
                            if hb > cutoff:
                                continue   # Owner alive — leave it alone
                            # Owner is dead — fall through to re-queue
                        else:
                            # Worker not registered (disappeared) — check how old
                            if claimed_at and claimed_at > cutoff:
                                continue   # Recently claimed — wait

                        # Re-queue the orphaned job atomically
                        result = await db.db[coll_name].find_one_and_update(
                            {"job_id": job_id, "status": "running"},
                            {"$set": {
                                "status":      "queued",
                                "worker_node": None,
                                "requeued_by": WORKER_NAME,
                                "requeued_at": now,
                            }}
                        )
                        if result:
                            logger.warning(
                                f"[{WORKER_NAME}] ♻️ Watchdog re-queued orphaned "
                                f"{task_type} job {job_id} (was on {job_worker})"
                            )
                except Exception as e:
                    logger.error(f"[{WORKER_NAME}] Watchdog error on {coll_name}: {e}")

        except Exception as e:
            logger.error(f"[{WORKER_NAME}] Watchdog loop error: {e}")

        await asyncio.sleep(60)   # scan every 60 seconds


# ── Atomic Job Claimer ────────────────────────────────────────────────────────
async def _claim_job(collection_name: str, job_id: str) -> bool:
    """
    Atomically set status queued→running tagging this worker.
    Only one worker wins — MongoDB atomic update guarantees this.
    """
    result = await db.db[collection_name].find_one_and_update(
        {"job_id": job_id, "status": "queued"},
        {"$set": {
            "status":      "running",
            "worker_node": WORKER_NAME,
            "claimed_at":  time.time(),
        }},
        return_document=True
    )
    return result is not None


# ── Per-Task Handlers ─────────────────────────────────────────────────────────
async def _handle_merger(job_id: str, bot):
    from plugins.merger import _mg_run_job, _mg_update_job, _db_get
    logger.info(f"[{WORKER_NAME}] 🔀 Merger job: {job_id}")
    await _set_worker_job(job_id, "merger")
    try:
        job = await _db_get(job_id)
        uid = job.get("user_id") if job else None
        await _mg_run_job(job_id, uid=uid, bot=bot)
    except Exception as e:
        logger.error(f"[{WORKER_NAME}] Merger {job_id} crashed: {e}")
        try:
            await _mg_update_job(job_id, status="error", error=str(e)[:400])
        except Exception:
            pass
    finally:
        await _clear_worker_job(job_id)


async def _handle_cleaner(job_id: str, bot):
    from plugins.cleaner import _cl_run_job, _cl_update_job
    logger.info(f"[{WORKER_NAME}] 🧹 Cleaner job: {job_id}")
    await _set_worker_job(job_id, "cleaner")
    try:
        await _cl_run_job(job_id, bot=bot)
    except Exception as e:
        logger.error(f"[{WORKER_NAME}] Cleaner {job_id} crashed: {e}")
        try:
            await _cl_update_job(job_id, {"status": "failed", "error": str(e)[:400]})
        except Exception:
            pass
    finally:
        await _clear_worker_job(job_id)


async def _handle_multijob(job_id: str, bot):
    logger.info(f"[{WORKER_NAME}] 📋 MultiJob: {job_id}")
    await _set_worker_job(job_id, "multijob")
    try:
        try:
            from plugins.multijob import _mj_run_job
            await _mj_run_job(job_id, bot=bot)
        except (ImportError, AttributeError):
            from plugins.multijob import run_multijob
            await run_multijob(job_id, bot=bot)
    except Exception as e:
        logger.error(f"[{WORKER_NAME}] MultiJob {job_id} crashed: {e}")
    finally:
        await _clear_worker_job(job_id)


async def _handle_taskjob(job_id: str, bot):
    logger.info(f"[{WORKER_NAME}] ⚙️ TaskJob: {job_id}")
    await _set_worker_job(job_id, "taskjob")
    try:
        from plugins.taskjob import run_task_job
        await run_task_job(job_id, bot=bot)
    except Exception as e:
        logger.error(f"[{WORKER_NAME}] TaskJob {job_id} crashed: {e}")
    finally:
        await _clear_worker_job(job_id)


# ── Task Map ──────────────────────────────────────────────────────────────────
TASK_MAP = {
    "merger":   ("mergejobs",    _handle_merger),
    "cleaner":  ("cleaner_jobs", _handle_cleaner),
    "multijob": ("multijobs",    _handle_multijob),
    "taskjob":  ("taskjobs",     _handle_taskjob),
}


# ── Main Polling Loop ─────────────────────────────────────────────────────────
async def poll_jobs(bot):
    logger.info(
        f"[{WORKER_NAME}] ▶ Polling | "
        f"Tasks: {', '.join(sorted(WORKER_TASKS))} | "
        f"Every: {POLL_INTERVAL}s"
    )
    while True:
        try:
            for task_type, (coll, handler) in TASK_MAP.items():
                if task_type not in WORKER_TASKS:
                    continue

                cursor = db.db[coll].find({"status": "queued"}).sort("created_at", 1).limit(1)
                async for job in cursor:
                    job_id = job.get("job_id")
                    if not job_id:
                        continue
                    claimed = await _claim_job(coll, job_id)
                    if not claimed:
                        continue

                    logger.info(f"[{WORKER_NAME}] ✅ Claimed {task_type}: {job_id}")
                    # Run in background — don't block polling of other types
                    asyncio.create_task(handler(job_id, bot))
                    await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"[{WORKER_NAME}] Poll error: {e}")

        await asyncio.sleep(POLL_INTERVAL)


# ── Entrypoint ────────────────────────────────────────────────────────────────
async def main():
    logger.info("=" * 60)
    logger.info(f"  AryaBot Worker Node v3 — {WORKER_NAME}")
    logger.info(f"  Host    : {socket.gethostname()}")
    logger.info(f"  PID     : {os.getpid()}")
    logger.info(f"  Tasks   : {', '.join(sorted(WORKER_TASKS))}")
    logger.info(f"  Failover: dead job threshold = {DEAD_JOB_SEC}s")
    logger.info("=" * 60)

    bot = Bot()
    await bot.start()
    logger.info(f"[{WORKER_NAME}] Bot client online.")

    await _register_worker()

    # Launch background loops
    asyncio.create_task(_heartbeat_loop())
    asyncio.create_task(_watchdog_loop())

    try:
        await poll_jobs(bot)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info(f"[{WORKER_NAME}] Shutdown signal.")
    finally:
        await _unregister_worker()
        try:
            await bot.stop()
        except Exception:
            pass
        logger.info(f"[{WORKER_NAME}] Stopped cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
