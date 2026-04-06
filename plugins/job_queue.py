"""
AryaJobQueue — Central concurrency limiter for heavy jobs.
===========================================================
Multi Job, Live Job, and Merger all register here.
If the max concurrent limit is reached, new jobs are queued
and auto-started when a slot opens up.

Usage:
    from plugins.job_queue import AryaJobQueue

    # At job start:
    pos = AryaJobQueue.enqueue(job_id, "merger")
    if pos > 0:
        await bot.send_message(uid, f"Queued at position {pos}, please wait...")
    await AryaJobQueue.acquire(job_id)   # blocks until slot free
    try:
        ...  # run job
    finally:
        AryaJobQueue.release(job_id)
"""
import asyncio
import logging
from enum import Enum

logger = logging.getLogger(__name__)


class JobType(str, Enum):
    MERGER    = "merger"
    MULTIJOB  = "multijob"
    LIVEJOB   = "livejob"
    CLEANER   = "cleaner"
    TASKJOB   = "taskjob"


# ── Concurrency limits per job type ──────────────────────────────────────────
_LIMITS = {
    JobType.MERGER:   1,   # Merges are extremely CPU+RAM heavy — 1 at a time
    JobType.MULTIJOB: 3,   # Multi Jobs: up to 3 simultaneous copy operations
    JobType.LIVEJOB:  5,   # Live Jobs: up to 5 simultaneous listeners
    JobType.CLEANER:  2,   # Cleaners: heavy FFmpeg re-encoding (2 max)
    JobType.TASKJOB:  3,   # Task Jobs: sequential copy (3 max)
}

# ── Semaphores ────────────────────────────────────────────────────────────────
_sems: dict[str, asyncio.Semaphore] = {
    jt: asyncio.Semaphore(_LIMITS[jt]) for jt in JobType
}

# ── Queue tracking ────────────────────────────────────────────────────────────
_waiting_order: dict[str, list[str]] = {jt: [] for jt in JobType}  # job_id lists


class AryaJobQueue:
    """Static class wrapping the central queue."""

    @staticmethod
    def queue_position(job_id: str, job_type: str) -> int:
        """Return position in queue (0 = running immediately, 1+ = waiting)."""
        jt   = JobType(job_type)
        sem  = _sems[jt]
        wait = _waiting_order[jt]
        if sem._value > 0:
            return 0  # slot available right now
        if job_id in wait:
            return wait.index(job_id) + 1
        return len(wait) + 1

    @staticmethod
    def max_slots(job_type: str) -> int:
        return _LIMITS.get(JobType(job_type), 1)

    @staticmethod
    async def acquire(job_id: str, job_type: str) -> int:
        """Acquire a slot, blocking if all slots are busy.
        Returns queue position that was reported (0 = immediate).
        """
        jt   = JobType(job_type)
        sem  = _sems[jt]
        wait = _waiting_order[jt]

        # Check if slot immediately available
        pos = 0
        if sem._value == 0:
            pos = len(wait) + 1
            wait.append(job_id)

        await sem.acquire()

        # Remove from waiting list once acquired
        try:
            wait.remove(job_id)
        except ValueError:
            pass
        return pos

    @staticmethod
    def release(job_id: str, job_type: str):
        """Release a slot."""
        jt  = JobType(job_type)
        sem = _sems[jt]
        try:
            sem.release()
        except Exception as e:
            logger.warning(f"[AryaJobQueue] Release error ({job_id}): {e}")
        # Clean up waiting list just in case
        try:
            _waiting_order[jt].remove(job_id)
        except ValueError:
            pass

    @staticmethod
    def is_busy(job_type: str) -> bool:
        """True if all slots are occupied."""
        return _sems[JobType(job_type)]._value == 0

    @staticmethod
    def active_count(job_type: str) -> int:
        """How many jobs of this type are currently running."""
        jt  = JobType(job_type)
        sem = _sems[jt]
        return _LIMITS[jt] - sem._value
