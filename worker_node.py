import asyncio
import os
import logging
from bot import Bot
from database import db
from plugins.cleaner import _cl_run_job, _cl_update_job, _cl_get_job
from plugins.merger import _mg_run_job, _mg_update_job, _mg_get_job

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("worker_node")

WORKER_NAME = os.environ.get("WORKER_NAME", "Worker-Node")

async def poll_jobs(bot):
    logger.info(f"[{WORKER_NAME}] Started polling for jobs...")
    while True:
        try:
            # Check for queued Cleaner Jobs
            cleaners_cur = db.cleaner_jobs.find({"status": "queued"}).sort("created_at", 1).limit(1)
            async for job in cleaners_cur:
                job_id = job["job_id"]
                logger.info(f"[{WORKER_NAME}] Claiming Cleaner Job: {job_id}")
                await _cl_update_job(job_id, {"status": "running", "worker_node": WORKER_NAME})
                try:
                    await _cl_run_job(job_id, bot)
                except Exception as e:
                    logger.error(f"[{WORKER_NAME}] Cleaner Job crashed: {e}")
                
            # Check for queued Merger Jobs
            mergers_cur = db.merger_jobs.find({"status": "queued"}).sort("created_at", 1).limit(1)
            async for job in mergers_cur:
                job_id = job["job_id"]
                logger.info(f"[{WORKER_NAME}] Claiming Merger Job: {job_id}")
                await _mg_update_job(job_id, {"status": "running", "worker_node": WORKER_NAME})
                try:
                    await _mg_run_job(job_id, bot)
                except Exception as e:
                    logger.error(f"[{WORKER_NAME}] Merger Job crashed: {e}")
                    
        except Exception as e:
            logger.error(f"Polling loop error: {e}")
        
        await asyncio.sleep(10)  # Check every 10 seconds

async def main():
    logger.info(f"Starting Independent Worker Node: {WORKER_NAME}")
    bot = Bot()
    await bot.start()
    
    # Run the polling loop forever
    await poll_jobs(bot)

if __name__ == "__main__":
    asyncio.run(main())
