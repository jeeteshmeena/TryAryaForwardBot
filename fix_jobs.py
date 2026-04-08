"""
Patch jobs.py global exception handler to auto-restart Live Jobs on network errors instead of dying.
"""

with open('plugins/jobs.py', 'r', encoding='utf-8') as f:
    text = f.read()

OLD = """    except Exception as e:
        err_str = str(e)
        if "AUTH_KEY_DUPLICATED" in err_str:
            # Session was used in 2 places — clear from cache so next restart is fresh
            logger.warning(f"[Job {job_id}] AUTH_KEY_DUPLICATED — clearing client cache and pausing job")
            if client:
                client_name = getattr(client, 'name', None)
                if client_name:
                    await release_client(client_name)
                    client = None   # prevent double-stop in finally
            # Don't mark job as error — just pause it so user can restart manually
            await _update_job(job_id, status="paused",
                              error="Session conflict (AUTH_KEY_DUPLICATED). Restart the job.")
        else:
            logger.error(f"[Job {job_id}] Fatal: {e}", exc_info=True)
            await _update_job(job_id, status="error", error=err_str)"""

NEW = """    except Exception as e:
        err_str = str(e)
        err_upper = err_str.upper()
        if "AUTH_KEY_DUPLICATED" in err_str:
            # Session was used in 2 places — clear from cache so next restart is fresh
            logger.warning(f"[Job {job_id}] AUTH_KEY_DUPLICATED — clearing client cache and pausing job")
            if client:
                client_name = getattr(client, 'name', None)
                if client_name:
                    await release_client(client_name)
                    client = None   # prevent double-stop in finally
            # Don't mark job as error — just pause it so user can restart manually
            await _update_job(job_id, status="paused",
                              error="Session conflict (AUTH_KEY_DUPLICATED). Restart the job.")
        elif any(kw in err_upper for kw in ("CONNECTION", "TIMEOUT", "NETWORK", "PING", "SOCKET")):
            logger.warning(f"[Job {job_id}] Transient Network error: {err_str} - Auto-restarting in 30s")
            # Don't crash out the job permanently! Wait and recreate the task
            async def _auto_resume():
                await __import__('asyncio').sleep(30)
                _start_job_task(job_id, user_id)
            __import__('asyncio').create_task(_auto_resume())
        else:
            logger.error(f"[Job {job_id}] Fatal: {e}", exc_info=True)
            await _update_job(job_id, status="error", error=err_str[:40])"""

if OLD in text:
    text = text.replace(OLD, NEW)
    print("Successfully patched global catch in jobs.py!")
else:
    print("Could not find the global catch block. Regex may have failed.")

# Additionally, let's wrap get_me() directly just in case it fails to allow Pyrogram a few tries,
# but the auto-restart will handle it fine regardless. 
OLD_GET_ME = """        #  First-run init: snapshot latest ID 
        me = await client.get_me()
        if from_chat == me.id or from_chat == me.username:"""

NEW_GET_ME = """        #  First-run init: snapshot latest ID 
        for _att in range(3):
            try:
                me = await client.get_me()
                break
            except Exception as e:
                if _att == 2: raise e
                await __import__('asyncio').sleep(5)
                
        if from_chat == me.id or from_chat == me.username:"""

if OLD_GET_ME in text:
    text = text.replace(OLD_GET_ME, NEW_GET_ME)
    print("Successfully patched get_me() loop in jobs.py!")

with open('plugins/jobs.py', 'w', encoding='utf-8') as f:
    f.write(text)
