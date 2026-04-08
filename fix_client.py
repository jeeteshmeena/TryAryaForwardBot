"""
Fix critical crash where start_clone_bot times out on get_me(),
and forcefully stops a client that is being used by another job.
Also add release_client cleanup to cleaner.py to prevent ref leaks.
"""

with open('plugins/test.py', 'r', encoding='utf-8') as f:
    test_content = f.read()

OLD_TEST = """               await asyncio.wait_for(existing.get_me(), timeout=5)
               logger.debug(f"[ClientCache] Reusing existing client: {cache_key}")
               _client_refcount[cache_key] = _client_refcount.get(cache_key, 1) + 1
               return existing   # ← return cached, skip new start entirely
           except Exception:
               # Dead — clean up and fall through to start a fresh one
               logger.warning(f"[ClientCache] Cached client {cache_key} dead, restarting.")
               _client_cache.pop(cache_key, None)"""

NEW_TEST = """               await asyncio.wait_for(existing.get_me(), timeout=15)
               logger.debug(f"[ClientCache] Reusing existing client: {cache_key}")
               _client_refcount[cache_key] = _client_refcount.get(cache_key, 1) + 1
               return existing   # ← return cached, skip new start entirely
           except Exception as e:
               if isinstance(e, __import__('asyncio').TimeoutError) or "Timeout" in str(e):
                   logger.warning(f"[ClientCache] Cached client {cache_key} is busy (timeout). Assuming alive.")
                   _client_refcount[cache_key] = _client_refcount.get(cache_key, 1) + 1
                   return existing
               # Dead — clean up and fall through to start a fresh one
               logger.warning(f"[ClientCache] Cached client {cache_key} dead ({e}), restarting.")
               _client_cache.pop(cache_key, None)"""

if OLD_TEST in test_content:
    test_content = test_content.replace(OLD_TEST, NEW_TEST)
    print("test.py fix applied successfully!")
else:
    print("test.py fix FAILED to find old block")

with open('plugins/test.py', 'w', encoding='utf-8') as f:
    f.write(test_content)


with open('plugins/cleaner.py', 'r', encoding='utf-8') as f:
    clean_content = f.read()

# Add a try..finally wrapper around the while loop to release the client
OLD_CLEAN = """            job = await _cl_get_job(job_id)
            if not job or job.get("status") in ("completed", "failed", "stopped"):
                return
"""

NEW_CLEAN = """            job = await _cl_get_job(job_id)
            if not job or job.get("status") in ("completed", "failed", "stopped"):
                return

        _cl_finally_client = None  # keep root track for cleanup
"""

OLD_CLEAN2 = """
            # Setup
            from_ch = job["from_chat"]"""

NEW_CLEAN2 = """
            _cl_finally_client = getattr(client, 'name', None) if client else None
            # Setup
            from_ch = job["from_chat"]"""

OLD_CLEAN3 = """            # Cleanup
            try:
                if local_cover and os.path.exists(local_cover): os.remove(local_cover)
            except: pass
            _cl_bot_ref.pop(job_id, None)
            break

"""

NEW_CLEAN3 = """            # Cleanup
            try:
                if local_cover and os.path.exists(local_cover): os.remove(local_cover)
            except: pass
            _cl_bot_ref.pop(job_id, None)
            
            # Note: We do NOT break here yet, because we moved the finally block OUTSIDE the while loop.
            # We break the infinite loop to reach the outer finally.
            break
"""

OLD_CLEAN_OUTER = """    async with ctx:
        while True:
            ev = _cl_paused.get(job_id)"""

NEW_CLEAN_OUTER = """    async with ctx:
        _cl_finally_client = None
        try:
            while True:
                ev = _cl_paused.get(job_id)"""

OLD_CLEAN_OUTER2 = """    @Client.on_callback_query(filters.regex(r"^cl#(main|new|view|pause|resume|stop|del|cfg)"))"""

NEW_CLEAN_OUTER2 = """        finally:
            if _cl_finally_client:
                try:
                    from plugins.test import release_client
                    await release_client(_cl_finally_client)
                except Exception as ex:
                    logger.error(f"[Cleaner {job_id}] Release client failed: {ex}")

# ─── UI Callback Handlers ────────────────────────────────────────────────────
    @Client.on_callback_query(filters.regex(r"^cl#(main|new|view|pause|resume|stop|del|cfg)"))"""

# Actually, replacing all this safely via regex/string replace might be messy.
# Let's cleanly inject it using simpler replaces.

import re

# test.py is already done via string replace

with open('plugins/cleaner.py', 'r', encoding='utf-8') as f:
    c = f.read()

# Instead of complex finally wrapping, let's just make sure "release_client" is called:
# 1) When job_failed is True and loop breaks
# 2) At the End of Loop Logic when it breaks

if "await release_client(" not in c:
    # We will just append the release_client block at the very end of the `while True:` loop's exit paths.
    old_break = """            # Cleanup
            try:
                if local_cover and os.path.exists(local_cover): os.remove(local_cover)
            except: pass
            _cl_bot_ref.pop(job_id, None)
            break"""
    
    new_break = """            # Cleanup
            try:
                if local_cover and os.path.exists(local_cover): os.remove(local_cover)
            except: pass
            _cl_bot_ref.pop(job_id, None)
            
            if client:
                try:
                    from plugins.test import release_client
                    cname = getattr(client, 'name', None)
                    if cname: await release_client(cname)
                except Exception: pass
            break"""
            
    c = c.replace(old_break, new_break)
    
    old_fail = """            if job_failed:
                # Fatal failure already logged above, break outer loop
                break"""
    
    new_fail = """            if job_failed:
                # Fatal failure already logged above, break outer loop
                if client:
                    try:
                        from plugins.test import release_client
                        cname = getattr(client, 'name', None)
                        if cname: await release_client(cname)
                    except Exception: pass
                break"""
    
    c = c.replace(old_fail, new_fail)

    with open('plugins/cleaner.py', 'w', encoding='utf-8') as f:
        f.write(c)
    print("cleaner.py fixed safely!")
else:
    print("cleaner.py already has release_client")
