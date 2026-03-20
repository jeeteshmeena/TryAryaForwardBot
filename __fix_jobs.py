import re

with open('plugins/jobs.py', 'r', encoding='utf-8') as f:
    content = f.read()

# ── FIX 1: _get_latest_id — always try get_messages first for all bot types ──
old_latest = """async def _get_latest_id(client, chat_id, is_bot: bool) -> int:
    \"\"\"Get the latest message ID in a chat.
    - For private chats (user DMs, saved messages): use get_chat_history (works for all account types via MTProto).
    - For channels/groups with a bot account: binary-search by get_messages.
    \"\"\"
    is_private_src = not str(chat_id).startswith('-')
    try:
        if not is_bot or is_private_src:
            # get_chat_history works for userbots always, and for bots in private chats
            async for msg in client.get_chat_history(chat_id, limit=1):
                return msg.id
        else:
            # Binary search via get_messages — efficient for channels
            lo, hi = 1, 9_999_999
            for _ in range(25):
                if hi - lo <= 50: break
                mid = (lo + hi) // 2
                try:
                    p = await client.get_messages(chat_id, [mid])
                    if not isinstance(p, list): p = [p]
                    if any(m and not m.empty for m in p):
                        lo = mid
                    else:
                        hi = mid
                except Exception:
                    hi = mid
            return hi
    except Exception:
        pass
    return 0"""

new_latest = """async def _get_latest_id(client, chat_id, is_bot: bool) -> int:
    \"\"\"Get the latest message ID in a chat.
    Strategy: try get_chat_history first (works for all types including DMs/bots).
    Fallback to binary search via get_messages for numeric channel IDs.
    \"\"\"
    # Try fastest method: get last message via history
    try:
        async for msg in client.get_chat_history(chat_id, limit=1):
            return msg.id
    except Exception:
        pass
    # Fallback: binary search (works for bots on public channels by numeric ID)
    try:
        lo, hi = 1, 9_999_999
        for _ in range(25):
            if hi - lo <= 50: break
            mid = (lo + hi) // 2
            try:
                p = await client.get_messages(chat_id, [mid])
                if not isinstance(p, list): p = [p]
                if any(m and not getattr(m, 'empty', True) for m in p):
                    lo = mid
                else:
                    hi = mid
            except Exception:
                hi = mid
        return hi
    except Exception:
        pass
    return 0"""

if old_latest in content:
    content = content.replace(old_latest, new_latest)
    print("Fixed _get_latest_id")
else:
    print("WARN: Could not find _get_latest_id")

# ── FIX 2: Batch fetch — don't re-raise ChatAdminRequired, try history fallback for ALL errors ──
old_batch_fetch = """                try:
                    msgs = []
                    try:
                        msgs = await client.get_messages(fc, list(range(cur, chunk_end + 1)))
                        if not isinstance(msgs, list): msgs = [msgs]
                    except Exception as ge:
                        if \"ChatAdminRequired\" in str(ge): raise
                        logger.warning(f\"[Job {job_id}] get_messages failed for batch {cur}: {ge}\")
                        # Fallback for bots unable to resolve message ID directly on arbitrary strings
                        col: list = []
                        async for msg in client.get_chat_history(fc, offset_id=chunk_end + 1, limit=BATCH_CHUNK):
                            if msg.id < cur: break
                            col.append(msg)
                        msgs = list(reversed(col))
                except FloodWait as fw: await asyncio.sleep(fw.value + 2); continue
                except asyncio.CancelledError: raise
                except Exception as e:
                    logger.warning(f\"[Job {job_id}] Batch fetch completely failed @ {cur}: {e}\")
                    cur += BATCH_CHUNK; await _update_job(job_id, batch_cursor=cur); continue"""

new_batch_fetch = """                try:
                    msgs = []
                    fetch_ok = False
                    # Primary: get_messages by ID list (works for bots on all public channels)
                    try:
                        msgs = await client.get_messages(fc, list(range(cur, chunk_end + 1)))
                        if not isinstance(msgs, list): msgs = [msgs]
                        fetch_ok = True
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 2); continue
                    except Exception as ge:
                        logger.warning(f\"[Job {job_id}] get_messages failed @ {cur}: {ge}\")
                    # Fallback: get_chat_history (works for all userbots and bot DMs)
                    if not fetch_ok:
                        try:
                            col: list = []
                            async for hmsg in client.get_chat_history(fc, offset_id=chunk_end + 1, limit=BATCH_CHUNK):
                                if hmsg.id < cur: break
                                col.append(hmsg)
                            msgs = list(reversed(col))
                            fetch_ok = True
                        except FloodWait as fw:
                            await asyncio.sleep(fw.value + 2); continue
                        except Exception as he:
                            logger.warning(f\"[Job {job_id}] history fallback also failed @ {cur}: {he}\")
                    if not fetch_ok:
                        cur += BATCH_CHUNK
                        await _update_job(job_id, batch_cursor=cur)
                        continue
                except asyncio.CancelledError: raise
                except Exception as e:
                    logger.warning(f\"[Job {job_id}] Batch fetch outer exception @ {cur}: {e}\")
                    cur += BATCH_CHUNK; await _update_job(job_id, batch_cursor=cur); continue"""

if old_batch_fetch in content:
    content = content.replace(old_batch_fetch, new_batch_fetch)
    print("Fixed batch fetch")
else:
    print("WARN: Could not find batch fetch block")

# ── FIX 3: Live fetch — same robust approach ──
old_live = """            try:
                probe = seen + 1
                for _ in range(4):  # limit to 4 chunks (200 msgs) per round
                    bids = list(range(probe, probe + 50))
                    msgs = []
                    try: 
                        msgs = await client.get_messages(fc, bids)
                        if not isinstance(msgs, list): msgs = [msgs]
                    except Exception as ge: 
                        if \"ChatAdminRequired\" in str(ge): raise
                        try:
                            co = []
                            async for gmsg in client.get_chat_history(fc, limit=50):
                                if gmsg.id <= probe - 1: break
                                co.append(gmsg)
                            msgs = list(reversed(co))
                        except Exception: break
                    
                    if not msgs: break
                    msgs.sort(key=lambda m: getattr(m, 'id', 0) if m else 0)
                    new.extend(msgs)
                    
                    new_max = max((getattr(m, 'id', 0) for m in new if m), default=0)
                    v = [m for m in msgs if m and not getattr(m, 'empty', False)]
                    if not msgs or len(v) < 20 or new_max <= probe:
                         break
                    probe = max(probe + 50, new_max + 1)
            except FloodWait as fw: await asyncio.sleep(fw.value + 1); continue
            except asyncio.CancelledError: raise
            except Exception as e:
                logger.warning(f\"[Job {job_id}] Live fetch: {e}\")
                await asyncio.sleep(15); continue"""

new_live = """            try:
                probe = seen + 1
                for _ in range(4):
                    bids = list(range(probe, probe + 50))
                    chunk_msgs = []
                    try:
                        chunk_msgs = await client.get_messages(fc, bids)
                        if not isinstance(chunk_msgs, list): chunk_msgs = [chunk_msgs]
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 1); break
                    except Exception:
                        # Fallback to history for bots that can't fetch by ID
                        try:
                            co = []
                            async for gmsg in client.get_chat_history(fc, limit=50):
                                if gmsg.id <= seen: break
                                co.append(gmsg)
                            chunk_msgs = list(reversed(co))
                        except Exception: break
                    if not chunk_msgs: break
                    chunk_msgs.sort(key=lambda m: getattr(m, 'id', 0) if m else 0)
                    # Only add messages strictly newer than seen
                    new.extend(m for m in chunk_msgs if m and getattr(m, 'id', 0) > seen)
                    valid = [m for m in chunk_msgs if m and not getattr(m, 'empty', True)]
                    if len(valid) < 20: break
                    probe = max(bids[-1] + 1, max((getattr(m,'id',0) for m in chunk_msgs if m), default=probe))
            except FloodWait as fw: await asyncio.sleep(fw.value + 1); continue
            except asyncio.CancelledError: raise
            except Exception as e:
                logger.warning(f\"[Job {job_id}] Live fetch: {e}\")
                await asyncio.sleep(15); continue"""

if old_live in content:
    content = content.replace(old_live, new_live)
    print("Fixed live fetch")
else:
    print("WARN: Could not find live fetch block")

with open('plugins/jobs.py', 'w', encoding='utf-8') as f:
    f.write(content)
