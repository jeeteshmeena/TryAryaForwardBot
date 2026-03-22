import re

def fix_jobs():
    with open('plugins/jobs.py', 'r', encoding='utf-8') as f:
        code = f.read()

    # 1. Update _run_job
    # Add fc_is_channel evaluation
    find_str_1 = """        fc            = job["from_chat"]"""
    repl_str_1 = """        fc            = job["from_chat"]

        # CRITICAL BUG FIX: determine if source is channel
        fc_is_channel = False
        try:
            if str(fc).startswith("-100"):
                fc_is_channel = True
            else:
                from pyrogram.enums import ChatType
                c_obj = await client.get_chat(fc)
                if getattr(c_obj, 'type', None) in (ChatType.CHANNEL, ChatType.SUPERGROUP):
                    fc_is_channel = True
        except Exception as e:
            logger.warning(f"Could not verify chat type for {fc}: {e}")"""
    
    code = code.replace(find_str_1, repl_str_1)

    # 2. Update Batch fetch in jobs.py
    find_str_2 = """                    # Primary: get_messages by ID list (works for bots on all public channels)
                    try:
                        msgs = await client.get_messages(fc, list(range(cur, chunk_end + 1)))
                        if not isinstance(msgs, list): msgs = [msgs]
                        fetch_ok = True
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 2); continue
                    except Exception as ge:
                        logger.warning(f"[Job {job_id}] get_messages failed @ {cur}: {ge}")"""
                        
    repl_str_2 = """                    # Primary: get_messages by ID list (ONLY works for channels/supergroups!)
                    if fc_is_channel:
                        try:
                            msgs = await client.get_messages(fc, list(range(cur, chunk_end + 1)))
                            if not isinstance(msgs, list): msgs = [msgs]
                            fetch_ok = True
                        except FloodWait as fw:
                            await asyncio.sleep(fw.value + 2); continue
                        except Exception as ge:
                            logger.warning(f"[Job {job_id}] get_messages failed @ {cur}: {ge}")
                    else:
                        fetch_ok = False  # DO NOT USE get_messages FOR DMs/BOTS! It fetches global inbox."""

    code = code.replace(find_str_2, repl_str_2)

    # 3. Update Live fetch in jobs.py
    find_str_3 = """                    try:
                        chunk_msgs = await client.get_messages(fc, bids)
                        if not isinstance(chunk_msgs, list): chunk_msgs = [chunk_msgs]
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 1); break
                    except Exception:
                        # Fallback to history for bots that can't fetch by ID
                        try:
                            co = []"""
                            
    repl_str_3 = """                    if fc_is_channel:
                        try:
                            chunk_msgs = await client.get_messages(fc, bids)
                            if not isinstance(chunk_msgs, list): chunk_msgs = [chunk_msgs]
                        except FloodWait as fw:
                            await asyncio.sleep(fw.value + 1); break
                        except Exception: pass
                    
                    if not chunk_msgs:
                        # Fallback to history for bots/users/normal groups
                        try:
                            co = []"""
                            
    code = code.replace(find_str_3, repl_str_3)

    # 4. Fix taskjob.py similarly
    with open('plugins/taskjob.py', 'r', encoding='utf-8') as f:
        tj_code = f.read()

    find_tj_1 = """        fc      = job["from_chat"]"""
    repl_tj_1 = """        fc      = job["from_chat"]

        # CRITICAL BUG FIX: determine if source is channel
        fc_is_channel = False
        try:
            if str(fc).startswith("-100"):
                fc_is_channel = True
            else:
                from pyrogram.enums import ChatType
                c_obj = await client.get_chat(fc)
                if getattr(c_obj, 'type', None) in (ChatType.CHANNEL, ChatType.SUPERGROUP):
                    fc_is_channel = True
        except Exception as e:
            logger.warning(f"Could not verify chat type for {fc}: {e}")"""
            
    tj_code = tj_code.replace(find_tj_1, repl_tj_1)

    find_tj_2 = """                # Try get_messages first (works for bots on all public channels)
                try:
                    msgs = await client.get_messages(fc, batch_ids)
                    if not isinstance(msgs, list): msgs = [msgs]
                    fetch_ok = True
                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 2); continue
                except Exception as ge:
                    logger.warning(f"[TaskJob {job_id}] get_messages failed @ {current}: {ge}")"""
                    
    repl_tj_2 = """                # Try get_messages first (works ONLY for channels/supergroups!)
                if fc_is_channel:
                    try:
                        msgs = await client.get_messages(fc, batch_ids)
                        if not isinstance(msgs, list): msgs = [msgs]
                        fetch_ok = True
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 2); continue
                    except Exception as ge:
                        logger.warning(f"[TaskJob {job_id}] get_messages failed @ {current}: {ge}")
                else:
                    fetch_ok = False"""

    tj_code = tj_code.replace(find_tj_2, repl_tj_2)

    with open('plugins/jobs.py', 'w', encoding='utf-8') as f:
        f.write(code)
    print("Fixed jobs.py")
    
    with open('plugins/taskjob.py', 'w', encoding='utf-8') as f:
        f.write(tj_code)
    print("Fixed taskjob.py")


if __name__ == '__main__':
    fix_jobs()
