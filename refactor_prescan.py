"""
Refactor share_jobs.py:
- Remove Steps 9, 10, 11 and Confirm from _create_share_flow
- Move those steps INSIDE _build_share_links AFTER the pre-scan diagnosis
"""

with open('plugins/share_jobs.py', 'r', encoding='utf-8') as f:
    content = f.read()

# ─── PART 1: Remove Steps 9,10,11 and Confirm from _create_share_flow ───────
# We'll replace from the start of "msg_batch = await _ask..." (step 9)
# to just before "except Exception as e:" at line 478
# and replace with a direct call to _build_share_links with default values

OLD_STEPS_9_TO_END = '''        msg_batch = await _ask(bot, user_id, 
            "<b>❪ STEP 9: EPISODES PER BUTTON ❫</b>\\n\\nHow many episodes per link button?\\nExample: <code>20</code>", 
            reply_markup=markup
        )
        if getattr(msg_batch, 'text', None) and any(x in msg_batch.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        if getattr(msg_batch, "text", None) and any(x in msg_batch.text.lower() for x in ["/undo", "undo", "uɴᴅᴏ", "↩️"]):
            # Re-ask end_id
            if not is_topic:
                msg_end2 = await _ask(bot, user_id,
                    "<b>❪ STEP 8 (REDO): LAST MESSAGE ❫</b>\\n\\nForward or paste the last message:",
                    reply_markup=markup
                )
                if getattr(msg_end2, 'text', None) and any(x in msg_end2.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
                new_share_job[user_id]['end_id'] = parse_id(msg_end2)
            msg_batch = await _ask(bot, user_id,
                "<b>❪ STEP 9: EPISODES PER BUTTON ❫</b>\\n\\nHow many episodes per link button?",
                reply_markup=markup
            )
            if getattr(msg_batch, 'text', None) and any(x in msg_batch.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        
        raw_b = (msg_batch.text or msg_batch.caption or "20").strip()
        batch_size = int(raw_b) if raw_b.isdigit() else 20
        if batch_size < 1: batch_size = 20
        new_share_job[user_id]['batch_size'] = batch_size

        msg_bpp = await _ask(bot, user_id, 
            "<b>❪ STEP 10: BUTTONS PER POST ❫</b>\\n\\nHow many buttons should appear in one post in the channel?\\nExample: <code>10</code>", 
            reply_markup=markup
        )
        if getattr(msg_bpp, 'text', None) and any(x in msg_bpp.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        if getattr(msg_bpp, "text", None) and any(x in msg_bpp.text.lower() for x in ["/undo", "undo", "uɴᴅᴏ", "↩️"]):
            # Re-ask batch_size
            msg_batch2 = await _ask(bot, user_id,
                "<b>❪ STEP 9 (REDO): EPISODES PER BUTTON ❫</b>\\n\\nHow many episodes per link button?",
                reply_markup=markup
            )
            if getattr(msg_batch2, 'text', None) and any(x in msg_batch2.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            raw_b2 = (msg_batch2.text or "20").strip()
            new_share_job[user_id]['batch_size'] = int(raw_b2) if raw_b2.isdigit() else 20
            msg_bpp = await _ask(bot, user_id,
                "<b>❪ STEP 10: BUTTONS PER POST ❫</b>\\n\\nHow many buttons per post?",
                reply_markup=markup
            )
            if getattr(msg_bpp, 'text', None) and any(x in msg_bpp.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
        
        raw_bpp = (msg_bpp.text or msg_bpp.caption or "10").strip()
        bpp = int(raw_bpp) if raw_bpp.isdigit() else 10
        if bpp < 1: bpp = 10
        new_share_job[user_id]['buttons_per_post'] = bpp

        if force_live:
            msg_live = await _ask(bot, user_id, 
                "<b>❪ STEP 11: LIVE MONITORING THRESHOLD ❫</b>\\n\\nHow many new episodes should arrive before posting a new batch automatically?\\n\\n<i>Send a number (e.g. <code>10</code>).</i>", 
                reply_markup=markup
            )
            if getattr(msg_live, 'text', None) and any(x in msg_live.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            raw_live = (msg_live.text or msg_live.caption or "10").strip()
            thresh = int(raw_live) if raw_live.isdigit() else 10
            if thresh < 1: thresh = 10
            new_share_job[user_id]['live_threshold'] = thresh
        else:
            msg_live = await _ask(bot, user_id, 
                "<b>❪ STEP 11: LIVE MONITORING ❫</b>\\n\\nHow many new episodes should arrive before posting a new batch automatically?\\n\\n<i>Send <code>0</code> or <code>Skip</code> to disable Live Monitoring. Send <code>10</code> to bundle 10 incoming files per batch.</i>", 
                reply_markup=markup
            )
            if getattr(msg_live, 'text', None) and any(x in msg_live.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔']): return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            if getattr(msg_live, "text", None) and any(x in msg_live.text.lower() for x in ["/undo", "undo", "uɴᴅᴏ", "↩️"]):
                return await bot.send_message(user_id, "<b>‣ Undo: Please restart the Batch Links flow from the menu.</b>", reply_markup=ReplyKeyboardRemove())
            
            raw_live = (msg_live.text or msg_live.caption or "0").strip()
            new_share_job[user_id]['live_threshold'] = int(raw_live) if raw_live.isdigit() else 0

        sj = new_share_job[user_id]
        
        is_tp = sj.get('is_topic')
        sub_str = f"<b>Source Topic ID:</b> {sj.get('topic_id', 'N/A')}\\n" if is_tp else f"<b>Msg ID Range:</b> {sj['start_id']} → {sj['end_id']}\\n"
        live_str = f"<b>Live Monitor:</b> {sj['live_threshold']} eps per batch\\n" if sj['live_threshold'] > 0 else f"<b>Live Monitor:</b> <code>Disabled</code>\\n"
        
        target_str = f"<code>{sj['target']}</code>"
        if sj.get('target_topic_id'):
            target_str += f" (Topic: <code>{sj['target_topic_id']}</code>)"

        markup_conf = ReplyKeyboardMarkup([["Gᴇɴᴇʀᴀᴛᴇ & Pᴏsᴛ Lɪɴᴋs"], ["‣  Cancel"]], resize_keyboard=True, one_time_keyboard=True)
        conf_msg = await _ask(bot, user_id,
            f"<b>»  CONFIRM SHARE BATCH</b>\\n\\n"
            f"<b>Story Name:</b> {sj['story']}\\n"
            f"<b>Status:</b> {'Completed' if sj.get('is_completed') else 'Ongoing'}\\n"
            f"<b>Source:</b> <code>{sj['source']}</code> ({'Topic' if is_tp else 'Channel'})\\n"
            f"<b>Target:</b> {target_str}\\n"
            f"{sub_str}"
            f"<b>Episodes/Button:</b> {sj['batch_size']}\\n"
            f"<b>Buttons/Post:</b> {sj['buttons_per_post']}\\n"
            f"{live_str}"
            f"\\n<i>»  Smart Parse active: Auto-groups duplicate eps smoothly.</i>",
            reply_markup=markup_conf
        )
        
        if not conf_msg.text or (getattr(conf_msg, 'text', None) and any(x in conf_msg.text.lower() for x in ['cancel', 'cᴀɴᴄᴇʟ', '⛔'])) or "Cancel" in conf_msg.text:
            new_share_job.pop(user_id, None)
            return await bot.send_message(user_id, "<i>Process Cancelled Successfully!</i>", reply_markup=ReplyKeyboardRemove())
            
        if "Generate" in conf_msg.text or "Gᴇɴᴇʀᴀᴛᴇ" in conf_msg.text:
            await _build_share_links(bot, user_id, sj, conf_msg)'''

NEW_STEPS_9_TO_END = '''        # ── Steps 9, 10, 11 are now asked AFTER the pre-scan inside _build_share_links ──
        # This ensures users see the diagnosis BEFORE configuring batch sizes.
        # Set placeholder defaults for now; _build_share_links will override them.
        new_share_job[user_id].setdefault('batch_size', 20)
        new_share_job[user_id].setdefault('buttons_per_post', 10)
        new_share_job[user_id].setdefault('live_threshold', 0)

        sj = new_share_job[user_id]
        # Send a quick summary before starting the scan
        is_tp = sj.get('is_topic')
        sub_str = f"Source: {sj.get('topic_id', 'N/A')} (Topic)" if is_tp else f"Range: {sj.get('start_id')} → {sj.get('end_id')}"
        notify_msg = await bot.send_message(
            user_id,
            f"<b>»  Starting Channel Scan…</b>\\n\\n"
            f"<b>Story:</b> {sj['story']}\\n"
            f"<b>{sub_str}</b>\\n\\n"
            f"<i>The Pre-Scan Diagnosis will appear next so you can review missing episodes before configuring button sizes.</i>",
            reply_markup=ReplyKeyboardRemove()
        )
        await _build_share_links(bot, user_id, sj, notify_msg)'''

if OLD_STEPS_9_TO_END in content:
    content = content.replace(OLD_STEPS_9_TO_END, NEW_STEPS_9_TO_END, 1)
    print("PART 1: Steps 9-11 removal - SUCCESS")
else:
    print("PART 1: NOT FOUND")
    # Try to find partial match
    idx = content.find("msg_batch = await _ask(bot, user_id, \n            \"<b>❪ STEP 9")
    print(f"Step 9 index: {idx}")

with open('plugins/share_jobs.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("File saved")
