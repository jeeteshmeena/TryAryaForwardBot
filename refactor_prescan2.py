"""
Part 2: Add Steps 9, 10, 11 INSIDE _build_share_links after the pre-scan diagnosis acceptance
"""

with open('plugins/share_jobs.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the line that comes right after the pre-scan acceptance
PRESCAN_ACCEPTANCE = '''            await bot.send_message(user_id, "<i>»  Pre-Scan Accepted. Generating unique secure links...</i>", reply_markup=ReplyKeyboardRemove())
            await safe_edit("<i>»  Pre-Scan Accepted. Generating unique secure links...</i>")
        except Exception:
            await bot.send_message(user_id, "<b>⏳ Pre-Scan Timed Out (30 mins). Job Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
            return await safe_edit("<b>⏳ Pre-Scan Timed Out (30 mins). Job Cancelled.</b>")'''

NEW_PRESCAN_ACCEPTANCE = '''            await bot.send_message(user_id, "<i>»  Pre-Scan Accepted!</i>", reply_markup=ReplyKeyboardRemove())
            await safe_edit("<i>»  Pre-Scan Accepted. Now collecting your batch settings...</i>")

            # ── Ask Steps 9, 10, 11 NOW (after scan so user has all context) ──
            from pyrogram.types import ReplyKeyboardMarkup as _RKM, ReplyKeyboardRemove as _RKR

            def _is_cancel(m): return getattr(m, 'text', None) and any(x in (m.text or '').lower() for x in ['cancel', '⛔', '/cancel'])

            # Step 9: Episodes per button
            _m9 = await _ask(bot, user_id,
                "<b>❪ STEP 9: EPISODES PER BUTTON ❫</b>\\n\\nHow many episodes per link button?\\n"
                f"<i>You have {total_count} files detected across {first_ep_num}–{last_ep_num}.</i>\\n\\nExample: <code>20</code>",
                reply_markup=_RKM([["5", "10", "20"], ["25", "50", "⛔ Cancel"]], resize_keyboard=True, one_time_keyboard=True)
            )
            if _is_cancel(_m9):
                await bot.send_message(user_id, "<i>Process Cancelled.</i>", reply_markup=_RKR())
                return await safe_edit("<i>Process Cancelled.</i>")
            _raw9 = (_m9.text or "20").strip()
            sj['batch_size'] = int(_raw9) if _raw9.isdigit() and int(_raw9) > 0 else 20
            batch_size = sj['batch_size']

            # Step 10: Buttons per post
            _m10 = await _ask(bot, user_id,
                "<b>❪ STEP 10: BUTTONS PER POST ❫</b>\\n\\nHow many buttons per channel post?\\nExample: <code>10</code>",
                reply_markup=_RKM([["5", "10", "15"], ["20", "25", "⛔ Cancel"]], resize_keyboard=True, one_time_keyboard=True)
            )
            if _is_cancel(_m10):
                await bot.send_message(user_id, "<i>Process Cancelled.</i>", reply_markup=_RKR())
                return await safe_edit("<i>Process Cancelled.</i>")
            _raw10 = (_m10.text or "10").strip()
            sj['buttons_per_post'] = int(_raw10) if _raw10.isdigit() and int(_raw10) > 0 else 10
            buttons_per_post = sj['buttons_per_post']

            # Step 11: Live monitoring (only for non-force-live)
            if not sj.get('live_threshold'):
                _m11 = await _ask(bot, user_id,
                    "<b>❪ STEP 11: LIVE MONITORING ❫</b>\\n\\nHow many new episodes should trigger auto-posting?\\n"
                    "Send <code>0</code> or <code>Skip</code> to disable.\\nExample: <code>10</code>",
                    reply_markup=_RKM([["0", "5", "10"], ["15", "25", "⛔ Cancel"]], resize_keyboard=True, one_time_keyboard=True)
                )
                if _is_cancel(_m11):
                    await bot.send_message(user_id, "<i>Process Cancelled.</i>", reply_markup=_RKR())
                    return await safe_edit("<i>Process Cancelled.</i>")
                _raw11 = (_m11.text or "0").strip()
                sj['live_threshold'] = int(_raw11) if _raw11.isdigit() else 0

            await bot.send_message(user_id, "<i>»  Generating unique secure links...</i>", reply_markup=_RKR())
            await safe_edit("<i>»  Generating unique secure links...</i>")
        except Exception:
            await bot.send_message(user_id, "<b>⏳ Pre-Scan Timed Out (30 mins). Job Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
            return await safe_edit("<b>⏳ Pre-Scan Timed Out (30 mins). Job Cancelled.</b>")'''

if PRESCAN_ACCEPTANCE in content:
    content = content.replace(PRESCAN_ACCEPTANCE, NEW_PRESCAN_ACCEPTANCE, 1)
    print("PART 2: Steps 9-11 in _build_share_links - SUCCESS")
else:
    print("PART 2: NOT FOUND. Searching around prescan...")
    idx = content.find("Pre-Scan Accepted.")
    print(f"Pre-Scan Accepted at: {idx}")
    if idx >= 0:
        print(repr(content[idx-200:idx+400]))

with open('plugins/share_jobs.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Done")
