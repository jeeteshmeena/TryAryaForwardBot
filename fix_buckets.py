"""
Fix: batch_size is read from sj at line 524 BEFORE the pre-scan.
After Step 9 is answered, we need to REBUILD the buckets with the real batch_size.
Solution: After step 9/10/11 answers are collected, re-run the bucket building logic
using the new batch_size, then proceed to generate links.

Also fix: buttons_per_post is read at line 518 BEFORE the prescan, won't pick up step 10's answer.
We need to re-read it after step 10.
"""

with open('plugins/share_jobs.py', 'r', encoding='utf-8') as f:
    content = f.read()

# After Step 9, 10, 11 are answered (around line 1044), we need to:
# 1. Re-read batch_size and buttons_per_post from sj
# 2. Rebuild buckets with the new batch_size

OLD_AFTER_STEPS = '''            await bot.send_message(user_id, "<i>»  Generating unique secure links...</i>", reply_markup=_RKR())
            await safe_edit("<i>»  Generating unique secure links...</i>")
        except Exception:
            await bot.send_message(user_id, "<b>⏳ Pre-Scan Timed Out (30 mins). Job Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
            return await safe_edit("<b>⏳ Pre-Scan Timed Out (30 mins). Job Cancelled.</b>")

        raw_buttons = []'''

NEW_AFTER_STEPS = '''            # ── NOW rebuild buckets with the real batch_size from Step 9 ──
            # The initial bucket building used placeholder batch_size=20.
            # We have the real answer now, so rebuild with the correct value.
            batch_size     = sj['batch_size']
            buttons_per_post = sj['buttons_per_post']

            import uuid as _uuid_mod
            # Rebuild buckets
            if GROUPED_MODE:
                buckets_final = buckets  # grouped mode doesn't depend on batch_size
            else:
                msg_to_ep  = {m.id: ep for m, ep, _, _ in parsed_msgs}
                msg_to_end = {m.id: ep_e for m, _, ep_e, _ in parsed_msgs}
                b_s2 = None; b_e2 = None; b_mids2 = []; pending2 = []
                buckets_final = []
                for m in sorted(all_valid_msgs, key=lambda x: x.id):
                    mid = m.id
                    if mid in msg_to_ep:
                        ep = msg_to_ep[mid]
                        math_start2 = ((ep - 1) // batch_size) * batch_size + 1
                        math_end2   = math_start2 + batch_size - 1
                        if b_s2 is None:
                            b_s2 = math_start2; b_e2 = math_end2
                        elif ep > b_e2:
                            if b_mids2: buckets_final.append([b_s2, b_e2, b_mids2])
                            b_s2 = math_start2; b_e2 = math_end2; b_mids2 = []
                        if pending2:
                            for umid2 in pending2:
                                if umid2 not in b_mids2: b_mids2.append(umid2)
                            pending2 = []
                        if mid not in b_mids2: b_mids2.append(mid)
                        span_e2 = msg_to_end.get(mid, ep)
                        if span_e2 > b_e2: b_e2 = span_e2
                    else:
                        if b_s2 is None:
                            pending2.append(mid)
                        else:
                            if len(b_mids2) >= batch_size:
                                buckets_final.append([b_s2, b_e2, b_mids2])
                                b_s2 = b_e2 + 1; b_e2 = b_s2 + batch_size - 1; b_mids2 = []
                            if mid not in b_mids2: b_mids2.append(mid)
                if b_s2 is not None and b_mids2:
                    buckets_final.append([b_s2, b_e2, b_mids2])
                elif pending2:
                    buckets_final.append(["Extra", "Files", pending2])
                if buckets_final and buckets_final[-1][0] != "Extra" and last_ep_num:
                    lb2 = buckets_final[-1]
                    buckets_final[-1] = (lb2[0], min(lb2[1], last_ep_num), lb2[2])

            await bot.send_message(user_id, "<i>»  Generating unique secure links...</i>", reply_markup=_RKR())
            await safe_edit("<i>»  Generating unique secure links...</i>")
        except Exception:
            await bot.send_message(user_id, "<b>⏳ Pre-Scan Timed Out (30 mins). Job Cancelled.</b>", reply_markup=ReplyKeyboardRemove())
            return await safe_edit("<b>⏳ Pre-Scan Timed Out (30 mins). Job Cancelled.</b>")

        raw_buttons = []
        # Use the rebuilt buckets (with real batch_size) if available, else fall back
        _buckets_to_use = buckets_final if 'buckets_final' in dir() else buckets'''

if OLD_AFTER_STEPS in content:
    content = content.replace(OLD_AFTER_STEPS, NEW_AFTER_STEPS, 1)
    print("PART A: bucket rebuild after steps - SUCCESS")
else:
    print("PART A: NOT FOUND")
    idx = content.find("Generating unique secure links")
    print(f"Found at: {idx}")
    print(repr(content[idx-100:idx+200]))

with open('plugins/share_jobs.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Done")
