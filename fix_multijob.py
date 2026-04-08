"""
Patch multijob.py unreachable fallback logic and fake forwarding stats.
"""

with open('plugins/multijob.py', 'r', encoding='utf-8') as f:
    text = f.read()

OLD1 = """                if "RESTRICTED" in err or "PROTECTED" in err:
                    # Try copy → forward fallback once for protected content
                    try:
                        await client.forward_messages(chat_id=chat, from_chat_id=msg.chat.id, message_ids=msg.id, **kw)
                        return True
                    except Exception:
                        pass
                    break  # give up cleanly for restricted content
                # For transient errors, retry up to 4 attempts
                if _send_attempt >= 3:
                    logger.warning(f"[MultiJob _send_one] All retries exhausted for msg {msg.id} to {chat}: {exc}")
                    return False
                await asyncio.sleep(3 * (_send_attempt + 1))
                continue
            
            # --- Fallback to Download/Re-upload for restricted sources ---
            try:
                media_obj = getattr(msg, msg.media.value, None) if msg.media else None
                original_name = getattr(media_obj, 'file_name', None) if media_obj else None
                if msg.media:
                    safe_name = f"downloads/{msg.id}_{original_name}" if original_name else f"downloads/{msg.id}"
                    fp = None
                    for _dl_try in range(3):
                        try:
                            fp = await client.download_media(msg, file_name=safe_name)
                            if fp: break
                        except FloodWait as fw:
                            await asyncio.sleep(fw.value + 2)
                        except Exception as dl_e:
                            err_dl = str(dl_e).upper()
                            if "TIMEOUT" in err_dl or "CONNECTION" in err_dl:
                                await asyncio.sleep(5)
                                continue
                            break
                    if not fp: raise Exception("DownloadFailed")
                    
                    up_kw = {"chat_id": chat, "caption": kw.get("caption", msg.caption or "")}
                    if thread: up_kw["message_thread_id"] = thread
                    
                    if msg.photo:      await client.send_photo(photo=fp, **up_kw)
                    elif msg.video:    await client.send_video(video=fp, file_name=original_name, **up_kw)
                    elif msg.document: await client.send_document(document=fp, file_name=original_name, **up_kw)
                    elif msg.audio:    await client.send_audio(audio=fp, file_name=original_name, **up_kw)
                    elif msg.voice:    await client.send_voice(voice=fp, **up_kw)
                    elif msg.animation: await client.send_animation(animation=fp, **up_kw)
                    elif msg.sticker:  await client.send_sticker(sticker=fp, **up_kw)
                    
                    import os
                    if os.path.exists(fp): os.remove(fp)
                else:
                    await client.send_message(chat_id=chat, text=new_text if new_text is not None else getattr(msg.text, "html", str(msg.text)) if msg.text else "", **kw)
            except Exception as fallback_e:
                logger.debug(f"[MultiJob _send_one] Fallback failed to {chat}: {fallback_e}")

    await _send_one(to_chat, thread_id)
    if to_chat_2:
        await _send_one(to_chat_2, thread_id_2)"""

NEW1 = """                if "RESTRICTED" in err or "PROTECTED" in err:
                    # Try copy → forward fallback once for protected content
                    try:
                        await client.forward_messages(chat_id=chat, from_chat_id=msg.chat.id, message_ids=msg.id, **kw)
                        return True
                    except Exception:
                        pass
                    
                    # --- Fallback to Download/Re-upload for restricted sources ---
                    try:
                        media_obj = getattr(msg, msg.media.value, None) if msg.media else None
                        original_name = getattr(media_obj, 'file_name', None) if media_obj else None
                        if msg.media:
                            safe_name = f"downloads/{msg.id}_{original_name}" if original_name else f"downloads/{msg.id}"
                            fp = None
                            for _dl_try in range(3):
                                try:
                                    fp = await client.download_media(msg, file_name=safe_name)
                                    if fp: break
                                except FloodWait as fw:
                                    await asyncio.sleep(fw.value + 2)
                                except Exception as dl_e:
                                    err_dl = str(dl_e).upper()
                                    if "TIMEOUT" in err_dl or "CONNECTION" in err_dl:
                                        await asyncio.sleep(5)
                                        continue
                                    break
                            if not fp: raise Exception("DownloadFailed")
                            
                            up_kw = {"chat_id": chat, "caption": kw.get("caption", msg.caption or "")}
                            if thread: up_kw["message_thread_id"] = thread
                            
                            if msg.photo:      await client.send_photo(photo=fp, **up_kw)
                            elif msg.video:    await client.send_video(video=fp, file_name=original_name, **up_kw)
                            elif msg.document: await client.send_document(document=fp, file_name=original_name, **up_kw)
                            elif msg.audio:    await client.send_audio(audio=fp, file_name=original_name, **up_kw)
                            elif msg.voice:    await client.send_voice(voice=fp, **up_kw)
                            elif msg.animation: await client.send_animation(animation=fp, **up_kw)
                            elif msg.sticker:  await client.send_sticker(sticker=fp, **up_kw)
                            
                            import os
                            if os.path.exists(fp): os.remove(fp)
                        else:
                            await client.send_message(chat_id=chat, text=new_text if new_text is not None else getattr(msg.text, "html", str(msg.text)) if msg.text else "", **kw)
                        return True
                    except Exception as fallback_e:
                        logger.debug(f"[MultiJob _send_one] Fallback failed to {chat}: {fallback_e}")
                        return False

                # For transient errors, retry up to 4 attempts
                if _send_attempt >= 3:
                    logger.warning(f"[MultiJob _send_one] All retries exhausted for msg {msg.id} to {chat}: {exc}")
                    return False
                await asyncio.sleep(3 * (_send_attempt + 1))
                continue

    success1 = await _send_one(to_chat, thread_id)
    success2 = False
    if to_chat_2:
        success2 = await _send_one(to_chat_2, thread_id_2)
    return success1 or success2"""

OLD2 = """                await _mj_forward(client, msg, to_chat, remove_caption, cap_tpl, forward_tag,
                                   to_thread, to_chat_2, to_thread_2, replacements, _remove_links)
                current = msg.id + 1
                await _mj_update(job_id, current_id=current)
                await _mj_inc(job_id, 1)"""

NEW2 = """                success = await _mj_forward(client, msg, to_chat, remove_caption, cap_tpl, forward_tag,
                                   to_thread, to_chat_2, to_thread_2, replacements, _remove_links)
                current = msg.id + 1
                await _mj_update(job_id, current_id=current)
                if success:
                    await _mj_inc(job_id, 1)"""

if OLD1 in text:
    text = text.replace(OLD1, NEW1)
    print("Patched fallback unreachable code!")
else:
    print("Could not find OLD1")

# Because there are TWO places where `_mj_forward` is called (DM batch and Channel batch), replace both.
text = text.replace(OLD2, NEW2)
print("Patched mj_inc loops!")

with open('plugins/multijob.py', 'w', encoding='utf-8') as f:
    f.write(text)
