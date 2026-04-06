from os import environ 
from config import Config
import motor.motor_asyncio
from pymongo import MongoClient

async def mongodb_version():
    x = MongoClient(Config.DATABASE_URI)
    mongodb_version = x.server_info()['version']
    return mongodb_version

class Database:
    
    def __init__(self, uri, database_name):
        self._client = motor.motor_asyncio.AsyncIOMotorClient(
            uri,
            tls=True,
            tlsAllowInvalidCertificates=True,   # Fix for Ubuntu 22.04 OpenSSL 3.0 TLSV1_ALERT_INTERNAL_ERROR
            serverSelectionTimeoutMS=30000,
            connectTimeoutMS=30000,
            socketTimeoutMS=30000,
        )
        self.db = self._client[database_name]
        self.bot = self.db.bots
        self.col = self.db.users
        self.nfy = self.db.notify
        self.chl = self.db.channels
        self.stats = self.db.global_stats
        self.share_links = self.db.share_links
        self.share_config = self.db.share_config  # global share bot settings
        
    async def set_share_bot_token(self, token: str):
        # Migrated: now handles multiple bots via array push, preserving backwards compatibility for singles initially if desired, or just override.
        pass

    async def get_share_bot_token(self):
        # Legacy
        doc = await self.stats.find_one({'_id': 'share_bot'})
        return doc.get('token') if doc else None
        
    async def get_share_bots(self) -> list:
        """Returns list of all configured share bots from DB."""
        doc = await self.stats.find_one({'_id': 'share_bots_list'})
        if doc and 'bots' in doc:
            return doc['bots']
        return []

    async def add_share_bot(self, b_id: int, token: str, username: str, name: str):
        """Adds a new share bot. Prevents duplicates by ID."""
        b_id_str = str(b_id)
        # Remove existing entry with same ID first (upsert-style)
        await self.stats.update_one(
            {'_id': 'share_bots_list'},
            {'$pull': {'bots': {'id': b_id_str}}},
            upsert=True
        )
        bot_dict = {'id': b_id_str, 'token': token, 'username': username, 'name': name}
        await self.stats.update_one(
            {'_id': 'share_bots_list'},
            {'$push': {'bots': bot_dict}},
            upsert=True
        )

    async def remove_share_bot(self, b_id: str):
        """Removes a share bot by its string ID."""
        await self.stats.update_one(
            {'_id': 'share_bots_list'},
            {'$pull': {'bots': {'id': str(b_id)}}}
        )

    async def set_share_protect_global(self, protect: bool):
        await self._set_share_cfg(protect=protect)

    async def get_share_protect_global(self) -> bool:
        return (await self._share_cfg()).get('protect', True)

    async def set_share_autodelete(self, user_id: int, minutes: int):
        await self.col.update_one({'_id': user_id}, {'$set': {'share_autodelete': minutes}}, upsert=True)

    async def get_share_autodelete(self, user_id: int) -> int:
        doc = await self.col.find_one({'_id': user_id})
        return doc.get('share_autodelete', 0) if doc else 0

    # ── Global Share Config ──────────────────────────────────────
    async def _share_cfg(self) -> dict:
        doc = await self.share_config.find_one({'_id': 'global'})
        return doc or {}

    async def _set_share_cfg(self, **kwargs):
        await self.share_config.update_one({'_id': 'global'}, {'$set': kwargs}, upsert=True)

    # Auto-delete (global, minutes)
    async def get_share_autodelete_global(self) -> int:
        return (await self._share_cfg()).get('auto_delete', 0)

    async def set_share_autodelete_global(self, minutes: int):
        await self._set_share_cfg(auto_delete=minutes)

    # Buttons per post (global)
    async def get_share_buttons_per_post(self) -> int:
        return (await self._share_cfg()).get('buttons_per_post', 10)

    async def set_share_buttons_per_post(self, n: int):
        await self._set_share_cfg(buttons_per_post=n)

    # Force-subscribe channels list [{chat_id, title, invite_link, join_request}]
    async def get_share_fsub_channels(self) -> list:
        return (await self._share_cfg()).get('fsub_channels', [])

    async def set_share_fsub_channels(self, channels: list):
        await self._set_share_cfg(fsub_channels=channels)

    # Customizable Texts (global fallback)
    async def get_share_text(self, key: str, default: str = "") -> str:
        return (await self._share_cfg()).get(key, default)

    async def set_share_text(self, key: str, value: str):
        if not value:
            await self.share_config.update_one({'_id': 'global'}, {'$unset': {key: ""}}, upsert=True)
        else:
            await self._set_share_cfg(**{key: value})

    # AI Image Enhancer Config
    async def get_enhancer_config(self) -> dict:
        cfg = await self._share_cfg()
        return cfg.get('enhancer', {
            'api_key': '',
            'enabled': False,
            'model': 'esrgan',
            'scale': 2
        })

    async def update_enhancer_config(self, **kwargs):
        cfg = await self.get_enhancer_config()
        cfg.update(kwargs)
        await self._set_share_cfg(enhancer=cfg)

    # ── Per-Bot Config ────────────────────────────────────────────
    async def _bot_cfg(self, bot_id: str) -> dict:
        if not bot_id:
            return {}
        doc = await self.share_config.find_one({'_id': f'bot_{bot_id}'})
        return doc or {}

    async def _set_bot_cfg(self, bot_id: str, **kwargs):
        if not bot_id:
            return
        await self.share_config.update_one(
            {'_id': f'bot_{bot_id}'}, {'$set': kwargs}, upsert=True
        )

    # Per-bot customizable texts (welcome_msg, delete_msg, success_msg, custom_caption, fsub_msg)
    async def get_share_bot_text(self, bot_id: str, key: str, default: str = "") -> str:
        return (await self._bot_cfg(bot_id)).get(key, default)

    async def set_share_bot_text(self, bot_id: str, key: str, value: str):
        if not bot_id:
            return
        if not value:
            await self.share_config.update_one(
                {'_id': f'bot_{bot_id}'}, {'$unset': {key: ""}}, upsert=True
            )
        else:
            await self._set_bot_cfg(bot_id, **{key: value})

    # Per-bot fsub channels
    async def get_bot_fsub_channels(self, bot_id: str) -> list:
        return (await self._bot_cfg(bot_id)).get('fsub_channels', [])

    async def set_bot_fsub_channels(self, bot_id: str, channels: list):
        await self._set_bot_cfg(bot_id, fsub_channels=channels)

    # FSub approval tracking
    async def save_user_fsub_approved(self, bot_id: str, user_id: int):
        """Mark user as FSub-approved for this bot"""
        await self.col.update_one(
            {'id': user_id},
            {'$addToSet': {'fsub_approved_bots': bot_id}},
            upsert=True
        )

    async def is_user_fsub_approved(self, bot_id: str, user_id: int) -> bool:
        """Check if user has been approved for FSub on this bot"""
        result = await self.col.find_one(
            {'id': user_id, 'fsub_approved_bots': bot_id}
        )
        return result is not None

    # Per-bot About section
    async def get_share_bot_about(self, bot_id: str) -> dict:
        return (await self._bot_cfg(bot_id)).get('about', {})

    async def set_share_bot_about(self, bot_id: str, about: dict):
        await self._set_bot_cfg(bot_id, about=about)

    # Per-bot delivery counter
    async def increment_bot_delivery_count(self, bot_id: str, count: int = 1):
        """Increment total files delivered by this bot."""
        if not bot_id: return
        await self.share_config.update_one(
            {'_id': f'bot_{bot_id}'},
            {'$inc': {'total_delivered': count}},
            upsert=True
        )

    async def get_bot_delivery_count(self, bot_id: str) -> int:
        """Return total files ever delivered by this bot."""
        if not bot_id: return 0
        doc = await self.share_config.find_one({'_id': f'bot_{bot_id}'})
        return (doc or {}).get('total_delivered', 0)

    # Per-bot user tracking
    async def add_share_bot_user(self, bot_id: str, user_id: int):
        """Track that this user has used this share bot."""
        if not bot_id: return
        await self.col.update_one(
            {'id': user_id},
            {
                '$addToSet': {'used_share_bots': bot_id},
                '$set': {'id': user_id}  # ensure document structure
            },
            upsert=True
        )

    # Per-bot fetching media (GIF/image/video shown while delivering files)
    async def get_bot_fetching_media(self, bot_id: str) -> list:
        """Return list of {'file_id': ..., 'media_type': 'photo'|'animation'|'video'} or []."""
        fm = (await self._bot_cfg(bot_id)).get('fetching_media', [])
        if isinstance(fm, dict):
            return [fm] if fm.get('file_id') else []
        return fm

    async def set_bot_fetching_media(self, bot_id: str, fetch_list: list):
        await self._set_bot_cfg(bot_id, fetching_media=fetch_list)

    async def clear_bot_fetching_media(self, bot_id: str):
        if not bot_id: return
        await self.share_config.update_one(
            {'_id': f'bot_{bot_id}'}, {'$unset': {'fetching_media': ''}}, upsert=True
        )

    # When a bot is removed, clean up its config too
    async def remove_share_bot_config(self, bot_id: str):
        await self.share_config.delete_one({'_id': f'bot_{bot_id}'})

    # ── AI Enhancer Config ──────────────────────────────────────────────────
    async def get_enhancer_config(self) -> dict:
        doc = await self.share_config.find_one({'_id': 'ai_enhancer_cfg'})
        return doc or {}

    async def update_enhancer_config(self, **kwargs):
        await self.share_config.update_one({'_id': 'ai_enhancer_cfg'}, {'$set': kwargs}, upsert=True)

    # ── Channel Index (full file list per database channel) ───────
    async def save_channel_index(self, chat_id: int, entries: list, meta: dict = None):
        """Save (or replace) the full scan index for a channel."""
        import time
        doc = {
            '_id': f'ch_index_{chat_id}',
            'chat_id': chat_id,
            'entries': entries,
            'count': len(entries),
            'scanned_at': time.time(),
            'meta': meta or {},
        }
        await self.share_config.update_one(
            {'_id': f'ch_index_{chat_id}'}, {'$set': doc}, upsert=True
        )

    async def get_channel_index(self, chat_id: int):
        """Return the stored index doc for this channel, or None."""
        return await self.share_config.find_one({'_id': f'ch_index_{chat_id}'})

    async def delete_channel_index(self, chat_id: int):
        """Remove the index for a channel."""
        await self.share_config.delete_one({'_id': f'ch_index_{chat_id}'})

    async def update_channel_index_entry(self, chat_id: int, entry: dict):
        """Append or update a single entry (indexed by msg_id) in the channel index."""
        import time
        await self.share_config.update_one(
            {'_id': f'ch_index_{chat_id}'},
            {
                '$pull': {'entries': {'msg_id': entry['msg_id']}},
            },
            upsert=True
        )
        await self.share_config.update_one(
            {'_id': f'ch_index_{chat_id}'},
            {
                '$push': {'entries': entry},
                '$set': {'scanned_at': time.time()},
                '$inc': {'count': 1},
            },
            upsert=True
        )



    # Per-bot Users Tracker
    async def add_share_bot_user(self, bot_id: str, user_id: int):
        if not bot_id: return
        await self.share_config.update_one(
            {'_id': f'bot_{bot_id}'},
            {'$addToSet': {'users': user_id}},
            upsert=True
        )

    async def get_share_bot_users(self, bot_id: str) -> list:
        return (await self._bot_cfg(bot_id)).get('users', [])

    # save_share_link — access_hash allows Share Bot to rebuild peer cache at delivery time
    async def save_share_link(self, uuid_str: str, message_ids: list, source_chat,
                              protect: bool = True, access_hash: int = 0):
        doc = {
            '_id': uuid_str,
            'message_ids': message_ids,
            'source_chat': source_chat,
            'protect': protect,
            'access_hash': access_hash,
        }
        await self.share_links.update_one({'_id': uuid_str}, {'$set': doc}, upsert=True)


    async def get_share_link(self, uuid_str: str):
        return await self.share_links.find_one({'_id': uuid_str})
        
    async def get_sys_mode(self) -> str:
        doc = await self.opt.find_one({"_id": "SYS_MODE"})
        return doc.get("mode", "vps") if doc else "vps"
        
    async def set_sys_mode(self, mode: str):
        await self.opt.update_one({"_id": "SYS_MODE"}, {"$set": {"mode": mode}}, upsert=True)

    async def get_global_stats(self):
        import time
        doc = await self.stats.find_one({'_id': 'bot_stats'})
        if not doc:
            doc = {
                '_id': 'bot_stats',
                'live_forward': 0,
                'batch_forward': 0,
                'normal_forward': 0,
                'total_files_downloaded': 0,
                'total_files_uploaded': 0,
                'total_data_usage_bytes': 0,
                'bot_start_time': time.time()
            }
            await self.stats.insert_one(doc)
        return doc
        
    async def update_global_stats(self, **kwargs):
        """Pass fields to update as keyword arguments, e.g. update_global_stats(live_forward=1)"""
        if not kwargs: return
        await self.stats.update_one({'_id': 'bot_stats'}, {'$inc': kwargs}, upsert=True)
        
    async def reset_global_stats(self):
        import time
        await self.stats.update_one({'_id': 'bot_stats'}, {'$set': {
            'live_forward': 0,
            'batch_forward': 0,
            'normal_forward': 0,
            'total_files_downloaded': 0,
            'total_files_uploaded': 0,
            'total_data_usage_bytes': 0,
            'bot_start_time': time.time()
        }}, upsert=True)
        
    def new_user(self, id, name):
        return dict(
            id = id,
            name = name,
            ban_status=dict(
                is_banned=False,
                ban_reason="",
            ),
        )
      
    async def add_user(self, id, name):
        user = self.new_user(id, name)
        await self.col.insert_one(user)
    
    async def is_user_exist(self, id):
        user = await self.col.find_one({'id':int(id)})
        return bool(user)
    
    async def total_users_bots_count(self):
        bcount = await self.bot.count_documents({})
        count = await self.col.count_documents({"name": {"$exists": True}})
        return count, bcount

    async def total_channels(self):
        count = await self.chl.count_documents({})
        return count
    
    async def remove_ban(self, id):
        ban_status = dict(
            is_banned=False,
            ban_reason=''
        )
        await self.col.update_one({'id': id}, {'$set': {'ban_status': ban_status}})
    
    async def ban_user(self, user_id, ban_reason="No Reason"):
        ban_status = dict(
            is_banned=True,
            ban_reason=ban_reason
        )
        await self.col.update_one({'id': user_id}, {'$set': {'ban_status': ban_status}})

    async def get_ban_status(self, id):
        default = dict(
            is_banned=False,
            ban_reason=''
        )
        user = await self.col.find_one({'id':int(id)})
        if not user:
            return default
        return user.get('ban_status', default)

    async def get_all_users(self):
        return self.col.find({})
    
    async def delete_user(self, user_id):
        await self.col.delete_many({'id': int(user_id)})
 
    async def get_banned(self):
        users = self.col.find({'ban_status.is_banned': True})
        b_users = [user['id'] async for user in users]
        return b_users

    async def update_configs(self, id, configs):
        await self.col.update_one({'id': int(id)}, {'$set': {'configs': configs}})
         
    async def get_configs(self, id):
        default = {
            'caption': None,
            'duplicate': True,
            'download': False,
            'forward_tag': False,
            'file_size': 0,
            'size_limit': None,
            'extension': None,
            'keywords': None,
            'protect': None,
            'button': None,
            'menu_image_id': None,
            'db_uri': None,
            'duration': 0,
            'filters': {
               'poll': True,
               'text': True,
               'audio': True,
               'voice': True,
               'video': True,
               'photo': True,
               'document': True,
               'animation': True,
               'sticker': True,
               'rm_caption': False
            }
        }
        user = await self.col.find_one({'id':int(id)})
        if user:
            user_configs = user.get('configs', {})
            # Merge with default to ensure new fields are populated
            merged = default.copy()
            merged.update(user_configs)
            if 'filters' in user_configs:
                merged_filters = default['filters'].copy()
                merged_filters.update(user_configs['filters'])
                merged['filters'] = merged_filters
            return merged
        return default 
       
    async def add_bot(self, datas):
       is_bot = datas.get('is_bot', True)
       count = await self.bot.count_documents({'user_id': datas['user_id'], 'is_bot': is_bot})
       if count >= 2: return "LIMIT_REACHED"
       exists = await self.bot.find_one({'user_id': datas['user_id'], 'id': datas['id']})
       if exists: return "EXISTS"
       
       total = await self.bot.count_documents({'user_id': datas['user_id']})
       datas['active'] = True if total == 0 else False
       await self.bot.insert_one(datas)
       return True
    
    async def remove_bot(self, user_id, bot_id=None):
       if bot_id:
           await self.bot.delete_one({'user_id': int(user_id), 'id': int(bot_id)})
       else:
           await self.bot.delete_many({'user_id': int(user_id)})
      
    async def get_bot(self, user_id: int, bot_id=None):
       query = {'user_id': user_id}
       if bot_id: query['id'] = int(bot_id)
       bots = self.bot.find(query)
       bots_list = [b async for b in bots]
       if not bots_list: return None
       if bot_id: return bots_list[0]
       
       for b in bots_list:
           if b.get('active'): return b
       return bots_list[0]
                                          
    async def get_bots(self, user_id: int):
       bots = self.bot.find({'user_id': user_id})
       return [b async for b in bots]
       
    async def set_active_bot(self, user_id: int, bot_id: int):
        # Only deactivate accounts of the same type (bot or userbot), not all
        target = await self.bot.find_one({'user_id': user_id, 'id': int(bot_id)})
        if not target: return
        is_bot = target.get('is_bot', True)
        await self.bot.update_many({'user_id': user_id, 'is_bot': is_bot}, {'$set': {'active': False}})
        await self.bot.update_one({'user_id': user_id, 'id': int(bot_id)}, {'$set': {'active': True}})
     
    async def get_active_bot(self, user_id: int):
        """Get the active normal bot for this user."""
        bots = [b async for b in self.bot.find({'user_id': user_id, 'is_bot': True})]
        for b in bots:
            if b.get('active'): return b
        return bots[0] if bots else None

    async def get_active_userbot(self, user_id: int):
        """Get the active userbot for this user."""
        ubots = [b async for b in self.bot.find({'user_id': user_id, 'is_bot': False})]
        for b in ubots:
            if b.get('active'): return b
        return ubots[0] if ubots else None
                                          
    async def is_bot_exist(self, user_id):
       bot = await self.bot.find_one({'user_id': user_id})
       return bool(bot)
                                          
    async def in_channel(self, user_id: int, chat_id: int) -> bool:
       channel = await self.chl.find_one({"user_id": int(user_id), "chat_id": int(chat_id)})
       return bool(channel)
    
    async def add_channel(self, user_id: int, chat_id: int, title, username):
       channel = await self.in_channel(user_id, chat_id)
       if channel:
         return False
       return await self.chl.insert_one({"user_id": user_id, "chat_id": chat_id, "title": title, "username": username})
    
    async def remove_channel(self, user_id: int, chat_id: int):
       channel = await self.in_channel(user_id, chat_id )
       if not channel:
         return False
       return await self.chl.delete_many({"user_id": int(user_id), "chat_id": int(chat_id)})
    
    async def get_channel_details(self, user_id: int, chat_id: int):
       return await self.chl.find_one({"user_id": int(user_id), "chat_id": int(chat_id)})
       
    async def get_user_channels(self, user_id: int):
       channels = self.chl.find({"user_id": int(user_id)})
       return [channel async for channel in channels]
     
    async def get_filters(self, user_id):
       filters = []
       filter = (await self.get_configs(user_id))['filters']
       for k, v in filter.items():
          if v == False:
            filters.append(str(k))
       return filters
              
    async def add_frwd(self, user_id):
       return await self.nfy.insert_one({'user_id': int(user_id)})
    
    async def rmve_frwd(self, user_id=0, all=False):
       data = {} if all else {'user_id': int(user_id)}
       return await self.nfy.delete_many(data)
    
    async def get_all_frwd(self):
       return self.nfy.find({})
    async def get_language(self, user_id: int) -> str:
        """Return user's preferred language: 'en', 'hi', or 'hinglish'. Default 'en'."""
        user = await self.col.find_one({'id': int(user_id)})
        if user:
            return user.get('language', 'en')
        return 'en'

    async def set_language(self, user_id: int, lang: str):
        await self.col.update_one({'id': int(user_id)}, {'$set': {'language': lang}}, upsert=True)

    async def get_total_users_count(self) -> int:
        return await self.col.count_documents({})

    async def get_active_forwardings_count(self) -> int:
        """Count users who are currently running a forwarding task."""
        return await self.nfy.count_documents({})

    async def get_active_jobs_count(self) -> int:
        """Count running Live Jobs."""
        return await self.db.jobs.count_documents({'status': 'running'})

    # ── AI Enhancer Config ────────────────────────────────────────────────────
    async def get_enhancer_config(self) -> dict:
        """Returns the global AI Enhancer config dict."""
        doc = await self.stats.find_one({'_id': 'ai_enhancer_config'})
        return doc or {}

    async def update_enhancer_config(self, **kwargs):
        """Update one or more keys in the AI Enhancer config."""
        await self.stats.update_one(
            {'_id': 'ai_enhancer_config'},
            {'$set': kwargs},
            upsert=True
        )

db = Database(Config.DATABASE_URI, Config.DATABASE_NAME)
