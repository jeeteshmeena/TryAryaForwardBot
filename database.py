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
        self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self.db = self._client[database_name]
        self.bot = self.db.bots
        self.col = self.db.users
        self.nfy = self.db.notify
        self.chl = self.db.channels 
        
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
        count = await self.col.count_documents({})
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

db = Database(Config.DATABASE_URI, Config.DATABASE_NAME)
