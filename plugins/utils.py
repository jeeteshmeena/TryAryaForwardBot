import time as tm
from database import db 
from .test import parse_buttons

STATUS = {}

class STS:
    def __init__(self, id):
        self.id = id
        self.data = STATUS
    
    def verify(self):
        return self.data.get(self.id)
    
    def store(self, From, to, skip, limit, continuous=False, reverse_order=False, bot_id=None, smart_order=True):
        self.data[self.id] = {"FROM": From, 'TO': to, 'total_files': 0, 'skip': skip, 'limit': limit,
                      'fetched': skip, 'filtered': 0, 'deleted': 0, 'duplicate': 0, 'total': limit,
                      'start': 0, 'continuous': continuous, 'reverse_order': reverse_order, 'bot_id': bot_id,
                      'smart_order': smart_order}
        self.get(full=True)
        return STS(self.id)
        
    def get(self, value=None, full=False):
        values = self.data.get(self.id)
        if not full:
           return values.get(value)
        for k, v in values.items():
            setattr(self, k, v)
        return self

    def add(self, key=None, value=1, time=False):
        if time:
          return self.data[self.id].update({'start': tm.time()})
        self.data[self.id].update({key: self.get(key) + value}) 
    
    def divide(self, no, by):
       by = 1 if int(by) == 0 else by 
       return int(no) / by 
    
    async def get_data(self, user_id):
        k, filters = self, await db.get_filters(user_id)
        flgs = await db.get_filter_flags(user_id)
        size, configs = None, await db.get_configs(user_id)
        
        # New explicit selection feature: Use the bot_id selected via the UI, else fallback to active
        explicit_bot_id = getattr(k, 'bot_id', None)
        bots = await db.get_bots(user_id)
        bot = None
        
        if explicit_bot_id:
            # Look up the exact account chosen by the user in the UI
            chosen = await db.get_bot(user_id, explicit_bot_id)
            if chosen:
                bot = chosen
                    
        # Fallback to active bot if explicit fails or isn't provided
        if not bot:
            for b in bots:
                if b.get('active'): bot = b
            if bot is None and bots: bot = bots[0]

        if configs['duplicate']:
           duplicate = [configs['db_uri'], self.TO]
        else:
           duplicate = False
        button = parse_buttons(configs['button'] if configs['button'] else '')
        if configs['file_size'] != 0:
            size = [configs['file_size'], configs['size_limit']]
        return bot, configs['caption'], configs['forward_tag'], {'download': configs.get('download', False), 'chat_id': k.FROM, 'limit': k.limit, 'offset': k.skip, 'filters': filters,
                'keywords': configs['keywords'], 'media_size': size, 'extensions': configs['extension'], 'skip_duplicate': duplicate, 'duration': configs.get('duration', 1), 'reverse_order': getattr(k, 'reverse_order', False), 'smart_order': getattr(k, 'smart_order', True),
                'rm_caption': flgs.get('rm_caption', False), 'block_links': flgs.get('block_links', False)}, configs['protect'], button
