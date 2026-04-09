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
    
    def store(self, From, to, skip, limit, continuous=False, reverse_order=False, bot_id=None, smart_order=True, from_thread=None):
        self.data[self.id] = {"FROM": From, 'TO': to, 'total_files': 0, 'skip': skip, 'limit': limit,
                      'fetched': skip, 'filtered': 0, 'deleted': 0, 'duplicate': 0, 'total': limit,
                      'start': 0, 'continuous': continuous, 'reverse_order': reverse_order, 'bot_id': bot_id,
                      'smart_order': smart_order, 'from_thread': from_thread}
        self.get(full=True)
        return STS(self.id)
        
    def get(self, value=None, full=False):
        values = self.data.get(self.id)
        if not full:
           return values.get(value)
        for k, v in values.items():
            setattr(self, k, v)
        return self

    def add(self, key=None, value=1, time=False, forward_type='normal'):
        if time:
          return self.data[self.id].update({'start': tm.time()})
        self.data[self.id].update({key: self.get(key) + value}) 
        if key == 'total_files':
            import asyncio
            stat_key = f"{forward_type}_forward"
            asyncio.create_task(db.update_global_stats(**{stat_key: value}))
    
    def divide(self, no, by):
       by = 1 if int(by) == 0 else by 
       return int(no) / by 
    
    async def get_data(self, user_id):
        k, filters = self, await db.get_filters(user_id)
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
        
        return bot, configs['caption'], configs['forward_tag'], {
            'download': configs.get('download', False), 'chat_id': k.FROM, 'limit': k.limit, 'offset': k.skip, 
            'filters': filters,                                   # list of disabled type names  
            'configs_filters': configs.get('filters', {}),        # full dict: rm_caption, links, etc.
            'rm_caption': configs.get('filters', {}).get('rm_caption', False),
            'keywords': configs['keywords'], 'media_size': size, 'extensions': configs['extension'], 
            'skip_duplicate': duplicate, 'duration': configs.get('duration', 1), 
            'reverse_order': getattr(k, 'reverse_order', False), 'smart_order': getattr(k, 'smart_order', True),
            'from_thread': getattr(k, 'from_thread', None),
            'replacements': configs.get('replacements', {})
        }, configs['protect'], button

def format_tg_error(e, context="Scan Error"):
    err_str = str(e)
    if "CHANNEL_PRIVATE" in err_str or "USER_BANNED" in err_str or "accessible" in err_str.lower():
        return f"<b>‣ {context}: Access Denied</b>\n<i>The Clone Client or Bot cannot access the channel. Please ensure it is added as an Admin with correct permissions.</i>\n\n<code>{err_str}</code>"
    if "CHAT_ADMIN_REQUIRED" in err_str:
        return f"<b>‣ {context}: Admin Required</b>\n<i>The bot requires Admin privileges to perform this action.</i>\n\n<code>{err_str}</code>"
    if "FLOOD_WAIT" in err_str or "420" in err_str:
        return f"<b>‣ {context}: Telegram API Rate Limit</b>\n<i>Too many requests. Please wait a few minutes before trying again.</i>\n\n<code>{err_str}</code>"
    return f"<b>‣ {context}:</b>\n<code>{err_str}</code>"


async def check_chat_protection(user_id: int, chat_id) -> str | None:
    """
    Checks if the source chat_id is protected from the given user_id.
    Returns an error message HTML string if blocked, else None.
    Enforces 'Last Protection' rules where owners/co-owners bypass checks,
    but normal users are blocked from explicitly protected chats OR any chat
    registered to an owner/co-owner account.
    """
    if not chat_id:
        return None
        
    try:
        chat_id_int = int(chat_id)
    except (ValueError, TypeError):
        chat_id_int = str(chat_id)

    from config import Config
    
    user_id_int = int(user_id)
    is_owner = (user_id_int in Config.BOT_OWNER_ID)
    is_co_owner = await db.is_co_owner(user_id_int)
    
    if is_owner or is_co_owner:
        return None

    # 1. Globally protected chats (explicitly added by owner)
    prot = await db.is_chat_protected(chat_id_int)
    if prot:
        reason_txt = prot.get('reason', '') or 'Owner has protected this chat.'
        title_txt  = prot.get('title', str(chat_id_int))
        return (
            f"🔒 Protection Active: <b>{title_txt}</b>\n\n"
            f"This source chat is protected by the owner and cannot be used "
            f"as a forwarding source.\n\n"
            f"<i>Reason: {reason_txt}</i>"
        )
        
    # 2. Implicit protection: Channels registered to Owner/Co-owners
    owners = list(Config.BOT_OWNER_ID) + await db.get_co_owners()
    for oid in set(owners):
        if await db.in_channel(oid, chat_id_int):
            return (
                f"🔒 Protection Active\n\n"
                f"This source chat belongs to the bot's administrators. "
                f"You are not allowed to forward content from it."
            )

    return None