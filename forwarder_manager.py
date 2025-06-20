"""
Forwarder Manager

This module handles message forwarding between Telegram chats and member management.
"""
import asyncio
import logging
import json
import time
from typing import Dict, List, Optional, Tuple, Union, Any, Set, Callable, Coroutine
from pathlib import Path
from datetime import datetime, timedelta
from telethon import TelegramClient, events, types
from telethon.tl.functions.channels import GetParticipantsRequest, InviteToChannelRequest, DeleteMessagesRequest
from telethon.tl.functions.messages import AddChatUserRequest, GetDialogsRequest
from telethon.tl.types import InputPeerChannel, InputPeerUser, ChannelParticipantsSearch, User, Channel, Chat, InputChannel, PeerChannel, PeerChat
from telethon.errors import (
    FloodWaitError, UserNotParticipantError, UserPrivacyRestrictedError,
    UserAlreadyParticipantError, ChatAdminRequiredError, ChannelPrivateError,
    ChatWriteForbiddenError, ChannelInvalidError, UserIdInvalidError,
    UserKickedError, ChatIdInvalidError, UserChannelsTooMuchError,
    UserNotMutualContactError, InviteHashInvalidError, InviteHashExpiredError,
    InviteRequestSentError, UserBlockedError, InputUserDeactivatedError
)

from account_manager import TelegramAccountManager, AccountError, AccountNotFoundError, FloodWaitError as AMFloodWaitError

logger = logging.getLogger(__name__)

class ForwarderError(Exception):
    """Base exception for forwarder-related errors."""
    pass

class ForwarderManager:
    """
    Manages message forwarding between Telegram chats and member management.
    """
    
    def __init__(self, account_manager: TelegramAccountManager, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the forwarder manager.
        
        Args:
            account_manager: Instance of TelegramAccountManager
            config: Configuration dictionary
        """
        self.account_manager = account_manager
        self.config = config or {}
        self.running = False
        self.forwarding_tasks: Dict[str, asyncio.Task] = {}
        self._forwarding_locks: Dict[str, asyncio.Lock] = {}
        self._member_adding_locks: Dict[str, asyncio.Lock] = {}
        self._scraping_locks: Dict[str, asyncio.Lock] = {}
        self._last_forward_time: Dict[str, float] = {}
        self._daily_counts: Dict[str, int] = {}
        self._last_reset_time = time.time()
        
        # Load forwarding rules from config
        self.forwarding_rules: List[Dict[str, Any]] = self.config.get('forwarding_rules', [])
        
        # Setup periodic tasks
        self._periodic_tasks: List[asyncio.Task] = []
        
        # Setup logging
        self._setup_logging()
    
    def _setup_logging(self):
        """Configure logging for the forwarder manager."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('forwarder.log'),
                logging.StreamHandler()
            ]
        )
    
    async def start(self):
        """Start the forwarder manager and all forwarding tasks."""
        if self.running:
            logger.warning("Forwarder manager is already running")
            return False
        
        logger.info("🚀 Starting Forwarder Manager...")
        self.running = True
        
        # Start periodic tasks
        self._periodic_tasks = [
            asyncio.create_task(self._periodic_reset_daily_counts()),
            asyncio.create_task(self._periodic_check_connections())
        ]
        
        # Start forwarding for all rules
        for rule in self.forwarding_rules:
            asyncio.create_task(self._start_forwarding(rule))
        
        logger.info("✅ Forwarder Manager started successfully")
        return True
    
    async def stop(self):
        """Stop the forwarder manager and all forwarding tasks."""
        if not self.running:
            return False
        
        logger.info("🛑 Stopping Forwarder Manager...")
        self.running = False
        
        # Cancel all periodic tasks
        for task in self._periodic_tasks:
            task.cancel()
        
        # Cancel all forwarding tasks
        for task in self.forwarding_tasks.values():
            task.cancel()
        
        # Wait for all tasks to complete
        await asyncio.gather(
            *self._periodic_tasks + list(self.forwarding_tasks.values()),
            return_exceptions=True
        )
        
        logger.info("✅ Forwarder Manager stopped successfully")
        return True
    
    async def _periodic_reset_daily_counts(self):
        """Reset daily counts every 24 hours."""
        while self.running:
            try:
                await asyncio.sleep(86400)  # 24 hours
                self._daily_counts.clear()
                self._last_reset_time = time.time()
                logger.info("♻️ Reset daily message and member add counts")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in periodic reset: {e}", exc_info=True)
                await asyncio.sleep(60)  # Retry after delay
    
    async def _periodic_check_connections(self):
        """Periodically check and maintain account connections."""
        while self.running:
            try:
                await asyncio.sleep(300)  # 5 minutes
                accounts = self.account_manager.get_all_accounts_status()
                for account in accounts:
                    if not account.get('is_connected', False):
                        logger.warning(f"Account {account.get('phone')} is disconnected. Attempting to reconnect...")
                        await self.account_manager.reconnect_account(account['phone'])
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in connection check: {e}", exc_info=True)
                await asyncio.sleep(60)  # Retry after delay
    
    async def _start_forwarding(self, rule: Dict[str, Any]):
        """
        Start forwarding messages based on the given rule.
        
        Args:
            rule: Dictionary containing forwarding rule configuration
        """
        rule_id = rule.get('id')
        if not rule_id:
            logger.error("Forwarding rule is missing 'id'")
            return
        
        if rule_id in self.forwarding_tasks:
            logger.warning(f"Forwarding rule {rule_id} is already running")
            return
        
        logger.info(f"🚀 Starting forwarding rule: {rule_id}")
        
        # Create a lock for this rule if it doesn't exist
        if rule_id not in self._forwarding_locks:
            self._forwarding_locks[rule_id] = asyncio.Lock()
        
        # Create and store the forwarding task
        self.forwarding_tasks[rule_id] = asyncio.create_task(self._forward_messages(rule))
    
    async def _forward_messages(self, rule: Dict[str, Any]):
        """
        Forward messages according to the given rule.
        
        Args:
            rule: Dictionary containing forwarding rule configuration
        """
        rule_id = rule.get('id')
        source_chats = rule.get('source_chats', [])
        target_chats = rule.get('target_chats', [])
        filters = rule.get('filters', {})
        
        if not source_chats or not target_chats:
            logger.error(f"Rule {rule_id}: No source or target chats specified")
            return
        
        logger.info(f"📡 Setting up forwarding for rule {rule_id}: {len(source_chats)} sources -> {len(target_chats)} targets")
        
        while self.running:
            try:
                # Get an available account for this rule
                account = await self._get_available_account(rule_id)
                if not account:
                    logger.warning(f"No available accounts for rule {rule_id}. Retrying in 60 seconds...")
                    await asyncio.sleep(60)
                    continue
                
                # Connect the account if not already connected
                if not account.get('is_connected', False):
                    success, message = await self.account_manager.connect_account(account['phone'])
                    if not success:
                        logger.error(f"Failed to connect account {account['phone']}: {message}")
                        await asyncio.sleep(60)
                        continue
                
                # Setup event handlers for this account
                client = self.account_manager._clients.get(account['phone'])
                if not client:
                    logger.error(f"No client found for account {account['phone']}")
                    await asyncio.sleep(60)
                    continue
                
                # Process each source chat
                for source in source_chats:
                    try:
                        await self._process_source_chat(account, client, source, target_chats, filters, rule_id)
                    except Exception as e:
                        logger.error(f"Error processing source chat {source}: {e}", exc_info=True)
                
                # Small delay between processing different sources
                await asyncio.sleep(5)
                
            except asyncio.CancelledError:
                logger.info(f"Forwarding task for rule {rule_id} was cancelled")
                raise
            except Exception as e:
                logger.error(f"Error in forwarding task {rule_id}: {e}", exc_info=True)
                await asyncio.sleep(60)  # Wait before retrying
    
    async def _process_source_chat(self, account: Dict[str, Any], client: TelegramClient, 
                                 source: Union[str, int], target_chats: List[Union[str, int]], 
                                 filters: Dict[str, Any], rule_id: str):
        """Process messages from a source chat and forward them to target chats."""
        try:
            # Get the source chat entity
            try:
                source_entity = await client.get_entity(source)
                source_id = getattr(source_entity, 'id', None)
                if not source_id:
                    logger.error(f"Could not get ID for source: {source}")
                    return
                
                # Get recent messages
                messages = await client.get_messages(source_entity, limit=10)
                
                # Process each message
                for message in messages:
                    if not message:
                        continue
                    
                    # Apply filters
                    if not self._message_matches_filters(message, filters):
                        continue
                    
                    # Forward to each target chat
                    for target in target_chats:
                        try:
                            await self._forward_message(account, client, message, target, rule_id)
                            await asyncio.sleep(1)  # Small delay between forwards
                        except Exception as e:
                            logger.error(f"Error forwarding message to {target}: {e}", exc_info=True)
            except ValueError as e:
                if "Could not find the entity" in str(e):
                    logger.error(f"Could not find source chat: {source}")
                else:
                    raise
        except Exception as e:
            logger.error(f"Error processing source chat {source}: {e}", exc_info=True)
    
    async def _forward_message(self, account: Dict[str, Any], client: TelegramClient, 
                            message: types.Message, target: Union[str, int], rule_id: str):
        """Forward a single message to the target chat."""
        try:
            # Get the target entity
            target_entity = await client.get_entity(target)
            
            # Check rate limits
            await self._check_rate_limits(account['phone'], rule_id)
            
            # Forward the message
            await client.forward_messages(target_entity, message)
            
            # Update last forward time and count
            self._last_forward_time[account['phone']] = time.time()
            self._daily_counts[account['phone']] = self._daily_counts.get(account['phone'], 0) + 1
            
            logger.info(f"✅ Forwarded message from {message.chat_id} to {getattr(target_entity, 'title', target_entity.id)}")
            
        except FloodWaitError as e:
            logger.warning(f"Flood wait for {e.seconds} seconds. Waiting...")
            await asyncio.sleep(e.seconds)
            # Retry the forward
            await self._forward_message(account, client, message, target, rule_id)
        except Exception as e:
            logger.error(f"Error forwarding message: {e}", exc_info=True)
            raise
    
    async def _get_available_account(self, rule_id: str) -> Optional[Dict[str, Any]]:
        """Get an available account for the given rule."""
        accounts = self.account_manager.get_all_accounts_status()
        
        # Filter available accounts
        available_accounts = [
            acc for acc in accounts 
            if acc.get('is_connected', False) and 
            acc.get('status') == 'active' and 
            not acc.get('is_limited', False)
        ]
        
        if not available_accounts:
            return None
        
        # Simple round-robin selection
        if rule_id not in self._last_used_accounts:
            self._last_used_accounts[rule_id] = 0
        
        idx = self._last_used_accounts[rule_id] % len(available_accounts)
        self._last_used_accounts[rule_id] = idx + 1
        
        return available_accounts[idx]
    
    def _message_matches_filters(self, message: types.Message, filters: Dict[str, Any]) -> bool:
        """Check if a message matches the given filters."""
        if not filters:
            return True
        
        # Text filter
        if 'text' in filters and message.text:
            text_filter = filters['text'].lower()
            if text_filter not in message.text.lower():
                return False
        
        # Sender filter
        if 'senders' in filters and message.sender_id:
            sender_id = str(message.sender_id)
            if sender_id not in filters['senders']:
                return False
        
        # Media type filter
        if 'media_types' in filters:
            media_type = self._get_media_type(message)
            if media_type and media_type not in filters['media_types']:
                return False
        
        # Date range filter
        if 'date_range' in filters:
            msg_date = message.date.replace(tzinfo=None)
            start_date = filters['date_range'].get('start')
            end_date = filters['date_range'].get('end')
            
            if start_date and msg_date < start_date:
                return False
            if end_date and msg_date > end_date:
                return False
        
        return True
    
    def _get_media_type(self, message: types.Message) -> Optional[str]:
        """Get the media type of a message."""
        if not message.media:
            return None
            
        if hasattr(message.media, 'document'):
            return 'document'
        elif hasattr(message.media, 'photo'):
            return 'photo'
        elif hasattr(message.media, 'audio'):
            return 'audio'
        elif hasattr(message.media, 'video'):
            return 'video'
        elif hasattr(message.media, 'voice'):
            return 'voice'
        elif hasattr(message.media, 'sticker'):
            return 'sticker'
        elif hasattr(message.media, 'gif'):
            return 'gif'
        
        return 'unknown'
    
    async def _check_rate_limits(self, phone: str, rule_id: str):
        """Check and enforce rate limits for the given account and rule."""
        # Get rule-specific rate limits
        rule = next((r for r in self.forwarding_rules if r.get('id') == rule_id), {})
        
        # Get rate limits from rule or use defaults
        min_delay = rule.get('min_delay', self.config.get('min_delay', 1))
        max_daily = rule.get('max_daily', self.config.get('max_daily', 100))
        
        # Check daily limit
        daily_count = self._daily_counts.get(phone, 0)
        if daily_count >= max_daily:
            raise Exception(f"Daily limit of {max_daily} messages reached for account {phone}")
        
        # Enforce minimum delay between messages
        last_time = self._last_forward_time.get(phone, 0)
        elapsed = time.time() - last_time
        if elapsed < min_delay:
            await asyncio.sleep(min_delay - elapsed)
    
    # Member Management Methods
    
    async def scrape_members(self, chat_id: Union[str, int], limit: int = 200) -> List[Dict[str, Any]]:
        """
        Scrape members from a chat or channel.
        
        Args:
            chat_id: ID or username of the chat/channel
            limit: Maximum number of members to scrape
            
        Returns:
            List of member information dictionaries
        """
        account = await self._get_available_account('scrape')
        if not account:
            raise ForwarderError("No available accounts for scraping")
        
        client = self.account_manager._clients.get(account['phone'])
        if not client:
            raise ForwarderError(f"No client found for account {account['phone']}")
        
        try:
            # Get the chat entity
            chat = await client.get_entity(chat_id)
            
            # Get participants
            participants = []
            offset = 0
            batch_size = min(limit, 200)  # Telegram API limit
            
            while len(participants) < limit:
                result = await client(GetParticipantsRequest(
                    channel=chat,
                    filter=ChannelParticipantsSearch(''),
                    offset=offset,
                    limit=batch_size,
                    hash=0
                ))
                
                if not result.users:
                    break
                
                for user in result.users:
                    participants.append({
                        'id': user.id,
                        'username': user.username,
                        'first_name': user.first_name,
                        'last_name': user.last_name,
                        'phone': user.phone,
                        'is_bot': user.bot,
                        'is_verified': user.verified,
                        'is_restricted': user.restricted,
                        'is_scam': user.scam,
                        'is_fake': user.fake,
                        'status': str(user.status) if hasattr(user, 'status') else None
                    })
                
                offset += len(result.users)
                if len(result.users) < batch_size:
                    break
                
                # Small delay to avoid rate limits
                await asyncio.sleep(1)
            
            return participants[:limit]
            
        except Exception as e:
            logger.error(f"Error scraping members: {e}", exc_info=True)
            raise ForwarderError(f"Failed to scrape members: {e}")
    
    async def add_members(self, source_chat_id: Union[str, int], 
                         target_chat_id: Union[str, int], 
                         user_ids: List[Union[int, str]],
                         delay: int = 5) -> Dict[str, Any]:
        """
        Add members from one chat to another.
        
        Args:
            source_chat_id: ID or username of the source chat
            target_chat_id: ID or username of the target chat
            user_ids: List of user IDs or usernames to add
            delay: Delay between adding members (in seconds)
            
        Returns:
            Dictionary with results and statistics
        """
        account = await self._get_available_account('add_members')
        if not account:
            raise ForwarderError("No available accounts for adding members")
        
        client = self.account_manager._clients.get(account['phone'])
        if not client:
            raise ForwarderError(f"No client found for account {account['phone']}")
        
        results = {
            'total': len(user_ids),
            'success': 0,
            'failed': 0,
            'errors': {}
        }
        
        try:
            # Get the target chat entity
            target_chat = await client.get_entity(target_chat_id)
            
            # Add each user
            for user_id in user_ids:
                try:
                    # Get user entity
                    user = await client.get_entity(user_id)
                    
                    # Add user to target chat
                    await client(AddChatUserRequest(
                        user_id=user.id,
                        fwd_limit=0,  # No forwarded messages
                        target_chat=target_chat.id
                    ))
                    
                    results['success'] += 1
                    logger.info(f"✅ Added user {user_id} to chat {target_chat_id}")
                    
                    # Delay between adds to avoid rate limits
                    await asyncio.sleep(delay)
                    
                except UserNotParticipantError:
                    error_msg = f"User {user_id} is not a member of the source chat"
                    logger.warning(error_msg)
                    results['errors'][str(user_id)] = error_msg
                    results['failed'] += 1
                    
                except UserPrivacyRestrictedError:
                    error_msg = f"User {user_id} has privacy restrictions"
                    logger.warning(error_msg)
                    results['errors'][str(user_id)] = error_msg
                    results['failed'] += 1
                    
                except UserAlreadyParticipantError:
                    error_msg = f"User {user_id} is already in the target chat"
                    logger.warning(error_msg)
                    results['errors'][str(user_id)] = error_msg
                    results['failed'] += 1
                    
                except FloodWaitError as e:
                    error_msg = f"Flood wait for {e.seconds} seconds"
                    logger.warning(f"{error_msg}. Waiting...")
                    results['errors'][str(user_id)] = error_msg
                    results['failed'] += 1
                    await asyncio.sleep(e.seconds)
                    # Retry this user after waiting
                    continue
                    
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Error adding user {user_id}: {error_msg}", exc_info=True)
                    results['errors'][str(user_id)] = error_msg
                    results['failed'] += 1
                    
                    # If it's a critical error, stop the operation
                    if isinstance(e, (ChatAdminRequiredError, ChannelPrivateError, 
                                    ChatWriteForbiddenError, ChannelInvalidError)):
                        raise
            
            return results
            
        except Exception as e:
            logger.error(f"Error in add_members: {e}", exc_info=True)
            raise ForwarderError(f"Failed to add members: {e}")
    
    async def get_chat_info(self, chat_id: Union[str, int]) -> Dict[str, Any]:
        """
        Get information about a chat or channel.
        
        Args:
            chat_id: ID or username of the chat/channel
            
        Returns:
            Dictionary with chat information
        """
        account = await self._get_available_account('info')
        if not account:
            raise ForwarderError("No available accounts for getting chat info")
        
        client = self.account_manager._clients.get(account['phone'])
        if not client:
            raise ForwarderError(f"No client found for account {account['phone']}")
        
        try:
            # Get the chat entity
            chat = await client.get_entity(chat_id)
            
            # Get basic info
            info = {
                'id': chat.id,
                'title': getattr(chat, 'title', None),
                'username': getattr(chat, 'username', None),
                'type': 'channel' if isinstance(chat, types.Channel) else 'group' if isinstance(chat, types.Chat) else 'private',
                'participants_count': getattr(chat, 'participants_count', None),
                'is_verified': getattr(chat, 'verified', False),
                'is_restricted': getattr(chat, 'restricted', False),
                'is_scam': getattr(chat, 'scam', False),
                'is_fake': getattr(chat, 'fake', False),
                'about': getattr(chat, 'about', None),
                'photo': str(chat.photo) if hasattr(chat, 'photo') and chat.photo else None
            }
            
            # Get admin list if possible
            try:
                if hasattr(chat, 'admin_rights'):
                    info['admin_rights'] = str(chat.admin_rights)
                elif hasattr(chat, 'creator'):
                    info['is_creator'] = chat.creator
            except Exception as e:
                logger.warning(f"Could not get admin info: {e}")
            
            return info
            
        except Exception as e:
            logger.error(f"Error getting chat info: {e}", exc_info=True)
            raise ForwarderError(f"Failed to get chat info: {e}")
    
    async def send_message(self, chat_id: Union[str, int], text: str, 
                          parse_mode: str = 'md', 
                          buttons: Optional[List[List[Dict[str, str]]]] = None) -> bool:
        """
        Send a message to a chat.
        
        Args:
            chat_id: ID or username of the target chat
            text: Message text
            parse_mode: Parse mode ('md' or 'html')
            buttons: Optional list of button rows
            
        Returns:
            bool: True if message was sent successfully
        """
        account = await self._get_available_account('send_message')
        if not account:
            raise ForwarderError("No available accounts for sending messages")
        
        client = self.account_manager._clients.get(account['phone'])
        if not client:
            raise ForwarderError(f"No client found for account {account['phone']}")
        
        try:
            # Get the chat entity
            chat = await client.get_entity(chat_id)
            
            # Prepare buttons if provided
            reply_markup = None
            if buttons:
                from telethon.tl.types import KeyboardButton, ReplyKeyboardMarkup, ReplyInlineMarkup, KeyboardButtonCallback
                
                kb_buttons = []
                for row in buttons:
                    kb_row = []
                    for btn in row:
                        if btn.get('url'):
                            kb_row.append(KeyboardButtonUrl(btn['text'], btn['url']))
                        elif btn.get('callback'):
                            kb_row.append(KeyboardButtonCallback(btn['text'], btn['callback'].encode()))
                        else:
                            kb_row.append(KeyboardButton(btn['text']))
                    kb_buttons.append(kb_row)
                
                # Use inline keyboard for URLs/callbacks, reply keyboard otherwise
                if any(btn.get('url') or btn.get('callback') for row in buttons for btn in row):
                    reply_markup = ReplyInlineMarkup(kb_buttons)
                else:
                    reply_markup = ReplyKeyboardMarkup(kb_buttons)
            
            # Send the message
            await client.send_message(
                entity=chat,
                message=text,
                parse_mode=parse_mode,
                buttons=reply_markup
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error sending message: {e}", exc_info=True)
            raise ForwarderError(f"Failed to send message: {e}")

# Example usage
async def example():
    # Initialize account manager
    account_manager = TelegramAccountManager({
        'api_id': 'your_api_id',
        'api_hash': 'your_api_hash',
        'sessions_path': 'sessions',
        'logs_path': 'logs'
    })
    
    # Initialize forwarder manager
    forwarder = ForwarderManager(account_manager, {
        'min_delay': 1,  # seconds between messages
        'max_daily': 100,  # max messages per day per account
        'forwarding_rules': [
            {
                'id': 'rule1',
                'source_chats': ['source_chat_username'],
                'target_chats': ['target_chat_username'],
                'filters': {
                    'text': 'important',  # only forward messages containing 'important'
                    'media_types': ['photo', 'document']  # only forward photos and documents
                }
            }
        ]
    })
    
    try:
        # Start the forwarder
        await forwarder.start()
        
        # Keep the script running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        # Cleanup
        await forwarder.stop()
        await account_manager.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(example())
