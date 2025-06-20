"""
Member Manager (Formerly Forwarder Manager)

This module handles member management for Telegram chats (scraping, adding).
Message forwarding capabilities have been removed.
"""
import asyncio
import logging
import json
import time
from typing import Dict, List, Optional, Tuple, Union, Any, Set, Callable, Coroutine
from pathlib import Path
from datetime import datetime, timedelta
from telethon import TelegramClient, events, types
from telethon.tl.functions.channels import GetParticipantsRequest, InviteToChannelRequest # Removed DeleteMessagesRequest
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

logger = logging.getLogger(__name__) # Use module-level logger

class MemberManagerError(Exception): # Renamed from ForwarderError
    """Base exception for member manager-related errors."""
    pass

class MemberManager: # Renamed from ForwarderManager
    """
    Manages member operations for Telegram chats (scraping, adding).
    """

    def __init__(self, account_manager: TelegramAccountManager, config: Optional[Dict[str, Any]] = None):
        self.account_manager = account_manager
        self.config = config or {}
        self.running = False # Indicates if periodic tasks are running

        # Attributes for member management
        self._member_adding_locks: Dict[str, asyncio.Lock] = {}
        self._scraping_locks: Dict[str, asyncio.Lock] = {}
        self._daily_counts: Dict[str, int] = {} # For daily limits (e.g., members added)
        self._last_reset_time = time.time()
        self._last_used_accounts: Dict[str, int] = {}

        self._periodic_tasks: List[asyncio.Task] = []
        self._setup_logging()

    def _setup_logging(self):
        # Consider if this is needed if main.py configures root logger.
        # For now, it's kept but could be removed for cleaner library behavior.
        log_file = Path(self.config.get('logs_path', 'logs')) / 'member_manager.log'
        log_file.parent.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            level=self.config.get('log_level', logging.INFO),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        # Reduce Telethon's verbosity unless specifically debugging it
        logging.getLogger('telethon').setLevel(self.config.get('telethon_log_level', logging.WARNING))

    async def start(self):
        """Start periodic tasks for the member manager (e.g., daily count reset)."""
        if self.running:
            logger.warning("Member manager is already running its periodic tasks.")
            return False

        logger.info("🚀 Starting Member Manager periodic tasks...")
        self.running = True

        self._periodic_tasks = [
            asyncio.create_task(self._periodic_reset_daily_counts()),
            # _periodic_check_connections was removed as it's more of a general account health check,
            # better suited for AccountManager or a higher-level application controller if needed.
        ]

        logger.info("✅ Member Manager periodic tasks started successfully.")
        return True

    async def stop(self):
        """Stop periodic tasks for the member manager."""
        if not self.running:
            logger.warning("Member manager periodic tasks are not running.")
            return False

        logger.info("🛑 Stopping Member Manager periodic tasks...")
        self.running = False

        for task in self._periodic_tasks:
            if task and not task.done():
                task.cancel()

        await asyncio.gather(*self._periodic_tasks, return_exceptions=True)
        self._periodic_tasks.clear()

        logger.info("✅ Member Manager periodic tasks stopped successfully.")
        return True

    async def _periodic_reset_daily_counts(self):
        """Reset daily counts (e.g., for member additions) every 24 hours."""
        while self.running:
            try:
                # Calculate seconds until next midnight or a fixed interval
                # For simplicity, using a fixed 24-hour interval from last reset.
                now = time.time()
                seconds_in_day = 86400
                elapsed_today = (now - self._last_reset_time) % seconds_in_day
                sleep_duration = seconds_in_day - elapsed_today

                await asyncio.sleep(sleep_duration)
                self._daily_counts.clear()
                self._last_reset_time = time.time() # Update reset time
                logger.info("♻️ Reset daily member add counts.")
            except asyncio.CancelledError:
                logger.info("Periodic daily count reset task cancelled.")
                break # Exit loop on cancellation
            except Exception as e:
                logger.error(f"Error in periodic daily count reset: {e}", exc_info=True)
                await asyncio.sleep(3600) # Retry after an hour if an error occurs

    async def _get_available_account(self, purpose: str) -> Optional[Dict[str, Any]]:
        """ Get an available account using account_manager. """
        return await self.account_manager.get_available_account(purpose)

    async def scrape_members(self, chat_id: Union[str, int], limit: int = 2000) -> List[Dict[str, Any]]: # Increased default limit
        # Ensure a lock for this specific chat_id to prevent concurrent scrapes
        if chat_id not in self._scraping_locks:
            self._scraping_locks[str(chat_id)] = asyncio.Lock()

        async with self._scraping_locks[str(chat_id)]:
            account_dict = await self._get_available_account(f'scrape_members_{chat_id}')
            if not account_dict:
                raise MemberManagerError(f"No available Telegram accounts for scraping members from {chat_id}.")

            phone = account_dict['phone']
            client = self.account_manager._clients.get(phone) # _clients stores active clients

            if not client or not client.is_connected():
                logger.info(f"Client for {phone} not connected for scraping {chat_id}. Attempting connect.")
                success, msg = await self.account_manager.connect_account(phone)
                if not success: raise MemberManagerError(f"Failed to connect account {phone} for scraping: {msg}")
                client = self.account_manager._clients.get(phone) # Re-fetch after connect
                if not client: raise MemberManagerError(f"Client for {phone} unavailable after connect attempt.")

            try:
                logger.info(f"Attempting to scrape members from '{chat_id}' using account {phone}.")
                chat_entity = await client.get_entity(chat_id)

                participants_data = []
                offset = 0
                while len(participants_data) < limit:
                    current_batch_limit = min(200, limit - len(participants_data))
                    if current_batch_limit <= 0: break

                    participants_batch = await client(GetParticipantsRequest(
                        channel=chat_entity, filter=ChannelParticipantsSearch(''),
                        offset=offset, limit=current_batch_limit, hash=0
                    ))

                    if not participants_batch.users: break

                    for user in participants_batch.users:
                        participants_data.append({
                            'id': user.id, 'username': user.username,
                            'first_name': user.first_name, 'last_name': user.last_name,
                            'phone': user.phone, 'is_bot': user.bot,
                            'status': str(user.status) if hasattr(user, 'status') and user.status else None
                        })
                    offset += len(participants_batch.users)
                    logger.debug(f"Scraped {len(participants_batch.users)} users, total now {len(participants_data)} from {chat_id}")
                    if len(participants_batch.users) < current_batch_limit: break
                    if len(participants_data) < limit: await asyncio.sleep(self.config.get('scrape_batch_delay_seconds', 1))

                logger.info(f"Successfully scraped {len(participants_data)} members from {chat_id} using {phone}.")
                return participants_data[:limit]

            except ValueError as e: # Specific error for entity not found
                 logger.error(f"Could not find chat/channel '{chat_id}' using account {phone}: {e}")
                 raise MemberManagerError(f"Chat/channel '{chat_id}' not found or account has no access.")
            except AMFloodWaitError as e: # Custom FloodWaitError from AccountManager (if it raises that)
                 logger.warning(f"Flood wait during scraping from {chat_id} with {phone}: {e.seconds}s. Aborting this attempt.")
                 raise MemberManagerError(f"Operation halted due to flood wait ({e.seconds}s) on account {phone}.")
            except FloodWaitError as e: # Telethon's FloodWaitError
                 logger.warning(f"Telethon Flood wait during scraping from {chat_id} with {phone}: {e.seconds}s. Aborting this attempt.")
                 raise MemberManagerError(f"Operation halted due to flood wait ({e.seconds}s) on account {phone}.")
            except Exception as e:
                logger.error(f"Error scraping members from {chat_id} using {phone}: {e}", exc_info=True)
                raise MemberManagerError(f"Failed to scrape members from {chat_id}: {type(e).__name__} - {e}")

    async def add_members(self, target_chat_id: Union[str, int],
                         user_ids_to_add: List[Union[int, str]],
                         delay_seconds: int = 10) -> Dict[str, Any]: # Increased default delay
        # Ensure a lock for this specific target_chat_id
        lock_key = str(target_chat_id)
        if lock_key not in self._member_adding_locks:
            self._member_adding_locks[lock_key] = asyncio.Lock()

        async with self._member_adding_locks[lock_key]:
            account_dict = await self._get_available_account(f'add_members_{target_chat_id}')
            if not account_dict: raise MemberManagerError(f"No available Telegram accounts for adding members to {target_chat_id}.")

            phone = account_dict['phone']
            client = self.account_manager._clients.get(phone)
            if not client or not client.is_connected():
                logger.info(f"Client for {phone} not connected for adding members to {target_chat_id}. Attempting connect.")
                success, msg = await self.account_manager.connect_account(phone)
                if not success: raise MemberManagerError(f"Failed to connect account {phone} for adding: {msg}")
                client = self.account_manager._clients.get(phone)
                if not client: raise MemberManagerError(f"Client for {phone} unavailable after connect attempt.")

            results = {'total_attempted': len(user_ids_to_add), 'successful': 0, 'failed': 0, 'already_participant': 0, 'privacy_restricted':0, 'errors': {}}

            try:
                logger.info(f"Attempting to add members to '{target_chat_id}' using account {phone}.")
                target_entity = await client.get_entity(target_chat_id)

                for i, user_id_or_username in enumerate(user_ids_to_add):
                    try:
                        # Resolve user ID/username to InputUser
                        user_to_add_input = await client.get_input_entity(user_id_or_username)

                        logger.debug(f"Attempting to add {user_id_or_username} to {target_chat_id}...")
                        # Using InviteToChannelRequest for both channels and megagroups (most common)
                        # For basic groups, AddChatUserRequest might be needed, but InviteToChannelRequest is more versatile.
                        await client(InviteToChannelRequest(channel=target_entity, users=[user_to_add_input]))
                        results['successful'] += 1
                        logger.info(f"Successfully sent invite for {user_id_or_username} to {target_chat_id} using {phone}.")

                    except UserAlreadyParticipantError:
                        logger.info(f"User {user_id_or_username} is already a participant in {target_chat_id}.")
                        results['already_participant'] += 1
                    except (UserPrivacyRestrictedError, UserNotMutualContactError):
                        logger.warning(f"Cannot add {user_id_or_username} to {target_chat_id} due to privacy restrictions.")
                        results['privacy_restricted'] += 1
                        results['failed'] +=1; results['errors'][str(user_id_or_username)] = "PrivacyRestricted/NotMutual"
                    except (UserBlockedError, InputUserDeactivatedError, UserIdInvalidError, UserKickedError):
                        logger.warning(f"Cannot add {user_id_or_username}: User issue (blocked, deactivated, invalid, or kicked).")
                        results['failed'] +=1; results['errors'][str(user_id_or_username)] = "UserBlocked/Deactivated/Invalid/Kicked"
                    except (ChatAdminRequiredError, ChannelPrivateError, ChatWriteForbiddenError) as e:
                        logger.error(f"Admin/permissions error for {target_chat_id} with {phone}: {e}. Stopping operation for this chat.")
                        results['operation_error'] = f"Permissions error: {e}"
                        raise # Re-raise to stop processing this chat with this account
                    except AMFloodWaitError as e: # Custom FloodWait from AccountManager
                        logger.warning(f"Account Manager Flood wait on {phone}: {e.seconds}s. Pausing this add operation.")
                        await asyncio.sleep(e.seconds + 1)
                        # Consider re-queueing this user or stopping, for now, count as failed for this batch
                        results['failed'] +=1; results['errors'][str(user_id_or_username)] = f"Account Flood Wait ({e.seconds}s)"
                        break # Stop current batch for this account
                    except FloodWaitError as e: # Telethon's FloodWaitError
                        logger.warning(f"Telethon Flood wait on {phone} adding to {target_chat_id}: {e.seconds}s. Pausing.")
                        await asyncio.sleep(e.seconds + 1)
                        results['failed'] +=1; results['errors'][str(user_id_or_username)] = f"Telethon Flood Wait ({e.seconds}s)"
                        # Potentially break or try next account if manager supports that kind of switch
                        break # Stop current batch for this account to respect flood wait
                    except Exception as e:
                        err_str = str(e)
                        logger.error(f"Error adding user {user_id_or_username} to {target_chat_id} using {phone}: {err_str}", exc_info=True)
                        results['failed'] +=1; results['errors'][str(user_id_or_username)] = f"{type(e).__name__}: {err_str}"

                    if i < len(user_ids_to_add) - 1: # If not the last user
                        logger.debug(f"Waiting for {delay_seconds}s before adding next user to {target_chat_id}.")
                        await asyncio.sleep(delay_seconds)

            except ValueError as e: # Specific error for entity not found (target_chat_id)
                 logger.error(f"Could not find target chat/channel '{target_chat_id}' using account {phone}: {e}")
                 results['operation_error'] = f"Target chat/channel '{target_chat_id}' not found or account has no access."
            except Exception as e:
                logger.error(f"Major error in add_members to {target_chat_id} using {phone}: {e}", exc_info=True)
                results['operation_error'] = f"Operation failed: {type(e).__name__} - {e}"

            return results

    async def get_chat_info(self, chat_id: Union[str, int]) -> Dict[str, Any]:
        account_dict = await self._get_available_account('get_chat_info')
        if not account_dict: raise MemberManagerError("No available accounts for getting chat info.")

        phone = account_dict['phone']
        client = self.account_manager._clients.get(phone)
        if not client or not client.is_connected():
            logger.info(f"Client for {phone} not connected for get_chat_info. Attempting connect.")
            success, msg = await self.account_manager.connect_account(phone)
            if not success: raise MemberManagerError(f"Failed to connect account {phone}: {msg}")
            client = self.account_manager._clients.get(phone)
            if not client: raise MemberManagerError(f"Client for {phone} unavailable.")

        try:
            entity = await client.get_entity(chat_id)
            info = {'id': entity.id, 'title': getattr(entity, 'title', None),
                    'username': getattr(entity, 'username', None),
                    'type': type(entity).__name__ }
            if hasattr(entity, 'participants_count'): info['participants_count'] = entity.participants_count
            return info
        except ValueError as e: # Entity not found
             logger.error(f"Could not find chat/channel '{chat_id}' using account {phone}: {e}")
             raise MemberManagerError(f"Chat/channel '{chat_id}' not found or account has no access.")
        except Exception as e:
            logger.error(f"Error getting info for chat {chat_id} using {phone}: {e}", exc_info=True)
            raise MemberManagerError(f"Failed to get chat info for {chat_id}: {e}")

    async def send_message(self, chat_id: Union[str, int], text: str,
                          parse_mode: Optional[str] = None,
                          buttons: Optional[List[List[Dict[str, str]]]] = None) -> bool:
        # This is a utility method, might be kept if general bot actions are managed here.
        account_dict = await self._get_available_account('send_message_utility')
        if not account_dict: raise MemberManagerError("No available accounts for sending message.")

        phone = account_dict['phone']
        client = self.account_manager._clients.get(phone)
        if not client or not client.is_connected():
            logger.info(f"Client for {phone} not connected for send_message. Attempting connect.")
            success, msg = await self.account_manager.connect_account(phone)
            if not success: raise MemberManagerError(f"Failed to connect account {phone}: {msg}")
            client = self.account_manager._clients.get(phone)
            if not client: raise MemberManagerError(f"Client for {phone} unavailable.")

        try:
            entity = await client.get_entity(chat_id)
            # Telethon button formatting is different, this example assumes simple text message.
            # If buttons are needed, they must be formatted for Telethon.
            # For now, ignoring 'buttons' parameter for simplicity as it requires Telethon-specific button objects.
            await client.send_message(entity=entity, message=text, parse_mode=parse_mode) # type: ignore
            logger.info(f"Message sent to {chat_id} using {phone}.")
            return True
        except ValueError as e: # Entity not found
             logger.error(f"Could not find chat/channel '{chat_id}' for sending message using account {phone}: {e}")
             raise MemberManagerError(f"Chat/channel '{chat_id}' not found or account has no access to send.")
        except Exception as e:
            logger.error(f"Error sending message to {chat_id} using {phone}: {e}", exc_info=True)
            raise MemberManagerError(f"Failed to send message to {chat_id}: {e}")

# # Standalone execution block is removed as per subtask instructions
# # if __name__ == "__main__":
# #     asyncio.run(example())
