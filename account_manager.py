"""
Telegram Account Manager

This module provides functionality to manage multiple Telegram accounts,
including authentication, session management, and rate limiting.
"""

import os
import json
import logging
import asyncio
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Union, Any, TypeVar, Callable, Coroutine, Set, DefaultDict
from pathlib import Path
from collections import defaultdict

import pytz
from telethon import TelegramClient, types, functions, errors
from telethon.sessions import StringSession
from telethon.tl.functions.account import UpdateStatusRequest
from telethon.tl.functions.channels import GetFullChannelRequest, JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.messages import GetDialogsRequest, ImportChatInviteRequest, CheckChatInviteRequest
from telethon.tl.types import (
    User, Channel, Chat, InputPeerUser, InputPeerChannel, InputPeerChat,
    UserStatusOnline, UserStatusOffline, UserStatusRecently, UserStatusLastWeek,
    UserStatusLastMonth, Message, MessageService, MessageActionChatAddUser
)

# Type aliases
T = TypeVar('T')
Account = Dict[str, Any]
AccountList = List[Account]
Result = Tuple[bool, str]
AsyncFunc = Callable[..., Coroutine[Any, Any, T]]

# Constants
DEFAULT_CONFIG = {
    'sessions_path': 'sessions',
    'logs_path': 'logs',
    'api_id': None,
    'api_hash': None,
    'system_version': '4.16.30-vxCustom',
    'device_model': 'Telegram Manager',
    'app_version': '1.0',
    'lang_code': 'en',
    'system_lang_code': 'en-US',
    'max_retries': 3,
    'retry_delay': 5,
    'flood_sleep_threshold': 60 * 60,  # 1 hour
    'max_flood_wait': 24 * 60 * 60,  # 1 day
    'auto_reconnect': True,
    'auto_reconnect_delay': 60,  # 1 minute
    'connection_timeout': 30,  # 30 seconds
    'request_retries': 3,
    'sleep_threshold': 30,  # seconds
    'auto_online': False,
    'offline_idle_timeout': 5 * 60,  # 5 minutes
    'max_connections': 5,
    'connection_retries': 3,
    'connection_retry_delay': 5,  # 5 seconds
    'download_retries': 3,
    'upload_retries': 3,
    'download_retry_delay': 5,  # 5 seconds
    'upload_retry_delay': 5,  # 5 seconds
    'proxy': None,
    'use_ipv6': False,
    'timeout': 30,  # 30 seconds
    'auto_reconnect_max_retries': 10,
    'auto_reconnect_base_delay': 1.0,  # 1 second
    'auto_reconnect_max_delay': 60.0,  # 1 minute
}


class AccountError(Exception):
    """Base exception for account-related errors."""
    pass


class AccountNotFoundError(AccountError):
    """Raised when an account is not found."""
    pass


class AccountLimitExceededError(AccountError):
    """Raised when the maximum number of accounts is exceeded."""
    pass


class AccountAlreadyExistsError(AccountError):
    """Raised when trying to add an account that already exists."""
    pass


class InvalidPhoneNumberError(AccountError):
    """Raised when an invalid phone number is provided."""
    pass


class InvalidApiCredentialsError(AccountError):
    """Raised when invalid API credentials are provided."""
    pass


class ConnectionError(AccountError):
    """Raised when there's a connection error."""
    pass


class FloodWaitError(AccountError):
    """Raised when a flood wait error occurs."""
    def __init__(self, seconds: int, *args, **kwargs):
        self.seconds = seconds
        super().__init__(f"Flood wait for {seconds} seconds", *args, **kwargs)


class SessionExpiredError(AccountError):
    """Raised when a session has expired."""
    pass


class TwoFactorAuthRequiredError(AccountError):
    """Raised when two-factor authentication is required."""
    pass


class PhoneNumberBannedError(AccountError):
    """Raised when a phone number is banned."""
    pass


class PhoneCodeInvalidError(AccountError):
    """Raised when an invalid phone code is provided."""
    pass


class PhoneCodeExpiredError(AccountError):
    """Raised when a phone code has expired."""
    pass


class SessionPasswordNeededError(AccountError):
    """Raised when a session password is needed."""
    pass


class PasswordHashInvalidError(AccountError):
    """Raised when an invalid password hash is provided."""
    pass


class TelegramAccountManager:
    """
    Manages multiple Telegram accounts with rate limiting, session management,
    and error handling.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the account manager with configuration.
        
        Args:
            config: Configuration dictionary. If None, default config will be used.
        """
        self.accounts: List[Dict[str, Any]] = []
        self.active_accounts: List[Dict[str, Any]] = []
        self.limited_accounts: List[Dict[str, Any]] = []
        self.banned_accounts: List[Dict[str, Any]] = []
        self._config_lock = asyncio.Lock()
        self._clients: Dict[str, TelegramClient] = {}
        self._config = {**DEFAULT_CONFIG, **(config or {})}
        self._account_locks: DefaultDict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._reconnect_tasks: Dict[str, asyncio.Task] = {}
        self._reconnect_events: Dict[str, asyncio.Event] = {}
        self._is_running = False
        self._shutdown_event = asyncio.Event()
        self._initialized = False
        self._db_connection = None
        
        # Setup logging
        self._setup_logging()
        
        # Ensure required directories exist
        self._ensure_directories()

    async def initialize(self):
        """Initialize the account manager asynchronously.
        Call this after the event loop is running.
        """
        if not self._initialized:
            # Initialize database connection
            self._db_connection = sqlite3.connect(
                os.path.join(self._config['sessions_path'], 'accounts.db'),
                check_same_thread=False
            )
            self._setup_database()
            
            # Load accounts
            await self._load_accounts_async()
            self._initialized = True
    
    def _ensure_directories(self) -> None:
        """Ensure required directories exist."""
        os.makedirs(self._config['sessions_path'], exist_ok=True)
        os.makedirs(self._config['logs_path'], exist_ok=True)
    
    def _setup_database(self) -> None:
        """Set up the SQLite database with required tables."""
        try:
            cursor = self._db_connection.cursor()
            
            # Create accounts table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS accounts (
                    phone TEXT PRIMARY KEY,
                    api_id INTEGER,
                    api_hash TEXT,
                    session_string TEXT,
                    status TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_used TIMESTAMP,
                    daily_requests INTEGER DEFAULT 0,
                    last_reset TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT 1
                )
            ''')
            
            # Create request history table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS request_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_phone TEXT,
                    request_type TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    success BOOLEAN,
                    response_time REAL,
                    error_message TEXT,
                    FOREIGN KEY (account_phone) REFERENCES accounts(phone) ON DELETE CASCADE
                )
            ''')
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_account_status ON accounts(status, is_active)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_request_history ON request_history(account_phone, timestamp)')
            
            self._db_connection.commit()
            logging.info("Database setup completed successfully")
            
        except sqlite3.Error as e:
            logging.error(f"Error setting up database: {e}")
            raise
    
    async def _load_accounts_async(self) -> None:
        """Asynchronously load accounts from the sessions directory."""
        try:
            sessions_dir = Path(self._config['sessions_path'])
            if not sessions_dir.exists():
                return
                
            session_files = list(sessions_dir.glob('*.session'))
            if not session_files:
                return
                
            logging.info(f"Found {len(session_files)} session files, loading accounts...")
            
            for session_file in session_files:
                phone = session_file.stem
                if not self.get_account(phone):
                    try:
                        # Try to create a client to validate the session
                        client = self._create_client(phone)
                        async with self._account_locks[phone]:
                            try:
                                await client.connect()
                                if not await client.is_user_authorized():
                                    logging.warning(f"Session for {phone} is not authorized, skipping...")
                                    await client.disconnect()
                                    continue
                                
                                # Get account info
                                me = await client.get_me()
                                
                                # Add account to the manager
                                account = {
                                    'phone': phone,
                                    'user_id': me.id,
                                    'username': me.username,
                                    'first_name': me.first_name,
                                    'last_name': me.last_name,
                                    'status': 'active',
                                    'last_online': datetime.now(pytz.UTC),
                                    'created_at': datetime.now(pytz.UTC),
                                    'updated_at': datetime.now(pytz.UTC),
                                    'client': client,
                                    'is_online': False,
                                    'is_connected': True,
                                    'error_count': 0,
                                    'last_error': None,
                                    'api_id': self._config.get('api_id'),
                                    'api_hash': self._config.get('api_hash'),
                                    'proxy': self._config.get('proxy'),
                                    'saved_messages': []
                                }
                                
                                self.accounts.append(account)
                                self.active_accounts.append(account)
                                self._clients[phone] = client
                                
                                logging.info(f"Successfully loaded account: {phone} ({me.first_name} {me.last_name or ''} @{me.username or 'N/A'})")
                                
                                # Start auto-reconnect if enabled
                                if self._config['auto_reconnect']:
                                    asyncio.create_task(self._auto_reconnect_loop(phone))
                                
                            except Exception as e:
                                logging.error(f"Error loading account {phone}: {e}")
                                await client.disconnect()
                                
                    except Exception as e:
                        logging.error(f"Error creating client for {phone}: {e}")
                        continue
            
            logging.info(f"Successfully loaded {len(self.accounts)} accounts")
            
        except Exception as e:
            logging.error(f"Error loading accounts: {e}", exc_info=True)
    
    def _create_client(self, phone: str) -> TelegramClient:
        """Create a new Telegram client for the given phone number."""
        session_path = os.path.join(self._config['sessions_path'], f"{phone}.session")
        
        client = TelegramClient(
            session=session_path,
            api_id=self._config['api_id'],
            api_hash=self._config['api_hash'],
            device_model=self._config['device_model'],
            system_version=self._config['system_version'],
            app_version=self._config['app_version'],
            lang_code=self._config['lang_code'],
            system_lang_code=self._config['system_lang_code'],
            timeout=self._config['timeout'],
            proxy=self._config.get('proxy'),
            request_retries=self._config['request_retries'],
            connection_retries=self._config['connection_retries'],
            retry_delay=self._config['connection_retry_delay'],
            auto_reconnect=False,  # We handle reconnection ourselves
            flood_sleep_threshold=self._config['flood_sleep_threshold'],
            raise_last_call_error=True,
            base_logger=f"telethon.client.updates({phone})"
        )
        
        # Configure client proxy if specified
        if 'proxy' in self._config and self._config['proxy']:
            client.set_proxy(self._config['proxy'])
        
        return client
    
    def _setup_logging(self) -> None:
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(os.path.join(self._config['logs_path'], 'account_manager.log')),
                logging.StreamHandler()
            ]
        )
        
        # Set log level for telethon
        logging.getLogger('telethon').setLevel(logging.WARNING)
    
    def get_account(self, phone: str) -> Optional[Dict[str, Any]]:
        """Get an account by phone number."""
        for account in self.accounts:
            if account.get('phone') == phone:
                return account
        return None
    
    @staticmethod
    def _validate_phone_number(phone: str) -> bool:
        """Validate phone number format."""
        # Simple validation for international format (+ and 10-15 digits)
        return bool(re.match(r'^\+[1-9]\d{9,14}$', phone.strip()))
    
    async def add_account(
        self,
        phone: str,
        api_id: Optional[Union[int, str]] = None,
        api_hash: Optional[str] = None,
        password: Optional[str] = None,
        code_callback: Optional[Callable[[str], Coroutine[Any, Any, str]]] = None,
        password_callback: Optional[Callable[[], Coroutine[Any, Any, str]]] = None,
        max_retries: int = 3,
        **kwargs
    ) -> Result:
        """Add a new Telegram account.
        
        Args:
            phone: Phone number in international format (e.g., '+1234567890')
            api_id: Telegram API ID (if not provided, uses the one from config)
            api_hash: Telegram API hash (if not provided, uses the one from config)
            password: 2FA password (if any)
            code_callback: Async function to get the verification code
            password_callback: Async function to get the 2FA password
            max_retries: Maximum number of retry attempts
            **kwargs: Additional account parameters
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        # Validate phone number
        if not self._validate_phone_number(phone):
            return False, "Invalid phone number format. Please use international format (e.g., +1234567890)"
        
        # Check if account already exists
        if self.get_account(phone):
            return False, f"Account with phone {phone} already exists"
        
        # Use provided API credentials or fall back to config
        api_id = api_id or self._config.get('api_id')
        api_hash = api_hash or self._config.get('api_hash')
        
        if not api_id or not api_hash:
            return False, "API credentials (api_id and api_hash) are required"
        
        # Create client
        client = self._create_client(phone)
        account = None
        
        try:
            # Connect to Telegram
            await client.connect()
            
            # Check if already authorized
            if await client.is_user_authorized():
                me = await client.get_me()
                await client.disconnect()
                return False, f"This phone number is already authorized as @{me.username or me.id}"
            
            # Send code request
            sent_code = await client.send_code_request(phone)
            
            # Get verification code
            if not code_callback:
                code = input(f"Enter the verification code sent to {phone}: ")
            else:
                code = await code_callback(phone)
            
            # Sign in with code
            try:
                await client.sign_in(phone, code=code)
            except errors.SessionPasswordNeededError:
                if not password and not password_callback:
                    await client.disconnect()
                    return False, "2FA password is required"
                
                # Get 2FA password
                pw = password or await password_callback()
                if not pw:
                    await client.disconnect()
                    return False, "2FA password is required"
                
                try:
                    await client.sign_in(password=pw)
                except errors.PasswordHashInvalidError:
                    await client.disconnect()
                    return False, "Invalid 2FA password"
            
            # Get account info
            me = await client.get_me()
            
            # Create account data
            account = {
                'phone': phone,
                'user_id': me.id,
                'username': me.username,
                'first_name': me.first_name,
                'last_name': me.last_name,
                'status': 'active',
                'last_online': datetime.now(pytz.UTC),
                'created_at': datetime.now(pytz.UTC),
                'updated_at': datetime.now(pytz.UTC),
                'client': client,
                'is_online': False,
                'is_connected': True,
                'error_count': 0,
                'last_error': None,
                'api_id': api_id,
                'api_hash': api_hash,
                'proxy': self._config.get('proxy'),
                'saved_messages': [],
                **kwargs
            }
            
            # Save account
            self.accounts.append(account)
            self.active_accounts.append(account)
            self._clients[phone] = client
            
            # Start auto-reconnect if enabled
            if self._config['auto_reconnect']:
                asyncio.create_task(self._auto_reconnect_loop(phone))
            
            return True, f"Successfully added account: @{me.username or me.id} ({me.first_name} {me.last_name or ''})"
            
        except errors.PhoneNumberInvalidError:
            return False, "Invalid phone number"
        except errors.PhoneCodeInvalidError:
            return False, "Invalid verification code"
        except errors.PhoneCodeExpiredError:
            return False, "Verification code has expired"
        except errors.PhoneNumberBannedError:
            return False, "This phone number is banned"
        except errors.SessionPasswordNeededError:
            return False, "2FA password is required"
        except errors.FloodWaitError as e:
            return False, f"Flood wait error. Please try again in {e.seconds} seconds"
        except errors.RPCError as e:
            return False, f"Telegram API error: {e}"
        except Exception as e:
            return False, f"Failed to add account: {str(e)}"
        finally:
            if client and client.is_connected() and not account:
                await client.disconnect()
    
    async def remove_account(self, phone: str, delete_session: bool = True) -> Result:
        """Remove an account from the manager.
        
        Args:
            phone: Phone number of the account to remove
            delete_session: Whether to delete the session file
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        account = self.get_account(phone)
        if not account:
            return False, f"Account {phone} not found"
        
        # Cancel any pending reconnection tasks
        if phone in self._reconnect_tasks:
            self._reconnect_tasks[phone].cancel()
            del self._reconnect_tasks[phone]
        
        if phone in self._reconnect_events:
            del self._reconnect_events[phone]
        
        # Disconnect client if connected
        client = account.get('client')
        if client and client.is_connected():
            try:
                await client.disconnect()
            except Exception as e:
                logging.warning(f"Error disconnecting client for {phone}: {e}")
        
        # Remove from account lists
        if account in self.accounts:
            self.accounts.remove(account)
        if account in self.active_accounts:
            self.active_accounts.remove(account)
        if account in self.limited_accounts:
            self.limited_accounts.remove(account)
        if account in self.banned_accounts:
            self.banned_accounts.remove(account)
        
        # Delete session file if requested
        if delete_session:
            session_path = os.path.join(self._config['sessions_path'], f"{phone}.session")
            try:
                if os.path.exists(session_path):
                    os.remove(session_path)
                    logging.info(f"Deleted session file for {phone}")
            except Exception as e:
                logging.error(f"Error deleting session file for {phone}: {e}")
        
        # Remove client from cache
        if phone in self._clients:
            del self._clients[phone]
        
        return True, f"Successfully removed account {phone}"
    
    async def connect_account(self, phone: str) -> Result:
        """Connect an account.
        
        Args:
            phone: Phone number of the account to connect
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        account = self.get_account(phone)
        if not account:
            return False, f"Account {phone} not found"
        
        if account.get('is_connected'):
            return True, f"Account {phone} is already connected"
        
        client = account.get('client')
        if not client:
            client = self._create_client(phone)
            account['client'] = client
            self._clients[phone] = client
        
        try:
            await client.connect()
            
            # Check if session is still valid
            if not await client.is_user_authorized():
                await client.disconnect()
                return False, f"Session for {phone} is invalid. Please re-add the account."
            
            account['is_connected'] = True
            account['status'] = 'active'
            account['last_online'] = datetime.now(pytz.UTC)
            
            # Update account lists
            if account not in self.active_accounts:
                self.active_accounts.append(account)
            
            if account in self.limited_accounts:
                self.limited_accounts.remove(account)
            
            # Start auto-reconnect if enabled
            if self._config['auto_reconnect'] and phone not in self._reconnect_tasks:
                asyncio.create_task(self._auto_reconnect_loop(phone))
            
            return True, f"Successfully connected account {phone}"
            
        except errors.FloodWaitError as e:
            wait_time = e.seconds
            account['status'] = 'flood_wait'
            account['last_error'] = f"Flood wait for {wait_time} seconds"
            
            if account in self.active_accounts:
                self.active_accounts.remove(account)
            
            if account not in self.limited_accounts:
                self.limited_accounts.append(account)
            
            return False, f"Flood wait error. Please try again in {wait_time} seconds"
            
        except errors.RPCError as e:
            error_msg = f"Telegram API error: {e}"
            account['last_error'] = error_msg
            account['error_count'] = account.get('error_count', 0) + 1
            
            if account['error_count'] >= 5:  # Too many errors, mark as limited
                account['status'] = 'error'
                
                if account in self.active_accounts:
                    self.active_accounts.remove(account)
                
                if account not in self.limited_accounts:
                    self.limited_accounts.append(account)
            
            return False, error_msg
            
        except Exception as e:
            error_msg = f"Failed to connect account {phone}: {str(e)}"
            logging.error(error_msg, exc_info=True)
            
            account['last_error'] = str(e)
            account['error_count'] = account.get('error_count', 0) + 1
            
            if account in self.active_accounts:
                self.active_accounts.remove(account)
            
            if account not in self.limited_accounts:
                self.limited_accounts.append(account)
            
            return False, error_msg
    
    async def disconnect_account(self, phone: str) -> Result:
        """Disconnect an account.
        
        Args:
            phone: Phone number of the account to disconnect
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        account = self.get_account(phone)
        if not account:
            return False, f"Account {phone} not found"
        
        if not account.get('is_connected'):
            return True, f"Account {phone} is already disconnected"
        
        # Cancel any pending reconnection tasks
        if phone in self._reconnect_tasks:
            self._reconnect_tasks[phone].cancel()
            del self._reconnect_tasks[phone]
        
        if phone in self._reconnect_events:
            del self._reconnect_events[phone]
        
        client = account.get('client')
        if client and client.is_connected():
            try:
                await client.disconnect()
                account['is_connected'] = False
                account['status'] = 'offline'
                
                if account in self.active_accounts:
                    self.active_accounts.remove(account)
                
                return True, f"Successfully disconnected account {phone}"
                
            except Exception as e:
                error_msg = f"Error disconnecting account {phone}: {str(e)}"
                logging.error(error_msg, exc_info=True)
                return False, error_msg
        
        return False, f"No active connection for account {phone}"
    
    async def reconnect_account(self, phone: str) -> Result:
        """Reconnect an account.
        
        Args:
            phone: Phone number of the account to reconnect
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        # First disconnect if connected
        if phone in self._clients and self._clients[phone].is_connected():
            await self.disconnect_account(phone)
        
        # Then connect again
        return await self.connect_account(phone)
    
    async def get_account_status(self, phone: str) -> Dict[str, Any]:
        """Get detailed status of an account.
        
        Args:
            phone: Phone number of the account
            
        Returns:
            Dictionary with account status information
        """
        account = self.get_account(phone)
        if not account:
            return {
                'exists': False,
                'error': 'Account not found'
            }
        
        status = {
            'exists': True,
            'phone': account['phone'],
            'user_id': account.get('user_id'),
            'username': account.get('username'),
            'first_name': account.get('first_name'),
            'last_name': account.get('last_name'),
            'status': account.get('status', 'unknown'),
            'is_online': account.get('is_online', False),
            'is_connected': account.get('is_connected', False),
            'last_online': account.get('last_online'),
            'created_at': account.get('created_at'),
            'updated_at': account.get('updated_at'),
            'error_count': account.get('error_count', 0),
            'last_error': account.get('last_error'),
            'is_authenticated': False,
            'saved_messages_count': len(account.get('saved_messages', [])),
            'in_groups': [],
            'in_channels': []
        }
        
        # Get more detailed info if connected
        if status['is_connected'] and 'client' in account and account['client'].is_connected():
            try:
                me = await account['client'].get_me()
                status.update({
                    'is_authenticated': True,
                    'username': me.username,
                    'first_name': me.first_name,
                    'last_name': me.last_name,
                    'phone': me.phone,
                    'is_bot': me.bot,
                    'is_verified': me.verified,
                    'is_restricted': me.restricted,
                    'is_scam': me.scam,
                    'is_fake': me.fake,
                    'premium': getattr(me, 'premium', False),
                    'lang_code': getattr(me, 'lang_code', None),
                    'mutual_contacts': getattr(me, 'mutual_contacts_count', 0),
                    'common_chats_count': getattr(me, 'common_chats_count', 0),
                    'bot_info_version': getattr(me, 'bot_info_version', None),
                    'restriction_reason': getattr(me, 'restriction_reason', []),
                    'bot_inline_placeholder': getattr(me, 'bot_inline_placeholder', None),
                    'bot_inline_geo': getattr(me, 'bot_inline_geo', False),
                    'bot_can_edit': getattr(me, 'bot_can_edit', False),
                    'bot_nochats': getattr(me, 'bot_nochats', False),
                })
                
                # Get common chats count if available
                if hasattr(me, 'common_chats_count'):
                    status['common_chats_count'] = me.common_chats_count
                
                # Get DC info
                dc = account['client'].session.dc_id
                if dc:
                    status['dc_id'] = dc
                    status['dc_region'] = f"DC{dc}"
                
                # Get connection status
                if account['client'].is_connected():
                    status['connection_status'] = 'connected'
                else:
                    status['connection_status'] = 'disconnected'
                
                # Get last seen time if available
                if hasattr(me, 'status'):
                    if isinstance(me.status, UserStatusOnline):
                        status['last_seen'] = 'online'
                        status['last_online'] = datetime.now(pytz.UTC)
                    elif isinstance(me.status, UserStatusOffline):
                        status['last_seen'] = 'last seen recently'
                        status['last_online'] = me.status.was_online
                    elif isinstance(me.status, UserStatusRecently):
                        status['last_seen'] = 'last seen recently'
                    elif isinstance(me.status, UserStatusLastWeek):
                        status['last_seen'] = 'last seen within a week'
                    elif isinstance(me.status, UserStatusLastMonth):
                        status['last_seen'] = 'last seen within a month'
                    else:
                        status['last_seen'] = 'long time ago'
                
                # Get account privacy settings
                try:
                    privacy_settings = await account['client'].functions.account.GetPrivacy(
                        types.InputPrivacyKeyStatusTimestamp()
                    )
                    status['privacy_settings'] = {
                        'last_seen_hidden': any(
                            isinstance(rule, types.PrivacyValueAllowAll) or 
                            isinstance(rule, types.PrivacyValueAllowUsers) or
                            isinstance(rule, types.PrivacyValueAllowChatParticipants)
                            for rule in privacy_settings.rules
                        )
                    }
                except Exception as e:
                    logging.warning(f"Could not fetch privacy settings: {e}")
                    status['privacy_settings'] = {'error': str(e)}
                
                # Get account TTL
                try:
                    account_ttl = await account['client'].functions.account.GetAccountTTL()
                    status['account_ttl_days'] = account_ttl.period // 86400  # Convert seconds to days
                except Exception as e:
                    logging.warning(f"Could not fetch account TTL: {e}")
                
                # Get active sessions
                try:
                    sessions = await account['client'].functions.account.GetAuthorizations()
                    status['active_sessions'] = len(sessions.authorizations)
                    status['current_session'] = next(
                        (s for s in sessions.authorizations if s.current),
                        None
                    )
                except Exception as e:
                    logging.warning(f"Could not fetch active sessions: {e}")
                
            except Exception as e:
                logging.error(f"Error getting detailed account info: {e}", exc_info=True)
                status['error'] = f"Error getting detailed info: {str(e)}"
        
        return status
    
    async def get_all_accounts_status(self) -> List[Dict[str, Any]]:
        """Get status for all accounts."""
        return [await self.get_account_status(acc['phone']) for acc in self.accounts]
    
    async def _auto_reconnect_loop(self, phone: str) -> None:
        """Automatically reconnect an account if it gets disconnected."""
        if phone in self._reconnect_tasks:
            return
        
        self._reconnect_events[phone] = asyncio.Event()
        self._reconnect_tasks[phone] = asyncio.create_task(self._auto_reconnect_task(phone))
    
    async def _auto_reconnect_task(self, phone: str) -> None:
        """Background task to handle automatic reconnection for an account."""
        reconnect_event = self._reconnect_events[phone]
        reconnect_attempts = 0
        max_attempts = self._config['auto_reconnect_max_retries']
        
        while not self._shutdown_event.is_set():
            try:
                # Wait for a disconnect event or shutdown
                await reconnect_event.wait()
                
                # Check if we should still try to reconnect
                if self._shutdown_event.is_set():
                    break
                
                account = self.get_account(phone)
                if not account:
                    logging.warning(f"Account {phone} not found, stopping auto-reconnect")
                    break
                
                # Reset the event
                reconnect_event.clear()
                
                # Calculate backoff delay
                delay = min(
                    self._config['auto_reconnect_base_delay'] * (2 ** reconnect_attempts),
                    self._config['auto_reconnect_max_delay']
                )
                
                logging.info(f"Waiting {delay:.1f}s before reconnecting {phone} (attempt {reconnect_attempts + 1}/{max_attempts})")
                await asyncio.sleep(delay)
                
                # Try to reconnect
                success, _ = await self.reconnect_account(phone)
                if success:
                    logging.info(f"Successfully reconnected account {phone}")
                    reconnect_attempts = 0
                else:
                    reconnect_attempts += 1
                    if reconnect_attempts >= max_attempts:
                        logging.error(f"Max reconnection attempts reached for {phone}, giving up")
                        break
                    
                    # Trigger another reconnection attempt
                    reconnect_event.set()
                
            except asyncio.CancelledError:
                logging.info(f"Auto-reconnect task for {phone} was cancelled")
                break
            except Exception as e:
                logging.error(f"Error in auto-reconnect task for {phone}: {e}")
                reconnect_attempts += 1
                if reconnect_attempts >= max_attempts:
                    logging.error(f"Max reconnection attempts reached for {phone}, giving up")
                    break
                
                # Wait a bit before retrying
                await asyncio.sleep(5)
                reconnect_event.set()
        
        # Clean up
        if phone in self._reconnect_events:
            del self._reconnect_events[phone]
        if phone in self._reconnect_tasks:
            del self._reconnect_tasks[phone]
    
    async def update_account_status(self, phone: str, status: str) -> bool:
        """Update the status of an account.
        
        Args:
            phone: Phone number of the account
            status: New status ('active', 'inactive', 'banned', 'flood_wait', 'error')
            
        Returns:
            bool: True if status was updated, False otherwise
        """
        account = self.get_account(phone)
        if not account:
            return False
        
        valid_statuses = {'active', 'inactive', 'banned', 'flood_wait', 'error'}
        if status not in valid_statuses:
            return False
        
        account['status'] = status
        account['updated_at'] = datetime.now(pytz.UTC)
        
        # Update account lists
        if status == 'active':
            if account not in self.active_accounts:
                self.active_accounts.append(account)
            if account in self.limited_accounts:
                self.limited_accounts.remove(account)
            if account in self.banned_accounts:
                self.banned_accounts.remove(account)
        elif status == 'banned':
            if account in self.active_accounts:
                self.active_accounts.remove(account)
            if account in self.limited_accounts:
                self.limited_accounts.remove(account)
            if account not in self.banned_accounts:
                self.banned_accounts.append(account)
        else:  # inactive, flood_wait, error
            if account in self.active_accounts:
                self.active_accounts.remove(account)
            if account not in self.limited_accounts:
                self.limited_accounts.append(account)
            if account in self.banned_accounts:
                self.banned_accounts.remove(account)
        
        return True
    
    async def set_online_status(self, phone: str, online: bool = True) -> Result:
        """Set the online status of an account.
        
        Args:
            phone: Phone number of the account
            online: Whether to appear online
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        account = self.get_account(phone)
        if not account:
            return False, f"Account {phone} not found"
        
        if not account.get('is_connected'):
            return False, f"Account {phone} is not connected"
        
        client = account.get('client')
        if not client or not client.is_connected():
            return False, f"No active connection for account {phone}"
        
        try:
            if online:
                await client(UpdateStatusRequest(offline=False))
                account['is_online'] = True
                account['last_online'] = datetime.now(pytz.UTC)
                return True, f"Account {phone} is now online"
            else:
                await client(UpdateStatusRequest(offline=True))
                account['is_online'] = False
                return True, f"Account {phone} is now offline"
        except Exception as e:
            error_msg = f"Failed to update online status for {phone}: {str(e)}"
            logging.error(error_msg, exc_info=True)
            return False, error_msg
    
    async def get_active_sessions(self, phone: str) -> Dict[str, Any]:
        """Get active sessions for an account.
        
        Args:
            phone: Phone number of the account
            
        Returns:
            Dictionary with active sessions information
        """
        account = self.get_account(phone)
        if not account:
            return {'error': 'Account not found'}
        
        if not account.get('is_connected'):
            return {'error': 'Account is not connected'}
        
        client = account.get('client')
        if not client or not client.is_connected():
            return {'error': 'No active connection'}
        
        try:
            result = await client(functions.account.GetAuthorizationsRequest())
            
            sessions = []
            for auth in result.authorizations:
                session = {
                    'hash': auth.hash,
                    'device_model': auth.device_model,
                    'platform': auth.platform,
                    'system_version': auth.system_version,
                    'api_id': auth.api_id,
                    'app_name': auth.app_name,
                    'app_version': auth.app_version,
                    'date_created': auth.date_created,
                    'date_active': auth.date_active,
                    'ip': auth.ip,
                    'country': auth.country,
                    'region': auth.region,
                    'current': auth.current,
                    'official_app': auth.official_app,
                    'password_pending': auth.password_pending,
                    'call_requests_disabled': getattr(auth, 'call_requests_disabled', None),
                    'unconfirmed': getattr(auth, 'unconfirmed', None),
                }
                sessions.append(session)
            
            return {
                'sessions': sessions,
                'authorization_ttl_days': result.authorization_ttl_days
            }
            
        except Exception as e:
            error_msg = f"Failed to get active sessions: {str(e)}"
            logging.error(error_msg, exc_info=True)
            return {'error': error_msg}
    
    async def terminate_session(self, phone: str, session_hash: int) -> Result:
        """Terminate a specific session.
        
        Args:
            phone: Phone number of the account
            session_hash: Hash of the session to terminate
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        account = self.get_account(phone)
        if not account:
            return False, "Account not found"
        
        if not account.get('is_connected'):
            return False, "Account is not connected"
        
        client = account.get('client')
        if not client or not client.is_connected():
            return False, "No active connection"
        
        try:
            await client(functions.account.ResetAuthorizationRequest(hash=session_hash))
            return True, f"Session {session_hash} terminated successfully"
        except Exception as e:
            error_msg = f"Failed to terminate session: {str(e)}"
            logging.error(error_msg, exc_info=True)
            return False, error_msg
    
    async def terminate_other_sessions(self, phone: str) -> Result:
        """Terminate all other active sessions.
        
        Args:
            phone: Phone number of the account
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        account = self.get_account(phone)
        if not account:
            return False, "Account not found"
        
        if not account.get('is_connected'):
            return False, "Account is not connected"
        
        client = account.get('client')
        if not client or not client.is_connected():
            return False, "No active connection"
        
        try:
            await client(functions.auth.ResetAuthorizationsRequest())
            return True, "All other sessions have been terminated"
        except Exception as e:
            error_msg = f"Failed to terminate other sessions: {str(e)}"
            logging.error(error_msg, exc_info=True)
            return False, error_msg
    
    async def close(self) -> None:
        """Clean up resources and close all connections."""
        self._shutdown_event.set()
        
        # Cancel all reconnection tasks
        for task in self._reconnect_tasks.values():
            task.cancel()
        
        # Disconnect all clients
        for account in self.accounts:
            client = account.get('client')
            if client and client.is_connected():
                try:
                    await client.disconnect()
                except Exception as e:
                    logging.error(f"Error disconnecting client for {account.get('phone')}: {e}")
        
        # Clear all data structures
        self.accounts.clear()
        self.active_accounts.clear()
        self.limited_accounts.clear()
        self.banned_accounts.clear()
        self._clients.clear()
        self._reconnect_tasks.clear()
        self._reconnect_events.clear()
        self._account_locks.clear()
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    def __del__(self):
        """Destructor to ensure cleanup."""
        if not self._shutdown_event.is_set():
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(self.close())
                else:
                    loop.run_until_complete(self.close())
            except Exception as e:
                logging.error(f"Error during cleanup: {e}", exc_info=True)

    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Load and validate configuration from JSON file.

        Args:
            config_path: Path to the configuration file.
            
        Returns:
            Dict containing the configuration.
            
        Raises:
            FileNotFoundError: If config file is not found.
            json.JSONDecodeError: If config file is not valid JSON.
            ValueError: If required fields are missing.
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Validate required fields
            required_fields = ['bot_token', 'authorized_users']
            for field in required_fields:
                if field not in config:
                    raise ValueError(f"Missing required config field: {field}")
            
            # Set defaults
            defaults = {
                'delay_min': 15,
                'delay_max': 45,
                'daily_limit': 45,
                'check_interval': 3600,
                'max_flood_wait': 300,
                'retry_attempts': 3,
                'database_path': 'forwarding.db',
                'sessions_path': 'sessions/',
                'logs_path': 'logs/'
            }
            
            for key, value in defaults.items():
                if key not in config:
                    config[key] = value
            
            return config
            
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in config file {config_path}: {e}")
            raise
        except FileNotFoundError:
            logging.error(f"Config file {config_path} not found")
            raise
    
    def _setup_logging(self) -> None:
        """Configure logging with file and console handlers."""
        log_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Create logs directory if it doesn't exist
        log_file = os.path.join(self._config['logs_path'], 'account_manager.log')
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        
        # Remove existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # File handler
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(log_formatter)
        root_logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        root_logger.addHandler(console_handler)
        
        # Configure telethon logging
        logging.getLogger('telethon').setLevel(logging.WARNING)
    
    @staticmethod
    def _validate_phone_number(phone: str) -> bool:
        """Validate phone number format.
        
        Args:
            phone: Phone number to validate.
            
        Returns:
            bool: True if phone number is valid, False otherwise.
        """
        # Simple phone number validation (can be enhanced)
        phone_pattern = r'^\+?[1-9]\d{1,14}$'  # E.164 format
        return bool(re.match(phone_pattern, phone))
    
    async def add_account(
        self, 
        phone: str, 
        api_id: Union[int, str], 
        api_hash: str, 
        proxy: Optional[str] = None,
        max_retries: int = 3
    ) -> Tuple[bool, str]:
        """Add a new Telegram account with validation and error handling.
        
        Args:
            phone: Phone number in international format.
            api_id: Telegram API ID.
            api_hash: Telegram API hash.
            proxy: Optional proxy configuration.
            max_retries: Maximum number of connection attempts.
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        # Input validation
        if not self._validate_phone_number(phone):
            return False, f"Invalid phone number format: {phone}"
            
        try:
            api_id = int(api_id)
        except (ValueError, TypeError):
            return False, "API ID must be a number"
            
        if not api_hash or not isinstance(api_hash, str):
            return False, "Invalid API hash"
            
        # Check if account already exists
        if any(acc['phone'] == phone for acc in self.accounts):
            return False, f"Account {phone} already exists"
        
        # Create account data structure
        account_data = {
            'phone': phone,
            'api_id': api_id,
            'api_hash': api_hash,
            'proxy': proxy,
            'added_today': 0,
            'last_reset': datetime.now().date(),
            'status': 'pending',
            'client': None,
            'last_activity': None,
            'created_at': datetime.now(),
            'error_count': 0
        }
        
        # Test account connection with retries
        success, message = await self._test_account_connection(account_data, max_retries)
        
        if success:
            async with self._config_lock:
                self.accounts.append(account_data)
                self.active_accounts.append(account_data)
                account_data['status'] = 'active'
                
            logging.info(f"Successfully added account: {phone}")
            return True, f"Account {phone} added successfully"
        else:
            logging.error(f"Failed to add account {phone}: {message}")
            return False, f"Failed to add account: {message}"
    
    async def _test_account_connection(
        self,
        account_data: Dict,
        max_retries: int = 3,
        retry_delay: int = 5
    ) -> Tuple[bool, str]:
        """Test if an account can connect to Telegram with retry logic.
        
        Args:
            account_data: Dictionary containing account details.
            max_retries: Maximum number of connection attempts.
            retry_delay: Delay between retry attempts in seconds.
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        phone = account_data['phone']
        proxy = account_data.get('proxy')
        
        proxy_dict = None
        if proxy:
            try:
                proxy_parts = proxy.split(':')
                if len(proxy_parts) < 2:
                    return False, "Invalid proxy format. Use: host:port[:username:password]"
                    
                proxy_dict = {
                    'proxy_type': 'socks5',
                    'addr': proxy_parts[0].strip(),
                    'port': int(proxy_parts[1].strip()),
                    'username': proxy_parts[2].strip() if len(proxy_parts) > 2 else None,
                    'password': proxy_parts[3].strip() if len(proxy_parts) > 3 else None,
                    'rdns': True
                }
            except (ValueError, IndexError) as e:
                return False, f"Invalid proxy configuration: {e}"
        
        client = None
        last_error = None
        
        for attempt in range(1, max_retries + 1):
            try:
                session_path = os.path.join(
                    self.config['sessions_path'],
                    f"{phone}.session"
                )
                
                client = TelegramClient(
                    session=session_path,
                    api_id=account_data['api_id'],
                    api_hash=account_data['api_hash'],
                    proxy=proxy_dict,
                    device_model="Telegram Manager Bot",
                    system_version="1.0",
                    app_version="1.0.0",
                    system_lang_code='en',
                    lang_code='en'
                )
                
                # Set connection timeout
                client.session.set_dc(1, '149.154.167.50', 443)
                
                # Connect with timeout
                await asyncio.wait_for(client.connect(), timeout=30)
                
                if not await client.is_user_authorized():
                    try:
                        await client.send_code_request(phone)
                        account_data['status'] = 'code_required'
                        account_data['client'] = client
                        return True, "Verification code sent to phone"
                    except errors.PhoneNumberBannedError:
                        return False, "This phone number is banned"
                    except errors.PhoneNumberInvalidError:
                        return False, "Invalid phone number"
                    except errors.FloodWaitError as e:
                        return False, f"Flood wait error: {e.seconds} seconds"
                
                # Test API access
                try:
                    me = await client.get_me()
                    if not me:
                        raise ValueError("Failed to get account info")
                        
                    # Test getting dialogs
                    dialogs = await client.get_dialogs(limit=1)
                    
                    account_data['status'] = 'active'
                    account_data['client'] = client
                    account_data['last_activity'] = datetime.now()
                    account_data['username'] = me.username
                    account_data['user_id'] = me.id
                    account_data['error_count'] = 0
                    
                    return True, f"Successfully connected as @{me.username or me.phone}"
                    
                except errors.ApiIdInvalidError:
                    return False, "Invalid API ID/API hash combination"
                except errors.AccessTokenInvalidError:
                    return False, "Invalid access token"
                except Exception as e:
                    return False, f"API test failed: {str(e)}"
                
            except asyncio.TimeoutError:
                last_error = "Connection timed out"
            except errors.FloodWaitError as e:
                wait_time = min(e.seconds, self.config.get('max_flood_wait', 300))
                last_error = f"Flood wait error: {wait_time} seconds"
                await asyncio.sleep(wait_time)
                continue
            except errors.RPCError as e:
                last_error = f"Telegram RPC error: {str(e)}"
            except ConnectionError as e:
                last_error = f"Connection error: {str(e)}"
            except Exception as e:
                last_error = f"Unexpected error: {str(e)}"
            finally:
                if client and not client.is_connected():
                    await client.disconnect()
            
            # Exponential backoff for retries
            if attempt < max_retries:
                wait_time = retry_delay * (2 ** (attempt - 1))
                logging.warning(
                    f"Attempt {attempt}/{max_retries} failed for {phone}. "
                    f"Retrying in {wait_time}s... Error: {last_error}"
                )
                await asyncio.sleep(wait_time)
        
        return False, f"Failed to connect after {max_retries} attempts. Last error: {last_error}"
    
    async def complete_auth(
        self, 
        phone: str, 
        code: str, 
        password: Optional[str] = None,
        max_retries: int = 3
    ) -> Tuple[bool, str]:
        """Complete 2FA authentication for an account.
        
        Args:
            phone: Phone number of the account.
            code: Verification code received via SMS/Telegram.
            password: 2FA password if enabled.
            max_retries: Maximum number of authentication attempts.
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        account = self.get_account_by_phone(phone)
        if not account or 'client' not in account or not account['client']:
            return False, "Account not found or client not initialized"
        
        client = account['client']
        last_error = None
        
        for attempt in range(1, max_retries + 1):
            try:
                # Sign in with code
                try:
                    await client.sign_in(phone=phone, code=code)
                    auth_success = True
                except errors.SessionPasswordNeededError:
                    if not password:
                        return False, "2FA password required"
                    try:
                        await client.sign_in(password=password)
                        auth_success = True
                    except errors.PasswordHashInvalidError:
                        return False, "Invalid 2FA password"
                except errors.PhoneCodeInvalidError:
                    return False, "Invalid verification code"
                except errors.PhoneCodeExpiredError:
                    return False, "Verification code has expired"
                except errors.PhoneCodeEmptyError:
                    return False, "Verification code cannot be empty"
                
                if auth_success:
                    # Verify authentication was successful
                    if not await client.is_user_authorized():
                        raise ValueError("Authentication failed: Not authorized after sign in")
                    
                    # Update account status
                    async with self._config_lock:
                        account['status'] = 'active'
                        account['last_activity'] = datetime.now()
                        account['error_count'] = 0
                        
                        # Add to active accounts if not already present
                        if account not in self.active_accounts:
                            self.active_accounts.append(account)
                    
                    # Get and store user info
                    try:
                        me = await client.get_me()
                        if me:
                            account['username'] = me.username
                            account['user_id'] = me.id
                    except Exception as e:
                        logging.warning(f"Failed to get user info after auth: {e}")
                    
                    logging.info(f"Successfully authenticated account {phone}")
                    return True, "Authentication completed successfully"
                
            except errors.FloodWaitError as e:
                wait_time = min(e.seconds, self.config.get('max_flood_wait', 300))
                last_error = f"Flood wait error: {wait_time} seconds"
                if attempt < max_retries:
                    await asyncio.sleep(wait_time)
                    continue
            except errors.RPCError as e:
                last_error = f"Telegram RPC error: {str(e)}"
            except ConnectionError as e:
                last_error = f"Connection error: {str(e)}"
            except Exception as e:
                last_error = f"Unexpected error: {str(e)}"
            
            # Log the error and retry if attempts remain
            logging.error(f"Auth attempt {attempt}/{max_retries} failed for {phone}: {last_error}")
            if attempt < max_retries:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
        
        # If we get here, all retries failed
        async with self._config_lock:
            account['status'] = 'auth_failed'
            account['error_count'] = account.get('error_count', 0) + 1
            account['last_error'] = last_error
            account['last_activity'] = datetime.now()
            
            # Move to limited accounts if too many errors
            if account['error_count'] >= 3:
                if account in self.active_accounts:
                    self.active_accounts.remove(account)
                if account not in self.limited_accounts:
                    self.limited_accounts.append(account)
        
        return False, f"Authentication failed after {max_retries} attempts: {last_error}"
    
    def get_account_by_phone(self, phone: str) -> Optional[Dict]:
        """Get account by phone number.
        
        Args:
            phone: Phone number to search for.
            
        Returns:
            Account dictionary if found, None otherwise.
        """
        if not phone:
            return None
            
        for account in self.accounts:
            if account.get('phone') == phone:
                return account
        return None
        
    def get_account_by_id(self, user_id: Union[int, str]) -> Optional[Dict]:
        """Get account by Telegram user ID.
        
        Args:
            user_id: Telegram user ID to search for.
            
        Returns:
            Account dictionary if found, None otherwise.
        """
        if not user_id:
            return None
            
        user_id = int(user_id)  # Ensure it's an integer for comparison
        for account in self.accounts:
            if account.get('user_id') == user_id:
                return account
        return None
        
    async def get_account_status(self, identifier: Union[str, int]) -> Dict:
        """Get detailed status of an account.
        
        Args:
            identifier: Phone number or user ID of the account.
            
        Returns:
            Dictionary containing account status information.
        """
        account = None
        if isinstance(identifier, str) and identifier.startswith('+'):
            account = self.get_account_by_phone(identifier)
        else:
            account = self.get_account_by_id(identifier)
            
        if not account:
            return {'status': 'not_found', 'message': 'Account not found'}
            
        status = {
            'phone': account.get('phone'),
            'username': account.get('username'),
            'user_id': account.get('user_id'),
            'status': account.get('status', 'unknown'),
            'added_today': account.get('added_today', 0),
            'daily_limit': self.config.get('daily_limit', 45),
            'last_activity': account.get('last_activity'),
            'created_at': account.get('created_at'),
            'error_count': account.get('error_count', 0),
            'is_active': account in self.active_accounts,
            'is_limited': account in self.limited_accounts
        }
        
        # Add connection status if client is available
        client = account.get('client')
        if client:
            try:
                status['is_connected'] = client.is_connected()
                if status['is_connected']:
                    status['connection_time'] = datetime.now() - account.get('last_activity', datetime.now())
            except Exception as e:
                status['connection_error'] = str(e)
                status['is_connected'] = False
                
        return status
    
    async def get_available_account(self, purpose: str = 'forwarding') -> Optional[Dict]:
        """Get an available account that hasn't reached daily limit.
        
        Args:
            purpose: The purpose for which the account is needed (e.g., 'forwarding', 'scraping').
                    Can be used to implement different rate limits for different purposes.
            
        Returns:
            Available account dictionary or None if no accounts are available.
        """
        current_date = datetime.now().date()
        available_accounts = []
        
        async with self._config_lock:
            for account in self.active_accounts:
                # Skip if account is not in a usable state
                if account.get('status') != 'active' or account in self.limited_accounts:
                    continue
                    
                # Reset daily counter if new day
                if account.get('last_reset') != current_date:
                    account['added_today'] = 0
                    account['last_reset'] = current_date
                
                # Check if account hasn't reached limit
                daily_limit = self.config.get('daily_limit', 45)
                if account.get('added_today', 0) < daily_limit:
                    # Calculate a score based on recent activity and usage
                    last_activity = account.get('last_activity')
                    time_since_last_activity = (datetime.now() - last_activity).total_seconds() if last_activity else float('inf')
                    
                    # Prefer accounts that have been idle longer
                    score = time_since_last_activity
                    
                    available_accounts.append((score, account))
        
        if not available_accounts:
            logging.warning("No available accounts found")
            return None
            
        # Sort by score (highest first) and return the best account
        available_accounts.sort(key=lambda x: x[0], reverse=True)
        best_account = available_accounts[0][1]
        
        # Update last activity
        best_account['last_activity'] = datetime.now()
        best_account['added_today'] = best_account.get('added_today', 0) + 1
        
        logging.info(f"Selected account {best_account.get('phone')} for {purpose}. "
                    f"Used {best_account.get('added_today', 0)}/{self.config.get('daily_limit', 45)} today")
        
        return best_account
    
    async def get_account_status_report(self, detailed: bool = False) -> Dict:
        """Generate a comprehensive status report for all accounts.
        
        Args:
            detailed: If True, include detailed information for each account.
            
        Returns:
            Dictionary containing account statistics and details.
        """
        current_time = datetime.now()
        
        # Initialize report structure
        report = {
            'timestamp': current_time.isoformat(),
            'total_accounts': len(self.accounts),
            'active_accounts': len(self.active_accounts),
            'limited_accounts': len(self.limited_accounts),
            'daily_limit': self.config.get('daily_limit', 45),
            'accounts': [],
            'statistics': {
                'total_added_today': 0,
                'total_remaining': 0,
                'by_status': {},
                'errors_last_24h': 0
            }
        }
        
        status_counts = {}
        
        # Process each account
        for account in self.accounts:
            status = account.get('status', 'unknown')
            
            # Update status counts
            status_counts[status] = status_counts.get(status, 0) + 1
            
            # Calculate time since last activity
            last_activity = account.get('last_activity')
            if last_activity:
                time_since_activity = (current_time - last_activity).total_seconds()
                time_since_activity_str = f"{int(time_since_activity // 3600)}h {int((time_since_activity % 3600) // 60)}m"
            else:
                time_since_activity_str = "Never"
            
            # Basic account info
            account_info = {
                'phone': account.get('phone'),
                'username': account.get('username'),
                'user_id': account.get('user_id'),
                'status': status,
                'added_today': account.get('added_today', 0),
                'daily_limit': self.config.get('daily_limit', 45),
                'remaining': max(0, self.config.get('daily_limit', 45) - account.get('added_today', 0)),
                'last_activity': account.get('last_activity', {}).isoformat() if account.get('last_activity') else None,
                'time_since_activity': time_since_activity_str,
                'is_active': account in self.active_accounts,
                'is_limited': account in self.limited_accounts,
                'error_count': account.get('error_count', 0)
            }
            
            # Add detailed info if requested
            if detailed:
                client = account.get('client')
                if client:
                    try:
                        account_info['is_connected'] = client.is_connected()
                        if account_info['is_connected']:
                            me = await client.get_me()
                            account_info['first_name'] = me.first_name
                            account_info['last_name'] = me.last_name
                            account_info['username'] = me.username
                            account_info['premium'] = getattr(me, 'premium', None)
                    except Exception as e:
                        account_info['connection_error'] = str(e)
            
            report['accounts'].append(account_info)
            
            # Update statistics
            report['statistics']['total_added_today'] += account.get('added_today', 0)
            report['statistics']['total_remaining'] += max(0, self.config.get('daily_limit', 45) - account.get('added_today', 0))
            report['statistics']['errors_last_24h'] += account.get('error_count', 0)
        
        # Add status counts to statistics
        report['statistics']['by_status'] = status_counts
        
        # Calculate overall health score (0-100)
        total_capacity = len(self.accounts) * self.config.get('daily_limit', 45)
        if total_capacity > 0:
            used_capacity = report['statistics']['total_added_today']
            report['statistics']['capacity_used_percent'] = min(100, int((used_capacity / total_capacity) * 100))
        else:
            report['statistics']['capacity_used_percent'] = 0
        
        return report
    
    async def save_accounts(self, filename: str = 'accounts.json') -> bool:
        """Save accounts to a JSON file with error handling and atomic write.
        
        Args:
            filename: Path to the file where accounts will be saved.
            
        Returns:
            bool: True if save was successful, False otherwise.
        """
        if not filename:
            logging.error("No filename provided for saving accounts")
            return False
            
        # Create backup of existing file if it exists
        backup_file = None
        if os.path.exists(filename):
            try:
                backup_file = f"{filename}.bak"
                if os.path.exists(backup_file):
                    os.remove(backup_file)
                os.rename(filename, backup_file)
            except Exception as e:
                logging.warning(f"Could not create backup of {filename}: {e}")
        
        # Prepare accounts data for serialization
        accounts_data = []
        async with self._config_lock:
            for account in self.accounts:
                try:
                    # Create a copy without the client object
                    account_copy = {
                        'phone': account.get('phone'),
                        'api_id': account.get('api_id'),
                        'api_hash': account.get('api_hash'),
                        'proxy': account.get('proxy'),
                        'added_today': account.get('added_today', 0),
                        'last_reset': account.get('last_reset', datetime.now().date()).isoformat(),
                        'status': account.get('status', 'unknown'),
                        'username': account.get('username'),
                        'user_id': account.get('user_id'),
                        'created_at': account.get('created_at', datetime.now()).isoformat(),
                        'last_activity': account.get('last_activity', datetime.now()).isoformat() if account.get('last_activity') else None,
                        'error_count': account.get('error_count', 0),
                        'last_error': account.get('last_error')
                    }
                    accounts_data.append(account_copy)
                except Exception as e:
                    logging.error(f"Error preparing account {account.get('phone')} for save: {e}")
        
        # Write to a temporary file first
        temp_file = f"{filename}.tmp"
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(accounts_data, f, indent=2, ensure_ascii=False, default=str)
            
            # Rename temp file to target file (atomic on Unix, not on Windows but best we can do)
            if os.path.exists(filename):
                os.remove(filename)
            os.rename(temp_file, filename)
            
            # Remove backup if everything succeeded
            if backup_file and os.path.exists(backup_file):
                os.remove(backup_file)
                
            logging.info(f"Successfully saved {len(accounts_data)} accounts to {filename}")
            return True
            
        except Exception as e:
            logging.error(f"Error saving accounts to {filename}: {e}")
            
            # Restore from backup if possible
            if backup_file and os.path.exists(backup_file):
                try:
                    if os.path.exists(filename):
                        os.remove(filename)
                    os.rename(backup_file, filename)
                    logging.info("Restored accounts from backup")
                except Exception as restore_error:
                    logging.error(f"Failed to restore from backup: {restore_error}")
            
            # Clean up temp file if it exists
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass
                    
            return False
    
    async def load_accounts(self, filename: str = 'accounts.json') -> bool:
        """Load accounts from a JSON file with validation and error handling.
        
        Args:
            filename: Path to the file containing account data.
            
        Returns:
            bool: True if load was successful, False otherwise.
        """
        if not os.path.exists(filename):
            logging.info(f"No accounts file found at {filename}")
            return False
            
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                accounts_data = json.load(f)
                
            if not isinstance(accounts_data, list):
                logging.error(f"Invalid accounts data format in {filename}")
                return False
            
            loaded_count = 0
            async with self._config_lock:
                for account_data in accounts_data:
                    try:
                        # Validate required fields
                        if not all(key in account_data for key in ['phone', 'api_id', 'api_hash']):
                            logging.warning("Skipping account with missing required fields")
                            continue
                            
                        # Convert string dates back to datetime objects
                        date_fields = ['last_reset', 'created_at', 'last_activity']
                        for field in date_fields:
                            if field in account_data and account_data[field]:
                                if isinstance(account_data[field], str):
                                    try:
                                        account_data[field] = datetime.fromisoformat(account_data[field])
                                    except (ValueError, TypeError) as e:
                                        logging.warning(f"Invalid date format for {field}: {e}")
                                        account_data[field] = datetime.now()
                                
                        # Initialize client as None - will be created when needed
                        account_data['client'] = None
                        
                        # Add to appropriate lists
                        self.accounts.append(account_data)
                        status = account_data.get('status', 'unknown')
                        
                        if status == 'active':
                            self.active_accounts.append(account_data)
                        elif status == 'limited':
                            self.limited_accounts.append(account_data)
                            
                        loaded_count += 1
                        
                    except Exception as e:
                        logging.error(f"Error loading account data: {e}")
                        continue
            
            logging.info(f"Successfully loaded {loaded_count} accounts from {filename}")
            return True
            
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in accounts file {filename}: {e}")
            return False
        except Exception as e:
            logging.error(f"Error loading accounts from {filename}: {e}")
            return False
            
    async def _test_account_connection(self, account: Dict[str, Any], max_retries: int = 3) -> Tuple[bool, str]:
        """Test connection to Telegram servers for an account.
        
        Args:
            account: The account dictionary to test.
            max_retries: Maximum number of connection attempts.
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        if not account or 'phone' not in account:
            return False, "Invalid account data"
            
        phone = account.get('phone')
        client = None
        
        for attempt in range(1, max_retries + 1):
            try:
                # Create a new client if needed
                if not client:
                    client = TelegramClient(
                        os.path.join(self.config['sessions_path'], str(phone)),
                        account['api_id'],
                        account['api_hash'],
                        proxy=account.get('proxy'),
                        device_model='Telegram Manager Bot',
                        system_version='1.0',
                        app_version='1.0',
                        lang_code='en',
                        system_lang_code='en-US'
                    )
                
                # Connect to Telegram
                await client.connect()
                
                # Check if we're authorized
                if not await client.is_user_authorized():
                    return False, "Account not authorized. Please sign in first."
                
                # Get user info to verify connection
                me = await client.get_me()
                if not me:
                    return False, "Failed to get account info"
                
                # Update account info
                async with self._config_lock:
                    account['username'] = me.username
                    account['user_id'] = me.id
                    account['last_activity'] = datetime.now()
                    account['status'] = 'active'
                    account['error_count'] = 0
                    account['last_error'] = None
                
                logging.info(f"Successfully connected account {phone}")
                return True, "Connection successful"
                
            except (ConnectionError, TimeoutError) as e:
                if attempt == max_retries:
                    error_msg = f"Connection error for {phone} (attempt {attempt}/{max_retries}): {e}"
                    logging.error(error_msg)
                    return False, f"Connection failed: {str(e)}"
                await asyncio.sleep(1 * attempt)  # Exponential backoff
                
            except FloodWaitError as e:
                wait_time = e.seconds
                error_msg = f"Flood wait for {phone}: {wait_time} seconds"
                logging.warning(error_msg)
                return False, f"Flood wait error: Please try again in {wait_time} seconds"
                
            except SessionPasswordNeededError:
                return False, "2FA password required. Please provide the password."
                
            except PhoneNumberBannedError:
                error_msg = f"Phone number {phone} is banned"
                logging.error(error_msg)
                async with self._config_lock:
                    account['status'] = 'banned'
                    account['last_error'] = "Phone number banned"
                return False, "This phone number has been banned by Telegram"
                
            except Exception as e:
                error_msg = f"Unexpected error connecting {phone}: {type(e).__name__}: {e}"
                logging.error(error_msg, exc_info=True)
                if attempt == max_retries:
                    async with self._config_lock:
                        account['error_count'] = account.get('error_count', 0) + 1
                        account['last_error'] = str(e)
                    return False, f"Connection failed: {str(e)}"
                await asyncio.sleep(1 * attempt)  # Exponential backoff
                
            finally:
                # Disconnect the test client
                if client and client.is_connected():
                    try:
                        await client.disconnect()
                    except Exception as e:
                        logging.warning(f"Error disconnecting test client: {e}")
        
        return False, "Max retries reached"
    
    async def disconnect_account(self, phone: str) -> Tuple[bool, str]:
        """Safely disconnect an account.
        
        Args:
            phone: Phone number of the account to disconnect.
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        account = self.get_account_by_phone(phone)
        if not account:
            return False, "Account not found"
            
        client = account.get('client')
        if not client:
            return True, "Account already disconnected"
            
        try:
            if client.is_connected():
                await client.disconnect()
                
            # Update status
            async with self._config_lock:
                account['client'] = None
                account['status'] = 'disconnected'
                if account in self.active_accounts:
                    self.active_accounts.remove(account)
                
            logging.info(f"Successfully disconnected account {phone}")
            return True, "Successfully disconnected account"
            
        except Exception as e:
            error_msg = f"Error disconnecting account {phone}: {e}"
            logging.error(error_msg)
            return False, error_msg
    
    async def reconnect_account(self, phone: str) -> Tuple[bool, str]:
        """Reconnect a disconnected account.
        
        Args:
            phone: Phone number of the account to reconnect.
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        account = self.get_account_by_phone(phone)
        if not account:
            return False, "Account not found"
            
        if account.get('client') and account['client'].is_connected():
            return True, "Account is already connected"
            
        # Test connection
        success, message = await self._test_account_connection(account, max_retries=2)
        
        if success:
            async with self._config_lock:
                account['status'] = 'active'
                if account not in self.active_accounts:
                    self.active_accounts.append(account)
                if account in self.limited_accounts:
                    self.limited_accounts.remove(account)
                    
            logging.info(f"Successfully reconnected account {phone}")
            return True, "Successfully reconnected account"
        else:
            return False, f"Failed to reconnect: {message}"
    
    async def remove_account(self, phone: str, delete_session: bool = False) -> Tuple[bool, str]:
        """Remove an account from the manager.
        
        Args:
            phone: Phone number of the account to remove.
            delete_session: If True, also delete the session file.
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        account = self.get_account_by_phone(phone)
        if not account:
            return False, "Account not found"
            
        # Disconnect if connected
        if account.get('client'):
            try:
                if account['client'].is_connected():
                    await account['client'].disconnect()
            except Exception as e:
                logging.warning(f"Error disconnecting account during removal: {e}")
            
        # Delete session file if requested
        if delete_session and 'phone' in account:
            session_path = os.path.join(
                self.config['sessions_path'],
                f"{account['phone']}.session"
            )
            if os.path.exists(session_path):
                try:
                    os.remove(session_path)
                    logging.info(f"Deleted session file for {phone}")
                except Exception as e:
                    logging.error(f"Error deleting session file: {e}")
        
        # Remove from all lists
        async with self._config_lock:
            if account in self.accounts:
                self.accounts.remove(account)
            if account in self.active_accounts:
                self.active_accounts.remove(account)
            if account in self.limited_accounts:
                self.limited_accounts.remove(account)
        
        logging.info(f"Removed account {phone} from manager")
        return True, "Account removed successfully"
    
    async def cleanup(self) -> None:
        """Clean up resources and disconnect all accounts."""
        logging.info("Starting account manager cleanup...")
        
        tasks = []
        for account in self.accounts:
            if account.get('client'):
                tasks.append(self.disconnect_account(account['phone']))
        
        # Wait for all disconnects to complete
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Save accounts state
        await self.save_accounts()
        
        logging.info("Account manager cleanup completed")
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - ensures cleanup."""
        await self.cleanup()
    
    def __del__(self):
        """Destructor - ensures cleanup."""
        if hasattr(self, 'accounts'):
            for account in self.accounts:
                if account.get('client') and account['client'].is_connected():
                    try:
                        asyncio.create_task(account['client'].disconnect())
                    except:
                        pass
