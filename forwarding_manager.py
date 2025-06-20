import sqlite3
import json
import logging
import asyncio
import traceback
from datetime import datetime
from telethon import events, errors
from telethon.tl.types import Channel, Chat

class TelegramForwardingManager:
    def __init__(self, account_manager):
        self.account_manager = account_manager
        self.forwarding_sessions = {}
        self.forwarding_rules = {}
        self._initialized = False
        
        # Initialize logger FIRST before anything else
        self.logger = logging.getLogger('ForwardingManager')
        if not self.logger.handlers:
            handler = logging.FileHandler('logs/forwarding.log')
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        
        # Setup database connection
        self.db_connection = None
        
    async def initialize(self):
        """Initialize the forwarding manager asynchronously"""
        if not self._initialized:
            # Setup database
            self.db_connection = sqlite3.connect('forwarding.db', check_same_thread=False)
            self.setup_database()
            self._initialized = True
        
    def setup_database(self):
        """Setup SQLite database for forwarding configuration"""
        try:
            cursor = self.db_connection.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS forwarding_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_phone TEXT,
                    source_chat_id INTEGER,
                    source_chat_name TEXT,
                    destination_chat_ids TEXT,
                    keywords TEXT,
                    status TEXT DEFAULT 'stopped',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_message_id INTEGER DEFAULT 0,
                    messages_forwarded INTEGER DEFAULT 0
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS forwarded_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rule_id INTEGER,
                    account_phone TEXT,
                    source_chat_id INTEGER,
                    source_message_id INTEGER,
                    destination_chat_id INTEGER,
                    destination_message_id INTEGER,
                    message_text TEXT,
                    forwarded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (rule_id) REFERENCES forwarding_rules (id)
                )
            ''')
            
            # Add error logging table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS forwarding_errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rule_id INTEGER,
                    account_phone TEXT,
                    error_type TEXT,
                    error_message TEXT,
                    occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            self.db_connection.commit()
            self.logger.info("Database setup completed")
            
        except Exception as e:
            print(f"Error setting up database: {e}")  # Fallback to print if logger fails
    
    async def get_account_groups(self, account_phone):
        """Get all groups/channels the account is member of with enhanced error handling"""
        account = self.account_manager.get_account_by_phone(account_phone)
        if not account or not account['client']:
            self.logger.error(f"Account {account_phone} not found or client not available")
            return []
        
        try:
            client = account['client']
            if not await client.is_user_authorized():
                self.logger.error(f"Account {account_phone} is not authorized")
                return []
            
            # Get dialogs with retry logic
            retries = 3
            for attempt in range(retries):
                try:
                    dialogs = await client.get_dialogs()
                    break
                except Exception as e:
                    if attempt == retries - 1:
                        raise e
                    await asyncio.sleep(2)
            
            groups = []
            for dialog in dialogs:
                try:
                    entity = dialog.entity
                    
                    if isinstance(entity, (Channel, Chat)):
                        if isinstance(entity, Channel):
                            # Include both channels and supergroups where we can send messages
                            if hasattr(entity, 'broadcast') and entity.broadcast:
                                # Skip broadcast channels unless we're admin
                                if not hasattr(entity, 'admin_rights') or not entity.admin_rights:
                                    continue
                                    
                            groups.append({
                                'id': entity.id,
                                'title': entity.title or f"Channel {entity.id}",
                                'username': getattr(entity, 'username', None),
                                'type': 'supergroup' if getattr(entity, 'megagroup', False) else 'channel',
                                'members_count': getattr(entity, 'participants_count', 0),
                                'can_send_messages': True
                            })
                        else:  # Regular group
                            groups.append({
                                'id': entity.id,
                                'title': entity.title or f"Group {entity.id}",
                                'username': None,
                                'type': 'group',
                                'members_count': getattr(entity, 'participants_count', 0),
                                'can_send_messages': True
                            })
                except Exception as e:
                    self.logger.warning(f"Error processing dialog entity: {e}")
                    continue
            
            self.logger.info(f"Found {len(groups)} groups for {account_phone}")
            return groups
            
        except Exception as e:
            self.logger.error(f"Error getting groups for {account_phone}: {e}")
            return []
    
    async def create_forwarding_rule(self, account_phone, source_chat_id, source_chat_name, 
                                   destination_chat_ids, keywords=None):
        """Create a new forwarding rule with enhanced validation"""
        try:
            cursor = self.db_connection.cursor()
            
            dest_ids_json = json.dumps(destination_chat_ids)
            keywords_json = json.dumps(keywords) if keywords else None
            
            cursor.execute('''
                INSERT INTO forwarding_rules 
                (account_phone, source_chat_id, source_chat_name, destination_chat_ids, keywords)
                VALUES (?, ?, ?, ?, ?)
            ''', (account_phone, source_chat_id, source_chat_name, dest_ids_json, keywords_json))
            
            rule_id = cursor.lastrowid
            self.db_connection.commit()
            
            self.logger.info(f"Created forwarding rule {rule_id} for {account_phone}")
            return rule_id
            
        except Exception as e:
            self.logger.error(f"Error creating forwarding rule: {e}")
            raise e
    
    async def start_forwarding(self, rule_id):
        """Start message forwarding for a specific rule with enhanced error handling"""
        try:
            cursor = self.db_connection.cursor()
            cursor.execute('SELECT * FROM forwarding_rules WHERE id = ?', (rule_id,))
            rule = cursor.fetchone()
            
            if not rule:
                return False, "Rule not found"
            
            account_phone = rule[1]
            source_chat_id = rule[2]
            destination_chat_ids = json.loads(rule[4])
            keywords = json.loads(rule[5]) if rule[5] else None
            
            account = self.account_manager.get_account_by_phone(account_phone)
            if not account or not account['client']:
                return False, "Account not available or not connected"
            
            # Verify account is authorized
            try:
                if not await account['client'].is_user_authorized():
                    return False, "Account is not authorized"
            except Exception as e:
                return False, f"Account connection error: {str(e)}"
            
            # Update rule status
            cursor.execute('UPDATE forwarding_rules SET status = ? WHERE id = ?', ('running', rule_id))
            self.db_connection.commit()
            
            # Start forwarding session
            session_key = f"{account_phone}_{rule_id}"
            self.forwarding_sessions[session_key] = {
                'rule_id': rule_id,
                'account': account,
                'source_chat_id': source_chat_id,
                'destination_chat_ids': destination_chat_ids,
                'keywords': keywords,
                'status': 'running',
                'messages_forwarded': 0,
                'last_activity': datetime.now()
            }
            
            # Set up event handler
            await self.setup_forwarding_handler(session_key)
            
            self.logger.info(f"Started forwarding for rule {rule_id}")
            return True, "Forwarding started successfully"
            
        except Exception as e:
            self.logger.error(f"Error starting forwarding for rule {rule_id}: {e}")
            self.log_error(rule_id, account_phone if 'account_phone' in locals() else 'unknown', 
                          'start_error', str(e))
            return False, f"Error starting forwarding: {str(e)}"
    
    async def setup_forwarding_handler(self, session_key):
        """Setup message forwarding event handler with enhanced debugging"""
        try:
            session = self.forwarding_sessions[session_key]
            client = session['account']['client']
            source_chat_id = session['source_chat_id']
            rule_id = session['rule_id']
            
            # Test if we can access the channel first
            try:
                entity = await client.get_entity(source_chat_id)
                self.logger.info(f"Successfully accessed source chat: {entity.title}")
                
                # Get recent messages to test access
                messages = await client.get_messages(entity, limit=1)
                self.logger.info(f"Can read messages from source chat: {len(messages)} messages found")
                
            except Exception as e:
                self.logger.error(f"Cannot access source chat {source_chat_id}: {e}")
                return
            
            @client.on(events.NewMessage(chats=source_chat_id))
            async def message_handler(event):
                try:
                    self.logger.info(f"🔥 NEW MESSAGE RECEIVED in session {session_key}")
                    self.logger.info(f"Message ID: {event.message.id}")
                    self.logger.info(f"Message text: {event.message.message[:100]}...")
                    self.logger.info(f"From chat: {event.chat_id}")
                    
                    if session_key not in self.forwarding_sessions:
                        self.logger.warning(f"Session {session_key} no longer exists")
                        return
                    
                    if self.forwarding_sessions[session_key]['status'] != 'running':
                        self.logger.warning(f"Session {session_key} is not running")
                        return
                    
                    # Update last activity
                    self.forwarding_sessions[session_key]['last_activity'] = datetime.now()
                    
                    await self.forward_message_to_destinations(session_key, event)
                    
                except Exception as e:
                    self.logger.error(f"Error in message handler for session {session_key}: {e}")
                    self.logger.error(traceback.format_exc())
            
            self.logger.info(f"✅ Forwarding handler setup completed for session {session_key}")
            
            # Test the handler by checking if we can catch existing messages
            try:
                test_messages = await client.get_messages(source_chat_id, limit=1)
                if test_messages:
                    self.logger.info(f"✅ Can access messages from source chat. Latest message ID: {test_messages[0].id}")
                else:
                    self.logger.warning(f"⚠️ No messages found in source chat")
            except Exception as e:
                self.logger.error(f"❌ Error testing message access: {e}")
            
        except Exception as e:
            self.logger.error(f"Error setting up forwarding handler for {session_key}: {e}")
            self.logger.error(traceback.format_exc())
            raise e
    
    async def forward_message_to_destinations(self, session_key, event):
        """Forward message to all destination chats with enhanced error handling"""
        session = self.forwarding_sessions[session_key]
        client = session['account']['client']
        keywords = session['keywords']
        destination_chat_ids = session['destination_chat_ids']
        rule_id = session['rule_id']
        
        try:
            message_text = event.message.message or ""
            
            # Check keywords filter
            if keywords:
                keyword_match = any(keyword.lower() in message_text.lower() for keyword in keywords)
                if not keyword_match:
                    self.logger.info(f"Message filtered out - no keyword match")
                    return  # Message doesn't contain required keywords
            
            successful_forwards = 0
            failed_forwards = 0
            
            # Forward to each destination with individual error handling
            for dest_chat_id in destination_chat_ids:
                try:
                    # Add delay to prevent rate limiting
                    await asyncio.sleep(1)
                    
                    # Forward the original message
                    forwarded_msg = await client.forward_messages(
                        entity=dest_chat_id,
                        messages=event.message,
                        from_peer=event.chat_id
                    )
                    
                    # Log successful forward
                    if forwarded_msg:
                        self.log_forwarded_message(
                            rule_id,
                            session['account']['phone'],
                            event.chat_id,
                            event.message.id,
                            dest_chat_id,
                            forwarded_msg[0].id,
                            message_text[:200]  # Store first 200 chars
                        )
                        successful_forwards += 1
                        
                        self.logger.info(f"✅ Message forwarded from {event.chat_id} to {dest_chat_id}")
                    
                except errors.FloodWaitError as e:
                    self.logger.warning(f"Flood wait error for destination {dest_chat_id}: {e.seconds} seconds")
                    self.log_error(rule_id, session['account']['phone'], 'flood_wait', 
                                 f"Destination {dest_chat_id}: {e.seconds}s")
                    failed_forwards += 1
                    # Wait for flood wait period
                    await asyncio.sleep(min(e.seconds, 300))  # Max 5 minutes
                    
                except errors.ChatWriteForbiddenError:
                    self.logger.warning(f"Cannot write to chat {dest_chat_id}")
                    self.log_error(rule_id, session['account']['phone'], 'write_forbidden', 
                                 f"Destination {dest_chat_id}")
                    failed_forwards += 1
                    
                except errors.PeerIdInvalidError:
                    self.logger.warning(f"Invalid peer ID {dest_chat_id}")
                    self.log_error(rule_id, session['account']['phone'], 'invalid_peer', 
                                 f"Destination {dest_chat_id}")
                    failed_forwards += 1
                    
                except Exception as e:
                    self.logger.error(f"Error forwarding to {dest_chat_id}: {e}")
                    self.log_error(rule_id, session['account']['phone'], 'forward_error', 
                                 f"Destination {dest_chat_id}: {str(e)}")
                    failed_forwards += 1
            
            # Update session statistics
            session['messages_forwarded'] += successful_forwards
            
            # Update database statistics
            cursor = self.db_connection.cursor()
            cursor.execute('''
                UPDATE forwarding_rules 
                SET messages_forwarded = messages_forwarded + ? 
                WHERE id = ?
            ''', (successful_forwards, rule_id))
            self.db_connection.commit()
            
            self.logger.info(f"Forwarding completed for rule {rule_id}: {successful_forwards} successful, {failed_forwards} failed")
            
        except Exception as e:
            self.logger.error(f"Error in forward_message_to_destinations: {e}")
            self.log_error(rule_id, session['account']['phone'], 'forward_general_error', str(e))
    
    def log_forwarded_message(self, rule_id, account_phone, source_chat_id, source_msg_id, 
                             dest_chat_id, dest_msg_id, message_text):
        """Log forwarded message to database"""
        try:
            cursor = self.db_connection.cursor()
            cursor.execute('''
                INSERT INTO forwarded_messages 
                (rule_id, account_phone, source_chat_id, source_message_id, 
                 destination_chat_id, destination_message_id, message_text)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (rule_id, account_phone, source_chat_id, source_msg_id, 
                  dest_chat_id, dest_msg_id, message_text))
            self.db_connection.commit()
        except Exception as e:
            self.logger.error(f"Error logging forwarded message: {e}")
    
    def log_error(self, rule_id, account_phone, error_type, error_message):
        """Log error to database"""
        try:
            cursor = self.db_connection.cursor()
            cursor.execute('''
                INSERT INTO forwarding_errors 
                (rule_id, account_phone, error_type, error_message)
                VALUES (?, ?, ?, ?)
            ''', (rule_id, account_phone, error_type, error_message))
            self.db_connection.commit()
        except Exception as e:
            self.logger.error(f"Error logging error: {e}")
    
    async def stop_forwarding(self, rule_id):
        """Stop forwarding for a specific rule"""
        try:
            cursor = self.db_connection.cursor()
            cursor.execute('SELECT account_phone FROM forwarding_rules WHERE id = ?', (rule_id,))
            result = cursor.fetchone()
            
            if not result:
                return False, "Rule not found"
            
            account_phone = result[0]
            session_key = f"{account_phone}_{rule_id}"
            
            # Update rule status
            cursor.execute('UPDATE forwarding_rules SET status = ? WHERE id = ?', ('stopped', rule_id))
            self.db_connection.commit()
            
            # Remove session
            if session_key in self.forwarding_sessions:
                self.forwarding_sessions[session_key]['status'] = 'stopped'
                del self.forwarding_sessions[session_key]
            
            self.logger.info(f"Stopped forwarding for rule {rule_id}")
            return True, "Forwarding stopped successfully"
            
        except Exception as e:
            self.logger.error(f"Error stopping forwarding for rule {rule_id}: {e}")
            return False, f"Error stopping forwarding: {str(e)}"
    
    async def get_forwarding_rules(self, account_phone=None):
        """Get all forwarding rules with enhanced information"""
        try:
            cursor = self.db_connection.cursor()
            
            if account_phone:
                cursor.execute('SELECT * FROM forwarding_rules WHERE account_phone = ? ORDER BY created_at DESC', (account_phone,))
            else:
                cursor.execute('SELECT * FROM forwarding_rules ORDER BY created_at DESC')
            
            rules = cursor.fetchall()
            
            formatted_rules = []
            for rule in rules:
                formatted_rules.append({
                    'id': rule[0],
                    'account_phone': rule[1],
                    'source_chat_id': rule[2],
                    'source_chat_name': rule[3],
                    'destination_chat_ids': json.loads(rule[4]),
                    'keywords': json.loads(rule[5]) if rule[5] else None,
                    'status': rule[6],
                    'created_at': rule[7],
                    'last_message_id': rule[8],
                    'messages_forwarded': rule[9] if len(rule) > 9 else 0
                })
            
            return formatted_rules
            
        except Exception as e:
            self.logger.error(f"Error getting forwarding rules: {e}")
            return []
    
    async def get_forwarding_statistics(self, rule_id=None):
        """Get comprehensive forwarding statistics"""
        try:
            cursor = self.db_connection.cursor()
            
            if rule_id:
                # Statistics for specific rule
                cursor.execute('''
                    SELECT COUNT(*) as total_forwarded,
                           COUNT(DISTINCT destination_chat_id) as unique_destinations,
                           MIN(forwarded_at) as first_forward,
                           MAX(forwarded_at) as last_forward
                    FROM forwarded_messages 
                    WHERE rule_id = ?
                ''', (rule_id,))
            else:
                # Global statistics
                cursor.execute('''
                    SELECT COUNT(*) as total_forwarded,
                           COUNT(DISTINCT destination_chat_id) as unique_destinations,
                           MIN(forwarded_at) as first_forward,
                           MAX(forwarded_at) as last_forward
                    FROM forwarded_messages
                ''')
            
            stats = cursor.fetchone()
            
            # Get error statistics
            if rule_id:
                cursor.execute('SELECT COUNT(*) FROM forwarding_errors WHERE rule_id = ?', (rule_id,))
            else:
                cursor.execute('SELECT COUNT(*) FROM forwarding_errors')
            
            error_count = cursor.fetchone()[0]
            
            return {
                'total_forwarded': stats[0] if stats else 0,
                'unique_destinations': stats[1] if stats else 0,
                'first_forward': stats[2] if stats else None,
                'last_forward': stats[3] if stats else None,
                'total_errors': error_count
            }
            
        except Exception as e:
            self.logger.error(f"Error getting forwarding statistics: {e}")
            return {
                'total_forwarded': 0,
                'unique_destinations': 0,
                'first_forward': None,
                'last_forward': None,
                'total_errors': 0
            }
    
    async def test_forwarding_manually(self, rule_id):
        """Manually test forwarding by getting the latest message"""
        try:
            cursor = self.db_connection.cursor()
            cursor.execute('SELECT * FROM forwarding_rules WHERE id = ?', (rule_id,))
            rule = cursor.fetchone()
            
            if not rule:
                return False, "Rule not found"
            
            account_phone = rule[1]
            source_chat_id = rule[2]
            destination_chat_ids = json.loads(rule[4])
            
            account = self.account_manager.get_account_by_phone(account_phone)
            if not account or not account['client']:
                return False, "Account not available"
            
            client = account['client']
            
            # Get the latest message from source
            messages = await client.get_messages(source_chat_id, limit=1)
            if not messages:
                return False, "No messages found in source chat"
            
            latest_message = messages[0]
            self.logger.info(f"Latest message: {latest_message.message[:100]}...")
            
            # Forward to first destination as test
            if destination_chat_ids:
                try:
                    forwarded = await client.forward_messages(
                        entity=destination_chat_ids[0],
                        messages=latest_message,
                        from_peer=source_chat_id
                    )
                    return True, f"Test forward successful: {forwarded[0].id}"
                except Exception as e:
                    return False, f"Test forward failed: {str(e)}"
            
            return False, "No destinations configured"
            
        except Exception as e:
            self.logger.error(f"Error in manual test: {e}")
            return False, str(e)
