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
        # self.forwarding_rules = {} # This likely was meant to be loaded from DB, get_forwarding_rules does that.
        self._initialized = False

        self.logger = logging.getLogger(__name__) # Use __name__ for module-level logger
        # Configure logger only if no handlers are already set (to avoid duplicate logs if main app configures root)
        if not self.logger.handlers and not logging.getLogger().handlers:
            log_file = Path(self.account_manager._config.get('logs_path', 'logs')) / 'forwarding_manager.log'
            log_file.parent.mkdir(parents=True, exist_ok=True)
            handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(self.account_manager._config.get('log_level', logging.INFO))

        self.db_connection = None

    async def initialize(self):
        """Initialize the forwarding manager asynchronously"""
        if not self._initialized:
            db_path = Path(self.account_manager._config['sessions_path']) / self.account_manager._config.get('forwarding_database_name', 'forwarding.db')
            self.db_connection = sqlite3.connect(db_path, check_same_thread=False)
            self.db_connection.row_factory = sqlite3.Row # Access columns by name
            self.setup_database()
            # Load active rules or prepare sessions on init if needed (currently lazy)
            await self.restart_active_rules_from_db()
            self._initialized = True

    def setup_database(self):
        """Setup SQLite database for forwarding configuration"""
        try:
            cursor = self.db_connection.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS forwarding_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_phone TEXT NOT NULL,
                    source_chat_id INTEGER NOT NULL,
                    source_chat_name TEXT,
                    destination_chat_ids TEXT NOT NULL, -- JSON list of chat IDs
                    keywords TEXT, -- JSON list of keywords or NULL
                    status TEXT DEFAULT 'stopped', -- 'running', 'stopped', 'error'
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_message_id INTEGER DEFAULT 0, -- For tracking last forwarded message
                    messages_forwarded INTEGER DEFAULT 0
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS forwarded_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rule_id INTEGER NOT NULL,
                    account_phone TEXT NOT NULL,
                    source_chat_id INTEGER NOT NULL,
                    source_message_id INTEGER NOT NULL,
                    destination_chat_id INTEGER NOT NULL,
                    destination_message_id INTEGER, -- Can be NULL if forward fails before getting new ID
                    message_text TEXT,
                    forwarded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (rule_id) REFERENCES forwarding_rules (id) ON DELETE CASCADE
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS forwarding_errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rule_id INTEGER,
                    account_phone TEXT,
                    error_type TEXT,
                    error_message TEXT,
                    occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (rule_id) REFERENCES forwarding_rules (id) ON DELETE CASCADE
                )
            ''')

            self.db_connection.commit()
            self.logger.info("Forwarding database setup/verified successfully.")

        except Exception as e:
            self.logger.error(f"Error setting up forwarding database: {e}", exc_info=True)
            # Fallback to print if logger itself failed during init
            if not self.logger.handlers: print(f"CRITICAL: Logger not set up. DB Error: {e}")
            raise # Propagate error if DB setup fails

    async def close(self):
        """Close database connection and clean up resources."""
        self.logger.info("Closing ForwardingManager resources...")
        # Stop all active forwarding sessions first
        # This might involve iterating through self.forwarding_sessions and calling stop_forwarding
        # For simplicity, we assume event handlers will stop if client disconnects or via status check
        # A more robust stop would explicitly remove event handlers.
        # For now, we just ensure the DB connection is closed.
        active_rule_ids = [session_data['rule_id'] for session_data in self.forwarding_sessions.values() if session_data.get('status') == 'running']
        for rule_id in active_rule_ids:
            await self.stop_forwarding(rule_id, update_db=False) # Stop without individual DB update, will be overall inactive

        if self.db_connection:
            try:
                self.db_connection.close()
                self.db_connection = None
                self.logger.info("ForwardingManager database connection closed.")
            except sqlite3.Error as e:
                self.logger.error(f"Error closing ForwardingManager database connection: {e}")
        self._initialized = False


    async def get_account_groups(self, account_phone):
        account = self.account_manager.get_account_by_phone(account_phone)
        if not account:
            self.logger.error(f"Account {account_phone} not found via account_manager.")
            return []

        client = self.account_manager._clients.get(account_phone) # Access client via _clients
        if not client:
            self.logger.error(f"Client for account {account_phone} not found in account_manager._clients.")
            # Try to connect it if AccountManager allows direct connect
            # This depends on AccountManager's design. For now, assume client should be ready.
            return []

        if not client.is_connected():
            self.logger.warning(f"Client for {account_phone} is not connected. Attempting to connect.")
            # This might be better handled by ensuring connect_account is called before this
            # For now, let's try a connect if AccountManager provides such a method readily.
            # success, _ = await self.account_manager.connect_account(account_phone) # Assuming this exists and works
            # if not success or not client.is_connected():
            #     self.logger.error(f"Failed to connect account {account_phone} for get_account_groups.")
            #     return []
            # For this refactor, we assume client is managed by AccountManager and should be ready/connectable
            # by the time this is called, or this method should handle it gracefully.
            # Simplification: if not connected, return empty for now. A robust app would ensure connection.
            self.logger.error(f"Account {account_phone} client not connected. Cannot fetch groups.")
            return []


        try:
            if not await client.is_user_authorized():
                self.logger.error(f"Account {account_phone} is not authorized.")
                return []

            dialogs = await client.get_dialogs()
            groups = []
            for dialog in dialogs:
                entity = dialog.entity
                if isinstance(entity, (types.Channel, types.Chat)): # Check if it's a Channel or Chat (group)
                    # Basic filtering: exclude private chats with users, focus on groups/channels
                    if isinstance(entity, types.User): continue

                    title = getattr(entity, 'title', f"Unknown Chat/Channel {entity.id}")
                    username = getattr(entity, 'username', None)
                    participants_count = getattr(entity, 'participants_count', 0)

                    is_channel = isinstance(entity, types.Channel)
                    is_broadcast = is_channel and getattr(entity, 'broadcast', False)
                    is_megagroup = is_channel and getattr(entity, 'megagroup', False)

                    chat_type = 'unknown'
                    if is_broadcast: chat_type = 'channel' # Broadcast channel
                    elif is_megagroup: chat_type = 'supergroup'
                    elif isinstance(entity, types.Chat): chat_type = 'group' # Basic group

                    # Can we send messages? (Simplistic check, real permissions are complex)
                    can_send = True # Assume true, refine if specific checks are needed
                    if hasattr(entity, 'admin_rights'):
                        can_send = entity.admin_rights.send_messages if entity.admin_rights else True
                    elif hasattr(entity, 'default_banned_rights'):
                        can_send = not entity.default_banned_rights.send_messages if entity.default_banned_rights else True

                    groups.append({
                        'id': entity.id, 'title': title, 'username': username,
                        'type': chat_type, 'members_count': participants_count,
                        'can_send_messages': can_send # Note: This is a basic check
                    })

            self.logger.info(f"Found {len(groups)} potential groups/channels for {account_phone}")
            return groups

        except Exception as e:
            self.logger.error(f"Error getting groups for {account_phone}: {e}", exc_info=True)
            return []

    async def create_forwarding_rule(self, account_phone, source_chat_id, source_chat_name,
                                   destination_chat_ids, keywords=None):
        if not self._initialized or not self.db_connection:
            self.logger.error("Database not initialized. Cannot create rule.")
            raise Exception("Database not initialized.")
        try:
            cursor = self.db_connection.cursor()
            dest_ids_json = json.dumps(list(set(destination_chat_ids))) # Ensure unique IDs
            keywords_json = json.dumps(keywords) if keywords else None
            now = datetime.now(timezone.utc) # Use timezone-aware datetime

            cursor.execute('''
                INSERT INTO forwarding_rules
                (account_phone, source_chat_id, source_chat_name, destination_chat_ids, keywords, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (account_phone, source_chat_id, source_chat_name, dest_ids_json, keywords_json, 'stopped', now, now))

            rule_id = cursor.lastrowid
            self.db_connection.commit()

            self.logger.info(f"Created forwarding rule {rule_id} for account {account_phone} from source {source_chat_name} ({source_chat_id}).")
            return rule_id
        except sqlite3.Error as e:
            self.logger.error(f"Database error creating forwarding rule: {e}", exc_info=True)
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error creating forwarding rule: {e}", exc_info=True)
            raise

    async def delete_forwarding_rule(self, rule_id: int) -> Tuple[bool, str]:
        """Stops forwarding and deletes the rule from the database."""
        if not self._initialized or not self.db_connection:
            self.logger.error("Database not initialized. Cannot delete rule.")
            return False, "Database not initialized."

        self.logger.info(f"Attempting to delete forwarding rule ID: {rule_id}")

        # First, ensure the rule is stopped
        stop_success, stop_message = await self.stop_forwarding(rule_id, update_db=False) # Stop session, don't update DB status yet
        if not stop_success and "Rule not found" not in stop_message : # If it wasn't found, it's already "stopped" in a way
            self.logger.warning(f"Could not stop rule {rule_id} (or it was already stopped): {stop_message}. Proceeding with deletion.")
            # If stop_forwarding failed for other reasons, we might still want to delete from DB.

        try:
            cursor = self.db_connection.cursor()
            cursor.execute("DELETE FROM forwarding_rules WHERE id = ?", (rule_id,))
            self.db_connection.commit()

            if cursor.rowcount > 0:
                self.logger.info(f"Successfully deleted forwarding rule {rule_id} from database.")
                return True, f"Rule {rule_id} deleted successfully."
            else:
                self.logger.warning(f"Rule {rule_id} not found in database for deletion.")
                return False, f"Rule {rule_id} not found in database."
        except sqlite3.Error as e:
            self.logger.error(f"Database error deleting rule {rule_id}: {e}", exc_info=True)
            return False, f"Database error: {e}"
        except Exception as e:
            self.logger.error(f"Unexpected error deleting rule {rule_id}: {e}", exc_info=True)
            return False, f"Unexpected error: {e}"

    async def start_forwarding(self, rule_id):
        if not self._initialized or not self.db_connection:
            self.logger.error("Database not initialized. Cannot start rule.")
            return False, "Database not initialized."
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("SELECT * FROM forwarding_rules WHERE id = ?", (rule_id,))
            rule_row = cursor.fetchone()

            if not rule_row: return False, "Rule not found"
            rule = dict(rule_row) # Convert to dict for easier access

            account_phone = rule['account_phone']
            source_chat_id = rule['source_chat_id']

            session_key = f"{account_phone}_{rule_id}"
            if session_key in self.forwarding_sessions and self.forwarding_sessions[session_key]['status'] == 'running':
                return True, "Forwarding already running for this rule."

            account = self.account_manager.get_account_by_phone(account_phone)
            client = self.account_manager._clients.get(account_phone)

            if not account or not client:
                return False, f"Account {account_phone} or its client not available/initialized."
            if not client.is_connected():
                self.logger.info(f"Client for {account_phone} not connected. Attempting connect for rule {rule_id}.")
                conn_success, conn_msg = await self.account_manager.connect_account(account_phone)
                if not conn_success: return False, f"Failed to connect account {account_phone}: {conn_msg}"
                # Client might be a new instance, re-fetch from account_manager
                client = self.account_manager._clients.get(account_phone)
                if not client: return False, "Client unavailable after reconnect attempt."


            if not await client.is_user_authorized():
                return False, f"Account {account_phone} is not authorized."

            now = datetime.now(timezone.utc)
            cursor.execute("UPDATE forwarding_rules SET status = 'running', updated_at = ? WHERE id = ?", (now, rule_id))
            self.db_connection.commit()

            self.forwarding_sessions[session_key] = {
                'rule_id': rule_id, 'account': account, 'client': client, # Store client directly
                'source_chat_id': source_chat_id,
                'destination_chat_ids': json.loads(rule['destination_chat_ids']),
                'keywords': json.loads(rule['keywords']) if rule['keywords'] else None,
                'status': 'running', 'messages_forwarded': rule['messages_forwarded'],
                'last_activity': now, 'last_message_id': rule.get('last_message_id', 0)
            }

            await self.setup_forwarding_handler(session_key)
            self.logger.info(f"Started forwarding for rule {rule_id} on account {account_phone}.")
            return True, "Forwarding started successfully."

        except Exception as e:
            self.logger.error(f"Error starting forwarding for rule {rule_id}: {e}", exc_info=True)
            self.log_error(rule_id, rule.get('account_phone') if 'rule' in locals() and rule else 'unknown', 'start_error', str(e))
            # Attempt to set status to 'error' in DB
            if self._initialized and self.db_connection:
                 cursor = self.db_connection.cursor()
                 cursor.execute("UPDATE forwarding_rules SET status = 'error', updated_at = ? WHERE id = ?", (datetime.now(timezone.utc), rule_id))
                 self.db_connection.commit()
            return False, f"Error: {str(e)}"

    async def setup_forwarding_handler(self, session_key):
        session = self.forwarding_sessions.get(session_key)
        if not session:
            self.logger.error(f"Session {session_key} not found for handler setup.")
            return

        client = session['client'] # Client is now stored in session data
        source_chat_id = session['source_chat_id']

        try:
            entity = await client.get_entity(source_chat_id)
            self.logger.info(f"Handler: Successfully accessed source chat '{getattr(entity, 'title', source_chat_id)}' for session {session_key}.")
        except ValueError as e: # Entity not found
            self.logger.error(f"Handler: Cannot access source chat {source_chat_id} for session {session_key}: {e}. Rule may not work.")
            self.log_error(session['rule_id'], session['account']['phone'], 'source_chat_inaccessible', str(e))
            # Update rule status to error in DB
            self._update_rule_status_in_db(session['rule_id'], 'error', f"Source chat {source_chat_id} inaccessible: {e}")
            session['status'] = 'error' # Update in-memory session too
            return
        except Exception as e: # Other errors like permissions, not connected (should be caught earlier)
            self.logger.error(f"Handler: Error getting entity for source chat {source_chat_id} in session {session_key}: {e}", exc_info=True)
            self.log_error(session['rule_id'], session['account']['phone'], 'source_chat_error', str(e))
            self._update_rule_status_in_db(session['rule_id'], 'error', f"Source chat error: {e}")
            session['status'] = 'error'
            return

        # Define unique handler name based on session_key to allow removal if needed, though Telethon typically overwrites.
        # However, direct removal by function object is more reliable if Telethon's internal list is accessible.
        # For now, rely on status check within handler.

        @client.on(events.NewMessage(chats=source_chat_id))
        async def message_handler(event: events.NewMessage.Event):
            # Check if this specific session is still active and running
            active_session = self.forwarding_sessions.get(session_key)
            if not active_session or active_session['status'] != 'running':
                self.logger.debug(f"Handler for session {session_key} invoked but session not running or found. Ignoring.")
                # Optionally, try to remove this specific handler instance if possible and if it's an old one.
                # client.remove_event_handler(message_handler) # This might be complex to do correctly here.
                return

            self.logger.info(f"🔥 MSG for rule {active_session['rule_id']} (session {session_key}): ID {event.message.id} from {event.chat_id}")
            active_session['last_activity'] = datetime.now(timezone.utc)

            # Update last_message_id in DB and session
            # This should be done *after* successful processing or based on requirements
            # For now, let's assume we process it, then update.
            # current_last_msg_id = active_session.get('last_message_id', 0)
            # if event.message.id > current_last_msg_id:
            # active_session['last_message_id'] = event.message.id
            # self._update_rule_last_message_id_in_db(active_session['rule_id'], event.message.id)

            try:
                await self.forward_message_to_destinations(session_key, event)
            except Exception as e: # Catch errors from forwarding logic
                self.logger.error(f"Error during forward_message_to_destinations for session {session_key}: {e}", exc_info=True)
                # Error is logged within forward_message_to_destinations already.

        # Add handler to client. Store it if removal is needed, though Telethon usually manages this.
        # client.add_event_handler(message_handler) # Not needed if using @client.on decorator and client is in scope.
        self.logger.info(f"✅ Event handler for NewMessage set up for session {session_key} on source {source_chat_id}.")


    async def forward_message_to_destinations(self, session_key, event):
        session = self.forwarding_sessions.get(session_key)
        if not session: self.logger.error(f"Session {session_key} disappeared during forwarding."); return

        client = session['client']
        message_text = event.message.message or ""
        rule_id = session['rule_id']
        account_phone = session['account']['phone']
        source_chat_id = session['source_chat_id'] # Actual chat_id from event if available event.chat_id

        if session['keywords']:
            if not any(k.lower() in message_text.lower() for k in session['keywords']):
                self.logger.debug(f"Rule {rule_id}: Message ID {event.message.id} filtered out (no keyword match).")
                return

        successful_forwards = 0
        for dest_chat_id in session['destination_chat_ids']:
            try:
                await asyncio.sleep(self.account_manager._config.get('delay_between_forwards_seconds', 1.5)) # Use a config value

                # Ensure client is still connected before each forward attempt
                if not client.is_connected():
                    self.logger.warning(f"Client for {account_phone} disconnected before forwarding to {dest_chat_id}. Attempting reconnect.")
                    conn_success, _ = await self.account_manager.connect_account(account_phone)
                    if not conn_success:
                        self.log_error(rule_id, account_phone, 'client_disconnected', f"Failed to reconnect for dest {dest_chat_id}")
                        continue # Skip this destination, try next or wait for next message
                    client = self.account_manager._clients.get(account_phone) # Get potentially new client instance
                    if not client: continue # Should not happen

                forwarded_msgs = await client.forward_messages(entity=dest_chat_id, messages=event.message, from_peer=source_chat_id)

                if forwarded_msgs:
                    dest_msg_id = forwarded_msgs[0].id if isinstance(forwarded_msgs, list) and forwarded_msgs else getattr(forwarded_msgs, 'id', None)
                    self.log_forwarded_message(rule_id, account_phone, source_chat_id, event.message.id, dest_chat_id, dest_msg_id, message_text[:250])
                    successful_forwards += 1
                self.logger.info(f"Rule {rule_id}: Forwarded msg {event.message.id} to {dest_chat_id} via {account_phone}")

            except errors.FloodWaitError as e:
                self.logger.warning(f"Rule {rule_id}: FloodWait to {dest_chat_id} ({e.seconds}s) via {account_phone}.")
                self.log_error(rule_id, account_phone, 'flood_wait', f"To {dest_chat_id}: {e.seconds}s")
                await asyncio.sleep(e.seconds + 5) # Wait out flood + buffer
            except (errors.ChatWriteForbiddenError, errors.ChannelPrivateError, errors.UserBannedInChannelError) as e:
                self.logger.warning(f"Rule {rule_id}: Permission error to {dest_chat_id} via {account_phone}: {e}")
                self.log_error(rule_id, account_phone, 'permission_error', f"To {dest_chat_id}: {type(e).__name__}")
                # Consider marking this destination as problematic for this rule
            except errors.PeerIdInvalidError:
                self.logger.warning(f"Rule {rule_id}: Invalid Peer ID for {dest_chat_id} via {account_phone}.")
                self.log_error(rule_id, account_phone, 'peer_invalid', f"To {dest_chat_id}")
            except Exception as e:
                self.logger.error(f"Rule {rule_id}: Error forwarding to {dest_chat_id} via {account_phone}: {e}", exc_info=True)
                self.log_error(rule_id, account_phone, 'general_forward_error', f"To {dest_chat_id}: {type(e).__name__} - {e}")

        if successful_forwards > 0 and self.db_connection:
            session['messages_forwarded'] += successful_forwards
            try:
                cursor = self.db_connection.cursor()
                cursor.execute("UPDATE forwarding_rules SET messages_forwarded = messages_forwarded + ?, updated_at = ? WHERE id = ?",
                               (successful_forwards, datetime.now(timezone.utc), rule_id))
                self.db_connection.commit()
            except sqlite3.Error as e:
                self.logger.error(f"DB Error updating messages_forwarded for rule {rule_id}: {e}")

        # Update last_message_id for the rule to prevent re-forwarding on restart
        # This should be the ID of the message from the source chat
        self._update_rule_last_message_id_in_db(rule_id, event.message.id)
        session['last_message_id'] = event.message.id


    def _update_rule_status_in_db(self, rule_id: int, status: str, error_message: Optional[str] = None):
        if not self._initialized or not self.db_connection: return
        try:
            cursor = self.db_connection.cursor()
            now = datetime.now(timezone.utc)
            if error_message:
                cursor.execute("UPDATE forwarding_rules SET status = ?, last_error = ?, updated_at = ? WHERE id = ?",
                               (status, error_message, now, rule_id))
            else:
                cursor.execute("UPDATE forwarding_rules SET status = ?, updated_at = ? WHERE id = ?", (status, now, rule_id))
            self.db_connection.commit()
        except sqlite3.Error as e:
            self.logger.error(f"DB Error updating status for rule {rule_id} to {status}: {e}")

    def _update_rule_last_message_id_in_db(self, rule_id: int, message_id: int):
        if not self._initialized or not self.db_connection: return
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("UPDATE forwarding_rules SET last_message_id = ?, updated_at = ? WHERE id = ?",
                           (message_id, datetime.now(timezone.utc), rule_id))
            self.db_connection.commit()
        except sqlite3.Error as e:
            self.logger.error(f"DB Error updating last_message_id for rule {rule_id}: {e}")


    def log_forwarded_message(self, rule_id, account_phone, source_chat_id, source_msg_id,
                             dest_chat_id, dest_msg_id, message_text):
        if not self._initialized or not self.db_connection: return
        try:
            cursor = self.db_connection.cursor()
            cursor.execute('''
                INSERT INTO forwarded_messages
                (rule_id, account_phone, source_chat_id, source_message_id,
                 destination_chat_id, destination_message_id, message_text, forwarded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (rule_id, account_phone, source_chat_id, source_msg_id,
                  dest_chat_id, dest_msg_id, message_text, datetime.now(timezone.utc)))
            self.db_connection.commit()
        except sqlite3.Error as e:
            self.logger.error(f"DB error logging forwarded message for rule {rule_id}: {e}")

    def log_error(self, rule_id, account_phone, error_type, error_message):
        if not self._initialized or not self.db_connection: return
        try:
            cursor = self.db_connection.cursor()
            cursor.execute('''
                INSERT INTO forwarding_errors
                (rule_id, account_phone, error_type, error_message, occurred_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (rule_id, account_phone, error_type, str(error_message)[:1000], datetime.now(timezone.utc))) # Limit error message length
            self.db_connection.commit()
        except sqlite3.Error as e:
            self.logger.error(f"DB error logging error for rule {rule_id}: {e}")

    async def stop_forwarding(self, rule_id, update_db=True):
        try:
            rule_to_stop = None
            # Find account_phone for session_key construction
            # This might be simplified if rule details are already in self.forwarding_sessions
            session_key_to_remove = None
            for sk, sess_data in self.forwarding_sessions.items():
                if sess_data['rule_id'] == rule_id:
                    session_key_to_remove = sk
                    rule_to_stop = sess_data
                    break

            if session_key_to_remove:
                self.forwarding_sessions[session_key_to_remove]['status'] = 'stopped'
                # TODO: Properly remove event handlers from client if possible/needed.
                # This is complex as handlers are anonymous within the decorator.
                # Relying on status check inside handler is current approach.
                del self.forwarding_sessions[session_key_to_remove]
                self.logger.info(f"Removed session {session_key_to_remove} from active sessions.")
            else: # Rule might not be in active sessions (e.g. already stopped, or error)
                 self.logger.info(f"Rule {rule_id} not found in active forwarding_sessions. Ensuring DB status is 'stopped'.")


            if update_db and self._initialized and self.db_connection:
                self._update_rule_status_in_db(rule_id, 'stopped')

            self.logger.info(f"Forwarding stopped for rule ID: {rule_id}")
            return True, "Forwarding stopped successfully."

        except Exception as e:
            self.logger.error(f"Error stopping forwarding for rule {rule_id}: {e}", exc_info=True)
            return False, f"Error: {str(e)}"

    async def restart_active_rules_from_db(self):
        """Load rules with 'running' status from DB and attempt to start them."""
        if not self._initialized or not self.db_connection:
            self.logger.error("Cannot restart rules, DB not initialized.")
            return
        self.logger.info("Checking for 'running' rules in DB to restart...")
        rules = await self.get_forwarding_rules() # Gets all rules
        restarted_count = 0
        for rule_data in rules:
            if rule_data['status'] == 'running':
                self.logger.info(f"Attempting to restart rule ID {rule_data['id']} (status was 'running')...")
                success, message = await self.start_forwarding(rule_data['id']) # start_forwarding handles client checks
                if success:
                    restarted_count += 1
                else:
                    self.logger.error(f"Failed to restart rule ID {rule_data['id']}: {message}. Setting status to 'error'.")
                    self._update_rule_status_in_db(rule_data['id'], 'error', f"Restart failed: {message}")
        self.logger.info(f"Restarted {restarted_count} active rules.")


    async def get_forwarding_rules(self, account_phone=None):
        if not self._initialized or not self.db_connection: return []
        try:
            cursor = self.db_connection.cursor()
            if account_phone:
                cursor.execute("SELECT * FROM forwarding_rules WHERE account_phone = ? ORDER BY created_at DESC", (account_phone,))
            else:
                cursor.execute("SELECT * FROM forwarding_rules ORDER BY created_at DESC")

            rules_rows = cursor.fetchall()
            formatted_rules = [dict(row) for row in rules_rows] # Convert rows to dicts
            for rule in formatted_rules: # Parse JSON fields
                if rule.get('destination_chat_ids'): rule['destination_chat_ids'] = json.loads(rule['destination_chat_ids'])
                if rule.get('keywords'): rule['keywords'] = json.loads(rule['keywords'])
            return formatted_rules
        except sqlite3.Error as e:
            self.logger.error(f"DB error getting forwarding rules: {e}", exc_info=True)
            return []

    async def get_forwarding_statistics(self, rule_id=None): # Assumes DB is initialized
        if not self._initialized or not self.db_connection: return {}
        try:
            # ... (implementation is mostly fine, ensure DB connection is used)
            # This method primarily queries DB, so it's okay.
            cursor = self.db_connection.cursor()
            base_query_messages = "FROM forwarded_messages"
            base_query_errors = "FROM forwarding_errors"
            params = ()

            if rule_id:
                base_query_messages += " WHERE rule_id = ?"
                base_query_errors += " WHERE rule_id = ?"
                params = (rule_id,)

            cursor.execute(f"SELECT COUNT(*), COUNT(DISTINCT destination_chat_id), MIN(forwarded_at), MAX(forwarded_at) {base_query_messages}", params)
            stats_msg = cursor.fetchone()
            cursor.execute(f"SELECT COUNT(*) {base_query_errors}", params)
            error_count = cursor.fetchone()[0]

            return {
                'total_forwarded': stats_msg[0] if stats_msg else 0,
                'unique_destinations': stats_msg[1] if stats_msg else 0,
                'first_forward': stats_msg[2] if stats_msg else None,
                'last_forward': stats_msg[3] if stats_msg else None,
                'total_errors': error_count
            }
        except sqlite3.Error as e:
            self.logger.error(f"DB error getting forwarding stats for rule {rule_id}: {e}", exc_info=True)
            return {} # Return empty on error

    async def test_forwarding_manually(self, rule_id): # Assumes DB is initialized
        if not self._initialized or not self.db_connection: return False, "DB not initialized."
        # ... (implementation is mostly fine, ensure client is fetched correctly if not in session)
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("SELECT * FROM forwarding_rules WHERE id = ?", (rule_id,))
            rule_row = cursor.fetchone()
            if not rule_row: return False, "Rule not found"
            rule = dict(rule_row)

            account_phone = rule['account_phone']
            source_chat_id = rule['source_chat_id']
            destination_chat_ids = json.loads(rule['destination_chat_ids'])

            account = self.account_manager.get_account_by_phone(account_phone)
            client = self.account_manager._clients.get(account_phone) # Get active client

            if not account or not client:
                 return False, f"Account {account_phone} or its client not active/available."
            if not client.is_connected():
                 conn_ok, conn_msg = await self.account_manager.connect_account(account_phone)
                 if not conn_ok: return False, f"Could not connect {account_phone}: {conn_msg}"
                 client = self.account_manager._clients.get(account_phone) # Re-fetch client

            messages = await client.get_messages(source_chat_id, limit=1)
            if not messages: return False, "No messages in source chat."

            if destination_chat_ids:
                await client.forward_messages(entity=destination_chat_ids[0], messages=messages[0], from_peer=source_chat_id)
                return True, f"Test forward of message {messages[0].id} successful."
            return False, "No destinations."
        except Exception as e:
            self.logger.error(f"Error in manual test for rule {rule_id}: {e}", exc_info=True)
            return False, str(e)

[end of forwarding_manager.py]
