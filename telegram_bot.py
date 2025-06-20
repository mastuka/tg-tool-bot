import logging
import asyncio
import traceback
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes, ConversationHandler
from telegram.error import TelegramError, Forbidden, BadRequest, TimedOut, NetworkError
import json

# Conversation states
SETUP_ACCOUNT, SETUP_SOURCE, SETUP_DESTINATIONS, SETUP_KEYWORDS = range(4)

class TelegramManagerBot:
    def __init__(self, config):
        self.config = config
        self.bot_token = config['bot_token']
        self.authorized_users = config['authorized_users']
        self.application = None
        
        # Initialize managers
        from account_manager import TelegramAccountManager
        from forwarding_manager import TelegramForwardingManager
        
        # Initialize account manager with config
        account_manager_config = {
            'api_id': config.get('api_id', ''),
            'api_hash': config.get('api_hash', ''),
            'sessions_path': 'sessions',
            'logs_path': 'logs'
        }
        self.account_manager = TelegramAccountManager(account_manager_config)
        self.forwarding_manager = TelegramForwardingManager(self.account_manager)
        
        # Setup enhanced logging
        self.setup_logging()
        
    async def initialize(self):
        """Initialize the bot asynchronously"""
        try:
            # Initialize the account manager
            await self.account_manager.initialize()
            
            # Initialize the forwarding manager
            await self.forwarding_manager.initialize()
            
            # Create the Application
            self.application = Application.builder().token(self.bot_token).build()
            
            # Add command handlers
            self.setup_handlers()
            
            # Add error handler
            self.application.add_error_handler(self.error_handler)
            
            # Add conversation handler
            self.application.add_handler(self.setup_conversation_handler())
            
            # Add message handler
            self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.message_handler))
            
            logging.info("🤖 Bot initialized successfully")
            
        except Exception as e:
            logging.error(f"Error initializing bot: {e}")
            raise
        
    def setup_handlers(self):
        """Setup all command and message handlers"""
        # Start command
        self.application.add_handler(CommandHandler("start", self.start_command))
        
        # Add other command handlers here
        self.application.add_handler(CommandHandler("test_rule", self.test_rule_command))
        
        # Add callback query handler for inline buttons
        self.application.add_handler(CallbackQueryHandler(self.button_handler))
        
    async def run_async(self):
        """Run the bot asynchronously"""
        if not self.application:
            await self.initialize()
            
        # Start the bot
        await self.application.initialize()
        await self.application.start()
        await self.application.run_polling()
        
    def run(self):
        """Synchronous run method for backward compatibility"""
        try:
            # Get or create event loop
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # Run the async code
            return loop.run_until_complete(self.run_async())
            
        except KeyboardInterrupt:
            logging.info("\n🛑 Received stop signal. Shutting down gracefully...")
            return 0
        except Exception as e:
            logging.error(f"Fatal error: {e}")
            logging.error(traceback.format_exc())
            return 1
        
    def setup_logging(self):
        """Setup enhanced logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/bot.log'),
                logging.StreamHandler()
            ]
        )
        
    async def error_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle errors and exceptions"""
        try:
            error = context.error
            logging.error(f"Update {update} caused error {error}")
            
            if isinstance(error, Forbidden):
                logging.warning("Bot was blocked by user or kicked from chat")
            elif isinstance(error, BadRequest):
                logging.warning(f"Bad request: {error}")
            elif isinstance(error, TimedOut):
                logging.warning("Request timed out")
            elif isinstance(error, NetworkError):
                logging.warning(f"Network error: {error}")
            else:
                logging.error(f"Unknown error: {error}")
                logging.error(traceback.format_exc())
            
            # Notify user if possible
            if update and update.effective_chat:
                try:
                    await context.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text="❌ An error occurred. Please try again later."
                    )
                except Exception:
                    pass
                    
        except Exception as e:
            logging.error(f"Error in error handler: {e}")
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Enhanced start command handler"""
        try:
            user_id = update.effective_user.id
            
            if user_id not in self.authorized_users:
                await update.message.reply_text("❌ You are not authorized to use this bot.")
                return
            
            keyboard = [
                [InlineKeyboardButton("📱 Account Manager", callback_data="account_manager")],
                [InlineKeyboardButton("📨 Auto Forwarding", callback_data="auto_forwarding")],
                [InlineKeyboardButton("👥 Member Manager", callback_data="member_manager")],
                [InlineKeyboardButton("📊 Status Report", callback_data="status_report")],
                [InlineKeyboardButton("⚙️ Settings", callback_data="settings")]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            welcome_text = (
                "🤖 **Telegram Manager Bot v2.0**\n\n"
                "**Enhanced Features:**\n"
                "📱 Multi-Account Management\n"
                "📨 Real-time Auto Forwarding\n"
                "👥 Advanced Member Management\n"
                "🔧 Enhanced Error Handling\n"
                "📊 Real-time Statistics\n\n"
                "Choose an option to get started:"
            )
            
            await update.message.reply_text(
                welcome_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            logging.error(f"Error in start_command: {e}")
            await update.message.reply_text("❌ Error starting bot. Please try again.")
            
    async def test_rule_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Test a forwarding rule manually"""
        try:
            user_id = update.effective_user.id
            if user_id not in self.authorized_users:
                return
            
            # Get rule ID from command
            if not context.args:
                await update.message.reply_text("Usage: /test_rule <rule_id>")
                return
            
            try:
                rule_id = int(context.args[0])
            except ValueError:
                await update.message.reply_text("Invalid rule ID")
                return
            
            progress_msg = await update.message.reply_text("🧪 Testing forwarding rule...")
            
            success, message = await self.forwarding_manager.test_forwarding_manually(rule_id)
            
            if success:
                await progress_msg.edit_text(f"✅ **Test Successful**\n\n{message}")
            else:
                await progress_msg.edit_text(f"❌ **Test Failed**\n\n{message}")
                
        except Exception as e:
            logging.error(f"Error in test_rule_command: {e}")
            await update.message.reply_text("❌ Error testing rule.")

    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Enhanced button handler with proper error handling"""
        try:
            query = update.callback_query
            await query.answer()
            
            user_id = update.effective_user.id
            if user_id not in self.authorized_users:
                await query.edit_message_text("❌ You are not authorized to use this bot.")
                return
            
            data = query.data
            logging.info(f"Button pressed: {data} by user {user_id}")
            
            # Main menu handlers
            if data == "account_manager":
                await self.account_manager_menu(query)
            elif data == "auto_forwarding":
                await self.forwarding_menu(query)
            elif data == "member_manager":
                await self.member_manager_menu(query)
            elif data == "status_report":
                await self.view_accounts(query)
            elif data == "settings":
                await self.settings_menu(query)
            elif data == "back_to_main":
                await self.show_main_menu(query)
                
            # Account management handlers
            elif data == "add_account":
                return await self.add_account_prompt(update, context)
            elif data == "view_accounts":
                await self.view_accounts(query)
            elif data == "refresh_accounts":
                await self.refresh_accounts(query)
            elif data == "test_connections":
                await self.test_connections(query)
                
            # Forwarding handlers
            elif data == "create_forward_rule":
                return await self.create_forwarding_rule_start(update, context)
            elif data == "view_forward_rules":
                await self.view_forwarding_rules(query)
            elif data == "forward_stats":
                await self.show_forwarding_stats(query)
            elif data == "manage_rules":
                await self.view_forwarding_rules(query)
            elif data.startswith("manage_rule_"):
                rule_id = int(data.split('_')[2])
                await self.manage_rule(query, rule_id)
            elif data.startswith("start_rule_"):
                rule_id = int(data.split('_')[2])
                await self.start_rule(query, rule_id)
            elif data.startswith("stop_rule_"):
                rule_id = int(data.split('_')[2])
                await self.stop_rule(query, rule_id)
            elif data.startswith("delete_rule_"):
                rule_id = int(data.split('_')[2])
                await self.delete_rule(query, rule_id)
            elif data.startswith("stats_rule_"):
                rule_id = int(data.split('_')[2])
                await self.show_rule_stats(query, rule_id)
                
            # Settings handlers
            elif data == "bot_settings":
                await self.bot_settings_menu(query)
            elif data == "system_info":
                await self.show_system_info(query)
            elif data == "export_data":
                await self.export_data(query)
            elif data == "clean_database":
                await self.clean_database(query)
                
            # Member management handlers
            elif data == "add_members":
                await self.add_members_menu(query)
            elif data == "scrape_members":
                await self.scrape_members_menu(query)
            elif data == "member_stats":
                await self.member_stats_menu(query)
                
        except Exception as e:
            logging.error(f"Error in button_handler: {e}")
            logging.error(traceback.format_exc())
            try:
                await query.edit_message_text("❌ An error occurred. Please try again.")
            except:
                pass
    
    async def show_main_menu(self, query):
        """Show main menu"""
        keyboard = [
            [InlineKeyboardButton("📱 Account Manager", callback_data="account_manager")],
            [InlineKeyboardButton("📨 Auto Forwarding", callback_data="auto_forwarding")],
            [InlineKeyboardButton("👥 Member Manager", callback_data="member_manager")],
            [InlineKeyboardButton("📊 Status Report", callback_data="status_report")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="settings")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "🤖 **Telegram Manager Bot v2.0**\n\n"
            "Choose an option:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    async def account_manager_menu(self, query):
        """Enhanced account manager menu"""
        keyboard = [
            [InlineKeyboardButton("➕ Add Account", callback_data="add_account")],
            [InlineKeyboardButton("📋 View Accounts", callback_data="view_accounts")],
            [InlineKeyboardButton("🔄 Refresh Status", callback_data="refresh_accounts")],
            [InlineKeyboardButton("🔍 Test Connections", callback_data="test_connections")],
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "📱 **Account Manager**\n\n"
            "Manage your Telegram accounts:\n"
            "• Add new accounts\n"
            "• View account status\n"
            "• Monitor daily limits\n"
            "• Test connections\n\n"
            "Choose an option:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    async def forwarding_menu(self, query):
        """Enhanced forwarding menu"""
        keyboard = [
            [InlineKeyboardButton("📝 Create Rule", callback_data="create_forward_rule")],
            [InlineKeyboardButton("📋 View Rules", callback_data="view_forward_rules")],
            [InlineKeyboardButton("📊 Statistics", callback_data="forward_stats")],
            [InlineKeyboardButton("🔄 Resume All", callback_data="resume_forwarding")],
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "📨 **Auto Forwarding Manager**\n\n"
            "Setup automatic message forwarding:\n"
            "• Forward from source to all groups\n"
            "• Keyword filtering\n"
            "• Multi-account support\n"
            "• Real-time monitoring\n\n"
            "Choose an option:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    async def member_manager_menu(self, query):
        """Enhanced member manager menu"""
        keyboard = [
            [InlineKeyboardButton("➕ Add Members", callback_data="add_members")],
            [InlineKeyboardButton("🔍 Scrape Members", callback_data="scrape_members")],
            [InlineKeyboardButton("📊 Member Stats", callback_data="member_stats")],
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "👥 **Member Manager**\n\n"
            "Manage group members:\n"
            "• Add members to groups\n"
            "• Scrape member lists\n"
            "• View statistics\n\n"
            "Choose an option:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    async def settings_menu(self, query):
        """Enhanced settings menu"""
        keyboard = [
            [InlineKeyboardButton("⚙️ Bot Settings", callback_data="bot_settings")],
            [InlineKeyboardButton("📊 System Info", callback_data="system_info")],
            [InlineKeyboardButton("🗂️ Export Data", callback_data="export_data")],
            [InlineKeyboardButton("🧹 Clean Database", callback_data="clean_database")],
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "⚙️ **Settings & System**\n\n"
            "Configure bot settings and manage system:\n"
            "• Bot configuration\n"
            "• System information\n"
            "• Data management\n\n"
            "Choose an option:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    async def add_account_prompt(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Enhanced add account prompt"""
        query = update.callback_query
        await query.answer()
        
        await query.edit_message_text(
            "📱 **Add New Account**\n\n"
            "Send account details in this format:\n"
            "`phone:api_id:api_hash:proxy(optional)`\n\n"
            "**Example:**\n"
            "`+1234567890:123456:abcdef123456`\n"
            "or with proxy:\n"
            "`+1234567890:123456:abcdef123456:proxy.com:1080:user:pass`\n\n"
            "**Get API credentials from:** https://my.telegram.org\n\n"
            "⚠️ **Note:** Make sure to use a valid phone number and correct API credentials.",
            parse_mode='Markdown'
        )
        
        context.user_data['conversation_state'] = 'waiting_account_details'
        return 'waiting_account_details'
    
    async def process_account_details(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Enhanced account details processing"""
        try:
            details = update.message.text.split(':')
            
            if len(details) < 3:
                await update.message.reply_text(
                    "❌ Invalid format. Please use:\n"
                    "`phone:api_id:api_hash:proxy(optional)`",
                    parse_mode='Markdown'
                )
                return 'waiting_account_details'
            
            phone = details[0]
            try:
                api_id = int(details[1])
            except ValueError:
                await update.message.reply_text("❌ API ID must be a number")
                return 'waiting_account_details'
                
            api_hash = details[2]
            proxy = ':'.join(details[3:]) if len(details) > 3 else None
            
            progress_msg = await update.message.reply_text("⏳ Adding account... Please wait.")
            
            # Add account
            success = await self.account_manager.add_account(phone, api_id, api_hash, proxy)
            
            if success:
                account = self.account_manager.get_account_by_phone(phone)
                if account and account['status'] == 'code_required':
                    await progress_msg.edit_text(
                        f"📱 **Account {phone} needs verification**\n\n"
                        f"Please send the verification code you received via SMS."
                    )
                    context.user_data['pending_phone'] = phone
                    context.user_data['conversation_state'] = 'waiting_auth_code'
                    return 'waiting_auth_code'
                else:
                    await progress_msg.edit_text(f"✅ Account {phone} added successfully!")
                    self.account_manager.save_accounts()
            else:
                await progress_msg.edit_text(f"❌ Failed to add account {phone}")
            
        except Exception as e:
            logging.error(f"Error processing account details: {e}")
            await update.message.reply_text("❌ Error processing account details. Please try again.")
        
        context.user_data['conversation_state'] = None
        return ConversationHandler.END
    
    async def process_auth_code(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process authentication code"""
        try:
            phone = context.user_data.get('pending_phone')
            if not phone:
                await update.message.reply_text("❌ No pending authentication")
                return ConversationHandler.END
            
            code = update.message.text.strip()
            
            progress_msg = await update.message.reply_text("⏳ Verifying code...")
            
            success, message = await self.account_manager.complete_auth(phone, code)
            
            if success:
                await progress_msg.edit_text(f"✅ Account {phone} authenticated successfully!")
                self.account_manager.save_accounts()
            else:
                if "2FA password required" in message:
                    await progress_msg.edit_text(
                        "🔐 **2FA Password Required**\n\n"
                        "Please send your 2FA password:"
                    )
                    context.user_data['conversation_state'] = 'waiting_2fa_password'
                    return 'waiting_2fa_password'
                else:
                    await progress_msg.edit_text(f"❌ Authentication failed: {message}")
            
            context.user_data['conversation_state'] = None
            return ConversationHandler.END
            
        except Exception as e:
            logging.error(f"Error processing auth code: {e}")
            await update.message.reply_text("❌ Error processing code.")
            return ConversationHandler.END
    
    async def process_2fa_password(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process 2FA password"""
        try:
            phone = context.user_data.get('pending_phone')
            password = update.message.text.strip()
            
            progress_msg = await update.message.reply_text("⏳ Verifying 2FA password...")
            
            success, message = await self.account_manager.complete_auth(phone, None, password)
            
            if success:
                await progress_msg.edit_text(f"✅ Account {phone} authenticated successfully!")
                self.account_manager.save_accounts()
            else:
                await progress_msg.edit_text(f"❌ 2FA authentication failed: {message}")
            
            context.user_data['conversation_state'] = None
            return ConversationHandler.END
            
        except Exception as e:
            logging.error(f"Error processing 2FA password: {e}")
            await update.message.reply_text("❌ Error processing 2FA password.")
            return ConversationHandler.END
    
    async def view_accounts(self, query):
        """Enhanced account viewing with real-time status"""
        try:
            await query.edit_message_text("⏳ Loading account status...")
            
            report = await self.account_manager.get_account_status_report()
            
            if report['total_accounts'] == 0:
                keyboard = [[InlineKeyboardButton("➕ Add First Account", callback_data="add_account")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await query.edit_message_text(
                    "📱 **No Accounts Found**\n\n"
                    "Add your first account to get started!",
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
                return
            
            status_text = f"📱 **Account Status Report**\n\n"
            status_text += f"📊 **Summary:**\n"
            status_text += f"• Total: {report['total_accounts']}\n"
            status_text += f"• Active: {report['active_accounts']}\n"
            status_text += f"• Limited: {report['limited_accounts']}\n\n"
            
            status_text += "📋 **Account Details:**\n"
            for i, account in enumerate(report['accounts_detail'][:5]):
                if account['status'] == 'active':
                    status_emoji = "✅"
                elif account['status'] == 'code_required':
                    status_emoji = "🔐"
                elif account['status'] == 'limited':
                    status_emoji = "⚠️"
                else:
                    status_emoji = "❌"
                
                status_text += (
                    f"{status_emoji} **{account['phone']}**\n"
                    f"   Status: {account['status']}\n"
                    f"   Today: {account['added_today']}/{account['daily_limit']}\n"
                    f"   Remaining: {account['remaining']}\n\n"
                )
            
            keyboard = [
                [InlineKeyboardButton("🔄 Refresh", callback_data="refresh_accounts")],
                [InlineKeyboardButton("➕ Add Account", callback_data="add_account")],
                [InlineKeyboardButton("🔙 Back", callback_data="account_manager")]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                status_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            logging.error(f"Error viewing accounts: {e}")
            await query.edit_message_text("❌ Error loading accounts. Please try again.")
    
    async def refresh_accounts(self, query):
        """Refresh account connections and status"""
        try:
            await query.edit_message_text("⏳ Refreshing account connections...")
            
            # Test all account connections
            for account in self.account_manager.accounts:
                await self.account_manager.test_account_connection(account)
            
            await asyncio.sleep(2)  # Give time for connections to establish
            await self.view_accounts(query)
            
        except Exception as e:
            logging.error(f"Error refreshing accounts: {e}")
            await query.edit_message_text("❌ Error refreshing accounts.")
    
    async def test_connections(self, query):
        """Test all account connections"""
        try:
            await query.edit_message_text("⏳ Testing account connections...")
            
            results = []
            for account in self.account_manager.accounts:
                try:
                    success = await self.account_manager.test_account_connection(account)
                    status = "✅ Connected" if success else "❌ Failed"
                    results.append(f"• {account['phone']}: {status}")
                except Exception as e:
                    results.append(f"• {account['phone']}: ❌ Error - {str(e)}")
            
            result_text = "🔍 **Connection Test Results**\n\n" + "\n".join(results)
            
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="account_manager")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                result_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            logging.error(f"Error testing connections: {e}")
            await query.edit_message_text("❌ Error testing connections.")
    
    async def create_forwarding_rule_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Enhanced forwarding rule creation"""
        try:
            query = update.callback_query
            await query.answer()
            
            # First ensure accounts are connected
            active_accounts = []
            for account in self.account_manager.accounts:
                if account['status'] == 'active' and account['client']:
                    try:
                        if await account['client'].is_user_authorized():
                            active_accounts.append(account)
                    except Exception as e:
                        logging.error(f"Error checking account {account['phone']}: {e}")
            
            if not active_accounts:
                await query.edit_message_text(
                    "❌ **No Active Accounts Available**\n\n"
                    "Please add and authenticate accounts first.\n\n"
                    "Steps:\n"
                    "1. Go to Account Manager\n"
                    "2. Add Account\n"
                    "3. Complete verification\n"
                    "4. Try again",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Back", callback_data="auto_forwarding")
                    ]]),
                    parse_mode='Markdown'
                )
                return ConversationHandler.END
            
            keyboard = []
            for i, account in enumerate(active_accounts):
                keyboard.append([InlineKeyboardButton(
                    f"📱 {account['phone']}", 
                    callback_data=f"select_account_{i}"
                )])
            
            keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="auto_forwarding")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                "📱 **Step 1: Select Account**\n\n"
                f"Found {len(active_accounts)} active accounts.\n"
                "Choose which account to use for forwarding:",
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            
            context.user_data['active_accounts'] = active_accounts
            return SETUP_ACCOUNT
            
        except Exception as e:
            logging.error(f"Error starting forwarding rule creation: {e}")
            await query.edit_message_text("❌ Error starting rule creation.")
            return ConversationHandler.END
    
    async def setup_account_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle account selection for forwarding"""
        try:
            query = update.callback_query
            await query.answer()
            
            if query.data == "auto_forwarding":
                await self.forwarding_menu(query)
                return ConversationHandler.END
            
            account_index = int(query.data.split('_')[2])
            selected_account = context.user_data['active_accounts'][account_index]
            context.user_data['selected_account'] = selected_account
            
            # Get groups for this account
            await query.edit_message_text("⏳ Loading groups...")
            groups = await self.forwarding_manager.get_account_groups(selected_account['phone'])
            
            if not groups:
                await query.edit_message_text(
                    f"❌ No groups found for account {selected_account['phone']}.\n\n"
                    "Make sure:\n"
                    "• The account is in some groups\n"
                    "• The account has proper permissions\n"
                    "• Try refreshing account status",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Back", callback_data="auto_forwarding")
                    ]]),
                    parse_mode='Markdown'
                )
                return ConversationHandler.END
            
            # Show source group selection
            keyboard = []
            for i, group in enumerate(groups[:15]):  # Show first 15 groups
                keyboard.append([InlineKeyboardButton(
                    f"📢 {group['title'][:35]}...", 
                    callback_data=f"select_source_{i}"
                )])
            
            keyboard.extend([
                [InlineKeyboardButton("🔄 Refresh Groups", callback_data=f"refresh_groups_{account_index}")],
                [InlineKeyboardButton("🔙 Back", callback_data="create_forward_rule")]
            ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                f"📢 **Step 2: Select Source Group**\n\n"
                f"Account: `{selected_account['phone']}`\n"
                f"Found {len(groups)} groups\n\n"
                "Choose the source group to forward FROM:",
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            
            context.user_data['groups'] = groups
            return SETUP_SOURCE
            
        except Exception as e:
            logging.error(f"Error in setup_account_selection: {e}")
            await query.edit_message_text("❌ Error loading account groups.")
            return ConversationHandler.END
    
    async def setup_source_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle source group selection"""
        try:
            query = update.callback_query
            await query.answer()
            
            if query.data == "create_forward_rule":
                return await self.create_forwarding_rule_start(update, context)
            
            source_index = int(query.data.split('_')[2])
            selected_source = context.user_data['groups'][source_index]
            context.user_data['selected_source'] = selected_source
            
            # Get available destinations (all groups except source)
            available_destinations = [g for g in context.user_data['groups'] 
                                    if g['id'] != selected_source['id']]
            
            if not available_destinations:
                await query.edit_message_text(
                    "❌ No destination groups available.\n"
                    "You need at least 2 groups to set up forwarding.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Back", callback_data="create_forward_rule")
                    ]])
                )
                return ConversationHandler.END
            
            keyboard = []
            keyboard.append([InlineKeyboardButton("✅ All Groups", callback_data="select_all_dest")])
            
            for i, group in enumerate(available_destinations[:10]):
                keyboard.append([InlineKeyboardButton(
                    f"📢 {group['title'][:35]}...", 
                    callback_data=f"toggle_dest_{i}"
                )])
            
            keyboard.extend([
                [InlineKeyboardButton("✅ Confirm Selection", callback_data="confirm_destinations")],
                [InlineKeyboardButton("🔙 Back", callback_data="select_source_back")]
            ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                f"🎯 **Step 3: Select Destination Groups**\n\n"
                f"Source: `{selected_source['title']}`\n"
                f"Available destinations: {len(available_destinations)}\n\n"
                "Choose where to forward TO:\n"
                "(Select 'All Groups' or choose specific ones)",
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            
            context.user_data['available_destinations'] = available_destinations
            context.user_data['selected_destinations'] = []
            return SETUP_DESTINATIONS
            
        except Exception as e:
            logging.error(f"Error in setup_source_selection: {e}")
            await query.edit_message_text("❌ Error setting up destinations.")
            return ConversationHandler.END
    
    async def setup_destinations(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle destination selection"""
        try:
            query = update.callback_query
            await query.answer()
            
            if query.data == "select_all_dest":
                context.user_data['selected_destinations'] = [
                    g['id'] for g in context.user_data['available_destinations']
                ]
                await query.answer("✅ All groups selected!")
                return SETUP_DESTINATIONS
            
            elif query.data.startswith("toggle_dest_"):
                dest_index = int(query.data.split('_')[2])
                dest_group = context.user_data['available_destinations'][dest_index]
                
                if dest_group['id'] in context.user_data['selected_destinations']:
                    context.user_data['selected_destinations'].remove(dest_group['id'])
                    await query.answer(f"❌ Removed {dest_group['title']}")
                else:
                    context.user_data['selected_destinations'].append(dest_group['id'])
                    await query.answer(f"✅ Added {dest_group['title']}")
                
                return SETUP_DESTINATIONS
            
            elif query.data == "confirm_destinations":
                if not context.user_data['selected_destinations']:
                    await query.answer("❌ Please select at least one destination!")
                    return SETUP_DESTINATIONS
                
                # Move to keywords setup
                keyboard = [
                    [InlineKeyboardButton("📝 Set Keywords", callback_data="set_keywords")],
                    [InlineKeyboardButton("⭐ No Filter (All Messages)", callback_data="no_keywords")],
                    [InlineKeyboardButton("🔙 Back", callback_data="back_to_destinations")]
                ]
                
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                dest_count = len(context.user_data['selected_destinations'])
                
                await query.edit_message_text(
                    f"🔍 **Step 4: Keywords Filter (Optional)**\n\n"
                    f"Source: `{context.user_data['selected_source']['title']}`\n"
                    f"Destinations: {dest_count} groups\n\n"
                    "Do you want to filter messages by keywords?\n"
                    "If yes, only messages containing these keywords will be forwarded.",
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
                
                return SETUP_KEYWORDS
            
            return SETUP_DESTINATIONS
            
        except Exception as e:
            logging.error(f"Error in setup_destinations: {e}")
            await query.edit_message_text("❌ Error setting up destinations.")
            return ConversationHandler.END
    
    async def setup_keywords(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle keywords setup"""
        try:
            query = update.callback_query
            await query.answer()
            
            if query.data == "no_keywords":
                context.user_data['keywords'] = None
                return await self.finalize_forwarding_rule(update, context)
            
            elif query.data == "set_keywords":
                await query.edit_message_text(
                    "🔍 **Keywords Setup**\n\n"
                    "Send keywords separated by commas.\n\n"
                    "Example: `buy, sell, trade, crypto, signal`\n\n"
                    "Only messages containing these keywords will be forwarded.\n"
                    "Keywords are case-insensitive.",
                    parse_mode='Markdown'
                )
                context.user_data['conversation_state'] = 'waiting_keywords'
                return SETUP_KEYWORDS
            
            return SETUP_KEYWORDS
            
        except Exception as e:
            logging.error(f"Error in setup_keywords: {e}")
            await query.edit_message_text("❌ Error setting up keywords.")
            return ConversationHandler.END
    
    async def handle_keywords_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle keywords input from user"""
        try:
            keywords_text = update.message.text.strip()
            keywords = [kw.strip() for kw in keywords_text.split(',') if kw.strip()]
            
            context.user_data['keywords'] = keywords
            
            await update.message.reply_text(
                f"✅ Keywords set: {', '.join(keywords)}\n\n"
                "Setting up forwarding rule..."
            )
            
            return await self.finalize_forwarding_rule(update, context)
            
        except Exception as e:
            logging.error(f"Error handling keywords input: {e}")
            await update.message.reply_text("❌ Error processing keywords.")
            return ConversationHandler.END
    
    async def finalize_forwarding_rule(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Create and start the forwarding rule"""
        try:
            account = context.user_data['selected_account']
            source = context.user_data['selected_source']
            destinations = context.user_data['selected_destinations']
            keywords = context.user_data.get('keywords')
            
            # Create the rule
            rule_id = await self.forwarding_manager.create_forwarding_rule(
                account['phone'],
                source['id'],
                source['title'],
                destinations,
                keywords
            )
            
            # Start forwarding
            success, message = await self.forwarding_manager.start_forwarding(rule_id)
            
            keyboard = [
                [InlineKeyboardButton("📋 View Rules", callback_data="view_forward_rules")],
                [InlineKeyboardButton("➕ Create Another", callback_data="create_forward_rule")],
                [InlineKeyboardButton("🏠 Main Menu", callback_data="back_to_main")]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            if success:
                result_text = (
                    f"✅ **Forwarding Rule Created & Started!**\n\n"
                    f"**Rule ID:** `{rule_id}`\n"
                    f"**Account:** `{account['phone']}`\n"
                    f"**Source:** `{source['title']}`\n"
                    f"**Destinations:** {len(destinations)} groups\n"
                    f"**Keywords:** {', '.join(keywords) if keywords else 'All messages'}\n"
                    f"**Status:** 🟢 Active & Running\n\n"
                    "✨ Auto-forwarding is now active!\n"
                    "Messages will be forwarded automatically."
                )
            else:
                result_text = (
                    f"⚠️ **Rule Created but Failed to Start**\n\n"
                    f"**Rule ID:** `{rule_id}`\n"
                    f"**Error:** {message}\n\n"
                    "You can try to start it manually from the rules menu."
                )
            
            if hasattr(update, 'callback_query'):
                await update.callback_query.edit_message_text(
                    result_text, 
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
            else:
                await update.message.reply_text(
                    result_text, 
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
            
        except Exception as e:
            logging.error(f"Error finalizing forwarding rule: {e}")
            error_text = f"❌ **Error creating rule:** {str(e)}"
            
            if hasattr(update, 'callback_query'):
                await update.callback_query.edit_message_text(error_text)
            else:
                await update.message.reply_text(error_text)
        
        context.user_data.clear()
        return ConversationHandler.END
    
    async def view_forwarding_rules(self, query):
        """Enhanced forwarding rules view with management options"""
        try:
            rules = await self.forwarding_manager.get_forwarding_rules()
            
            if not rules:
                keyboard = [
                    [InlineKeyboardButton("📝 Create First Rule", callback_data="create_forward_rule")],
                    [InlineKeyboardButton("🔙 Back", callback_data="auto_forwarding")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await query.edit_message_text(
                    "📋 **No Forwarding Rules Found**\n\n"
                    "Create your first rule to start auto-forwarding!",
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
                return
            
            rules_text = "📋 **Forwarding Rules**\n\n"
            
            keyboard = []
            for i, rule in enumerate(rules[:10]):  # Show first 10 rules
                status_emoji = "🟢" if rule['status'] == 'running' else "🔴"
                dest_count = len(rule['destination_chat_ids'])
                keywords_info = f" | {len(rule['keywords'])} keywords" if rule['keywords'] else " | All messages"
                
                rules_text += (
                    f"**{i+1}.** {status_emoji} Rule #{rule['id']}\n"
                    f"   📱 {rule['account_phone']}\n"
                    f"   📢 {rule['source_chat_name'][:25]}...\n"
                    f"   🎯 {dest_count} destinations{keywords_info}\n\n"
                )
                
                # Add management buttons for each rule
                keyboard.append([
                    InlineKeyboardButton(f"⚙️ Rule #{rule['id']}", callback_data=f"manage_rule_{rule['id']}"),
                    InlineKeyboardButton(f"📊 Stats", callback_data=f"stats_rule_{rule['id']}")
                ])
            
            keyboard.extend([
                [InlineKeyboardButton("📝 Create New Rule", callback_data="create_forward_rule")],
                [InlineKeyboardButton("🔄 Refresh", callback_data="view_forward_rules")],
                [InlineKeyboardButton("🔙 Back", callback_data="auto_forwarding")]
            ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                rules_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            logging.error(f"Error viewing forwarding rules: {e}")
            await query.edit_message_text("❌ Error loading forwarding rules.")
    
    async def manage_rule(self, query, rule_id):
        """Manage specific forwarding rule"""
        try:
            rules = await self.forwarding_manager.get_forwarding_rules()
            rule = next((r for r in rules if r['id'] == rule_id), None)
            
            if not rule:
                await query.edit_message_text("❌ Rule not found.")
                return
            
            keyboard = []
            
            if rule['status'] == 'running':
                keyboard.append([InlineKeyboardButton("⏹️ Stop Forwarding", callback_data=f"stop_rule_{rule_id}")])
            else:
                keyboard.append([InlineKeyboardButton("▶️ Start Forwarding", callback_data=f"start_rule_{rule_id}")])
            
            keyboard.extend([
                [InlineKeyboardButton("🗑️ Delete Rule", callback_data=f"delete_rule_{rule_id}")],
                [InlineKeyboardButton("📊 View Statistics", callback_data=f"stats_rule_{rule_id}")],
                [InlineKeyboardButton("📋 View Rules", callback_data="view_forward_rules")],
                [InlineKeyboardButton("🔙 Back", callback_data="auto_forwarding")]
            ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            status_emoji = "🟢 Running" if rule['status'] == 'running' else "🔴 Stopped"
            dest_count = len(rule['destination_chat_ids'])
            keywords_text = ', '.join(rule['keywords']) if rule['keywords'] else 'All messages'
            
            rule_text = (
                f"⚙️ **Managing Rule #{rule['id']}**\n\n"
                f"**Status:** {status_emoji}\n"
                f"**Account:** `{rule['account_phone']}`\n"
                f"**Source:** `{rule['source_chat_name']}`\n"
                f"**Destinations:** {dest_count} groups\n"
                f"**Keywords:** {keywords_text}\n"
                f"**Created:** {rule['created_at']}\n\n"
                "Choose an action:"
            )
            
            await query.edit_message_text(
                rule_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            logging.error(f"Error managing rule {rule_id}: {e}")
            await query.edit_message_text("❌ Error managing rule.")
    
    async def start_rule(self, query, rule_id):
        """Start a forwarding rule"""
        try:
            success, message = await self.forwarding_manager.start_forwarding(rule_id)
            
            if success:
                await query.answer("✅ Forwarding started!")
                await self.manage_rule(query, rule_id)
            else:
                await query.answer(f"❌ Failed to start: {message}")
                
        except Exception as e:
            logging.error(f"Error starting rule {rule_id}: {e}")
            await query.answer("❌ Error starting rule.")
    
    async def stop_rule(self, query, rule_id):
        """Stop a forwarding rule"""
        try:
            success, message = await self.forwarding_manager.stop_forwarding(rule_id)
            
            if success:
                await query.answer("✅ Forwarding stopped!")
                await self.manage_rule(query, rule_id)
            else:
                await query.answer(f"❌ Failed to stop: {message}")
                
        except Exception as e:
            logging.error(f"Error stopping rule {rule_id}: {e}")
            await query.answer("❌ Error stopping rule.")
    
    async def delete_rule(self, query, rule_id):
        """Delete a forwarding rule"""
        try:
            # Stop the rule first
            await self.forwarding_manager.stop_forwarding(rule_id)
            
            # Delete from database
            cursor = self.forwarding_manager.db_connection.cursor()
            cursor.execute('DELETE FROM forwarding_rules WHERE id = ?', (rule_id,))
            self.forwarding_manager.db_connection.commit()
            
            await query.answer("✅ Rule deleted!")
            await self.view_forwarding_rules(query)
            
        except Exception as e:
            logging.error(f"Error deleting rule {rule_id}: {e}")
            await query.answer("❌ Error deleting rule.")
    
    async def show_rule_stats(self, query, rule_id):
        """Show statistics for specific rule"""
        try:
            stats = await self.forwarding_manager.get_forwarding_statistics(rule_id)
            
            stats_text = (
                f"📊 **Rule #{rule_id} Statistics**\n\n"
                f"**Messages Forwarded:** {stats['total_forwarded']}\n"
                f"**Unique Destinations:** {stats['unique_destinations']}\n"
                f"**First Forward:** {stats['first_forward'] or 'Never'}\n"
                f"**Last Forward:** {stats['last_forward'] or 'Never'}\n"
                f"**Total Errors:** {stats['total_errors']}\n"
            )
            
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data=f"manage_rule_{rule_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                stats_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            logging.error(f"Error showing rule stats: {e}")
            await query.edit_message_text("❌ Error loading statistics.")
    
    async def show_forwarding_stats(self, query):
        """Show comprehensive forwarding statistics"""
        try:
            rules = await self.forwarding_manager.get_forwarding_rules()
            
            stats_text = "📊 **Forwarding Statistics**\n\n"
            
            if not rules:
                stats_text += "No forwarding rules found."
            else:
                total_rules = len(rules)
                active_rules = len([r for r in rules if r['status'] == 'running'])
                
                # Get message statistics
                cursor = self.forwarding_manager.db_connection.cursor()
                cursor.execute('SELECT COUNT(*) FROM forwarded_messages')
                total_forwarded = cursor.fetchone()[0]
                
                cursor.execute('''
                    SELECT COUNT(*) FROM forwarded_messages 
                    WHERE forwarded_at >= datetime('now', '-1 day')
                ''')
                forwarded_today = cursor.fetchone()[0]
                
                stats_text += (
                    f"**Rule Summary:**\n"
                    f"• Total Rules: {total_rules}\n"
                    f"• Active Rules: {active_rules}\n"
                    f"• Stopped Rules: {total_rules - active_rules}\n\n"
                    f"**Message Statistics:**\n"
                    f"• Total Forwarded: {total_forwarded}\n"
                    f"• Forwarded Today: {forwarded_today}\n\n"
                )
            
            keyboard = [
                [InlineKeyboardButton("🔄 Refresh", callback_data="forward_stats")],
                [InlineKeyboardButton("🔙 Back", callback_data="auto_forwarding")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                stats_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            logging.error(f"Error showing forwarding stats: {e}")
            await query.edit_message_text("❌ Error loading statistics.")
    
    async def resume_forwarding_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Resume all active forwarding rules (command version)"""
        try:
            user_id = update.effective_user.id
            if user_id not in self.authorized_users:
                return
            
            rules = await self.forwarding_manager.get_forwarding_rules()
            active_rules = [r for r in rules if r['status'] == 'running']
            
            if not active_rules:
                await update.message.reply_text("No active forwarding rules to resume.")
                return
            
            progress_msg = await update.message.reply_text("⏳ Resuming forwarding rules...")
            
            resumed = 0
            for rule in active_rules:
                try:
                    success, message = await self.forwarding_manager.start_forwarding(rule['id'])
                    if success:
                        resumed += 1
                        logging.info(f"✅ Resumed forwarding for rule {rule['id']}")
                    else:
                        logging.error(f"❌ Failed to resume rule {rule['id']}: {message}")
                except Exception as e:
                    logging.error(f"Error resuming rule {rule['id']}: {e}")
            
            await progress_msg.edit_text(
                f"✅ **Forwarding Resume Complete**\n\n"
                f"• Resumed: {resumed}\n"
                f"• Failed: {len(active_rules) - resumed}\n"
                f"• Total Rules: {len(active_rules)}"
            )
            
        except Exception as e:
            logging.error(f"Error in resume_forwarding_command: {e}")
            await update.message.reply_text("❌ Error resuming forwarding rules.")
    
    # Placeholder methods for missing functionality
    async def bot_settings_menu(self, query):
        """Bot settings menu placeholder"""
        await query.edit_message_text(
            "⚙️ **Bot Settings**\n\n"
            "Settings functionality coming soon...",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="settings")
            ]]),
            parse_mode='Markdown'
        )
    
    async def show_system_info(self, query):
        """Show system information"""
        try:
            import sys
            from datetime import datetime
            
            info_text = (
                f"📊 **System Information**\n\n"
                f"**Python:** {sys.version.split()[0]}\n"
                f"**Bot Status:** Running\n"
                f"**Accounts:** {len(self.account_manager.accounts)}\n"
                f"**Active Sessions:** {len(self.forwarding_manager.forwarding_sessions)}\n"
                f"**Current Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            )
            
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="settings")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                info_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            logging.error(f"Error showing system info: {e}")
            await query.edit_message_text("❌ Error loading system information.")
    
    async def export_data(self, query):
        """Export data placeholder"""
        await query.edit_message_text(
            "🗂️ **Export Data**\n\n"
            "Data export functionality coming soon...",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="settings")
            ]]),
            parse_mode='Markdown'
        )
    
    async def clean_database(self, query):
        """Clean database placeholder"""
        await query.edit_message_text(
            "🧹 **Clean Database**\n\n"
            "Database cleaning functionality coming soon...",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="settings")
            ]]),
            parse_mode='Markdown'
        )
    
    async def add_members_menu(self, query):
        """Add members menu placeholder"""
        await query.edit_message_text(
            "👥 **Add Members**\n\n"
            "Member adding functionality coming soon...",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="member_manager")
            ]]),
            parse_mode='Markdown'
        )
    
    async def scrape_members_menu(self, query):
        """Scrape members menu placeholder"""
        await query.edit_message_text(
            "🔍 **Scrape Members**\n\n"
            "Member scraping functionality coming soon...",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="member_manager")
            ]]),
            parse_mode='Markdown'
        )
    
    async def member_stats_menu(self, query):
        """Member stats menu placeholder"""
        await query.edit_message_text(
            "📊 **Member Statistics**\n\n"
            "Member statistics functionality coming soon...",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="member_manager")
            ]]),
            parse_mode='Markdown'
        )
    
    async def message_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle text messages"""
        try:
            user_id = update.effective_user.id
            
            if user_id not in self.authorized_users:
                return
            
            current_state = context.user_data.get('conversation_state')
            
            if current_state == 'waiting_account_details':
                return await self.process_account_details(update, context)
            elif current_state == 'waiting_auth_code':
                return await self.process_auth_code(update, context)
            elif current_state == 'waiting_2fa_password':
                return await self.process_2fa_password(update, context)
            elif current_state == 'waiting_keywords':
                return await self.handle_keywords_input(update, context)
                
        except Exception as e:
            logging.error(f"Error in message_handler: {e}")
    
    def setup_conversation_handler(self):
        """Enhanced conversation handler"""
        conv_handler = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(self.add_account_prompt, pattern="add_account"),
                CallbackQueryHandler(self.create_forwarding_rule_start, pattern="create_forward_rule")
            ],
            states={
                'waiting_account_details': [MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_account_details)],
                'waiting_auth_code': [MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_auth_code)],
                'waiting_2fa_password': [MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_2fa_password)],
                'waiting_keywords': [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_keywords_input)],
                SETUP_ACCOUNT: [CallbackQueryHandler(self.setup_account_selection)],
                SETUP_SOURCE: [CallbackQueryHandler(self.setup_source_selection)],
                SETUP_DESTINATIONS: [CallbackQueryHandler(self.setup_destinations)],
                SETUP_KEYWORDS: [CallbackQueryHandler(self.setup_keywords)]
            },
            fallbacks=[
                CallbackQueryHandler(self.show_main_menu, pattern="back_to_main"),
                CallbackQueryHandler(self.forwarding_menu, pattern="auto_forwarding")
            ],
            per_message=False
        )
        return conv_handler
    
    async def run_async(self):
        """Enhanced bot runner with error handling"""
        try:
            if not self.application:
                await self.initialize()
            
            logging.info("🤖 Starting bot polling...")
            
            # Start the bot
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling(drop_pending_updates=True)
            
            logging.info("🤖 Bot is now running. Press Ctrl+C to stop.")
            
            # Keep the application running
            while True:
                await asyncio.sleep(1)
                
        except asyncio.CancelledError:
            # Handle graceful shutdown
            pass
            
        except Exception as e:
            logging.error(f"Fatal error in bot: {e}")
            logging.error(traceback.format_exc())
            raise
            
        finally:
            # Ensure proper cleanup
            if self.application:
                await self.application.stop()
                await self.application.shutdown()
