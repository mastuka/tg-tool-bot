"""
Telegram Manager Bot - Core Functionality

This module provides the core functionality for the Telegram Manager Bot,
including account management, message forwarding, and user interactions.
"""

import os
import sys
import json
import logging
import asyncio
import signal
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any, Callable, Coroutine
from pathlib import Path

import pytz
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, ReplyKeyboardRemove, WebAppInfo
)
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ConversationHandler
)
from telegram.error import (
    TelegramError, Forbidden, BadRequest, TimedOut, NetworkError,
    ChatMigrated, RetryAfter, Conflict
)

# Local imports
from account_manager import TelegramAccountManager
from forwarding_manager import TelegramForwardingManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Conversation states
MAIN_MENU, ACCOUNT_MENU, FORWARD_MENU, MEMBER_MENU, SETTINGS_MENU = range(5)

class TelegramManagerBot:
    """Main bot class for managing Telegram accounts and forwarding."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the bot with configuration."""
        self.config = config
        self.bot_token = config['bot_token']
        self.authorized_users = set(config['authorized_users'])
        
        # Initialize managers
        self.account_manager = TelegramAccountManager(config)
        self.forwarding_manager = TelegramForwardingManager(self.account_manager, config)
        
        # Bot application instance
        self.application = None
        
        # Load existing accounts and rules
        self._load_initial_data()
        
        # Register signal handlers
        self._register_signal_handlers()
    
    def _load_initial_data(self) -> None:
        """Load initial data like accounts and forwarding rules."""
        try:
            # Load accounts
            self.account_manager.load_accounts()
            logger.info(f"Loaded {len(self.account_manager.accounts)} accounts")
            
            # Load forwarding rules
            self.forwarding_manager.load_rules()
            logger.info(f"Loaded {len(self.forwarding_manager.rules)} forwarding rules")
            
        except Exception as e:
            logger.error(f"Error loading initial data: {e}", exc_info=True)
    
    def _register_signal_handlers(self) -> None:
        """Register signal handlers for graceful shutdown."""
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._handle_shutdown)
    
    def _handle_shutdown(self, signum, frame) -> None:
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        if self.application:
            asyncio.create_task(self._shutdown())
    
    async def _shutdown(self) -> None:
        """Gracefully shut down the application."""
        logger.info("Starting graceful shutdown...")
        
        # Stop all running tasks
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        
        # Wait for tasks to finish
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Close the application
        if self.application.running:
            await self.application.stop()
            await self.application.shutdown()
        
        logger.info("Shutdown complete")
    
    def setup_handlers(self) -> None:
        """Setup all command and message handlers."""
        # Create application
        self.application = Application.builder().token(self.bot_token).build()
        
        # Add error handler
        self.application.add_error_handler(self.error_handler)
        
        # Add conversation handler for main menu
        conv_handler = ConversationHandler(
            entry_points=[CommandHandler('start', self.start_command)],
            states={
                MAIN_MENU: [
                    CallbackQueryHandler(self.main_menu_handler, pattern='^main_menu$'),
                    CallbackQueryHandler(self.account_menu_handler, pattern='^account_'),
                    CallbackQueryHandler(self.forward_menu_handler, pattern='^forward_'),
                    CallbackQueryHandler(self.member_menu_handler, pattern='^member_'),
                    CallbackQueryHandler(self.settings_menu_handler, pattern='^settings_')
                ],
                ACCOUNT_MENU: [
                    CallbackQueryHandler(self.account_action_handler)
                ],
                FORWARD_MENU: [
                    CallbackQueryHandler(self.forward_action_handler)
                ],
                MEMBER_MENU: [
                    CallbackQueryHandler(self.member_action_handler)
                ],
                SETTINGS_MENU: [
                    CallbackQueryHandler(self.settings_action_handler)
                ]
            },
            fallbacks=[CommandHandler('cancel', self.cancel_command)],
            allow_reentry=True
        )
        
        self.application.add_handler(conv_handler)
        
        # Add command handlers
        self.application.add_handler(CommandHandler('help', self.help_command))
        self.application.add_handler(CommandHandler('status', self.status_command))
        
        # Add message handler for account setup
        self.application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            self.handle_message
        ))
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle the /start command."""
        user = update.effective_user
        
        # Check authorization
        if user.id not in self.authorized_users:
            await update.message.reply_text(
                "❌ You are not authorized to use this bot.\n"
                "Please contact the administrator to get access."
            )
            return ConversationHandler.END
        
        # Show main menu
        await self.show_main_menu(update, context)
        return MAIN_MENU
    
    async def show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Display the main menu."""
        keyboard = [
            [InlineKeyboardButton("👤 Account Manager", callback_data="account_menu")],
            [InlineKeyboardButton("📨 Auto Forwarding", callback_data="forward_menu")],
            [InlineKeyboardButton("👥 Member Manager", callback_data="member_menu")],
            [InlineKeyboardButton("📊 Status & Reports", callback_data="status_menu")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="settings_menu")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            "🤖 *Telegram Manager Bot*\n\n"
            "*Features:*\n"
            "• Manage multiple Telegram accounts\n"
            "• Auto-forward messages between chats\n"
            "• Manage group members\n"
            "• View detailed statistics\n\n"
            "Select an option to continue:"
        )
        
        if update.callback_query:
            await update.callback_query.edit_message_text(
                text=text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                text=text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
    
    async def main_menu_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle main menu callbacks."""
        query = update.callback_query
        await query.answer()
        
        if query.data == 'main_menu':
            await self.show_main_menu(update, context)
        
        return MAIN_MENU
    
    async def account_menu_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle account menu callbacks."""
        query = update.callback_query
        await query.answer()
        
        if query.data == 'account_menu':
            await self.show_account_menu(update, context)
        
        return ACCOUNT_MENU
    
    async def forward_menu_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle forwarding menu callbacks."""
        query = update.callback_query
        await query.answer()
        
        if query.data == 'forward_menu':
            await self.show_forward_menu(update, context)
        
        return FORWARD_MENU
    
    async def member_menu_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle member management menu callbacks."""
        query = update.callback_query
        await query.answer()
        
        if query.data == 'member_menu':
            await self.show_member_menu(update, context)
        
        return MEMBER_MENU
    
    async def settings_menu_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle settings menu callbacks."""
        query = update.callback_query
        await query.answer()
        
        if query.data == 'settings_menu':
            await self.show_settings_menu(update, context)
        
        return SETTINGS_MENU
    
    async def show_account_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Display the account management menu."""
        keyboard = [
            [InlineKeyboardButton("➕ Add Account", callback_data="add_account")],
            [InlineKeyboardButton("📋 List Accounts", callback_data="list_accounts")],
            [InlineKeyboardButton("🔄 Update Account", callback_data="update_account")],
            [InlineKeyboardButton("❌ Remove Account", callback_data="remove_account")],
            [InlineKeyboardButton("🔙 Back to Main Menu", callback_data="main_menu")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            "👤 *Account Manager*\n\n"
            "Manage your Telegram accounts here.\n"
            "• Add new accounts\n"
            "• View account status\n"
            "• Update account settings\n"
            "• Remove accounts\n\n"
            "Select an option:"
        )
        
        await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    async def show_forward_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Display the forwarding management menu."""
        keyboard = [
            [InlineKeyboardButton("➕ Add Rule", callback_data="add_rule")],
            [InlineKeyboardButton("📋 List Rules", callback_data="list_rules")],
            [InlineKeyboardButton("⚙️ Edit Rule", callback_data="edit_rule")],
            [InlineKeyboardButton("❌ Remove Rule", callback_data="remove_rule")],
            [InlineKeyboardButton("▶️ Start All", callback_data="start_all_rules")],
            [InlineKeyboardButton("⏹️ Stop All", callback_data="stop_all_rules")],
            [InlineKeyboardButton("🔙 Back to Main Menu", callback_data="main_menu")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        active_rules = sum(1 for rule in self.forwarding_manager.rules if rule.get('active', False))
        total_rules = len(self.forwarding_manager.rules)
        
        text = (
            "📨 *Auto Forwarding*\n\n"
            f"• Active Rules: {active_rules}/{total_rules}\n"
            "\nManage your forwarding rules here.\n"
            "• Create new forwarding rules\n"
            "• View and manage existing rules\n"
            "• Start/Stop forwarding\n\n"
            "Select an option:"
        )
        
        await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    async def show_member_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Display the member management menu."""
        keyboard = [
            [InlineKeyboardButton("👥 Add Members", callback_data="add_members")],
            [InlineKeyboardButton("👤 Remove Members", callback_data="remove_members")],
            [InlineKeyboardButton("📊 Member Stats", callback_data="member_stats")],
            [InlineKeyboardButton("📋 Export Members", callback_data="export_members")],
            [InlineKeyboardButton("🔙 Back to Main Menu", callback_data="main_menu")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            "👥 *Member Manager*\n\n"
            "Manage group members across your accounts.\n"
            "• Add members to groups\n"
            "• Remove members from groups\n"
            "• View member statistics\n"
            "• Export member lists\n\n"
            "Select an option:"
        )
        
        await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    async def show_settings_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Display the settings menu."""
        keyboard = [
            [InlineKeyboardButton("⚙️ Bot Settings", callback_data="bot_settings")],
            [InlineKeyboardButton("📊 Performance", callback_data="performance_settings")],
            [InlineKeyboardButton("🔐 Security", callback_data="security_settings")],
            [InlineKeyboardButton("📝 Logs", callback_data="view_logs")],
            [InlineKeyboardButton("🔄 Update", callback_data="check_updates")],
            [InlineKeyboardButton("🔙 Back to Main Menu", callback_data="main_menu")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            "⚙️ *Settings*\n\n"
            "Configure bot settings and preferences.\n"
            "• Bot behavior and appearance\n"
            "• Performance optimization\n"
            "• Security settings\n"
            "• View system logs\n"
            "• Check for updates\n\n"
            "Select an option:"
        )
        
        await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Send a message when the command /help is issued."""
        help_text = (
            "🤖 *Telegram Manager Bot Help*\n\n"
            "*Available Commands:*\n"
            "/start - Start the bot and show main menu\n"
            "/help - Show this help message\n"
            "/status - Show bot status and statistics\n"
            "/cancel - Cancel the current operation\n\n"
            "*Features:*\n"
            "• Manage multiple Telegram accounts\n"
            "• Auto-forward messages between chats\n"
            "• Manage group members\n"
            "• View detailed statistics\n\n"
            "For support, contact @YourSupportHandle"
        )
        
        await update.message.reply_text(
            help_text,
            parse_mode='Markdown',
            disable_web_page_preview=True
        )
    
    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Send bot status and statistics."""
        status_text = (
            "📊 *Bot Status*\n\n"
            f"• *Accounts:* {len(self.account_manager.accounts)}\n"
            f"• *Active Accounts:* {len([a for a in self.account_manager.accounts if a.get('status') == 'active'])}\n"
            f"• *Forwarding Rules:* {len(self.forwarding_manager.rules)}\n"
            f"• *Active Rules:* {sum(1 for r in self.forwarding_manager.rules if r.get('active', False))}\n\n"
            "*System Status:*\n"
            f"• Uptime: {self._get_uptime()}\n"
            f"• Version: 2.0.0\n"
            f"• Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        await update.message.reply_text(
            status_text,
            parse_mode='Markdown'
        )
    
    def _get_uptime(self) -> str:
        """Calculate and return bot uptime."""
        if not hasattr(self, '_start_time'):
            self._start_time = datetime.now()
        
        uptime = datetime.now() - self._start_time
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        return f"{days}d {hours}h {minutes}m {seconds}s"
    
    async def cancel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Cancel and end the conversation."""
        await update.message.reply_text(
            'Operation cancelled.',
            reply_markup=ReplyKeyboardRemove()
        )
        await self.show_main_menu(update, context)
        return ConversationHandler.END
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle incoming messages."""
        # This is a placeholder for handling non-command messages
        # You can implement message processing logic here
        pass
    
    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Log errors caused by updates and handle them gracefully."""
        # Log the error
        logger.error("Exception while handling an update:", exc_info=context.error)
        
        # Try to send a message to the user
        if update and hasattr(update, 'effective_chat') and update.effective_chat:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="❌ An error occurred while processing your request. "
                         "The error has been logged and will be investigated."
                )
            except Exception as e:
                logger.error(f"Error sending error message: {e}")
    
    def run(self) -> None:
        """Run the bot."""
        if not self.application:
            logger.error("Application not initialized. Call setup_handlers() first.")
            return
        
        logger.info("Starting bot...")
        self._start_time = datetime.now()
        
        try:
            # Start the Bot
            self.application.run_polling(allowed_updates=Update.ALL_TYPES)
        except Exception as e:
            logger.critical(f"Fatal error: {e}", exc_info=True)
            sys.exit(1)
        finally:
            # Perform cleanup
            logger.info("Bot stopped")

# Helper function to load configuration
def load_config() -> Dict[str, Any]:
    """Load configuration from file."""
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        
        # Set defaults
        defaults = {
            'delay_min': 15,
            'delay_max': 45,
            'daily_limit': 45,
            'check_interval': 3600,
            'max_flood_wait': 300,
            'retry_attempts': 3,
            'database_path': 'forwarding.db',
            'sessions_path': 'sessions',
            'logs_path': 'logs'
        }
        
        for key, value in defaults.items():
            if key not in config:
                config[key] = value
        
        return config
        
    except FileNotFoundError:
        print("❌ Error: config.json not found. Please create it first.")
        sys.exit(1)
    except json.JSONDecodeError:
        print("❌ Error: Invalid JSON in config.json")
        sys.exit(1)

def main() -> None:
    """Run the bot."""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/bot.log'),
            logging.StreamHandler()
        ]
    )
    
    logger.info("🚀 Starting Telegram Manager Bot...")
    
    # Ensure directories exist
    for directory in ['sessions', 'logs']:
        os.makedirs(directory, exist_ok=True)
    
    try:
        # Load configuration
        config = load_config()
        
        # Create and start the bot
        bot = TelegramManagerBot(config)
        bot.setup_handlers()
        bot.run()
        
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
