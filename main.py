#!/usr/bin/env python3
"""
Telegram Manager Bot - Main Entry Point
"""

import os
import sys
import json
import logging
import asyncio
from pathlib import Path

def setup_project_structure():
    """Create necessary directories"""
    directories = ['sessions', 'logs']
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"✅ Created directory: {directory}")

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/main.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def load_config():
    """Load configuration from file"""
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)

        # Validate required fields
        required_fields = ['bot_token', 'authorized_users', 'api_id', 'api_hash']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required field: {field}")

        # Set defaults
        defaults = {
            'delay_min': 15,
            'delay_max': 45,
            'daily_limit': 45,
            'check_interval': 3600
        }

        for key, value in defaults.items():
            if key not in config:
                config[key] = value

        return config

    except FileNotFoundError:
        print("❌ config.json not found!")
        print("Please create config.json with your bot token and authorized users.")
        sys.exit(1)
    except json.JSONDecodeError:
        print("❌ Invalid JSON in config.json!")
        sys.exit(1)
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        sys.exit(1)

async def async_main():
    """Async main function"""
    print("🚀 Starting Telegram Manager Bot...")

    # Setup project structure
    setup_project_structure()

    # Setup logging
    setup_logging()

    # Load configuration
    config = load_config()

    # Validate bot token
    if config['bot_token'] == "YOUR_BOT_TOKEN_HERE":
        print("❌ Please update your bot token in config.json")
        sys.exit(1)

    # Validate authorized users
    if config['authorized_users'] == [123456789]:
        print("⚠️  Warning: Using default authorized user ID")
        print("Please update authorized_users in config.json with your Telegram user ID")

    # Validate API credentials
    if config.get('api_id') == "YOUR_API_ID" or config.get('api_hash') == "YOUR_API_HASH":
        print("⚠️  Warning: Using default API ID or API Hash.")
        print("Please update api_id and api_hash in config.json with your Telegram API credentials.")

    print(f"✅ Configuration loaded successfully")
    print(f"✅ Authorized users: {config['authorized_users']}")

    # Import and start bot
    try:
        from telegram_bot import TelegramManagerBot

        bot = TelegramManagerBot(config)

        # Initialize the account manager asynchronously
        await bot.initialize()

        print("✅ Bot initialized successfully")
        print("🤖 Starting bot polling...")

        await bot.run_async()

    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure all Python files are in the same directory")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        print(f"❌ Fatal error: {e}")
        sys.exit(1)

def main():
    """Main function that runs the async main"""
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("\n👋 Bot stopped by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
