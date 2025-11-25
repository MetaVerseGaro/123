#!/usr/bin/env python3
"""
Modular Trading Bot - Supports multiple exchanges
"""

import argparse
import asyncio
import logging
from pathlib import Path
import sys
import json
import os
import dotenv
from decimal import Decimal
from trading_bot import TradingBot, TradingConfig
from exchanges import ExchangeFactory


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Modular Trading Bot - Supports multiple exchanges')

    parser.add_argument('config', nargs='?', default=None,
                        help='Path to JSON config file (overrides CLI args)')
    parser.add_argument('--config', dest='config_flag', default=None,
                        help='Path to JSON config file (overrides CLI args)')

    # Exchange selection
    parser.add_argument('--exchange', type=str, default='edgex',
                        choices=ExchangeFactory.get_supported_exchanges(),
                        help='Exchange to use (default: edgex). '
                             f'Available: {", ".join(ExchangeFactory.get_supported_exchanges())}')

    # Trading parameters
    parser.add_argument('--ticker', type=str, default='ETH',
                        help='Ticker (default: ETH)')
    parser.add_argument('--quantity', type=Decimal, default=Decimal(0.1),
                        help='Order quantity (default: 0.1)')
    parser.add_argument('--take-profit', type=Decimal, default=Decimal(0.02),
                        help='Take profit in USDT (default: 0.02)')
    parser.add_argument('--direction', type=str, default='buy', choices=['buy', 'sell'],
                        help='Direction of the bot (default: buy)')
    parser.add_argument('--max-orders', type=int, default=40,
                        help='Maximum number of active orders (default: 40)')
    parser.add_argument('--wait-time', type=int, default=450,
                        help='Wait time between orders in seconds (default: 450)')
    parser.add_argument('--env-file', type=str, default=".env",
                        help=".env file path (default: .env)")
    parser.add_argument('--grid-step', type=str, default='-100',
                        help='The minimum distance in percentage to the next close order price (default: -100)')
    parser.add_argument('--stop-price', type=Decimal, default=-1,
                        help='Price to stop trading and exit. Buy: exits if price >= stop-price.'
                        'Sell: exits if price <= stop-price. (default: -1, no stop)')
    parser.add_argument('--stop-loss-price', type=Decimal, default=-1,
                        help='Fixed price stop-loss. Buy: close all if price <= stop-loss-price. '
                        'Sell: close all if price >= stop-loss-price. (default: -1, disabled)')
    parser.add_argument('--pause-price', type=Decimal, default=-1,
                        help='Pause trading and wait. Buy: pause if price >= pause-price.'
                        'Sell: pause if price <= pause-price. (default: -1, no pause)')
    parser.add_argument('--boost', action='store_true',
                        help='Use the Boost mode for volume boosting')

    return parser.parse_args()


def load_config(config_path: str) -> dict:
    """Load JSON config from file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Failed to load config file {config_path}: {e}")
        sys.exit(1)


def merge_config(args, cfg: dict):
    """Merge JSON config into argparse args."""
    if not cfg:
        return args

    # Optional env vars to set before running (e.g., LOG_FILE_PREFIX)
    for key, val in cfg.get("env", {}).items():
        os.environ[str(key)] = str(val)

    def set_if_present(field, transform=lambda x: x):
        if field in cfg and cfg[field] is not None:
            setattr(args, field.replace('-', '_'), transform(cfg[field]))

    set_if_present("exchange", str)
    set_if_present("ticker", str)
    set_if_present("quantity", Decimal)
    set_if_present("take_profit", Decimal)
    set_if_present("direction", str)
    set_if_present("max_orders", int)
    set_if_present("wait_time", int)
    set_if_present("env_file", str)
    set_if_present("grid_step", Decimal)
    set_if_present("stop_price", Decimal)
    set_if_present("stop_loss_price", Decimal)
    set_if_present("pause_price", Decimal)
    if cfg.get("boost") is not None:
        setattr(args, "boost", bool(cfg["boost"]))
    # Feature toggles to env
    for k in ("enable_auto_reverse", "enable_dynamic_sl"):
        if cfg.get(k) is not None:
            os.environ[k.upper()] = str(cfg[k]).lower()

    return args


def setup_logging(log_level: str):
    """Setup global logging configuration."""
    # Convert string level to logging constant
    level = getattr(logging, log_level.upper(), logging.INFO)

    # Clear any existing handlers to prevent duplicates
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Configure root logger WITHOUT adding a console handler
    # This prevents duplicate logs when TradingLogger adds its own console handler
    root_logger.setLevel(level)

    # Suppress websockets debug logs unless DEBUG level is explicitly requested
    if log_level.upper() != 'DEBUG':
        logging.getLogger('websockets').setLevel(logging.WARNING)

    # Suppress other noisy loggers
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)

    # Suppress Lighter SDK debug logs
    logging.getLogger('lighter').setLevel(logging.WARNING)
    # Also suppress any root logger DEBUG messages that might be coming from Lighter
    if log_level.upper() != 'DEBUG':
        # Set root logger to WARNING to suppress DEBUG messages from Lighter SDK
        root_logger.setLevel(logging.WARNING)


async def main():
    """Main entry point."""
    args = parse_arguments()

    # Handle config file (positional or --config)
    config_path = args.config_flag or args.config
    cfg = None
    if config_path:
        cfg = load_config(config_path)
        args = merge_config(args, cfg)

    # Setup logging first
    setup_logging("WARNING")

    # Validate boost-mode can only be used with aster and backpack exchange
    if args.boost and args.exchange.lower() != 'aster' and args.exchange.lower() != 'backpack':
        print(f"Error: --boost can only be used when --exchange is 'aster' or 'backpack'. "
              f"Current exchange: {args.exchange}")
        sys.exit(1)

    env_path = Path(args.env_file)
    if not env_path.exists():
        print(f"Env file not find: {env_path.resolve()}")
        sys.exit(1)
    dotenv.load_dotenv(args.env_file)

    # Create configuration
    config = TradingConfig(
        ticker=args.ticker.upper(),
        contract_id='',  # will be set in the bot's run method
        tick_size=Decimal(0),
        quantity=args.quantity,
        take_profit=args.take_profit,
        direction=args.direction.lower(),
        max_orders=args.max_orders,
        wait_time=args.wait_time,
        exchange=args.exchange.lower(),
        grid_step=Decimal(args.grid_step),
        stop_price=Decimal(args.stop_price),
        stop_loss_price=Decimal(args.stop_loss_price),
        pause_price=Decimal(args.pause_price),
        boost_mode=args.boost
    )

    # Create and run the bot
    bot = TradingBot(config)
    try:
        await bot.run()
    except Exception as e:
        print(f"Bot execution failed: {e}")
        # The bot's run method already handles graceful shutdown
        return


if __name__ == "__main__":
    asyncio.run(main())
