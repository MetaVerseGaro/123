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
    parser.add_argument('--pause-price', type=Decimal, default=-1,
                        help='Pause trading and wait. Buy: pause if price >= pause-price.'
                        'Sell: pause if price <= pause-price. (default: -1, no pause)')
    parser.add_argument('--boost', action='store_true',
                        help='Use the Boost mode for volume boosting')
    # Optional min order size; typically set via config JSON
    parser.add_argument('--min-order-size', dest='min_order_size', type=Decimal, default=None,
                        help='Minimal order size for this market (override config if set)')

    return parser.parse_args()


def load_config(config_path: str) -> dict:
    """Load JSON config from file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            raw = f.readlines()
        # 支持行内 # 注释（非 JSON 标准）
        cleaned_lines = []
        for line in raw:
            in_str = False
            new_line = ""
            for ch in line:
                if ch == '"' and (not new_line or new_line[-1] != '\\'):
                    in_str = not in_str
                if ch == '#' and not in_str:
                    break
                new_line += ch
            cleaned_lines.append(new_line)
        return json.loads("\n".join(cleaned_lines))
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

    trading = cfg.get("trading", {})
    # 兼容旧版扁平配置
    flat = cfg

    def set_from(section, key, transform=lambda x: x, target=None):
        data = section if key in section else flat
        if key in data and data[key] is not None:
            setattr(args, target or key.replace('-', '_'), transform(data[key]))

    set_from(cfg, "exchange", str)
    set_from(trading, "ticker", str)
    set_from(trading, "direction", str)
    set_from(trading, "quantity", Decimal)
    set_from(trading, "take_profit", Decimal)
    set_from(trading, "max_orders", int)
    set_from(trading, "wait_time", int)
    set_from(cfg, "env_file", str)
    set_from(trading, "grid_step", Decimal)
    set_from(trading, "stop_price", Decimal)
    set_from(trading, "pause_price", Decimal)
    set_from(trading, "min_order_size", Decimal, target="min_order_size")
    if "boost" in trading:
        setattr(args, "boost", bool(trading["boost"]))
    # Risk control env
    risk = cfg.get("risk", {})
    adv = risk.get("advanced", {})
    basic = risk.get("basic", {})
    if adv.get("risk_pct") is not None:
        os.environ["RISK_PCT"] = str(adv["risk_pct"])
    if adv.get("release_timeout_minutes") is not None:
        os.environ["RELEASE_TIMEOUT_MINUTES"] = str(adv["release_timeout_minutes"])
    if adv.get("stop_new_orders_equity_threshold") is not None:
        os.environ["STOP_NEW_ORDERS_EQUITY_THRESHOLD"] = str(adv["stop_new_orders_equity_threshold"])
    if adv.get("enable") is not None:
        setattr(args, "enable_advanced_risk", bool(adv["enable"]))
    if basic.get("enable") is not None:
        setattr(args, "enable_basic_risk", bool(basic["enable"]))
    if basic.get("max_position_count") is not None:
        setattr(args, "max_position_count", int(basic["max_position_count"]))
    if basic.get("basic_release_timeout_minutes") is not None:
        setattr(args, "basic_release_timeout_minutes", int(basic["basic_release_timeout_minutes"]))
    # Feature toggles to env (ZigZag 视作进阶风险的一部分，优先 risk.advanced.zigzag，兼容顶层 zigzag)
    zig = adv.get("zigzag", {}) if isinstance(adv, dict) else cfg.get("zigzag", {})
    flags = cfg.get("flags", {})
    for key in ("enable_auto_reverse", "enable_zigzag", "auto_reverse_fast"):
        val = zig.get(key, flat.get(key, flags.get(key)))
        if val is not None:
            os.environ[key.upper()] = str(val).lower()
    if adv.get("enable_stop_loss") is not None:
        os.environ["STOP_LOSS_ENABLED"] = str(adv["enable_stop_loss"]).lower()
    if zig.get("break_buffer_ticks") is not None:
        os.environ["ZIGZAG_BREAK_BUFFER_TICKS"] = str(zig["break_buffer_ticks"])
    if zig.get("zigzag_depth") is not None:
        os.environ["ZIGZAG_DEPTH"] = str(zig["zigzag_depth"])
    if zig.get("zigzag_deviation") is not None:
        os.environ["ZIGZAG_DEVIATION"] = str(zig["zigzag_deviation"])
    if zig.get("zigzag_backstep") is not None:
        os.environ["ZIGZAG_BACKSTEP"] = str(zig["zigzag_backstep"])
    if zig.get("zigzag_timeframe") is not None:
        os.environ["ZIGZAG_TIMEFRAME"] = str(zig["zigzag_timeframe"])
    if zig.get("zigzag_warmup_candles") is not None:
        os.environ["ZIGZAG_WARMUP_CANDLES"] = str(zig["zigzag_warmup_candles"])
    # Notifications
    notify = cfg.get("notifications", {})
    if notify.get("enable_notifications") is not None:
        os.environ["ENABLE_NOTIFICATIONS"] = str(notify["enable_notifications"]).lower()
    if notify.get("daily_pnl_report") is not None:
        os.environ["DAILY_PNL_REPORT"] = str(notify["daily_pnl_report"]).lower()

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
        pause_price=Decimal(args.pause_price),
        boost_mode=args.boost,
        min_order_size=getattr(args, "min_order_size", None),
        max_position_count=getattr(args, "max_position_count", 0),
        basic_release_timeout_minutes=getattr(args, "basic_release_timeout_minutes", 0),
        enable_advanced_risk=getattr(args, "enable_advanced_risk", True),
        enable_basic_risk=getattr(args, "enable_basic_risk", True)
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
