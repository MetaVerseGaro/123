"""Notification helpers extracted from the monolith."""

import os
import time
from decimal import Decimal
from typing import Optional, Callable, Awaitable

from helpers.lark_bot import LarkBot
from helpers.telegram_bot import TelegramBot
from helpers.logger import TradingLogger


class NotificationManager:
    def __init__(self, logger: TradingLogger, account_name: str, mode_tag: str, enable_notifications: bool, daily_pnl_report: bool):
        self.logger = logger
        self.account_name = account_name
        self.mode_tag = mode_tag
        self.enable_notifications = enable_notifications
        self.daily_pnl_report = daily_pnl_report
        self.last_error_notified_msg: Optional[str] = None
        self.last_error_notified_ts: float = 0.0
        self.last_daily_pnl_date: Optional[str] = None
        self.daily_pnl_baseline: Optional[Decimal] = None
        self.last_daily_sent_date: Optional[str] = None
        self.last_pnl_fetch_ts: float = 0.0

    def _prefix(self) -> str:
        labels = [lbl for lbl in (self.account_name, self.mode_tag) if lbl]
        return f"[{'|'.join(labels)}] " if labels else ""

    async def send_notification(self, message: str, level: str = "INFO"):
        if not self.enable_notifications:
            return

        full_message = f"{self._prefix()}{message}"

        lark_token = os.getenv("LARK_TOKEN") or os.getenv("LARK_BOT_TOKEN")
        if lark_token:
            try:
                async with LarkBot(lark_token) as lark_bot:
                    await lark_bot.send_text(full_message)
            except Exception as e:
                self.logger.log(f"Failed to send Lark notification: {e}", "ERROR")

        telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if telegram_token and telegram_chat_id:
            try:
                with TelegramBot(telegram_token, telegram_chat_id) as tg_bot:
                    tg_bot.send_text(full_message)
            except Exception as e:
                self.logger.log(f"Failed to send Telegram notification: {e}", "ERROR")

        if level == "ERROR":
            self.last_error_notified_msg = full_message
            self.last_error_notified_ts = time.time()

    async def notify_error_once(self, message: str, dedup_seconds: int = 300):
        if not self.enable_notifications:
            return
        full_message = f"{self._prefix()}{message}"
        now = time.time()
        if full_message == self.last_error_notified_msg and (now - self.last_error_notified_ts) < dedup_seconds:
            return
        self.last_error_notified_msg = full_message
        self.last_error_notified_ts = now
        await self.send_notification(message, level="ERROR")

    async def maybe_send_daily_pnl(self, exchange_client, get_equity_snapshot: Callable[[], Awaitable[Optional[Decimal]]]):
        if not (self.enable_notifications and self.daily_pnl_report):
            return
        telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not (telegram_token and telegram_chat_id):
            return
        now = time.gmtime()
        current_date = f"{now.tm_year:04d}-{now.tm_mon:02d}-{now.tm_mday:02d}"
        equity = await get_equity_snapshot()
        now_ts = time.time()
        if self.last_daily_pnl_date != current_date:
            self.daily_pnl_baseline = equity
            self.last_daily_pnl_date = current_date
            self.last_daily_sent_date = None
            self.last_pnl_fetch_ts = 0.0
            return
        if now.tm_hour == 12 and self.last_daily_sent_date != current_date:
            if hasattr(exchange_client, "get_account_pnl"):
                if now_ts - self.last_pnl_fetch_ts > 300:
                    try:
                        pnl_val = await exchange_client.get_account_pnl()
                        self.last_pnl_fetch_ts = now_ts
                        if pnl_val is not None:
                            equity = pnl_val
                    except Exception as e:
                        self.logger.log(f"[PNL] get_account_pnl failed: {e}", "WARNING")
            if equity is None or self.daily_pnl_baseline is None:
                with TelegramBot(telegram_token, telegram_chat_id) as tg_bot:
                    tg_bot.send_text(f"{self._prefix()}[PNL] Daily PnL unavailable (missing equity data)")
            else:
                pnl = equity - self.daily_pnl_baseline
                msg = f"{self._prefix()}[PNL] {current_date} Equity: {equity} | PnL: {pnl}"
                with TelegramBot(telegram_token, telegram_chat_id) as tg_bot:
                    tg_bot.send_text(msg)
            self.last_daily_sent_date = current_date
