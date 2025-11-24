"""
Trading logger with structured output and error handling.
"""

import os
import csv
import logging
from datetime import datetime, timedelta
import pytz
from decimal import Decimal


class TradingLogger:
    """Enhanced logging with structured output and error handling."""

    def __init__(self, exchange: str, ticker: str, log_to_console: bool = False):
        self.exchange = exchange
        self.ticker = ticker
        # Ensure logs directory exists (configurable via LOG_DIR, otherwise default to project_root/logs)
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        logs_dir = os.getenv('LOG_DIR')
        if logs_dir:
            logs_dir = os.path.abspath(logs_dir)
        else:
            logs_dir = os.path.join(project_root, 'logs')
        os.makedirs(logs_dir, exist_ok=True)

        # Automatically clean up old log files based on retention policy
        self.timezone = pytz.timezone(os.getenv('TIMEZONE', 'Asia/Shanghai'))
        self._cleanup_old_logs(logs_dir)

        prefix = os.getenv('LOG_FILE_PREFIX', '')

        order_file_name = f"{prefix}{exchange}_{ticker}_orders.csv"
        debug_log_file_name = f"{prefix}{exchange}_{ticker}_activity.log"

        account_name = os.getenv('ACCOUNT_NAME')
        if account_name:
            order_file_name = f"{prefix}{exchange}_{ticker}_{account_name}_orders.csv"
            debug_log_file_name = f"{prefix}{exchange}_{ticker}_{account_name}_activity.log"

        # Log file paths inside logs directory
        self.log_file = os.path.join(logs_dir, order_file_name)
        self.debug_log_file = os.path.join(logs_dir, debug_log_file_name)
        self.logger = self._setup_logger(log_to_console)

    def _setup_logger(self, log_to_console: bool) -> logging.Logger:
        """Setup the logger with proper configuration."""
        logger = logging.getLogger(f"trading_bot_{self.exchange}_{self.ticker}")
        logger.setLevel(logging.INFO)

        # Prevent propagation to root logger to avoid duplicate messages
        logger.propagate = False

        # Prevent duplicate handlers
        if logger.handlers:
            return logger

        class TimeZoneFormatter(logging.Formatter):
            def __init__(self, fmt=None, datefmt=None, tz=None):
                super().__init__(fmt=fmt, datefmt=datefmt)
                self.tz = tz

            def formatTime(self, record, datefmt=None):
                dt = datetime.fromtimestamp(record.created, tz=self.tz)
                if datefmt:
                    return dt.strftime(datefmt)
                return dt.isoformat()

@@ -88,25 +97,46 @@ class TradingLogger:
            self.logger.info(formatted_message)
        elif level.upper() == "WARNING":
            self.logger.warning(formatted_message)
        elif level.upper() == "ERROR":
            self.logger.error(formatted_message)
        else:
            self.logger.info(formatted_message)

    def log_transaction(self, order_id: str, side: str, quantity: Decimal, price: Decimal, status: str):
        """Log a transaction to CSV file."""
        try:
            timestamp = datetime.now(self.timezone).strftime("%Y-%m-%d %H:%M:%S")
            row = [timestamp, order_id, side, quantity, price, status]

            # Check if file exists to write headers
            file_exists = os.path.isfile(self.log_file)

            with open(self.log_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if not file_exists:
                    writer.writerow(['Timestamp', 'OrderID', 'Side', 'Quantity', 'Price', 'Status'])
                writer.writerow(row)

        except Exception as e:
            self.log(f"Failed to log transaction: {e}", "ERROR")

    def _cleanup_old_logs(self, logs_dir: str):
        """Remove log files older than the retention window."""
        retention_days = int(os.getenv('LOG_RETENTION_DAYS', '7'))
        if retention_days <= 0:
            return

        cutoff = datetime.now(self.timezone) - timedelta(days=retention_days)

        try:
            for filename in os.listdir(logs_dir):
                file_path = os.path.join(logs_dir, filename)
                if not os.path.isfile(file_path):
                    continue

                modified_time = datetime.fromtimestamp(os.path.getmtime(file_path), tz=self.timezone)
                if modified_time < cutoff:
                    os.remove(file_path)
        except Exception:
            # Avoid interrupting logger setup due to cleanup issues
            pass
