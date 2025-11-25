"""
Lighter exchange client implementation.
"""

import os
import asyncio
import time
import logging
from decimal import Decimal
from typing import Dict, Any, List, Optional, Tuple

from .base import BaseExchangeClient, OrderResult, OrderInfo, query_retry
from helpers.logger import TradingLogger

# Import official Lighter SDK for API client
import lighter
from lighter import SignerClient, ApiClient, Configuration

# Import custom WebSocket implementation
from .lighter_custom_websocket import LighterCustomWebSocketManager

# Suppress Lighter SDK debug logs
logging.getLogger('lighter').setLevel(logging.WARNING)
# Also suppress root logger DEBUG messages that might be coming from Lighter SDK
root_logger = logging.getLogger()
if root_logger.level == logging.DEBUG:
    root_logger.setLevel(logging.WARNING)


class LighterClient(BaseExchangeClient):
    """Lighter exchange client implementation."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize Lighter client."""
        super().__init__(config)

        # Lighter credentials from environment
        self.api_key_private_key = os.getenv('API_KEY_PRIVATE_KEY')
        self.account_index = int(os.getenv('LIGHTER_ACCOUNT_INDEX', '0'))
        self.api_key_index = int(os.getenv('LIGHTER_API_KEY_INDEX', '0'))
        self.base_url = "https://mainnet.zklighter.elliot.ai"

        if not self.api_key_private_key:
            raise ValueError("API_KEY_PRIVATE_KEY must be set in environment variables")

        # Initialize logger
        self.logger = TradingLogger(exchange="lighter", ticker=self.config.ticker, log_to_console=False)
        self._order_update_handler = None

        # Initialize Lighter client (will be done in connect)
        self.lighter_client = None

        # Initialize API client (will be done in connect)
        self.api_client = None

        # Market configuration
        self.base_amount_multiplier = None
        self.price_multiplier = None
        self.orders_cache = {}
        self.current_order_client_id = None
        self.current_order = None
        self._order_id_seed = int(time.time() * 1000)
        # Request throttling and rate-limit backoff
        self._rest_min_interval = float(os.getenv("LIGHTER_REST_MIN_INTERVAL", "0.3"))
        self._last_rest_ts = 0.0
        self._rate_limit_backoff = float(os.getenv("LIGHTER_BACKOFF_START", "0.5"))
        self._rate_limit_backoff_max = float(os.getenv("LIGHTER_BACKOFF_MAX", "8"))

    async def _throttle_rest(self):
        """Enforce minimal interval between REST calls to avoid rate limits."""
        now = time.time()
        wait = self._rest_min_interval - (now - self._last_rest_ts)
        if wait > 0:
            await asyncio.sleep(wait)
        self._last_rest_ts = time.time()

    def _reset_rate_limit_backoff(self):
        self._rate_limit_backoff = float(os.getenv("LIGHTER_BACKOFF_START", "0.5"))

    def _is_rate_limit_error(self, err) -> bool:
        msg = str(err).lower()
        status = getattr(err, "status", None)
        return (status == 429 or "too many request" in msg or "rate limit" in msg or "429" in msg)

    async def _handle_rate_limit_backoff(self):
        await asyncio.sleep(self._rate_limit_backoff)
        self._rate_limit_backoff = min(self._rate_limit_backoff * 2, self._rate_limit_backoff_max)

    def _validate_config(self) -> None:
        """Validate Lighter configuration."""
        required_env_vars = ['API_KEY_PRIVATE_KEY', 'LIGHTER_ACCOUNT_INDEX', 'LIGHTER_API_KEY_INDEX']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

    async def _get_market_config(self, ticker: str) -> Tuple[int, int, int]:
        """Get market configuration for a ticker using official SDK."""
        try:
            # Use shared API client
            order_api = lighter.OrderApi(self.api_client)

            # Get order books to find market info
            order_books = await order_api.order_books()

            for market in order_books.order_books:
                if market.symbol == ticker:
                    market_id = market.market_id
                    base_multiplier = pow(10, market.supported_size_decimals)
                    price_multiplier = pow(10, market.supported_price_decimals)

                    # Store market info for later use
                    self.config.market_info = market

                    self.logger.log(
                        f"Market config for {ticker}: ID={market_id}, "
                        f"Base multiplier={base_multiplier}, Price multiplier={price_multiplier}",
                        "INFO"
                    )
                    return market_id, base_multiplier, price_multiplier

            raise Exception(f"Ticker {ticker} not found in available markets")

        except Exception as e:
            self.logger.log(f"Error getting market config: {e}", "ERROR")
            raise

    async def _initialize_lighter_client(self):
        """Initialize the Lighter client using official SDK."""
        if self.lighter_client is None:
            try:
                self.lighter_client = SignerClient(
                    url=self.base_url,
                    private_key=self.api_key_private_key,
                    account_index=self.account_index,
                    api_key_index=self.api_key_index,
                )

                # Check client
                err = self.lighter_client.check_client()
                if err is not None:
                    raise Exception(f"CheckClient error: {err}")

                self.logger.log("Lighter client initialized successfully", "INFO")
            except Exception as e:
                self.logger.log(f"Failed to initialize Lighter client: {e}", "ERROR")
                raise
        return self.lighter_client

    async def connect(self) -> None:
        """Connect to Lighter."""
        try:
            # Initialize shared API client
            self.api_client = ApiClient(configuration=Configuration(host=self.base_url))

            # Initialize Lighter client
            await self._initialize_lighter_client()

            # Add market config to config for WebSocket manager
            self.config.market_index = self.config.contract_id
            self.config.account_index = self.account_index
            self.config.lighter_client = self.lighter_client

            # Initialize WebSocket manager (using custom implementation)
            self.ws_manager = LighterCustomWebSocketManager(
                config=self.config,
                order_update_callback=self._handle_websocket_order_update
            )

            # Set logger for WebSocket manager
            self.ws_manager.set_logger(self.logger)

            # Start WebSocket connection in background task
            asyncio.create_task(self.ws_manager.connect())
            # Wait a moment for connection to establish
            await asyncio.sleep(2)

        except Exception as e:
            self.logger.log(f"Error connecting to Lighter: {e}", "ERROR")
            raise

    async def disconnect(self) -> None:
        """Disconnect from Lighter."""
        try:
            if hasattr(self, 'ws_manager') and self.ws_manager:
                await self.ws_manager.disconnect()

            # Close shared API client
            if self.api_client:
                await self.api_client.close()
                self.api_client = None
        except Exception as e:
            self.logger.log(f"Error during Lighter disconnect: {e}", "ERROR")

    def get_exchange_name(self) -> str:
        """Get the exchange name."""
        return "lighter"

    def setup_order_update_handler(self, handler) -> None:
        """Setup order update handler for WebSocket."""
        self._order_update_handler = handler

    def _handle_websocket_order_update(self, order_data_list: List[Dict[str, Any]]):
        """Handle order updates from WebSocket."""
        for order_data in order_data_list:
            if order_data['market_index'] != self.config.contract_id:
                continue

            side = 'sell' if order_data['is_ask'] else 'buy'
            if side == self.config.close_order_side:
                order_type = "CLOSE"
            else:
                order_type = "OPEN"

            order_id = order_data['order_index']
            status = order_data['status'].upper()
            filled_size = Decimal(order_data['filled_base_amount'])
            size = Decimal(order_data['initial_base_amount'])
            price = Decimal(order_data['price'])
            remaining_size = Decimal(order_data['remaining_base_amount'])

            if order_id in self.orders_cache.keys():
                if (self.orders_cache[order_id]['status'] == 'OPEN' and
                        status == 'OPEN' and
                        filled_size == self.orders_cache[order_id]['filled_size']):
                    continue
                elif status in ['FILLED', 'CANCELED']:
                    del self.orders_cache[order_id]
                else:
                    self.orders_cache[order_id]['status'] = status
                    self.orders_cache[order_id]['filled_size'] = filled_size
            elif status == 'OPEN':
                self.orders_cache[order_id] = {'status': status, 'filled_size': filled_size}

            if status == 'OPEN' and filled_size > 0:
                status = 'PARTIALLY_FILLED'

            if status == 'OPEN':
                self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                f"{size} @ {price}", "INFO")
            else:
                self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                f"{filled_size} @ {price}", "INFO")

            if order_data['client_order_index'] == self.current_order_client_id or order_type == 'OPEN':
                current_order = OrderInfo(
                    order_id=order_id,
                    side=side,
                    size=size,
                    price=price,
                    status=status,
                    filled_size=filled_size,
                    remaining_size=remaining_size,
                    cancel_reason=''
                )
                self.current_order = current_order

            if status in ['FILLED', 'CANCELED']:
                self.logger.log_transaction(order_id, side, filled_size, price, status)

    @query_retry(default_return=(0, 0))
    async def fetch_bbo_prices(self, contract_id: str) -> Tuple[Decimal, Decimal]:
        """Get orderbook using official SDK."""
        # Use WebSocket data if available
        if (hasattr(self, 'ws_manager') and
                self.ws_manager.best_bid and self.ws_manager.best_ask):
            best_bid = Decimal(str(self.ws_manager.best_bid))
            best_ask = Decimal(str(self.ws_manager.best_ask))

            if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
                self.logger.log("Invalid bid/ask prices", "ERROR")
                raise ValueError("Invalid bid/ask prices")
        else:
            self.logger.log("Unable to get bid/ask prices from WebSocket.", "ERROR")
            raise ValueError("WebSocket not running. No bid/ask prices available")

        return best_bid, best_ask

    async def _submit_order_with_retry(self, order_params: Dict[str, Any]) -> OrderResult:
        """Submit an order with Lighter using official SDK, with throttling and rate-limit backoff."""
        # Ensure client is initialized
        if self.lighter_client is None:
            raise ValueError("Lighter client not initialized. Call connect() first.")

        max_attempts = 5
        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            await self._throttle_rest()
            try:
                create_order, tx_hash, error = await self.lighter_client.create_order(**order_params)
            except Exception as exc:
                if self._is_rate_limit_error(exc):
                    self.logger.log(f"[RATE_LIMIT] create_order attempt {attempt}: {exc}", "WARNING")
                    await self._handle_rate_limit_backoff()
                    continue
                return OrderResult(
                    success=False,
                    order_id=str(order_params.get('client_order_index')),
                    error_message=str(exc)
                )

            if error is not None:
                if self._is_rate_limit_error(error):
                    self.logger.log(f"[RATE_LIMIT] create_order attempt {attempt}: {error}", "WARNING")
                    await self._handle_rate_limit_backoff()
                    continue
                return OrderResult(
                    success=False, order_id=str(order_params['client_order_index']),
                    error_message=f"Order creation error: {error}")

            # Success
            self._reset_rate_limit_backoff()
            return OrderResult(success=True, order_id=str(order_params['client_order_index']))

        return OrderResult(success=False, order_id=str(order_params.get('client_order_index')),
                           error_message="Order creation exceeded max retries due to rate limits")

    def _generate_client_order_index(self) -> int:
        """Generate a unique client order index for tracking orders."""
        self._order_id_seed = (self._order_id_seed + 1) % 1000000
        return self._order_id_seed

    async def _calculate_post_only_price(self, side: str, desired_price: Optional[Decimal]) -> Decimal:
        """Calculate a non-marketable (post-only) price based on current BBO and desired price."""
        best_bid, best_ask = await self.fetch_bbo_prices(self.config.contract_id)
        tick = self.config.tick_size

        if side.lower() == 'buy':
            target = desired_price if desired_price is not None else best_bid
            price = min(target, best_bid - tick)
            price = self.round_to_tick(max(price, tick))
            # Ensure we do not cross the ask
            if price >= best_ask:
                price = self.round_to_tick(best_bid - tick)
        elif side.lower() == 'sell':
            target = desired_price if desired_price is not None else best_ask
            price = max(target, best_ask + tick)
            price = self.round_to_tick(price)
            # Ensure we do not cross the bid
            if price <= best_bid:
                price = self.round_to_tick(best_ask + tick)
        else:
            raise Exception(f"Invalid side: {side}")

        return price

    async def place_limit_order(self, contract_id: str, quantity: Decimal, price: Decimal,
                                side: str, reduce_only: bool = False) -> OrderResult:
        """Place a post-only order with retry until it is accepted as maker."""
        if self.lighter_client is None:
            await self._initialize_lighter_client()

        if side.lower() == 'buy':
            is_ask = False
        elif side.lower() == 'sell':
            is_ask = True
        else:
            raise Exception(f"Invalid side: {side}")

        attempt = 0
        desired_price = price

        while True:
            attempt += 1
            maker_price = await self._calculate_post_only_price(side, desired_price)

            client_order_index = self._generate_client_order_index()
            self.current_order_client_id = client_order_index

            order_params = {
                'market_index': self.config.contract_id,
                'client_order_index': client_order_index,
                'base_amount': int(quantity * self.base_amount_multiplier),
                'price': int(maker_price * self.price_multiplier),
                'is_ask': is_ask,
                'order_type': self.lighter_client.ORDER_TYPE_LIMIT,
                'time_in_force': self.lighter_client.ORDER_TIME_IN_FORCE_POST_ONLY,
                'reduce_only': reduce_only,
                'trigger_price': 0,
            }

            order_result = await self._submit_order_with_retry(order_params)
            if order_result.success:
                return OrderResult(
                    success=True,
                    order_id=str(client_order_index),
                    side=side,
                    size=quantity,
                    price=maker_price,
                    status='OPEN'
                )

            # If post-only rejected (or other error), log and retry with safer price
            self.logger.log(
                f"[POST_ONLY] Attempt {attempt} failed ({order_result.error_message}). "
                f"Retrying with adjusted price.", "WARNING"
            )

            if side.lower() == 'buy':
                desired_price = maker_price - self.config.tick_size
                if desired_price <= 0:
                    desired_price = maker_price
            else:
                desired_price = maker_price + self.config.tick_size

            await asyncio.sleep(0.3)

    async def place_reduce_only_close_order(self, quantity: Decimal, side: str) -> OrderResult:
        """Place a reduce-only post-only close order using current BBO as reference."""
        maker_price = await self._calculate_post_only_price(side, None)
        return await self.place_limit_order(self.config.contract_id, quantity, maker_price, side, reduce_only=True)

    async def reduce_only_close_with_retry(self, quantity: Decimal, side: str, timeout_sec: float = 5.0,
                                           max_attempts: int = 5) -> OrderResult:
        """Retry reduce-only post-only close; cancel after timeout and re-post until filled or attempts exhausted."""
        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            order_result = await self.place_reduce_only_close_order(quantity, side)
            if not order_result.success:
                self.logger.log(f"[REDUCE_ONLY] Attempt {attempt} failed to place: {order_result.error_message}", "WARNING")
                await asyncio.sleep(0.3)
                continue

            target_client_id = self.current_order_client_id
            start = time.time()
            while time.time() - start < timeout_sec:
                if self.current_order and self.current_order_client_id == target_client_id:
                    status = self.current_order.status
                    if status == "FILLED":
                        return OrderResult(
                            success=True,
                            order_id=str(self.current_order.order_id),
                            side=side,
                            size=self.current_order.filled_size,
                            price=self.current_order.price,
                            status=status,
                            filled_size=self.current_order.filled_size
                        )
                    if status == "CANCELED":
                        break
                await asyncio.sleep(0.2)

            # Not filled within timeout, cancel and retry
            cancel_id = None
            if self.current_order and self.current_order_client_id == target_client_id:
                cancel_id = self.current_order.order_id
            if cancel_id is None:
                cancel_id = order_result.order_id

            try:
                await self.cancel_order(str(cancel_id))
            except Exception as e:
                self.logger.log(f"[REDUCE_ONLY] Cancel after timeout failed: {e}", "WARNING")

            self.logger.log(f"[REDUCE_ONLY] Attempt {attempt} timed out; retrying", "WARNING")

        return OrderResult(success=False, error_message="Reduce-only close exceeded max attempts")

    async def place_stop_loss_order(self, quantity: Decimal, trigger_price: Decimal, side: str) -> OrderResult:
        """Place a native stop-loss (IOC) order."""
        if self.lighter_client is None:
            await self._initialize_lighter_client()

        is_ask = True if side.lower() == "sell" else False
        client_order_index = self._generate_client_order_index()
        base_amount = int(quantity * self.base_amount_multiplier)
        trig_px = int(trigger_price * self.price_multiplier)

        max_attempts = 5
        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            await self._throttle_rest()
            try:
                create_order, tx_hash, error = await self.lighter_client.create_sl_order(
                    market_index=self.config.contract_id,
                    client_order_index=client_order_index,
                    base_amount=base_amount,
                    trigger_price=trig_px,
                    price=trig_px,
                    is_ask=is_ask,
                    reduce_only=True
                )
            except Exception as exc:
                if self._is_rate_limit_error(exc):
                    self.logger.log(f"[RATE_LIMIT] stop_loss attempt {attempt}: {exc}", "WARNING")
                    await self._handle_rate_limit_backoff()
                    continue
                return OrderResult(success=False, order_id=str(client_order_index), error_message=str(exc))

            if error is not None:
                if self._is_rate_limit_error(error):
                    self.logger.log(f"[RATE_LIMIT] stop_loss attempt {attempt}: {error}", "WARNING")
                    await self._handle_rate_limit_backoff()
                    continue
                return OrderResult(success=False, order_id=str(client_order_index), error_message=str(error))

            self._reset_rate_limit_backoff()
            return OrderResult(success=True, order_id=str(client_order_index), side=side, price=trigger_price)

        return OrderResult(success=False, order_id=str(client_order_index), error_message="Stop-loss exceeded max retries due to rate limits")

    async def cancel_all_orders(self):
        """Cancel all active orders for current contract only."""
        orders = await self.get_active_orders(self.config.contract_id)
        for order in orders:
            try:
                await self.cancel_order(order.order_id)
            except Exception as e:
                self.logger.log(f"Failed to cancel order {order.order_id}: {e}", "WARNING")

    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """Place an open order with Lighter using official SDK."""

        self.current_order = None
        self.current_order_client_id = None
        order_price = await self.get_order_price(direction)

        order_price = self.round_to_tick(order_price)
        order_result = await self.place_limit_order(contract_id, quantity, order_price, direction, reduce_only=False)
        if not order_result.success:
            raise Exception(f"[OPEN] Error placing order: {order_result.error_message}")

        start_time = time.time()
        order_status = 'OPEN'

        # While waiting for order to be filled
        while time.time() - start_time < 10 and order_status != 'FILLED':
            await asyncio.sleep(0.1)
            if self.current_order is not None:
                order_status = self.current_order.status

        return OrderResult(
            success=True,
            order_id=self.current_order.order_id,
            side=direction,
            size=quantity,
            price=order_result.price,
            status=self.current_order.status
        )

    async def _get_active_close_orders(self, contract_id: str) -> int:
        """Get active close orders for a contract using official SDK."""
        active_orders = await self.get_active_orders(contract_id)
        active_close_orders = 0
        for order in active_orders:
            if order.side == self.config.close_order_side:
                active_close_orders += 1
        return active_close_orders

    async def place_close_order(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        """Place a close order with Lighter using official SDK."""
        self.current_order = None
        self.current_order_client_id = None
        order_result = await self.place_limit_order(contract_id, quantity, price, side, reduce_only=True)

        # wait for 5 seconds to ensure order is placed
        await asyncio.sleep(5)
        if order_result.success:
            return OrderResult(
                success=True,
                order_id=order_result.order_id,
                side=side,
                size=quantity,
                price=order_result.price,
                status='OPEN'
            )
        else:
            raise Exception(f"[CLOSE] Error placing order: {order_result.error_message}")
    
    async def get_order_price(self, side: str = '') -> Decimal:
        """Get the price of an order with Lighter using official SDK."""
        # Get current market prices
        best_bid, best_ask = await self.fetch_bbo_prices(self.config.contract_id)
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            self.logger.log("Invalid bid/ask prices", "ERROR")
            raise ValueError("Invalid bid/ask prices")

        order_price = (best_bid + best_ask) / 2

        active_orders = await self.get_active_orders(self.config.contract_id)
        close_orders = [order for order in active_orders if order.side == self.config.close_order_side]
        for order in close_orders:
            if side == 'buy':
                order_price = min(order_price, order.price - self.config.tick_size)
            else:
                order_price = max(order_price, order.price + self.config.tick_size)

        return order_price

    async def cancel_order(self, order_id: str) -> OrderResult:
        """Cancel an order with Lighter."""
        # Ensure client is initialized
        if self.lighter_client is None:
            await self._initialize_lighter_client()

        max_attempts = 5
        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            await self._throttle_rest()
            cancel_order, tx_hash, error = await self.lighter_client.cancel_order(
                market_index=self.config.contract_id,
                order_index=int(order_id)  # Assuming order_id is the order index
            )

            if error is not None:
                if self._is_rate_limit_error(error):
                    self.logger.log(f"[RATE_LIMIT] cancel_order attempt {attempt}: {error}", "WARNING")
                    await self._handle_rate_limit_backoff()
                    continue
                return OrderResult(success=False, error_message=f"Cancel order error: {error}")

            if tx_hash:
                self._reset_rate_limit_backoff()
                return OrderResult(success=True)
            else:
                return OrderResult(success=False, error_message='Failed to send cancellation transaction')

        return OrderResult(success=False, error_message='Cancel order exceeded max retries due to rate limits')

    async def cancel_all_orders(self):
        """Cancel all active orders for current contract."""
        orders = await self.get_active_orders(self.config.contract_id)
        for order in orders:
            try:
                await self.cancel_order(order.order_id)
            except Exception as e:
                self.logger.log(f"Failed to cancel order {order.order_id}: {e}", "WARNING")

    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        """Get order information from Lighter using official SDK."""
        try:
            # Use shared API client to get account info
            account_api = lighter.AccountApi(self.api_client)

            # Get account orders
            account_data = await account_api.account(by="index", value=str(self.account_index))

            # Look for the specific order in account positions
            for position in account_data.positions:
                if position.symbol == self.config.ticker:
                    position_amt = abs(float(position.position))
                    if position_amt > 0.001:  # Only include significant positions
                        return OrderInfo(
                            order_id=order_id,
                            side="buy" if float(position.position) > 0 else "sell",
                            size=Decimal(str(position_amt)),
                            price=Decimal(str(position.avg_price)),
                            status="FILLED",  # Positions are filled orders
                            filled_size=Decimal(str(position_amt)),
                            remaining_size=Decimal('0')
                        )

            return None

        except Exception as e:
            self.logger.log(f"Error getting order info: {e}", "ERROR")
            return None

    @query_retry(reraise=True)
    async def _fetch_orders_with_retry(self) -> List[Dict[str, Any]]:
        """Get orders using official SDK."""
        # Ensure client is initialized
        if self.lighter_client is None:
            await self._initialize_lighter_client()

        try:
            await self._throttle_rest()
            auth_token, error = self.lighter_client.create_auth_token_with_expiry()
            if error is not None:
                self.logger.log(f"Error creating auth token: {error}", "ERROR")
                raise ValueError(f"Error creating auth token: {error}")

            order_api = lighter.OrderApi(self.api_client)
            await self._throttle_rest()
            orders_response = await order_api.account_active_orders(
                account_index=self.account_index,
                market_id=self.config.contract_id,
                auth=auth_token
            )

            if not orders_response:
                self.logger.log("Failed to get orders", "ERROR")
                raise ValueError("Failed to get orders")

            self._reset_rate_limit_backoff()
            return orders_response.orders

        except Exception as e:
            if self._is_rate_limit_error(e):
                self.logger.log(f"[RATE_LIMIT] fetch_orders: {e}", "WARNING")
                await self._handle_rate_limit_backoff()
            raise

    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        """Get active orders for a contract using official SDK."""
        order_list = await self._fetch_orders_with_retry()

        # Filter orders for the specific market
        contract_orders = []
        for order in order_list:
            # Convert Lighter Order to OrderInfo
            side = "sell" if order.is_ask else "buy"
            size = Decimal(order.initial_base_amount)
            price = Decimal(order.price)

            # Only include orders with remaining size > 0
            if size > 0:
                contract_orders.append(OrderInfo(
                    order_id=str(order.order_index),
                    side=side,
                    size=Decimal(order.remaining_base_amount),  # FIXME: This is wrong. Should be size
                    price=price,
                    status=order.status.upper(),
                    filled_size=Decimal(order.filled_base_amount),
                    remaining_size=Decimal(order.remaining_base_amount)
                ))

        return contract_orders

    @query_retry(reraise=True)
    async def _fetch_positions_with_retry(self) -> List[Dict[str, Any]]:
        """Get positions using official SDK."""
        # Use shared API client
        try:
            account_api = lighter.AccountApi(self.api_client)
            await self._throttle_rest()
            account_data = await account_api.account(by="index", value=str(self.account_index))

            if not account_data or not account_data.accounts:
                self.logger.log("Failed to get positions", "ERROR")
                raise ValueError("Failed to get positions")

            self._reset_rate_limit_backoff()
            return account_data.accounts[0].positions
        except Exception as e:
            if self._is_rate_limit_error(e):
                self.logger.log(f"[RATE_LIMIT] fetch_positions: {e}", "WARNING")
                await self._handle_rate_limit_backoff()
            raise

    async def get_account_positions(self) -> Decimal:
        """Get account positions using official SDK."""
        # Get account info which includes positions
        positions = await self._fetch_positions_with_retry()

        # Find position for current market
        for position in positions:
            if position.market_id == self.config.contract_id:
                return Decimal(position.position)

        return Decimal(0)

    async def get_contract_attributes(self) -> Tuple[str, Decimal]:
        """Get contract ID for a ticker."""
        ticker = self.config.ticker
        if len(ticker) == 0:
            self.logger.log("Ticker is empty", "ERROR")
            raise ValueError("Ticker is empty")

        order_api = lighter.OrderApi(self.api_client)
        candle_api = lighter.CandlestickApi(self.api_client)
        try:
            await self._throttle_rest()
            order_books = await order_api.order_books()

            market_info = None
            for market in order_books.order_books:
                if market.symbol == ticker:
                    market_info = market
                    break

            if market_info is None:
                self.logger.log("Failed to get markets", "ERROR")
                raise ValueError("Failed to get markets")

            await self._throttle_rest()
            market_summary = await order_api.order_book_details(market_id=market_info.market_id)
            order_book_details = market_summary.order_book_details[0]

            self.config.contract_id = market_info.market_id
            self.base_amount_multiplier = pow(10, market_info.supported_size_decimals)
            self.price_multiplier = pow(10, market_info.supported_price_decimals)

            try:
                self.config.tick_size = Decimal("1") / (Decimal("10") ** order_book_details.price_decimals)
            except Exception:
                self.logger.log("Failed to get tick size", "ERROR")
                raise ValueError("Failed to get tick size")

            self._reset_rate_limit_backoff()
            return self.config.contract_id, self.config.tick_size

        except Exception as e:
            if self._is_rate_limit_error(e):
                self.logger.log(f"[RATE_LIMIT] get_contract_attributes: {e}", "WARNING")
                await self._handle_rate_limit_backoff()
            raise

    async def fetch_history_candles(self, limit: int = 200, timeframe: str = "1m"):
        """Fetch historical candles for warmup (default 200 x 1m)."""
        candle_api = lighter.CandlestickApi(self.api_client)
        try:
            await self._throttle_rest()
            resp = await candle_api.candlesticks(
                market_id=self.config.contract_id,
                interval=timeframe,
                limit=limit
            )
            self._reset_rate_limit_backoff()
            return resp.candlesticks if resp else []
        except Exception as e:
            if self._is_rate_limit_error(e):
                self.logger.log(f"[RATE_LIMIT] fetch_history_candles: {e}", "WARNING")
                await self._handle_rate_limit_backoff()
            raise
