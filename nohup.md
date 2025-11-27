# nohup 启动说明（使用 JSON 配置）

- 单实例（使用 `botA.json`，env 文件在 JSON 中指定）：
  ```bash
  nohup python3 runbot.py botA.json > botA.out 2>&1 &
  ```

- 多实例：复制上面一行，替换输出文件/日志前缀/配置文件（如 `botB.json`），并确保各自的 env 文件不同。

- 查看/排障：
  ```bash
  tail -f botA.out        # 实时日志
  ps aux | grep runbot    # 查看进程
  ```
2025-11-27 13:09:41 | ERROR | [LIGHTER][ETH] Error placing order: [OPEN] Error cancelling order: CANCELED-POST-ONLY
2025-11-27 13:09:41 | ERROR | [LIGHTER][ETH] Traceback: Traceback (most recent call last):
  File "/home/lighter/123/trading_bot.py", line 852, in _place_and_monitor_open_order
    handled = await self._handle_order_result(order_result)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/lighter/123/trading_bot.py", line 923, in _handle_order_result
    raise Exception(f"[OPEN] Error cancelling order: {self.exchange_client.current_order.status}")
Exception: [OPEN] Error cancelling order: CANCELED-POST-ONLY
