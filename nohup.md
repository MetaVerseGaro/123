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
2025-11-28 01:53:12 | ERROR | [LIGHTER][ETH] Traceback: Traceback (most recent call last):
  File "/home/lighter/123/trading_bot.py", line 847, in _place_and_monitor_open_order
    order_result = await self.exchange_client.place_open_order(
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/lighter/123/exchanges/lighter.py", line 578, in place_open_order
    order_id=self.current_order.order_id,
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'NoneType' object has no attribute 'order_id'

2025-11-30 20:45:24 | WARNING | [LIGHTER][ETH] [POST_ONLY] Attempt 75522 failed (Order creation error: HTTP response body: code=21706 message='invalid order base or quote amount' additional_properties={}). Retrying with adjusted price.
