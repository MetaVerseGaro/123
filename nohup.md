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
Traceback (most recent call last):
  File "/home/lighter/123/runbot.py", line 15, in <module>
    from trading_bot import TradingBot, TradingConfig
  File "/home/lighter/123/trading_bot.py", line 590
    active_orders = await self._get_active_orders_cached()
IndentationError: unexpected indent
