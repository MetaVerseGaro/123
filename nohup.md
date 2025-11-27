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
Operation: [_fetch_positions_with_retry] failed after 5 retries, exception: Cannot connect to host mainnet.zklighter.elliot.ai:443 ssl:default [Connection reset by peer]
--------------------------------
Operation: [fetch_bbo_prices] failed after 5 retries, exception: Invalid bid/ask prices from REST
Bot execution failed: No bid/ask data available
ERROR:asyncio:Unclosed client session
client_session: <aiohttp.client.ClientSession object at 0x7f042cadf440>
ERROR:asyncio:Unclosed connector
connections: ['deque([(<aiohttp.client_proto.ResponseHandler object at 0x7f04281272f0>, 329310.671717134)])']
connector: <aiohttp.connector.TCPConnector object at 0x7f042c13fcb0>
ERROR:asyncio:Unclosed client session
client_session: <aiohttp.client.ClientSession object at 0x7f042c0d7b60>
