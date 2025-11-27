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
2025-11-27 11:29:02 | WARNING | [LIGHTER][ETH] Error get_account_pnl: (400)
Reason: Bad Request
HTTP response headers: <CIMultiDictProxy('Date': 'Thu, 27 Nov 2025 03:29:01 GMT', 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': '44', 'Connection': 'keep-alive', 'Access-Control-Allow-Credentials': 'true', 'Access-Control-Allow-Headers': 'Content-Type, Origin, X-CSRF-Token, Authorization, AccessToken, Token, Range', 'Access-Control-Allow-Methods': 'GET, HEAD, POST, PATCH, PUT, DELETE', 'Access-Control-Allow-Origin': '*', 'Access-Control-Expose-Headers': 'Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers', 'Access-Control-Max-Age': '86400', 'Vary': 'Origin', 'Vary': 'Origin')>
HTTP response body: code=21100 message='account not found' additional_properties={}
