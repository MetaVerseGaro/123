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
2025-11-27 12:02:28 | WARNING | [LIGHTER][ETH] [RISK] get_account_equity failed: (429)
Reason: Too Many Requests
HTTP response headers: <CIMultiDictProxy('Date': 'Thu, 27 Nov 2025 04:02:25 GMT', 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': '45', 'Connection': 'keep-alive', 'Access-Control-Allow-Credentials': 'true', 'Access-Control-Allow-Headers': 'Content-Type, Origin, X-CSRF-Token, Authorization, AccessToken, Token, Range', 'Access-Control-Allow-Methods': 'GET, HEAD, POST, PATCH, PUT, DELETE', 'Access-Control-Allow-Origin': '*', 'Access-Control-Expose-Headers': 'Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers', 'Access-Control-Max-Age': '86400', 'Vary': 'Origin', 'Vary': 'Origin')>
HTTP response body: {"code":23000,"message":"Too Many Requests!"}

2025-11-27 12:02:28 | INFO | [LIGHTER][ETH] Current Position: 0.0700 | Active closing amount: 0.0700 | Order quantity: 7
2025-11-27 12:02:31 | WARNING | [LIGHTER][ETH] [RATE_LIMIT] get_account_pnl: (429)
Reason: Too Many Requests
HTTP response headers: <CIMultiDictProxy('Date': 'Thu, 27 Nov 2025 04:02:30 GMT', 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': '45', 'Connection': 'keep-alive', 'Access-Control-Allow-Credentials': 'true', 'Access-Control-Allow-Headers': 'Content-Type, Origin, X-CSRF-Token, Authorization, AccessToken, Token, Range', 'Access-Control-Allow-Methods': 'GET, HEAD, POST, PATCH, PUT, DELETE', 'Access-Control-Allow-Origin': '*', 'Access-Control-Expose-Headers': 'Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers', 'Access-Control-Max-Age': '86400', 'Vary': 'Origin', 'Vary': 'Origin')>
HTTP response body: {"code":23000,"message":"Too Many Requests!"}

2025-11-27 12:02:33 | WARNING | [LIGHTER][ETH] [RATE_LIMIT] get_account_pnl: (429)
Reason: Too Many Requests
HTTP response headers: <CIMultiDictProxy('Date': 'Thu, 27 Nov 2025 04:02:32 GMT', 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': '45', 'Connection': 'keep-alive', 'Access-Control-Allow-Credentials': 'true', 'Access-Control-Allow-Headers': 'Content-Type, Origin, X-CSRF-Token, Authorization, AccessToken, Token, Range', 'Access-Control-Allow-Methods': 'GET, HEAD, POST, PATCH, PUT, DELETE', 'Access-Control-Allow-Origin': '*', 'Access-Control-Expose-Headers': 'Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers', 'Access-Control-Max-Age': '86400', 'Vary': 'Origin', 'Vary': 'Origin')>
HTTP response body: {"code":23000,"message":"Too Many Requests!"}

2025-11-27 12:02:34 | WARNING | [LIGHTER][ETH] [RATE_LIMIT] fetch_orders: (429)
Reason: Too Many Requests
HTTP response headers: <CIMultiDictProxy('Date': 'Thu, 27 Nov 2025 04:02:33 GMT', 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': '45', 'Connection': 'keep-alive', 'Access-Control-Allow-Credentials': 'true', 'Access-Control-Allow-Headers': 'Content-Type, Origin, X-CSRF-Token, Authorization, AccessToken, Token, Range', 'Access-Control-Allow-Methods': 'GET, HEAD, POST, PATCH, PUT, DELETE', 'Access-Control-Allow-Origin': '*', 'Access-Control-Expose-Headers': 'Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers', 'Access-Control-Max-Age': '86400', 'Vary': 'Origin', 'Vary': 'Origin')>
HTTP response body: {"code":23000,"message":"Too Many Requests!"}

2025-11-27 12:02:37 | WARNING | [LIGHTER][ETH] [RATE_LIMIT] get_account_equity: (429)
Reason: Too Many Requests
HTTP response headers: <CIMultiDictProxy('Date': 'Thu, 27 Nov 2025 04:02:36 GMT', 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': '45', 'Connection': 'keep-alive', 'Access-Control-Allow-Credentials': 'true', 'Access-Control-Allow-Headers': 'Content-Type, Origin, X-CSRF-Token, Authorization, AccessToken, Token, Range', 'Access-Control-Allow-Methods': 'GET, HEAD, POST, PATCH, PUT, DELETE', 'Access-Control-Allow-Origin': '*', 'Access-Control-Expose-Headers': 'Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers', 'Access-Control-Max-Age': '86400', 'Vary': 'Origin', 'Vary': 'Origin')>
HTTP response body: {"code":23000,"message":"Too Many Requests!"}

2025-11-27 12:02:38 | WARNING | [LIGHTER][ETH] [RISK] get_account_equity failed: (429)
Reason: Too Many Requests
HTTP response headers: <CIMultiDictProxy('Date': 'Thu, 27 Nov 2025 04:02:36 GMT', 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': '45', 'Connection': 'keep-alive', 'Access-Control-Allow-Credentials': 'true', 'Access-Control-Allow-Headers': 'Content-Type, Origin, X-CSRF-Token, Authorization, AccessToken, Token, Range', 'Access-Control-Allow-Methods': 'GET, HEAD, POST, PATCH, PUT, DELETE', 'Access-Control-Allow-Origin': '*', 'Access-Control-Expose-Headers': 'Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers', 'Access-Control-Max-Age': '86400', 'Vary': 'Origin', 'Vary': 'Origin')>
HTTP response body: {"code":23000,"message":"Too Many Requests!"}

2025-11-27 12:02:38 | WARNING | [LIGHTER][ETH] [RATE_LIMIT] get_available_balance: (429)
Reason: Too Many Requests
HTTP response headers: <CIMultiDictProxy('Date': 'Thu, 27 Nov 2025 04:02:37 GMT', 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': '45', 'Connection': 'keep-alive', 'Access-Control-Allow-Credentials': 'true', 'Access-Control-Allow-Headers': 'Content-Type, Origin, X-CSRF-Token, Authorization, AccessToken, Token, Range', 'Access-Control-Allow-Methods': 'GET, HEAD, POST, PATCH, PUT, DELETE', 'Access-Control-Allow-Origin': '*', 'Access-Control-Expose-Headers': 'Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers', 'Access-Control-Max-Age': '86400', 'Vary': 'Origin', 'Vary': 'Origin')>
HTTP response body: {"code":23000,"message":"Too Many Requests!"}

2025-11-27 12:02:41 | WARNING | [LIGHTER][ETH] [RATE_LIMIT] get_account_pnl: (429)
Reason: Too Many Requests
HTTP response headers: <CIMultiDictProxy('Date': 'Thu, 27 Nov 2025 04:02:40 GMT', 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': '45', 'Connection': 'keep-alive', 'Access-Control-Allow-Credentials': 'true', 'Access-Control-Allow-Headers': 'Content-Type, Origin, X-CSRF-Token, Authorization, AccessToken, Token, Range', 'Access-Control-Allow-Methods': 'GET, HEAD, POST, PATCH, PUT, DELETE', 'Access-Control-Allow-Origin': '*', 'Access-Control-Expose-Headers': 'Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers', 'Access-Control-Max-Age': '86400', 'Vary': 'Origin', 'Vary': 'Origin')>
HTTP response body: {"code":23000,"message":"Too Many Requests!"}
