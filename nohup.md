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
2025-11-27 13:20:34 | INFO | [LIGHTER][ETH] [CLOSE] [281475743111443] FILLED 0.0100 @ 3039.38
2025-11-27 13:20:34 | INFO | [LIGHTER][ETH] [CLOSE] [281475743111443] FILLED 0.0100 @ 3039.38
2025-11-27 13:20:34 | INFO | [LIGHTER][ETH] [CLOSE] [281475743110655] FILLED 0.0100 @ 3039.44
2025-11-27 13:20:34 | INFO | [LIGHTER][ETH] [CLOSE] [281475743110655] FILLED 0.0100 @ 3039.44
2025-11-27 13:21:06 | INFO | [LIGHTER][ETH] Current Position: 0.1300 | Active closing amount: 0.1373 | Order quantity: 14
2025-11-27 13:21:49 | INFO | [LIGHTER][ETH] [OPEN] [562949219600096] OPEN 0.0100 @ 3037.41
2025-11-27 13:21:49 | INFO | [LIGHTER][ETH] [OPEN] [562949219600096] OPEN 0.0100 @ 3037.41
