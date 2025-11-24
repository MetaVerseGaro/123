

## 直接可用的双实例示例
- 进程 A（日志前缀 `botA_`，输出文件 `botA.out`，使用 `acc1.env`）：
  ```bash
  LOG_FILE_PREFIX=botA_ LOG_RETENTION_DAYS=1 \
  nohup python3 runbot.py --exchange lighter --ticker ETH --quantity 0.01 --take-profit 0.03 \
    --max-orders 40 --wait-time 450 --grid-step -100 --stop-price -1 --stop-loss-price 2761 \
    --direction buy --env-file acc1.env > botA.out 2>&1 &
  ```
- 进程 B（日志前缀 `botB_`，输出文件 `botB.out`，使用 `acc2.env`，其余参数相同，可按需调整）：
  ```bash
  LOG_FILE_PREFIX=botB_ LOG_RETENTION_DAYS=1 \
  nohup python3 runbot.py --exchange lighter --ticker ETH --quantity 0.01 --take-profit 0.03 \
    --max-orders 40 --wait-time 450 --grid-step -100 --stop-price -1 --stop-loss-price 2761 \
    --direction buy --env-file acc2.env > botB.out 2>&1 &
  ```
- 上述两条命令可直接复制粘贴使用，日志会自动清理（保留 7 天）；如需关闭清理，将 `LOG_RETENTION_DAYS` 设为 0 或负数。

- ```bash
  tail -f botA.out
  ```
