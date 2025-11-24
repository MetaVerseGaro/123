# LOG_RETENTION_DAYS=1 TIMEZONE=Asia/Shanghai nohup python runbot.py --exchange lighter --ticker ETH --quantity 0.01 --take-profit 0.03  --max-orders 40 --wait-time 450 --grid-step -100 --stop-price -1  --stop-loss-price 2761 --direction buy --env-file acc1.env

  ```bash
  LOG_DIR=/data/runbot-logs LOG_FILE_PREFIX=botA_ \
  LOG_RETENTION_DAYS=14 TIMEZONE=Asia/Shanghai \
  nohup python3 runbot.py ... > runbot.out 2>&1 &
  ```
