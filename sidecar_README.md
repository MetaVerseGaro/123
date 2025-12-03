# shared BBO sidecar 使用说明

目标：在同一台机器上复用一份实时 BBO（优先 WS，REST 兜底），让多个 `trading_bot.py` 进程共用本地缓存，减少重复 REST 拉取和 rate limit 压力。

## 1) 配置来源：直接读 `botA.json`
- 默认读取当前目录的 `botA.json`（或设置 `SIDECAR_CONFIG` 指向其他 JSON）。会使用其中的：
  - `exchange`、`trading.ticker`
  - `env_file`：自动加载交易所密钥（dotenv）
  - `env` 区块：写入到当前进程环境
  - `sidecar` 区块：sidecar 的专用参数
- 因为需要向交易所取 BBO，sidecar 需要同样的 API 密钥。确保密钥已写入 `env_file`（或已在系统环境中），sidecar 会自动加载。

`botA.json` 中的 sidecar 示例：
```jsonc
"sidecar": {
  "shared_bbo_file": "/tmp/shared_bbo.json",
  "publish_interval_sec": 0.25,
  "rest_interval_sec": 5,
  "shared_bbo_max_age_sec": 1.5,
  "shared_bbo_cleanup_sec": 30
}
```

## 2) 启动 sidecar
在仓库根目录执行（nohup 后台，日志写到本地文件）：
```bash
mkdir -p logs
nohup python bbo_sidecar.py > logs/bbo_sidecar.log 2>&1 &
# 使用其他配置文件：
# SIDECAR_CONFIG=./another.json nohup python bbo_sidecar.py > logs/bbo_sidecar.log 2>&1 &
# 临时覆盖节奏：
# export BBO_PUBLISH_INTERVAL_SEC=0.25
# export BBO_SIDECAR_REST_INTERVAL_SEC=5
```
行为：
- 优先从 WS 读取 `best_bid/best_ask`，写入 `SHARED_BBO_FILE`（原子写，避免半写损坏）。
- WS 丢失或长时间无更新时，按 `BBO_SIDECAR_REST_INTERVAL_SEC` 才会触发一次 REST，再写回。
- 读失败会重建文件并告警。

## 3) 交易进程如何使用
在运行 `trading_bot.py` / `runbot.py` 前设置相同的共享文件路径（通常和 sidecar 的一致）：
```bash
export SHARED_BBO_FILE=/tmp/shared_bbo.json
```
逻辑：
- 先读共享文件，若数据新鲜则直接使用，不再调用本进程的 REST。
- 共享数据缺失/过期/损坏时，回退到本进程的 WS（如有）或 REST，成功后写回共享文件。
- 写回前若数据相同且仍新鲜，则跳过写盘，减少磁盘 I/O。

## 4) 健壮性与故障表现
- sidecar 起不来（WS 连不上或合约属性获取失败）：直接退出；交易进程仍可独立用自身 WS/REST。
- 共享文件不存在/权限错误/内容坏掉：交易进程会跳过共享缓存，继续自身 WS/REST，并记录警告日志。
- 无 busy loop；REST 调用在 sidecar 中按最小间隔节流，交易进程也仅在共享数据过期时才会触发自己的 REST。

## 5) t3.micro 建议参数（默认值已符合）
- `BBO_PUBLISH_INTERVAL_SEC=0.25`：降低磁盘写频率。
- `BBO_SIDECAR_REST_INTERVAL_SEC=5`：WS 正常时几乎不用 REST；WS 失效时最多每 5s 才拉一次。
- `SHARED_BBO_CLEANUP_SEC=30`：保持文件小，避免频繁磁盘 I/O。
- `log_retention_days`（配置在 `botA.json.sidecar`，示例为 1 天）：建议配合 `find logs -name 'bbo_sidecar.log*' -mtime +1 -delete` 定期清理 nohup 日志。

## 6) 日志与排查
- sidecar 控制台日志前缀 `[BBO-SIDECAR]`（读写失败、REST 兜底、启动/停止）。
- 交易进程如共享读写失败，开启 `DEBUG_CACHE=true` 会看到 `[SHARED-BBO]` 相关警告，便于确认是否已回退到自身 WS/REST。
