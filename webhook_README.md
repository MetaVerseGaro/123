# TradingView Webhook Server 使用手册

本程序（`zigzag_webhook_server.py`）是一个独立进程，用来接收 TradingView 的 webhook 消息，并将数据落盘给 `trading_bot.py` 周期性读取。下面按小白流程说明。

## 功能概览
- **ZigZag pivot 接收与持久化**：接收 HH/HL/LH/LL + ticker + timeframe + pivot 收盘时间（UTC），按 base symbol + timeframe 分组，最多保留最近 10 条，写入 JSON 文件（默认 `zigzag_pivots.json`）。
- **基础风控方向接收与持久化**：接收 buy/sell + ticker，记录每个 base symbol 最新方向，写入 JSON 文件（默认 `webhook_basic_direction.json`）。
- **日志**：按天写入 `logs/zigzag_webhook_YYYY-MM-DD.log`，启动时清理 7 天前的日志。

## 运行依赖
- 已包含在根目录 `requirements.txt` 的 `aiohttp>=3.8.0`，无需额外安装。确保虚拟环境或系统已安装项目依赖：`pip install -r requirements.txt`。

## 启动方式
1. 进入项目根目录 `123/`。
2. 直接启动（前台）：`python zigzag_webhook_server.py`
3. 后台运行示例：`nohup python zigzag_webhook_server.py > webhook.out 2>&1 &`
4. 环境变量（可选）：
   - `WEBHOOK_HOST`：监听地址，默认 `0.0.0.0`
   - `WEBHOOK_PORT`：监听端口，默认 `8080`
   - `WEBHOOK_LOG_DIR`：日志目录，默认 `logs/`
   - `WEBHOOK_LOG_RETENTION_DAYS`：日志保留天数，默认 `7`
   - `ZIGZAG_PIVOT_FILE`：pivot JSON 路径，默认 `zigzag_pivots.json`
   - `WEBHOOK_BASIC_DIRECTION_FILE`：buy/sell JSON 路径，默认 `webhook_basic_direction.json`

## 接口说明
服务基于 HTTP（aiohttp），主要路由：

### `POST /webhook`
支持 JSON 或简单文本。

1) **ZigZag pivot 消息**
- JSON 字段示例：
  ```json
  {
    "label": "HH",
    "ticker": "ETHUSDT.P",
    "tf": 1,
    "close_time_utc": "2025-11-28T18:05:00Z"
  }
  ```
- 也支持文本格式：`ZigZag HL | ETHUSDT.P | TF=1 | pivot bar close (UTC)=2025-11-28 18:05`
- 行为：按 base(ETH) + timeframe(1) 追加，超 10 条自动丢弃最旧，立即写入 `zigzag_pivots.json`。

2) **基础方向 buy/sell 消息**
- JSON 示例：
  ```json
  {
    "direction": "buy",
    "ticker": "ETHUSDT.P"
  }
  ```
- 文本格式：`buy | ETHUSDT.P`
- 行为：覆盖该 base(ETH) 最新方向，写入 `webhook_basic_direction.json`，记录 `updated_at`。

返回：`{"status":"ok", ...}`，解析失败返回 400。

### `GET /health`
- 健康检查，返回 `{"status":"ok"}`。

## 日志与清理
- 日志文件：`logs/zigzag_webhook_YYYY-MM-DD.log`（可用 `WEBHOOK_LOG_DIR` 改目录）。
- 每次启动会扫描日志目录并删除 7 天前的日志（天数可配置）。

## 数据文件结构
- `zigzag_pivots.json`（示例）：
  ```json
  {
    "ETH": {
      "1": [
        {
          "label": "HH",
          "raw_ticker": "ETHUSDT.P",
          "tf": "1",
          "close_time_utc": "2025-11-28T18:05:00Z"
        }
      ]
    }
  }
  ```
- `webhook_basic_direction.json`（示例）：
  ```json
  {
    "ETH": {
      "direction": "buy",
      "updated_at": "2025-11-28T21:59:00Z"
    }
  }
  ```

## 与 trading_bot / runbot / lighter 的协同
- `runbot.py` 会把配置中 `risk.basic.webhook_sl/webhook_sl_fast/webhook_reverse` 以及 `zigzag.pivot_file`、`basic.webhook_basic_direction_file` 写入环境变量，`trading_bot.py` 启动时读取。
- `trading_bot.py` 周期性读取 `ZIGZAG_PIVOT_FILE` 来获取最新 pivot，并用对应交易所（如 `lighter.py`）的 OHLC 定位价格；也会按同周期读取 `WEBHOOK_BASIC_DIRECTION_FILE` 处理基础 buy/sell 止损/反手逻辑。
- `lighter.py` 增加了 `fetch_candle_by_close_time` / `get_last_traded_price`，供 `trading_bot.py` 用 pivot 的 UTC 收盘时间精确取价。
- 如果文件路径使用默认值，保证 webhook server 与 bot 在同一工作目录或共享路径；如需自定义路径，请同时设置 webhook 端和 bot 端环境变量/配置保持一致。

## 快速验证
1. 启动服务：`python zigzag_webhook_server.py`
2. 发送 pivot 测试：
   ```sh
   curl -X POST http://127.0.0.1:8080/webhook \
     -H "Content-Type: application/json" \
     -d '{"label":"HH","ticker":"ETHUSDT.P","tf":1,"close_time_utc":"2025-11-28T18:05:00Z"}'
   ```
3. 发送方向测试：
   ```sh
   curl -X POST http://127.0.0.1:8080/webhook \
     -H "Content-Type: application/json" \
     -d '{"direction":"sell","ticker":"ETHUSDT.P"}'
   ```
4. 确认 `zigzag_pivots.json` / `webhook_basic_direction.json` 文件已更新，`logs/` 有当天日志。

## 常见问题
- **无法写文件**：确认运行目录有写权限，或用环境变量显式指定可写路径。
- **端口占用**：修改 `WEBHOOK_PORT` 或释放占用端口。
- **性能/日志**：日志每日切割，保留天数可调；如消息量大，可考虑前置反向代理做限流。
  - TradingView Webhook URL：TradingView 仅支持 80/443。可用 `http://<公网IP>/webhook` 或 `https://<域名>/webhook`，如需内部端口，使用反代映射到内部端口（默认 8080）。

## 端口与权限（AWS EC2 上的 ec2-user）
- 80/443 是特权端口，普通用户直接监听会报 “Permission denied”。
- 方案 A（简单但需 sudo）：`sudo WEBHOOK_PORT=80 python zigzag_webhook_server.py`（或 443）。
- 方案 B（推荐）：程序跑高位端口（如 8080），再用 nginx/ALB/反代把公网 80/443 `/webhook` 转发到 `127.0.0.1:8080/webhook`，TradingView 填 80/443，无需程序升权。
- 安全组/防火墙：放行 80/443（或你的映射端口），防火墙/SELinux 不要拦截。

## 无域名反向代理示例（nginx + 公网 IP）
1) 安装 nginx（Amazon Linux）：`sudo yum install -y nginx`  
2) 运行 webhook（示例 8080）：`WEBHOOK_PORT=8080 nohup python zigzag_webhook_server.py > webhook.out 2>&1 &`  
3) 配置 `/etc/nginx/conf.d/webhook.conf`：  
   ```nginx
   server {
     listen 80;
     server_name <公网IP>;
     location /webhook {
       proxy_pass http://127.0.0.1:8080;
       proxy_set_header Host $host;
       proxy_set_header X-Real-IP $remote_addr;
       proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
     }
     location /health {
       proxy_pass http://127.0.0.1:8080/health;
     }
   }
   ```
4) 检查并重启 nginx：`sudo nginx -t && sudo systemctl restart nginx`  
5) 安全组放行 80（如需 HTTPS 再放 443；可自签或用 Let’s Encrypt 获取证书）。  
6) TradingView 填：`http://<公网IP>/webhook`（或 `https://<公网IP>/webhook`）。  
7) 验证：`curl -X POST http://<公网IP>/webhook -d 'buy | ETHUSDT.P'`，检查文件是否更新。

