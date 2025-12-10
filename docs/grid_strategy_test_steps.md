# GridStrategy 手工测试步骤（可由实例环境执行）

目的：验证重构后的 `GridStrategy` 在 CoreServices 注入下的基本行为，无需真实交易所。

## 前置条件
- 已激活项目 Python 环境（确保可导入项目根目录模块）。
- 依赖已安装：`pip install -r requirements.txt`

## 快速烟囱测试（离线）
1. 在项目根目录执行：
   ```bash
   python -m unittest tests.grid_strategy_harness
   ```
2. 预期：
   - 日志打印至少一次下单与对应 close 单的生成（模拟 FILLED → 下 TP）。
   - 测试通过且无错误通知（`notifications.errors` 为空）。

## 可选：自定义 BBO 序列对拍
- 编辑 `tests/grid_strategy_harness.py` 中的 `DummyDataFeeds._bbo_iter`，替换为记录的 BBO 序列（按 `(bid, ask)` 列表）。
- 运行同一命令，观察日志节奏与 legacy grid 的触发时点是否一致。

## 可选：最小单量/持仓上限验证
- 在 `DummyConfig` 中设置 `min_order_size` 或 `max_position_limit`，再次运行测试以验证：
  - 下单数量不低于 `min_order_size`。
  - 当 `max_position_limit` 触发时停止新开仓，并在超时后尝试 reduce-only 释放。

## 若需查看详细日志
- 将 `DummyLogger` 的输出重定向到文件，或在真实实例里替换为 `helpers.logger.TradingLogger`，以便对拍 legacy 日志。
