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

(env) lighter@LAPTOP-300BN8JQ:~/123$ pytest tests/test_zigzag_timing_parity.py
============================================= test session starts ==============================================
platform linux -- Python 3.12.3, pytest-9.0.2, pluggy-1.6.0
rootdir: /home/lighter/123
plugins: anyio-4.12.0
collected 1 item

tests/test_zigzag_timing_parity.py F                                                                     [100%]

=================================================== FAILURES ===================================================
_______________________________ ZigZagParityTestCase.test_parity_simple_fixture ________________________________

self = <test_zigzag_timing_parity.ZigZagParityTestCase testMethod=test_parity_simple_fixture>

    def test_parity_simple_fixture(self):
        fixture = Path(__file__).parent / "fixtures" / "zigzag_timing_parity_simple.json"
        result = asyncio.run(run_fixture_once(fixture))
        self.assertEqual(result["direction_lock"], "buy")
>       self.assertIsNotNone(result["stop_price"])
E       AssertionError: unexpectedly None

tests/test_zigzag_timing_parity.py:200: AssertionError
=========================================== short test summary info ============================================
FAILED tests/test_zigzag_timing_parity.py::ZigZagParityTestCase::test_parity_simple_fixture - AssertionError: unexpectedly None
============================================== 1 failed in 0.22s ===============================================
