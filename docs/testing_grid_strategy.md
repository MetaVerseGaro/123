# GridStrategy 离线快速验证指南

目标：在本地不访问交易所的前提下，跑几轮 `GridStrategy.on_tick()`，确认迁移后的策略循环与 legacy grid 路径一致（流程/日志/状态更新）。

## 文件
- `tests/grid_strategy_harness.py`：离线测试脚本，使用 `botA.json` + `.env` 的配置，全面 mock 交易所与数据读取，无网络依赖。

## 如何运行
在仓库根目录执行：
```bash
python tests/grid_strategy_harness.py
```

## 预期行为
- 不会下真实单，也不会访问网络。
- 日志打印 3 个 tick 的执行过程，包含：
  - 每 tick 的开始提示。
  - 模拟的下单调用 `[GRID] mock place_and_monitor_open_order()`
  - 若触发通知则前缀 `[NOTIFY]` 打印。
- 如需观察更多 tick，可修改脚本内循环次数。

## 若要对拍/扩展
- 调整 `bbo_sequence`（脚本内）模拟更多价格场景。
- 将 `fake_*` 方法替换为真实实现或半实盘（谨慎）：
  - 删掉 `ExchangeFactory.create_exchange` 的 monkeypatch，即可回落到真实交易所客户端。
  - 移除 `_place_and_monitor_open_order` 的 mock，改为实际下单时请先在测试账户/只读模式验证。
- 如果需要记录日志到文件，可在 `TradingLogger` 配置中添加文件输出，或在脚本里简单重定向 `bot.logger.log`。 

(env) lighter@LAPTOP-300BN8JQ:~/123$ python tests/grid_strategy_harness.py
2025-12-10 22:10:35 | INFO | [LIGHTER][ETH] [INIT] Shared BBO file not set; using local WS/REST only
Traceback (most recent call last):
  File "/home/lighter/123/tests/grid_strategy_harness.py", line 194, in <module>
    asyncio.run(main())
  File "/usr/lib/python3.12/asyncio/runners.py", line 194, in run
    return runner.run(main)
           ^^^^^^^^^^^^^^^^
  File "/usr/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/usr/lib/python3.12/asyncio/base_events.py", line 687, in run_until_complete
    return future.result()
           ^^^^^^^^^^^^^^^
  File "/home/lighter/123/tests/grid_strategy_harness.py", line 175, in main
    bot = build_bot_from_config("botA.json")
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/lighter/123/tests/grid_strategy_harness.py", line 105, in build_bot_from_config
    return TradingBot(tc)
           ^^^^^^^^^^^^^^
  File "/home/lighter/123/trading_bot.py", line 368, in __init__
    self._setup_websocket_handlers()
  File "/home/lighter/123/trading_bot.py", line 1912, in _setup_websocket_handlers
    self.exchange_client.setup_order_update_handler(order_update_handler)
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'DummyExchange' object has no attribute 'setup_order_update_handler'
