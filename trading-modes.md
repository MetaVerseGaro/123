# 交易模式配置说明（grid / zigzag_timing）

本文件说明如何在配置文件中切换模式，以及进阶/基础风险的搭配建议。以 `botA.json` 为例，其他配置文件同理。

## 模式切换
- 配置键：`trading.trading_mode`
  - `grid`（默认）：原网格/阶梯止盈逻辑，使用 `wait_time`、`grid_step`、`take_profit` 等。
  - `zigzag_timing`：依据 ZigZag 破位择时，post-only 拆单入/离场，固定 1:2 TP（50% 仓位）+ 动态 SL。
- 示例：
  ```json
  "trading": {
    ...
    "trading_mode": "zigzag_timing"
  }
  ```

## zigzag_timing 模式下的风险开关建议
- 必开：
  - `risk.advanced.enable` = true（冗余校验、动态 SL、ZigZag 判向等都属于进阶风险板块）。
  - `risk.advanced.enable_stop_loss` = true（动态 SL 必须开启）。
  - `risk.advanced.zigzag.enable_zigzag` = true。
  - `risk.advanced.zigzag.enable_auto_reverse` = false（zigzag_timing 内部已有破位逻辑，需关闭原网格反手）。
  - `risk.advanced.zigzag.auto_reverse_fast` = false。
- 可选：
  - `risk.advanced.risk_pct`：风险敞口百分比，决定按止损距离算出的目标仓位。
  - `risk.advanced.release_timeout_minutes`、`stop_new_orders_equity_threshold`：沿用现有冗余/低 equity 保护。
- 建议关闭：
  - `risk.basic.enable` = false（基础风险与进阶风控目标重叠，zigzag_timing 依赖进阶模块）。
  - 如需同时保留基础风控，请确保 `max_position_count` 等不会限制 zigzag_timing 的按风险下单，但推荐二选一。

## 关键配置回顾（与模式相关）
- `trading.max_fast_close_qty`：单笔最大下单量上限，zigzag_timing 入/离场拆单都用此值。
- `env.ZIGZAG_PIVOT_FILE` / `env.WEBHOOK_BASIC_DIRECTION_FILE`：pivot/方向文件路径，保持与交易所实例一致。
- `env.ZIGZAG_BREAK_BUFFER_TICKS`：破位缓冲 ticks，决定入场/止损的距离计算。

## 使用建议
1) 切换到 zigzag_timing：设置 `trading_mode: "zigzag_timing"`，同步调整上面的进阶风险开关（开启 advanced，关闭 auto_reverse）。
2) 保留 grid：设置 `trading_mode: "grid"`，维持原网格参数；auto_reverse 可按原策略配置。
3) 多实例并行时，确保 pivot 文件路径一致且无写冲突（当前逻辑只读）。***
