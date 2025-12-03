# Zigzag 择时模式下的 REST 调用取舍

仅针对 `zigzag_timing` 模式列出各类 REST 接口的必要性，便于在限频场景下暂停不需要的调用。默认前提：行情/订单推送已开启，核心交易链路必须保持实时。

| REST 调用 | 在 zigzag_timing 中的作用 | 建议 | 备注 |
| --- | --- | --- | --- |
| place_post_only_order / place_limit_order / place_open_order / place_close_order / reduce_only_close_with_retry / cancel_order / cancel_all_orders | 开仓、止损、TP、反手的下单与撤单 | 必须保留 | 交易主链路，暂停会直接漏单或无法撤单 |
| fetch_bbo_prices | 破位判定、锚定入场价、止损复核 | 必须保留（破位/下单前强制刷新） | 维持短 TTL 或强刷，保证止损判断和报价准确 |
| get_active_orders | 过滤并撤挂单、跟踪 TP | 保留（可短 TTL 缓存） | 无订单推送时需要；有 WS 也建议偶尔兜底 |
| get_account_positions | 同步方向锁、判断持仓 | 保留（短 TTL 缓存） | 止损判定与方向锁依赖 |
| get_order_info | 仅在缺少订单 WS 时查询单状态 | 有订单 WS 时可暂停 | zigzag 循环不主动调用，纯 fallback |
| get_account_equity / get_available_balance | 按 risk_pct 计算目标仓位 | 低频保留 | 60s 缓存即可；非实时也能满足按风险下单 |
| get_position_detail | 取均价参与冗余校验 | 可选 | 如持仓 WS/positions 已含均价，可暂不调此 REST |
| fetch_history_candles / fetch_candle_by_close_time | 新 pivot 缺价时回填 | 条件使用 | pivot 文件已有价格或已回填后即可暂停 |
| get_account_pnl | 每日 PnL 报告 | 可暂停（无需日报时） | 纯统计，不影响交易 |
| get_order_price | grid 阶梯报价/补价 | zigzag_timing 可暂停 | 仅网格模式使用 |

Tips:
- 可选/条件接口在正式停用前，确认替代数据源（WS 或持久化文件）可用。
- 保留接口仍可通过 TTL/缓存降低频率，但在下单、止损等关键动作前应强制刷新 BBO/持仓。
