#!/usr/bin/env python3
# zigzag_watcher.py
import asyncio
import json
import os
import sys
import time
from collections import deque
from decimal import Decimal
from pathlib import Path
from typing import Optional

import lighter  # 官方 Lighter SDK
from lighter import ApiClient, Configuration
from dotenv import load_dotenv

from helpers.zigzag_tracker import ZigZagTracker, ZigZagEvent
from helpers.telegram_bot import TelegramBot


def load_config(config_path: str) -> dict:
    """
    兼容 botA.json 里的注释 (#) 的加载函数，
    直接拷贝自 runbot.py 的思路（略简化）。
    """
    p = Path(config_path)
    if not p.exists():
        print(f"[zigzag-watcher] config file not found: {config_path}")
        sys.exit(1)

    try:
        with p.open("r", encoding="utf-8") as f:
            lines = f.readlines()

        cleaned_lines = []
        for line in lines:
            line = line.rstrip("\n")
            if "#" in line:
                in_string = False
                new_line = ""
                for ch in line:
                    if ch == '"':
                        in_string = not in_string
                    if ch == "#" and not in_string:
                        break
                    new_line += ch
                cleaned_lines.append(new_line)
            else:
                cleaned_lines.append(line)

        return json.loads("\n".join(cleaned_lines))
    except Exception as e:
        print(f"[zigzag-watcher] Failed to load config {config_path}: {e}")
        sys.exit(1)


def parse_timeframe_sec(tf: str) -> int:
    """把 '1m' / '5m' / '1h' 转成秒"""
    tf = tf.strip().lower()
    if tf.endswith("h"):
        return int(tf[:-1]) * 3600
    if tf.endswith("m"):
        return int(tf[:-1]) * 60
    if tf.endswith("s"):
        return int(tf[:-1])
    # 默认按分钟算
    return int(tf) * 60


async def resolve_market_id(order_api, ticker: str) -> int:
    """通过 ticker 找到对应的 market_id。"""
    ob = await order_api.order_books()
    for m in ob.order_books:
        if m.symbol == ticker:
            return m.market_id
    raise ValueError(f"Ticker {ticker} not found in Lighter order_books")


async def fetch_candles(candle_api, market_id: int, timeframe: str, limit: int):
    """拉近期 K 线，封装一下方便复用。"""
    now_ms = int(time.time() * 1000)
    tf_sec = parse_timeframe_sec(timeframe)
    start_ms = now_ms - limit * tf_sec * 1000

    resp = await candle_api.candlesticks(
        market_id=market_id,
        resolution=timeframe,
        start_timestamp=start_ms,
        end_timestamp=now_ms,
        count_back=limit,
        set_timestamp_to_end=True,
    )
    return resp.candlesticks if resp else []


def build_pivot_message(recent_pivots: deque[ZigZagEvent]) -> str:
    """把最近 4 个 pivot 拼出一行文字。"""
    if not recent_pivots:
        return "[ZIGZAG WATCH] 暂无已确认高低点"

    items = [f"{p.label}@{p.price}" for p in recent_pivots]
    msg = "[ZIGZAG WATCH] 最新确认高低点: " + " | ".join(items)
    return msg


async def zigzag_watch(config_path: str):
    cfg = load_config(config_path)

    # 1) env_file + ticker + zigzag 参数
    env_file = cfg.get("env_file", "acc1.env")
    trading_cfg = cfg.get("trading", {}) or {}
    ticker = trading_cfg.get("ticker", "ETH")

    # risk.advanced.zigzag 部分
    risk_cfg = cfg.get("risk", {}) or {}
    adv_cfg = risk_cfg.get("advanced", {}) or {}
    zig_cfg = adv_cfg.get("zigzag", {}) or {}

    depth = int(zig_cfg.get("zigzag_depth", 15))
    deviation = Decimal(str(zig_cfg.get("zigzag_deviation", 5)))
    backstep = int(zig_cfg.get("zigzag_backstep", 2))
    timeframe = zig_cfg.get("zigzag_timeframe", "1m")
    warmup_candles = int(zig_cfg.get("zigzag_warmup_candles", 200))

    print(f"[zigzag-watcher] config={config_path} ticker={ticker} tf={timeframe} "
          f"depth={depth} deviation={deviation}% backstep={backstep} warmup={warmup_candles}")

    # 2) 加载 .env（至少需要 Telegram 的信息）
    if env_file and Path(env_file).exists():
        load_dotenv(env_file)
        print(f"[zigzag-watcher] Loaded env from {env_file}")
    else:
        print(f"[zigzag-watcher] env_file not found: {env_file} (still using process env)")

    telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
    telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
    account_name = os.getenv("ACCOUNT_NAME", "").strip()

    if not telegram_token or not telegram_chat_id:
        print("[zigzag-watcher] TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID 未配置，无法发送通知")
        # 仍然可以继续跑，只是在终端打印
    else:
        print("[zigzag-watcher] Telegram notifications enabled")

    # 3) 初始化 lighter 公共 API 客户端
    cfg_lighter = Configuration(host="https://mainnet.zklighter.elliot.ai")
    api_client = ApiClient(configuration=cfg_lighter)
    order_api = lighter.OrderApi(api_client)
    candle_api = lighter.CandlestickApi(api_client)

    # 4) 解析 market_id
    print(f"[zigzag-watcher] Resolving market_id for ticker={ticker} ...")
    market_id = await resolve_market_id(order_api, ticker)
    print(f"[zigzag-watcher] market_id={market_id}")

    # 5) 初始化 ZigZagTracker
    zz = ZigZagTracker(depth=depth, deviation_pct=deviation, backstep=backstep)
    recent_pivots: deque[ZigZagEvent] = deque(maxlen=4)

    # 6) 预热
    print(f"[zigzag-watcher] Warmup ZigZag with last {warmup_candles} candles...")
    candles = await fetch_candles(candle_api, market_id, timeframe, warmup_candles)
    last_ts: Optional[int] = None

    for c in candles:
        high = Decimal(str(c.high))
        low = Decimal(str(c.low))
        ts_val = getattr(c, "timestamp", None)
        evt = zz.update(high, low, ts=ts_val)
        if evt:
            recent_pivots.append(evt)
        if ts_val is not None:
            # timestamp 一般是 ms，这里不用太纠结单位，只要相对大小一致即可
            last_ts = ts_val

    print(f"[zigzag-watcher] Warmup done. current pivots={len(recent_pivots)}")
    if recent_pivots:
        msg = build_pivot_message(recent_pivots)
        print(msg)
        if telegram_token and telegram_chat_id:
            if account_name:
                msg = f"[{account_name}] {msg}"
            with TelegramBot(telegram_token, telegram_chat_id) as tg_bot:
                tg_bot.send_text(msg)

    # 7) 主循环：轮询最新 K 线，发现新 pivot 就通知
    tf_sec = parse_timeframe_sec(timeframe)
    poll_interval = max(10, tf_sec // 4)  # 每个 bar 内大约轮询 3～6 次

    print(f"[zigzag-watcher] Start loop: poll_interval={poll_interval}s")
    while True:
        try:
            await asyncio.sleep(poll_interval)
            candles = await fetch_candles(candle_api, market_id, timeframe, limit=3)
            if not candles:
                print("[zigzag-watcher] No candles returned")
                continue

            # 取 timestamp 最大的一根作为最新 bar
            latest = max(candles, key=lambda x: getattr(x, "timestamp", 0))
            ts_val = getattr(latest, "timestamp", None)

            if last_ts is not None and ts_val is not None and ts_val <= last_ts:
                # 没有新 bar 收盘
                continue

            # 有新 bar
            last_ts = ts_val
            high = Decimal(str(latest.high))
            low = Decimal(str(latest.low))

            evt = zz.update(high, low, ts=ts_val)
            if evt:
                recent_pivots.append(evt)
                msg = build_pivot_message(recent_pivots)
                print(msg)
                if telegram_token and telegram_chat_id:
                    send_text = msg
                    if account_name:
                        send_text = f"[{account_name}] {msg}"
                    with TelegramBot(telegram_token, telegram_chat_id) as tg_bot:
                        tg_bot.send_text(send_text)
        except Exception as e:
            print(f"[zigzag-watcher] loop error: {e}")
            # 避免死循环狂刷错误
            await asyncio.sleep(poll_interval)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Standalone ZigZag watcher for Lighter.")
    parser.add_argument(
        "--config",
        dest="config",
        default="botA.json",
        help="Path to bot config JSON (default: botA.json)",
    )
    args = parser.parse_args()

    asyncio.run(zigzag_watch(args.config))
