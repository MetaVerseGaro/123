#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZigZag Webhook Server

功能：
- 接收 TradingView Webhook 的 ZigZag 报价信息
- 解析出：label(HH/HL/LH/LL)、symbol_base(ETH/BTC...)、timeframe、pivot bar close UTC 时间
- 按 (symbol_base, timeframe) 维护最近 4 个 pivot
- 持久化到本地 zigzag_pivots.json，供 trading_bot 读取

依赖：
    pip install fastapi uvicorn

启动示例：
    python3 zigzag_webhook_server.py
    # 默认监听 0.0.0.0:8000
"""

import os
import json
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

# =========================
# 配置
# =========================

# 本地保存 pivot 的文件路径（可以用环境变量覆盖）
PIVOTS_FILE_PATH = os.environ.get("ZIGZAG_PIVOTS_FILE", "zigzag_pivots.json")

# TradingView 用的消息模板（示例）：
# ZigZag HL | ETHUSDT.P | TF=1 | pivot bar close (UTC)=2025-11-28 18:05


# =========================
# 全局状态
# =========================

# 结构：pivots_store[symbol_base][timeframe] = [pivot_dict, ...]  最多 4 条
pivots_store: Dict[str, Dict[str, list[Dict[str, Any]]]] = {}
store_lock = asyncio.Lock()

app = FastAPI(title="ZigZag Webhook Receiver", version="1.0.0")


# =========================
# 工具函数
# =========================

def _now_utc_iso() -> str:
    """当前 UTC 时间 ISO 字符串，精确到秒。"""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def extract_symbol_base(raw_ticker: str) -> str:
    """
    从 TradingView 的完整符号里提取 base 部分。
    规则：
    - 去掉结尾的 ".P"
    - 去掉结尾的 "USDT"
    例如：
    - ETHUSDT.P -> ETH
    - BTCUSDT   -> BTC
    - SOLUSDT.P -> SOL
    """
    t = raw_ticker.strip()

    if t.endswith(".P"):
        t = t[:-2]  # 去掉 ".P"

    if t.upper().endswith("USDT"):
        t = t[:-4]

    return t


def load_pivots_from_file() -> None:
    """程序启动时从本地文件加载已有 pivots。"""
    global pivots_store

    if not os.path.exists(PIVOTS_FILE_PATH):
        pivots_store = {}
        return

    try:
        with open(PIVOTS_FILE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        # 兼容结构：{"version":1, "pivots":{...}}
        if isinstance(data, dict) and "pivots" in data:
            pivots_store = data["pivots"] or {}
        else:
            pivots_store = data or {}
    except Exception as e:
        # 读失败就从空开始
        print(f"[zigzag-webhook] Failed to load pivots file: {e}")
        pivots_store = {}


async def save_pivots_to_file_locked() -> None:
    """
    保存 pivots_store 到本地文件。
    需要在 store_lock 内部调用，保证线程安全。
    使用临时文件 + 原子替换，避免写到一半损坏。
    """
    data = {
        "version": 1,
        "pivots": pivots_store,
        "updated_at_utc": _now_utc_iso(),
    }

    tmp_path = PIVOTS_FILE_PATH + ".tmp"

    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, PIVOTS_FILE_PATH)
    except Exception as e:
        print(f"[zigzag-webhook] Failed to save pivots file: {e}")


async def add_pivot(symbol_base: str, timeframe: str, pivot: Dict[str, Any]) -> None:
    """
    将新的 pivot 加入 pivots_store，保持每个 (symbol_base, timeframe) 最多 4 条。
    """
    async with store_lock:
        sym_map = pivots_store.setdefault(symbol_base, {})
        tf_list = sym_map.setdefault(timeframe, [])

        tf_list.append(pivot)
        if len(tf_list) > 4:
            # 只保留最新 4 条
            sym_map[timeframe] = tf_list[-4:]

        await save_pivots_to_file_locked()


def parse_zigzag_message(raw_msg: str) -> Dict[str, Any]:
    """
    解析 TradingView 发来的 ZigZag 文本消息。
    期望格式：
        ZigZag HL | ETHUSDT.P | TF=1 | pivot bar close (UTC)=2025-11-28 18:05
    返回：
        {
          "label": "HL",
          "raw_ticker": "ETHUSDT.P",
          "symbol_base": "ETH",
          "timeframe": "1",
          "pivot_close_time_utc": "2025-11-28 18:05",
        }
    """
    text = raw_msg.strip()
    if not text:
        raise ValueError("empty message")

    parts = [p.strip() for p in text.split("|")]
    if len(parts) < 4:
        raise ValueError(f"invalid message format, need 4 parts, got {len(parts)}: {text}")

    # 第一段: ZigZag HL
    first = parts[0]
    first_tokens = first.split()
    if len(first_tokens) < 2:
        raise ValueError(f"invalid first part: {first}")
    # 允许 "ZigZag++ HL" / "ZigZag HL" 等，只取最后一个 token 当 label
    label = first_tokens[-1].upper()
    if label not in ("HH", "HL", "LH", "LL"):
        raise ValueError(f"invalid zigzag label: {label}")

    # 第二段: ETHUSDT.P
    raw_ticker = parts[1]
    if not raw_ticker:
        raise ValueError("empty ticker")
    symbol_base = extract_symbol_base(raw_ticker)

    # 第三段: TF=1
    tf_part = parts[2]
    if "TF=" not in tf_part:
        raise ValueError(f"invalid TF part: {tf_part}")
    tf_str = tf_part.split("TF=", 1)[1].strip()
    if not tf_str:
        raise ValueError(f"empty timeframe in: {tf_part}")

    # 第四段: pivot bar close (UTC)=2025-11-28 18:05
    time_part = parts[3]
    if "=" not in time_part:
        raise ValueError(f"invalid time part: {time_part}")
    pivot_close_time_utc = time_part.split("=", 1)[1].strip()
    if not pivot_close_time_utc:
        raise ValueError(f"empty pivot close time in: {time_part}")

    return {
        "label": label,
        "raw_ticker": raw_ticker,
        "symbol_base": symbol_base,
        "timeframe": tf_str,
        "pivot_close_time_utc": pivot_close_time_utc,
    }


# =========================
# FastAPI 事件 & 路由
# =========================

@app.on_event("startup")
async def on_startup():
    print("[zigzag-webhook] Starting up, loading pivots from file...")
    load_pivots_from_file()
    print(f"[zigzag-webhook] Loaded pivots for symbols: {list(pivots_store.keys())}")


@app.post("/zigzag")
async def receive_zigzag(request: Request):
    """
    TradingView Webhook 入口：
    - 支持两种方式：
      1) 发送 JSON: {"message": "ZigZag HL | ETHUSDT.P | TF=1 | pivot bar close (UTC)=..."}
      2) 直接发送纯文本 "ZigZag HL | ETHUSDT.P | TF=1 | pivot bar close (UTC)=..."

    返回：
        {"status": "ok"} 或错误信息
    """
    # 先尝试按 JSON 解析
    raw_body = await request.body()

    text_message = None

    # 尝试 JSON
    try:
        data = json.loads(raw_body.decode("utf-8"))
        if isinstance(data, dict) and "message" in data:
            text_message = str(data["message"])
    except Exception:
        # 不是 JSON，后面当纯文本处理
        pass

    # 如果没有 message 字段，就当纯文本
    if text_message is None:
        try:
            text_message = raw_body.decode("utf-8")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"invalid body: {e}")

    text_message = text_message.strip()
    if not text_message:
        raise HTTPException(status_code=400, detail="empty message")

    try:
        parsed = parse_zigzag_message(text_message)
    except ValueError as e:
        # 返回 400，让你在 TV 日志里能看到具体错误
        raise HTTPException(status_code=400, detail=str(e))

    pivot = {
        "label": parsed["label"],
        "raw_ticker": parsed["raw_ticker"],
        "symbol_base": parsed["symbol_base"],
        "timeframe": parsed["timeframe"],
        "pivot_close_time_utc": parsed["pivot_close_time_utc"],
        "received_at_utc": _now_utc_iso(),
        "raw_message": text_message,
    }

    await add_pivot(parsed["symbol_base"], parsed["timeframe"], pivot)

    print(
        f"[zigzag-webhook] New pivot: "
        f"{pivot['symbol_base']} TF={pivot['timeframe']} "
        f"{pivot['label']} @ {pivot['pivot_close_time_utc']}"
    )

    return JSONResponse({"status": "ok"})


@app.get("/pivots")
async def get_pivots():
    """
    简单的调试接口：
    GET /pivots
    返回当前内存里的所有 pivot（和文件内容基本一致）。
    """
    async with store_lock:
        data = {
            "version": 1,
            "pivots": pivots_store,
        }
    return JSONResponse(data)


if __name__ == "__main__":
    # 让你可以直接用 python3 运行，不用手动敲 uvicorn
    import uvicorn

    host = os.environ.get("ZIGZAG_WEBHOOK_HOST", "0.0.0.0")
    port_str = os.environ.get("ZIGZAG_WEBHOOK_PORT", "8000")
    try:
        port = int(port_str)
    except ValueError:
        port = 8000

    print(f"[zigzag-webhook] Listening on {host}:{port}, pivots file = {PIVOTS_FILE_PATH}")
    uvicorn.run("zigzag_webhook_server:app", host=host, port=port, reload=False)
