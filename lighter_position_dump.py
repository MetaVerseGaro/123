#!/usr/bin/env python3
"""
Quick helper to dump raw Lighter positions for debugging sign/fields.

Usage:
    python lighter_position_dump.py [ENV_FILE]

ENV_FILE defaults to acc1.env/.env in current directory if present.
"""

import asyncio
import json
import os
import sys
from decimal import Decimal

import dotenv
import lighter
from lighter import ApiClient, Configuration


def load_env(path: str) -> None:
    if path and os.path.exists(path):
        dotenv.load_dotenv(path)
    else:
        # fallback: try acc1.env then .env if no arg provided
        for candidate in ("acc1.env", ".env"):
            if os.path.exists(candidate):
                dotenv.load_dotenv(candidate)
                break


def serialize_position(pos) -> dict:
    """Extract useful fields from a Lighter position object."""
    fields = [
        "symbol",
        "market_id",
        "position",
        "side",
        "position_side",
        "is_long",
        "is_short",
        "avg_entry_price",
        "avg_price",
        "leverage",
        "entry_notional",
        "position_margin",
        "unrealized_pnl",
    ]
    data = {}
    for f in fields:
        val = getattr(pos, f, None)
        # Convert Decimals to str for JSON safety
        if isinstance(val, Decimal):
            val = str(val)
        data[f] = val
    # Also include any to_dict representation if available
    if hasattr(pos, "to_dict"):
        try:
            data["to_dict"] = pos.to_dict()
        except Exception:
            pass
    return data


async def main():
    env_arg = sys.argv[1] if len(sys.argv) > 1 else None
    load_env(env_arg)

    base_url = os.getenv("LIGHTER_BASE_URL", "https://mainnet.zklighter.elliot.ai")
    account_index = os.getenv("LIGHTER_ACCOUNT_INDEX", "0")
    cfg = Configuration(host=base_url)
    api_client = ApiClient(configuration=cfg)
    account_api = lighter.AccountApi(api_client)

    print(f"[INFO] BASE_URL={base_url} ACCOUNT_INDEX={account_index}")
    resp = await account_api.account(by="index", value=str(account_index))
    accounts = getattr(resp, "accounts", None)
    if not accounts:
        print("[ERROR] No accounts returned")
        return
    acct = accounts[0]
    positions = getattr(acct, "positions", [])
    print(f"[INFO] Positions count: {len(positions)}")
    for idx, pos in enumerate(positions):
        print(f"\n# Position {idx}:")
        data = serialize_position(pos)
        print(json.dumps(data, ensure_ascii=False, indent=2))

    try:
        await api_client.close()
    except Exception:
        pass


if __name__ == "__main__":
    asyncio.run(main())
