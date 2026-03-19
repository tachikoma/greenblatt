#!/usr/bin/env python3
"""Simple runnable script to test OrderWatch against the kiwoom mock API.

Usage:
  python3 scripts/run_orderwatch_mock.py

This script:
- Instantiates LiveTradingConfig with mock mode enabled (minimal override).
- Connects adapter, submits a small mock order, and watches it with OrderWatch.
- Prints the watch result and exits.

Note: adapt `ticker`/`quantity`/`price` to your mock behavior if needed.
"""

import asyncio
import time
import os
import sys
from pprint import pprint
from dataclasses import asdict
from types import SimpleNamespace

# Ensure project root is on PYTHONPATH
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from live_trading.config import LiveTradingConfig
from live_trading.kiwoom_adapter import KiwoomBrokerAdapter, KiwoomAPIError
from live_trading.order_watch import OrderWatch


async def main():
    # Minimal config: prefer existing environment/.env handling in project if available
    # Load configuration from environment/.env so appkey/secret are picked up
    cfg = LiveTradingConfig.from_env()
    # Ensure mock mode for safe local testing unless explicitly set otherwise
    cfg.mode = cfg.mode or "mock"

    # Validate presence of required secrets and advise user if missing
    missing = cfg.validate()
    if missing:
        print("Missing required configuration keys for Kiwoom API:", missing)
        print("Please create a .env file with KIWOOM_APPKEY and KIWOOM_SECRETKEY, or set the environment variables.")
        sys.exit(1)

    adapter = KiwoomBrokerAdapter(cfg)
    print("Connecting adapter (mock mode)...")
    await adapter.connect()

    try:
        # Pre-register REAL(subscription type '00') for the ticker before submitting
        print("Registering REAL for ticker before submit...")
        norm = adapter.normalize_ticker("015760")
        grp_no = f"pre_{norm}_{int(time.time() * 1000)}"
        try:
            await adapter.register_real_for_orders(grp_no, [norm], refresh="1")
            print(f"Registered REAL grp={grp_no} ticker={norm}")
        except Exception as exc:
            print(f"Warning: failed to pre-register REAL: {exc}")

        # Start OrderWatch before submitting so websocket listener/polling are active
        dummy = SimpleNamespace(order_no=None, ticker=norm, requested_qty=1, side="BUY", price=48000)
        ow = OrderWatch(adapter, dummy, poll_interval=0.2, timeout_seconds=20, max_amend=1)
        try:
            await ow.register_listeners()
            print("Starting OrderWatch (pre-submit, legacy poll mode)...")
        except Exception as exc:
            print(f"Warning: failed to start OrderWatch pre-submit: {exc}")

        # Submit a mock order; change ticker/qty/price for your mock setup
        print("Submitting mock order...")
        try:
            submitted = await adapter.submit_order(ticker="015760", side="BUY", quantity=1, price=48000)
        except KiwoomAPIError as exc:
            # If configured, fallback to market order on specific return codes (e.g. RC4027)
            rc = getattr(exc, "return_code", None)
            print(f"Order submit failed: return_code={rc} return_msg={exc.return_msg}")
            try:
                if rc in (cfg.fallback_to_market_return_codes or ()):  # attempt market order fallback
                    print("Attempting market-order fallback...")
                    submitted = await adapter.submit_order(ticker="015760", side="BUY", quantity=1, price=48000, order_type="3")
                else:
                    raise
            except KiwoomAPIError:
                raise

        print("Submitted:")
        try:
            pprint(asdict(submitted))
        except Exception:
            pprint(str(submitted))

        # attach submitted to pre-started OrderWatch and await result (legacy poll mode)
        ow.order = submitted
        try:
            res = await ow.wait_for_fill(enable_poll_loop=True)
        finally:
            try:
                await ow.stop_poll_loop()
            except Exception:
                pass
            try:
                await ow.unregister_listeners()
            except Exception:
                pass
        print("OrderWatch result:")
        pprint(res)

    finally:
            # ensure we remove the pre-registered real subscription if created
            try:
                await adapter.remove_real_registration(grp_no, [norm], types="00")
            except Exception:
                pass
    try:
        await adapter.close()
    except Exception:
        pass


if __name__ == "__main__":
    asyncio.run(main())
