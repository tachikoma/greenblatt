#!/usr/bin/env python3
"""키움 모의 API를 대상으로 `OrderWatch` 동작을 확인하는 간단 실행 스크립트입니다.

사용법:
    python3 scripts/run_orderwatch_mock.py

이 스크립트는:
- `LiveTradingConfig`를 모의(mock) 모드로 초기화합니다 (최소 설정).
- 어댑터를 연결하고 간단한 모의 주문을 제출한 뒤 `OrderWatch`로 모니터링합니다.
- 모니터링 결과를 출력하고 종료합니다.

참고: 모의 환경에 따라 `ticker`/`quantity`/`price` 값을 조정하세요.
"""

import asyncio
import time
import os
import sys
from pprint import pprint
from dataclasses import asdict
from types import SimpleNamespace

# 프로젝트 루트가 PYTHONPATH에 포함되었는지 확인합니다
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from live_trading.config import LiveTradingConfig
from live_trading.kiwoom_adapter import KiwoomBrokerAdapter, KiwoomAPIError
from live_trading.order_watch import OrderWatch


async def main():
    # 최소 설정: 가능하면 프로젝트의 기존 환경변수/.env 방식을 사용합니다.
    # .env 또는 환경변수에서 설정을 로드하여 appkey/secret을 확보합니다.
    cfg = LiveTradingConfig.from_env()
    # 명시적으로 설정되지 않은 경우 로컬 테스트 안전성을 위해 모의(mock) 모드로 설정합니다
    cfg.mode = cfg.mode or "mock"

    # 필수 시크릿 존재 여부를 검사하고 없으면 사용자에게 알립니다
    missing = cfg.validate()
    if missing:
        print("Missing required configuration keys for Kiwoom API:", missing)
        print("Please create a .env file with KIWOOM_APPKEY and KIWOOM_SECRETKEY, or set the environment variables.")
        sys.exit(1)

    adapter = KiwoomBrokerAdapter(cfg)
    print("Connecting adapter (mock mode)...")
    await adapter.connect()

    try:
            # 제출 전에 해당 티커에 대해 REAL(구독 타입 '00')을 사전 등록합니다
        print("Registering REAL for ticker before submit...")
        norm = adapter.normalize_ticker("015760")
        grp_no = f"pre_{norm}_{int(time.time() * 1000)}"
        try:
            await adapter.register_real_for_orders(grp_no, [norm], refresh="1")
            print(f"Registered REAL grp={grp_no} ticker={norm}")
        except Exception as exc:
            print(f"Warning: failed to pre-register REAL: {exc}")

        # 제출 전에 OrderWatch를 시작하여 웹소켓 리스너/폴링이 활성화되게 합니다
        dummy = SimpleNamespace(order_no=None, ticker=norm, requested_qty=1, side="BUY", price=48000)
        ow = OrderWatch(adapter, dummy, poll_interval=0.2, timeout_seconds=20, max_amend=1)
        try:
            await ow.register_listeners()
            print("Starting OrderWatch (pre-submit, legacy poll mode)...")
        except Exception as exc:
            print(f"Warning: failed to start OrderWatch pre-submit: {exc}")

        # 모의 주문을 제출합니다; 모의 환경에 맞게 ticker/qty/price를 조정하세요
        print("Submitting mock order...")
        try:
            submitted = await adapter.submit_order(ticker="015760", side="BUY", quantity=1, price=48000)
        except KiwoomAPIError as exc:
            # 구성된 경우 특정 리턴 코드(예: RC4027)에 대해 시장가 주문으로 폴백합니다
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

        # 제출된 주문 정보를 사전에 시작한 OrderWatch에 연결하고 결과를 대기합니다 (레거시 폴 모드)
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
            # 생성한 사전 등록 REAL 구독이 있으면 제거합니다
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
