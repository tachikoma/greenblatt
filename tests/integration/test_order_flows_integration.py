import os
import pytest
from live_trading.config import LiveTradingConfig
from live_trading.kiwoom_adapter import KiwoomBrokerAdapter


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mock_server_order_flow():
    """키움 모의 서버의 연결 및 주문 엔드포인트를 점검하는 통합 테스트입니다.

    이 테스트는 환경변수 `RUN_KIWOOM_MOCK_TESTS`가 truthy로 설정되고
    유효한 `KIWOOM_APPKEY`/`KIWOOM_SECRETKEY`가 제공된 경우에만 실행됩니다.
    테스트는 비파괴적 호출을 수행하며 모의 서버가 예외를 발생시키지 않고 응답하는지 확인합니다.
    """
    if not os.getenv("RUN_KIWOOM_MOCK_TESTS"):
        pytest.skip("Set RUN_KIWOOM_MOCK_TESTS=1 to run mock-server integration tests")

    # 모의 서버용 자격증명 필요
    appkey = os.getenv("KIWOOM_APPKEY")
    secret = os.getenv("KIWOOM_SECRETKEY")
    if not appkey or not secret:
        pytest.skip("KIWOOM_APPKEY/KIWOOM_SECRETKEY not set for mock server")

    # 모드: mock으로 실행하되 실제 API 클라이언트를 사용합니다
    os.environ["KIWOOM_MODE"] = "mock"
    cfg = LiveTradingConfig.from_env()
    cfg.mode = "mock"

    # 작고 비파괴적인 파라미터
    ticker = os.getenv("INTEGRATION_TEST_TICKER", "003670")
    qty = int(os.getenv("INTEGRATION_TEST_QTY", "1"))
    price = int(os.getenv("INTEGRATION_TEST_PRICE", "1000"))

    from live_trading.kiwoom_adapter import KiwoomAPIError

    async with KiwoomBrokerAdapter(cfg) as kb:
        # 1) 주문 제출(매수). RC4027에 대한 폴백은 호출자 정책에 맡깁니다.
        try:
            submitted = await kb.submit_order(ticker=ticker, side="BUY", quantity=qty, price=price)
        except KiwoomAPIError as exc:
            # 모의 서버가 상/하한가(4027)를 표시하면 여기서 시장가 재시도를 시도합니다.
            msg = str(exc.return_msg or "")
            try:
                rc = int(exc.return_code) if exc.return_code is not None else None
            except Exception:
                rc = None

            if rc == 4027 or "RC4027" in msg or "4027" in msg:
                submitted = await kb.submit_order(ticker=ticker, side="BUY", quantity=qty, price=price, order_type="3")
            else:
                raise

        # 모의 서버가 return_code를 제공하는 경우, 기본 응답이 성공을 표시해야 합니다
        if isinstance(submitted.raw_response, dict) and "return_code" in submitted.raw_response:
            assert int(submitted.raw_response.get("return_code") or 0) == 0, f"submit failed: {submitted.raw_response}"

        # 2) query status
        pending = await kb.query_open_order_pending_qty(submitted)

        # 3) attempt modify if order_no present; otherwise attempt cancel
        if submitted.order_no:
            mod_resp = None
            try:
                mod_resp = await kb.modify_order(submitted, new_qty=max(1, qty), new_price=max(1, price))
            except KiwoomAPIError as exc:
                # Mock server may refuse modification when there's nothing to modify (RC4033).
                # Treat RC4033 as non-fatal for integration tests; re-raise other errors.
                try:
                    rc = int(exc.return_code) if exc.return_code is not None else None
                except Exception:
                    rc = None
                msg = str(exc.return_msg or "")
                if rc == 4033 or "RC4033" in msg or "4033" in msg:
                    # acceptable mock condition; continue
                    mod_resp = exc.body
                else:
                    pytest.fail(f"modify_order failed: {exc}")
            except Exception as exc:
                pytest.fail(f"modify_order failed: {exc}")

                if isinstance(mod_resp, dict) and "return_code" in mod_resp:
                    rc = int(mod_resp.get("return_code") or 0)
                    if rc != 0:
                        # Allow RC4033 from mock server as a non-fatal condition for modify
                        msg = str(mod_resp.get("return_msg") or "")
                        if rc == 4033 or "RC4033" in msg or "4033" in msg:
                            # acceptable mock condition; continue
                            pass
                        else:
                            assert False, f"modify failed: {mod_resp}"
        else:
            try:
                cancel_resp = await kb.cancel_order(submitted, pending)
            except Exception as exc:
                pytest.fail(f"cancel_order failed: {exc}")

            if isinstance(cancel_resp, dict) and "return_code" in cancel_resp:
                assert int(cancel_resp.get("return_code") or 0) == 0, f"cancel failed: {cancel_resp}"
