import os
import pytest
from live_trading.config import LiveTradingConfig
from live_trading.kiwoom_adapter import KiwoomBrokerAdapter


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mock_server_order_flow():
    """Integration test that exercises connect + order endpoints on Kiwoom mock server.

    This test is skipped unless the environment variable `RUN_KIWOOM_MOCK_TESTS` is set to a
    truthy value and valid `KIWOOM_APPKEY`/`KIWOOM_SECRETKEY` are provided. The test performs
    non-destructive calls and asserts that the mock server responds without raising exceptions.
    """
    if not os.getenv("RUN_KIWOOM_MOCK_TESTS"):
        pytest.skip("Set RUN_KIWOOM_MOCK_TESTS=1 to run mock-server integration tests")

    # require credentials for mock server
    appkey = os.getenv("KIWOOM_APPKEY")
    secret = os.getenv("KIWOOM_SECRETKEY")
    if not appkey or not secret:
        pytest.skip("KIWOOM_APPKEY/KIWOOM_SECRETKEY not set for mock server")

    # use mock mode and real API client
    os.environ["KIWOOM_MODE"] = "mock"
    cfg = LiveTradingConfig.from_env()
    cfg.mode = "mock"

    # small, non-destructive parameters
    ticker = os.getenv("INTEGRATION_TEST_TICKER", "003670")
    qty = int(os.getenv("INTEGRATION_TEST_QTY", "1"))
    price = int(os.getenv("INTEGRATION_TEST_PRICE", "1000"))

    from live_trading.kiwoom_adapter import KiwoomAPIError

    async with KiwoomBrokerAdapter(cfg) as kb:
        # 1) submit order (buy). Let the caller decide fallback on RC4027.
        try:
            submitted = await kb.submit_order(ticker=ticker, side="BUY", quantity=qty, price=price)
        except KiwoomAPIError as exc:
            # If mock server indicates 상/하한가(4027), attempt a market-order retry here.
            msg = str(exc.return_msg or "")
            try:
                rc = int(exc.return_code) if exc.return_code is not None else None
            except Exception:
                rc = None

            if rc == 4027 or "RC4027" in msg or "4027" in msg:
                submitted = await kb.submit_order(ticker=ticker, side="BUY", quantity=qty, price=price, order_type="3")
            else:
                raise

        # require the underlying response to signal success if the mock server provides a return_code
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
