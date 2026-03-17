import asyncio
import os
import pytest
from live_trading.config import LiveTradingConfig
from live_trading.kiwoom_adapter import KiwoomBrokerAdapter, SubmittedOrder


class DummyResp:
    def __init__(self, body):
        self._body = body

    def json(self):
        return self._body


class MockAPIBehavior:
    """컨트롤 가능한 mock API 동작을 제공하는 헬퍼

    - 상태를 설정해 각 TR 호출에 대해 성공/실패/미체결을 시뮬레이션할 수 있음
    """

    def __init__(self, cfg: LiveTradingConfig):
        self.cfg = cfg
        # 기본 동작: submit -> 미체결(빈 ord_no), modify -> success, cancel -> success, status -> pending 1
        self.behavior = {
            # submit_sequence: list of ord_no values returned on successive submit calls
            "submit_sequence": [""],
            "modify_success": True,
            "cancel_success": True,
            "status_pending_qty": 1,
        }
        self._submit_calls = 0

    def set(self, **kwargs):
        self.behavior.update(kwargs)

    async def request(self, endpoint, api_id=None, data=None):
        # 미체결/체결 조회
        if api_id == self.cfg.order_status_api_id:
            rec = {"stk_cd": data.get("stk_cd") or "003670", "rmn_qty": str(self.behavior["status_pending_qty"]), "ord_no": data.get("orig_ord_no", "")}
            return DummyResp({"list": [rec], "return_code": 0})

        # 정정
        if api_id == self.cfg.order_modify_api_id:
            if self.behavior["modify_success"]:
                return DummyResp({"ord_no": "M12345", "mdfy_qty": data.get("mdfy_qty"), "return_code": 0})
            return DummyResp({"return_code": 999, "return_msg": "mock modify failed"})

        # 취소
        if api_id == self.cfg.order_cancel_api_id:
            if self.behavior["cancel_success"]:
                return DummyResp({"ord_no": "C12345", "cncl_qty": data.get("ord_qty"), "return_code": 0})
            return DummyResp({"return_code": 998, "return_msg": "mock cancel failed"})

        # 제출 (support sequence of submit responses)
        if api_id in (self.cfg.order_buy_api_id, self.cfg.order_sell_api_id, self.cfg.order_api_id):
            seq = self.behavior.get("submit_sequence", [""])
            idx = min(self._submit_calls, len(seq) - 1)
            val = seq[idx]
            self._submit_calls += 1
            # support dict entries for richer mock responses
            if isinstance(val, dict):
                return DummyResp(val)
            # by default consider submit successful (return_code 0) unless special marker provided
            return DummyResp({"ord_no": val, "return_code": 0})

        return DummyResp({})


class DummyAPI:
    def __init__(self, behavior):
        self._behavior = behavior

    async def connect(self):
        return None

    async def close(self):
        return None

    def token(self):
        return "dummy-token"

    async def request(self, endpoint, api_id=None, data=None):
        return await self._behavior.request(endpoint, api_id=api_id, data=data)


@pytest.mark.asyncio
async def test_buy_modify_flow(tmp_path, monkeypatch):
    os.environ["KIWOOM_MODE"] = "mock"
    cfg = LiveTradingConfig.from_env()
    cfg.mode = "mock"
    cfg.appkey = "dummy"
    cfg.secretkey = "dummy"
    # Ensure cancel TR is distinct from modify TR for tests
    cfg.order_cancel_api_id = "kt10003"

    # create a dummy API client that implements connect/close/token and delegates request
    class DummyAPI:
        def __init__(self, behavior):
            self._behavior = behavior

        async def connect(self):
            return None

        async def close(self):
            return None

        def token(self):
            return "dummy-token"

        async def request(self, endpoint, api_id=None, data=None):
            return await self._behavior.request(endpoint, api_id=api_id, data=data)

    mock = MockAPIBehavior(cfg)
    # submit returns empty ord_no (미체결)
    mock.set(submit_sequence=[""], modify_success=True, cancel_success=True, status_pending_qty=1)
    dummy_api = DummyAPI(mock)

    async with KiwoomBrokerAdapter(cfg, api_client=dummy_api) as kb:
        submitted = await kb.submit_order(ticker="003670", side="BUY", quantity=1, price=1000)
        assert submitted.order_no == ""

        # run_retry_cycle should attempt modify (succeeds) and return retried order with ord_no M12345
        retries, checks = await kb.run_retry_cycle([submitted])
        assert len(retries) == 1
        assert retries[0].order_no == "M12345"
        assert checks[0].pending_qty == 1


@pytest.mark.asyncio
async def test_modify_fallback_cancel_and_resubmit(tmp_path, monkeypatch):
    os.environ["KIWOOM_MODE"] = "mock"
    cfg = LiveTradingConfig.from_env()
    cfg.mode = "mock"
    cfg.appkey = "dummy"
    cfg.secretkey = "dummy"
    cfg.order_cancel_api_id = "kt10003"

    mock = MockAPIBehavior(cfg)
    # modify 실패, cancel 성공, initial submit returns empty, retry submit returns R67890
    mock.set(submit_sequence=["", "R67890"], modify_success=False, cancel_success=True, status_pending_qty=1)
    dummy_api = DummyAPI(mock)

    async with KiwoomBrokerAdapter(cfg, api_client=dummy_api) as kb:
        submitted = await kb.submit_order(ticker="003670", side="BUY", quantity=1, price=1000)
        assert submitted.order_no == ""

        retries, checks = await kb.run_retry_cycle([submitted])
        # fallback path should have produced a new submitted order from submit_order
        assert len(retries) == 1
        assert retries[0].order_no == "R67890"


@pytest.mark.asyncio
async def test_full_sell_flow_with_modify_and_cancel(tmp_path, monkeypatch):
    os.environ["KIWOOM_MODE"] = "mock"
    cfg = LiveTradingConfig.from_env()
    cfg.mode = "mock"
    cfg.appkey = "dummy"
    cfg.secretkey = "dummy"
    cfg.order_cancel_api_id = "kt10003"

    mock = MockAPIBehavior(cfg)
    # simulate sell path: submit sell returns empty ord_no -> modify succeeds
    mock.set(submit_sequence=[""], modify_success=True, cancel_success=True, status_pending_qty=2)
    dummy_api = DummyAPI(mock)

    async with KiwoomBrokerAdapter(cfg, api_client=dummy_api) as kb:
        submitted = await kb.submit_order(ticker="003670", side="SELL", quantity=2, price=2000)
        assert submitted.order_no == ""

        retries, checks = await kb.run_retry_cycle([submitted])
        assert len(retries) == 1
        assert retries[0].order_no == "M12345"
        assert checks[0].pending_qty == 2


@pytest.mark.asyncio
async def test_submit_rc4027_fallback_to_market(tmp_path, monkeypatch):
    os.environ["KIWOOM_MODE"] = "mock"
    cfg = LiveTradingConfig.from_env()
    cfg.mode = "mock"
    cfg.appkey = "dummy"
    cfg.secretkey = "dummy"
    cfg.order_cancel_api_id = "kt10003"

    mock = MockAPIBehavior(cfg)
    # First submit returns RC4027 error, second submit (market retry) returns ord_no MKT123
    mock.set(submit_sequence=[{"return_code": 4027, "return_msg": "RC4027: 상/하한가 오류"}, "MKT123"])
    dummy_api = DummyAPI(mock)

    async with KiwoomBrokerAdapter(cfg, api_client=dummy_api) as kb:
        # First submit should raise KiwoomAPIError (RC4027)
        from live_trading.kiwoom_adapter import KiwoomAPIError

        with pytest.raises(KiwoomAPIError) as ei:
            await kb.submit_order(ticker="003670", side="BUY", quantity=1, price=1000)

        # Caller decides to retry as market order
        submitted = await kb.submit_order(ticker="003670", side="BUY", quantity=1, price=1000, order_type="3")
        assert submitted.order_no == "MKT123"
