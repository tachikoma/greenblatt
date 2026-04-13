import pytest
from datetime import datetime
from live_trading.config import LiveTradingConfig
import run_live_trading


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        dt = datetime(2026, 4, 13, 8, 59, 59, 200000)
        return dt.replace(tzinfo=tz) if tz is not None else dt


class AfterOpenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        dt = datetime(2026, 4, 13, 9, 0, 1)
        return dt.replace(tzinfo=tz) if tz is not None else dt


@pytest.mark.asyncio
async def test_wait_until_order_time_uses_ceiling_for_subsecond_delay(monkeypatch):
    cfg = LiveTradingConfig()
    cfg.order_time_wait_enabled = True
    cfg.order_time_hhmm = "09:00"
    cfg.order_time_grace_seconds = 30

    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(run_live_trading, "datetime", FixedDateTime)
    monkeypatch.setattr(run_live_trading.asyncio, "sleep", fake_sleep)

    await run_live_trading._wait_until_order_time(cfg)

    assert sleep_calls == [31]


@pytest.mark.asyncio
async def test_wait_until_order_time_skips_after_order_time(monkeypatch):
    cfg = LiveTradingConfig()
    cfg.order_time_wait_enabled = True
    cfg.order_time_hhmm = "09:00"
    cfg.order_time_grace_seconds = 30

    monkeypatch.setattr(run_live_trading, "datetime", AfterOpenDateTime)
    monkeypatch.setattr(run_live_trading.asyncio, "sleep", lambda _: None)

    await run_live_trading._wait_until_order_time(cfg)


@pytest.mark.asyncio
async def test_wait_until_order_time_rejects_invalid_format():
    cfg = LiveTradingConfig()
    cfg.order_time_wait_enabled = True
    cfg.order_time_hhmm = "0900"

    with pytest.raises(ValueError, match="Invalid order_time_hhmm value"):
        await run_live_trading._wait_until_order_time(cfg)
