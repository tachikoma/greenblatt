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
async def test_wait_until_market_open_uses_ceiling_for_subsecond_delay(monkeypatch):
    cfg = LiveTradingConfig()
    cfg.open_wait_enabled = True
    cfg.market_open_hhmm = "09:00"
    cfg.market_open_grace_seconds = 30

    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(run_live_trading, "datetime", FixedDateTime)
    monkeypatch.setattr(run_live_trading.asyncio, "sleep", fake_sleep)

    await run_live_trading._wait_until_market_open(cfg)

    assert sleep_calls == [31]


@pytest.mark.asyncio
async def test_wait_until_market_open_skips_after_market_open(monkeypatch):
    cfg = LiveTradingConfig()
    cfg.open_wait_enabled = True
    cfg.market_open_hhmm = "09:00"
    cfg.market_open_grace_seconds = 30

    monkeypatch.setattr(run_live_trading, "datetime", AfterOpenDateTime)
    monkeypatch.setattr(run_live_trading.asyncio, "sleep", lambda _: None)

    await run_live_trading._wait_until_market_open(cfg)


@pytest.mark.asyncio
async def test_wait_until_market_open_rejects_invalid_format():
    cfg = LiveTradingConfig()
    cfg.open_wait_enabled = True
    cfg.market_open_hhmm = "0900"

    with pytest.raises(ValueError, match="Invalid market_open_hhmm value"):
        await run_live_trading._wait_until_market_open(cfg)
