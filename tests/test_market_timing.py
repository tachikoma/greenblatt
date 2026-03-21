import pandas as pd

import market_timing


class _DummyStock:
    def __init__(self, closes):
        self._closes = closes

    def get_index_ohlcv(self, start, end, index_code):
        return pd.DataFrame({"종가": self._closes})


def test_market_timing_below_ma(monkeypatch):
    closes = [100.0] * 200 + [95.0] * 20 + [80.0]
    monkeypatch.setattr(market_timing, "stock", _DummyStock(closes))

    decision = market_timing.compute_market_timing_decision(
        "2026-03-20",
        0.95,
        enabled=True,
        mode="below_ma",
        ma_window=200,
        ma_trend_lookback=20,
        multiplier=0.7,
        index_code="1001",
    )

    assert decision.reason == "ok"
    assert decision.is_below_ma is True
    assert decision.should_derisk is True
    assert decision.effective_ratio == 0.95 * 0.7


def test_market_timing_above_ma(monkeypatch):
    closes = [100.0] * 200 + [105.0] * 20 + [120.0]
    monkeypatch.setattr(market_timing, "stock", _DummyStock(closes))

    decision = market_timing.compute_market_timing_decision(
        "2026-03-20",
        0.95,
        enabled=True,
        mode="below_ma_and_ma_falling",
        ma_window=200,
        ma_trend_lookback=20,
        multiplier=0.7,
        index_code="1001",
    )

    assert decision.reason == "ok"
    assert decision.is_below_ma is False
    assert decision.should_derisk is False
    assert decision.effective_ratio == 0.95


def test_market_timing_fail_open_on_short_history(monkeypatch):
    closes = [100.0] * 20
    monkeypatch.setattr(market_timing, "stock", _DummyStock(closes))

    decision = market_timing.compute_market_timing_decision(
        "2026-03-20",
        0.95,
        enabled=True,
        mode="below_ma_and_ma_falling",
        ma_window=200,
        ma_trend_lookback=20,
        multiplier=0.7,
        index_code="1001",
    )

    assert decision.reason == "insufficient_history"
    assert decision.effective_ratio == 0.95


def test_market_timing_disabled(monkeypatch):
    monkeypatch.setattr(market_timing, "stock", _DummyStock([100.0] * 210))

    decision = market_timing.compute_market_timing_decision(
        "2026-03-20",
        0.95,
        enabled=False,
        mode="below_ma_and_ma_falling",
        ma_window=200,
        ma_trend_lookback=20,
        multiplier=0.7,
        index_code="1001",
    )

    assert decision.reason == "disabled"
    assert decision.effective_ratio == 0.95


def test_market_timing_and_mode_requires_ma_falling(monkeypatch):
    # MA는 상승 중인데 마지막 값만 아래로 내려온 케이스: below_ma=True, ma_falling=False
    closes = [100.0] * 200 + [130.0] * 20 + [90.0]
    monkeypatch.setattr(market_timing, "stock", _DummyStock(closes))

    decision = market_timing.compute_market_timing_decision(
        "2026-03-20",
        0.95,
        enabled=True,
        mode="below_ma_and_ma_falling",
        ma_window=200,
        ma_trend_lookback=20,
        multiplier=0.7,
        index_code="1001",
    )

    assert decision.reason == "ok"
    assert decision.is_below_ma is True
    assert decision.is_ma_falling is False
    assert decision.should_derisk is False
    assert decision.effective_ratio == 0.95
