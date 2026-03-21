from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd

try:
    from pykrx import stock
except ImportError:  # pragma: no cover
    stock = None


@dataclass(slots=True)
class MarketTimingDecision:
    enabled: bool
    is_below_ma: bool
    is_ma_falling: bool
    should_derisk: bool
    mode: str
    base_ratio: float
    effective_ratio: float
    multiplier: float
    ma_window: int
    ma_trend_lookback: int
    index_code: str
    index_close: float | None
    ma_value: float | None
    data_points: int
    reason: str


def _normalize_date(date_str: str) -> str:
    return date_str.replace("-", "")


def _resolve_close_series(frame: pd.DataFrame) -> pd.Series:
    if "종가" in frame.columns:
        return pd.to_numeric(frame["종가"], errors="coerce").dropna()
    if "close" in frame.columns:
        return pd.to_numeric(frame["close"], errors="coerce").dropna()

    for col in frame.columns:
        series = pd.to_numeric(frame[col], errors="coerce").dropna()
        if not series.empty:
            return series
    return pd.Series(dtype="float64")


def compute_market_timing_decision(
    as_of_date: str,
    base_ratio: float,
    *,
    enabled: bool,
    ma_window: int,
    multiplier: float,
    index_code: str = "1001",
    mode: str = "below_ma_and_ma_falling",
    ma_trend_lookback: int = 20,
) -> MarketTimingDecision:
    """지수-이동평균 기반 유효 투자비율을 계산한다.

    - 조건 충족: 지수 종가 < MA(ma_window) -> base_ratio * multiplier
    - 조건 미충족: base_ratio 유지
    - 데이터 부족/조회 실패: base_ratio 유지 (fail-open)
    """
    safe_base = max(0.0, min(1.0, float(base_ratio)))
    safe_multiplier = max(0.0, min(1.0, float(multiplier)))
    safe_window = max(2, int(ma_window))
    safe_trend_lookback = max(1, int(ma_trend_lookback))
    safe_index_code = str(index_code or "1001").strip() or "1001"
    safe_mode = str(mode or "below_ma_and_ma_falling").strip().lower()
    if safe_mode not in {"below_ma", "below_ma_and_ma_falling", "below_ma_or_ma_falling"}:
        safe_mode = "below_ma_and_ma_falling"

    if not enabled:
        return MarketTimingDecision(
            enabled=False,
            is_below_ma=False,
            is_ma_falling=False,
            should_derisk=False,
            mode=safe_mode,
            base_ratio=safe_base,
            effective_ratio=safe_base,
            multiplier=safe_multiplier,
            ma_window=safe_window,
            ma_trend_lookback=safe_trend_lookback,
            index_code=safe_index_code,
            index_close=None,
            ma_value=None,
            data_points=0,
            reason="disabled",
        )

    if stock is None:
        return MarketTimingDecision(
            enabled=True,
            is_below_ma=False,
            is_ma_falling=False,
            should_derisk=False,
            mode=safe_mode,
            base_ratio=safe_base,
            effective_ratio=safe_base,
            multiplier=safe_multiplier,
            ma_window=safe_window,
            ma_trend_lookback=safe_trend_lookback,
            index_code=safe_index_code,
            index_close=None,
            ma_value=None,
            data_points=0,
            reason="pykrx_unavailable",
        )

    try:
        end_dt = datetime.strptime(_normalize_date(as_of_date), "%Y%m%d")
    except Exception:
        return MarketTimingDecision(
            enabled=True,
            is_below_ma=False,
            is_ma_falling=False,
            should_derisk=False,
            mode=safe_mode,
            base_ratio=safe_base,
            effective_ratio=safe_base,
            multiplier=safe_multiplier,
            ma_window=safe_window,
            ma_trend_lookback=safe_trend_lookback,
            index_code=safe_index_code,
            index_close=None,
            ma_value=None,
            data_points=0,
            reason="invalid_date",
        )

    # 비거래일/결측 구간을 감안해 충분한 버퍼를 두고 조회한다.
    start_dt = end_dt - timedelta(days=max(450, safe_window * 3))
    start_str = start_dt.strftime("%Y%m%d")
    end_str = end_dt.strftime("%Y%m%d")

    try:
        frame = stock.get_index_ohlcv(start_str, end_str, safe_index_code)
    except Exception:
        frame = None

    if frame is None or frame.empty:
        return MarketTimingDecision(
            enabled=True,
            is_below_ma=False,
            is_ma_falling=False,
            should_derisk=False,
            mode=safe_mode,
            base_ratio=safe_base,
            effective_ratio=safe_base,
            multiplier=safe_multiplier,
            ma_window=safe_window,
            ma_trend_lookback=safe_trend_lookback,
            index_code=safe_index_code,
            index_close=None,
            ma_value=None,
            data_points=0,
            reason="index_fetch_failed",
        )

    close_series = _resolve_close_series(frame)
    min_points = safe_window + safe_trend_lookback
    if len(close_series) < min_points:
        return MarketTimingDecision(
            enabled=True,
            is_below_ma=False,
            is_ma_falling=False,
            should_derisk=False,
            mode=safe_mode,
            base_ratio=safe_base,
            effective_ratio=safe_base,
            multiplier=safe_multiplier,
            ma_window=safe_window,
            ma_trend_lookback=safe_trend_lookback,
            index_code=safe_index_code,
            index_close=float(close_series.iloc[-1]) if len(close_series) > 0 else None,
            ma_value=None,
            data_points=int(len(close_series)),
            reason="insufficient_history",
        )

    ma_series = close_series.rolling(safe_window).mean().dropna()
    if len(ma_series) <= safe_trend_lookback:
        return MarketTimingDecision(
            enabled=True,
            is_below_ma=False,
            is_ma_falling=False,
            should_derisk=False,
            mode=safe_mode,
            base_ratio=safe_base,
            effective_ratio=safe_base,
            multiplier=safe_multiplier,
            ma_window=safe_window,
            ma_trend_lookback=safe_trend_lookback,
            index_code=safe_index_code,
            index_close=float(close_series.iloc[-1]),
            ma_value=None,
            data_points=int(len(close_series)),
            reason="insufficient_history",
        )

    ma_value = float(ma_series.iloc[-1])
    ma_prev = float(ma_series.iloc[-1 - safe_trend_lookback])
    index_close = float(close_series.iloc[-1])
    is_below_ma = index_close < ma_value
    is_ma_falling = ma_value < ma_prev

    if safe_mode == "below_ma":
        should_derisk = is_below_ma
    elif safe_mode == "below_ma_or_ma_falling":
        should_derisk = is_below_ma or is_ma_falling
    else:
        should_derisk = is_below_ma and is_ma_falling

    effective_ratio = safe_base * safe_multiplier if should_derisk else safe_base

    return MarketTimingDecision(
        enabled=True,
        is_below_ma=bool(is_below_ma),
        is_ma_falling=bool(is_ma_falling),
        should_derisk=bool(should_derisk),
        mode=safe_mode,
        base_ratio=safe_base,
        effective_ratio=float(effective_ratio),
        multiplier=safe_multiplier,
        ma_window=safe_window,
        ma_trend_lookback=safe_trend_lookback,
        index_code=safe_index_code,
        index_close=index_close,
        ma_value=ma_value,
        data_points=int(len(close_series)),
        reason="ok",
    )
