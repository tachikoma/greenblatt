from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

import pandas as pd


@dataclass(slots=True)
class StrategySignal:
    signal_date: str
    trading_date: str
    selected: pd.DataFrame


class SelectionEngine(Protocol):
    strategy_mode: str

    def nearest_trading_date(self, signal_date: str) -> str: ...

    def previous_trading_date(self, date_str: str) -> str: ...

    def select_stocks(self, trading_date: str) -> pd.DataFrame: ...


def build_rebalance_signal(engine: SelectionEngine, signal_date: str, signal_date_lag: int = 0) -> StrategySignal:
    """백테스트 전략 로직을 재사용해 실거래 리밸런싱 신호를 생성한다.

    signal_date_lag > 0이면 해당 거래일 수만큼 이전 확정 종가를 신호 생성에 사용한다.
    장중 현재가 오염 방지 목적으로 1을 설정하면 T-1 종가 기준으로 종목이 선정된다.
    """
    trading_yyyymmdd = engine.nearest_trading_date(signal_date)
    trading_date = datetime.strptime(trading_yyyymmdd, "%Y%m%d").strftime("%Y-%m-%d")

    # T-N 종가 기준 종목 선정 (signal_date_lag=1 → T-1 확정 종가)
    selection_date = trading_date
    if signal_date_lag > 0:
        lagged_yyyymmdd = trading_date
        for _ in range(signal_date_lag):
            lagged_yyyymmdd = engine.previous_trading_date(lagged_yyyymmdd)
        selection_date = datetime.strptime(lagged_yyyymmdd, "%Y%m%d").strftime("%Y-%m-%d")

    selected = engine.select_stocks(selection_date)

    selected = selected.copy()
    # 상위 데이터 수집에 실패하면 selector가 컬럼 없는 빈 DataFrame을 반환할 수 있습니다.
    # 호출자가 기존의 SKIPPED 경로를 안전하게 처리할 수 있도록 이 경우를 정규화합니다.
    if selected.empty:
        for col in ["ticker", "close"]:
            if col not in selected.columns:
                selected[col] = pd.Series(dtype="object" if col == "ticker" else "float64")
    elif "ticker" not in selected.columns or "close" not in selected.columns:
        raise ValueError("strategy output must include ticker and close columns")

    return StrategySignal(
        signal_date=signal_date,
        trading_date=trading_date,
        selected=selected,
    )
