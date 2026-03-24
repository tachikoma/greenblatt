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

    def select_stocks(self, trading_date: str) -> pd.DataFrame: ...


def build_rebalance_signal(engine: SelectionEngine, signal_date: str) -> StrategySignal:
    """백테스트 전략 로직을 재사용해 실거래 리밸런싱 신호를 생성한다."""
    trading_yyyymmdd = engine.nearest_trading_date(signal_date)
    trading_date = datetime.strptime(trading_yyyymmdd, "%Y%m%d").strftime("%Y-%m-%d")

    selected = engine.select_stocks(trading_date)

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
