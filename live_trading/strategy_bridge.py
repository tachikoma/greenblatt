from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from greenblatt_korea_full_backtest import KoreaStockBacktest


@dataclass(slots=True)
class StrategySignal:
    signal_date: str
    trading_date: str
    selected: pd.DataFrame


def build_rebalance_signal(backtest: KoreaStockBacktest, signal_date: str) -> StrategySignal:
    """백테스트 전략 로직을 재사용해 실거래 리밸런싱 신호를 생성한다."""
    trading_yyyymmdd = backtest._get_nearest_trading_date(signal_date.replace("-", ""))
    trading_date = datetime.strptime(trading_yyyymmdd, "%Y%m%d").strftime("%Y-%m-%d")

    if backtest.strategy_mode == "mixed":
        selected = backtest.screen_stocks_mixed(trading_date)
    else:
        selected = backtest.screen_stocks_pykrx_roe(trading_date)

    selected = selected.copy()
    if "ticker" not in selected.columns or "close" not in selected.columns:
        raise ValueError("strategy output must include ticker and close columns")

    return StrategySignal(
        signal_date=signal_date,
        trading_date=trading_date,
        selected=selected,
    )
