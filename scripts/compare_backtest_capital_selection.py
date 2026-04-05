from __future__ import annotations

import contextlib
import io
import os

from greenblatt_korea_full_backtest import KoreaStockBacktest
from defaults import DEFAULT_REBALANCE_MONTHS


def run_case(enabled: bool) -> dict[str, float | int | str]:
    from utils.env import env_get

    commission_fee_rate = float(env_get("COMMISSION_FEE_RATE", default="0.0015"))
    tax_rate = float(env_get("TAX_RATE", default="0.002"))
    backtest_start_date = env_get("START_DATE", fallback_keys=["BACKTEST_START_DATE"], default="2025-01-01")
    backtest_end_date = env_get("END_DATE", fallback_keys=["BACKTEST_END_DATE"], default="2025-12-31")
    backtest_initial_capital = int(env_get("INITIAL_CAPITAL", fallback_keys=["BACKTEST_INITIAL_CAPITAL"], default="5000000"))

    common = dict(
        start_date=backtest_start_date,
        end_date=backtest_end_date,
        initial_capital=backtest_initial_capital,
        investment_ratio=0.95,
        num_stocks=40,
        commission_fee_rate=commission_fee_rate,
        tax_rate=tax_rate,
        rebalance_months=DEFAULT_REBALANCE_MONTHS,
        strategy_mode="mixed",
        mixed_filter_profile="large_cap",
        sell_losers_enabled=True,
        kosdaq_target_ratio=None,
        momentum_enabled=True,
        momentum_months=3,
        momentum_weight=0.60,
        momentum_filter_enabled=True,
        large_cap_min_mcap=None,
        fundamental_source="pykrx",
        capital_constrained_selection_enabled=enabled,
        capital_constrained_min_stocks=20,
        capital_constrained_max_stocks=40,
    )

    bt = KoreaStockBacktest(**common)
    # 백테스트 진행 로그는 숨기고 결과 숫자만 수집합니다.
    with contextlib.redirect_stdout(io.StringIO()):
        result = bt.run_backtest()

    portfolio_df = result["portfolio_df"]
    avg_holdings = float(portfolio_df["num_holdings"].mean()) if "num_holdings" in portfolio_df.columns else float("nan")

    return {
        "mode": "ON" if enabled else "OFF",
        "actual_start_date": str(result["actual_start_date"]),
        "actual_end_date": str(result["actual_end_date"]),
        "total_return_pct": float(result["total_return_pct"]),
        "cagr_pct": float(result["cagr_pct"]),
        "mdd_pct": float(result["mdd_pct"]),
        "num_trades": int(result["num_trades"]),
        "avg_holdings": avg_holdings,
        "final_value": float(result["final_value"]),
    }


def main() -> None:
    off = run_case(False)
    on = run_case(True)

    print("period", off["actual_start_date"], off["actual_end_date"])
    print("OFF", off)
    print("ON", on)
    print("DIFF total_return_pct", round(on["total_return_pct"] - off["total_return_pct"], 4))
    print("DIFF cagr_pct", round(on["cagr_pct"] - off["cagr_pct"], 4))
    print("DIFF mdd_pct", round(on["mdd_pct"] - off["mdd_pct"], 4))
    print("DIFF num_trades", on["num_trades"] - off["num_trades"])
    print("DIFF avg_holdings", round(on["avg_holdings"] - off["avg_holdings"], 4))
    print("DIFF final_value", round(on["final_value"] - off["final_value"], 2))


if __name__ == "__main__":
    main()
