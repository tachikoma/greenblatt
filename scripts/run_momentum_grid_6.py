"""모멘텀 6개 조합 백테스트 일괄 실행 스크립트.

요구사항:
- momentum_weight: 0.40, 0.50, 0.60
- momentum_months: 3, 6
- 총 6개 조합만 실행

특징:
- 비용/슬리피지/체결 규칙은 .env 기반 BacktestConfig를 그대로 사용
- 모멘텀 파라미터(momentum_weight, momentum_months)만 조합별로 변경
- 결과를 콘솔 표 + CSV로 저장

사용 예시:
    uv run scripts/run_momentum_grid_6.py
    uv run scripts/run_momentum_grid_6.py --start 2017-03-01 --end 2026-03-31
    uv run scripts/run_momentum_grid_6.py --csv results/momentum_grid_6_summary.csv
"""

from __future__ import annotations

import argparse
import inspect
import os
import sys
from dataclasses import asdict
from itertools import product

import pandas as pd


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from greenblatt_korea_full_backtest import KoreaStockBacktest
from live_trading.strategy_config import BacktestConfig


WEIGHTS = (0.40, 0.50, 0.60)
MONTHS = (3, 6)


def _build_base_kwargs(cfg: BacktestConfig) -> dict:
    """BacktestConfig를 KoreaStockBacktest 생성자 인자에 맞춰 변환한다."""
    cfg_dict = asdict(cfg)
    sig = inspect.signature(KoreaStockBacktest.__init__)
    valid_keys = set(sig.parameters.keys()) - {"self"}
    return {k: v for k, v in cfg_dict.items() if k in valid_keys}


def _run_single(base_kwargs: dict, momentum_weight: float, momentum_months: int) -> dict:
    params = dict(base_kwargs)
    params["momentum_weight"] = float(momentum_weight)
    params["momentum_months"] = int(momentum_months)

    label = f"m={momentum_weight:.2f}, months={momentum_months}"
    print("\n" + "=" * 72)
    print(f"[실행] {label}")
    print("=" * 72)

    backtest = KoreaStockBacktest(**params)
    result = backtest.run_backtest()
    if not result:
        return {
            "label": label,
            "momentum_weight": momentum_weight,
            "momentum_months": momentum_months,
            "status": "failed",
        }

    return {
        "label": label,
        "momentum_weight": momentum_weight,
        "momentum_months": momentum_months,
        "status": "ok",
        "total_return_pct": round(float(result["total_return_pct"]), 2),
        "cagr_pct": round(float(result["cagr_pct"]), 2),
        "mdd_pct": round(float(result["mdd_pct"]), 2),
        "win_rate_pct": round(float(result["win_rate_pct"]), 2),
        "sharpe_ratio": round(float(result["sharpe_ratio"]), 3),
        "num_trades": int(result["num_trades"]),
        "final_value": float(result["final_value"]),
        "actual_start_date": result["actual_start_date"],
        "actual_end_date": result["actual_end_date"],
    }


def _print_summary(df: pd.DataFrame) -> None:
    print("\n" + "=" * 96)
    print("모멘텀 6개 조합 비교 결과")
    print("=" * 96)
    if df.empty:
        print("실행 결과가 없습니다.")
        return

    ordered_cols = [
        "momentum_weight",
        "momentum_months",
        "total_return_pct",
        "cagr_pct",
        "mdd_pct",
        "win_rate_pct",
        "sharpe_ratio",
        "num_trades",
        "final_value",
        "status",
    ]
    display_df = df.copy()
    for c in ordered_cols:
        if c not in display_df.columns:
            display_df[c] = pd.NA

    display_df = display_df[ordered_cols].sort_values(
        by=["cagr_pct", "sharpe_ratio"], ascending=[False, False], na_position="last"
    )
    print(display_df.to_string(index=False))

    best = display_df[display_df["status"] == "ok"].head(1)
    if not best.empty:
        row = best.iloc[0]
        print(
            "\nBEST: "
            f"weight={row['momentum_weight']:.2f}, "
            f"months={int(row['momentum_months'])}, "
            f"CAGR={row['cagr_pct']:.2f}%, MDD={row['mdd_pct']:.2f}%"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="모멘텀 6개 조합 일괄 백테스트")
    parser.add_argument("--start", type=str, default=None, help="시작일(YYYY-MM-DD), 미지정 시 .env 사용")
    parser.add_argument("--end", type=str, default=None, help="종료일(YYYY-MM-DD), 미지정 시 .env 사용")
    parser.add_argument(
        "--csv",
        type=str,
        default="results/momentum_grid_6_summary.csv",
        help="요약 결과 CSV 경로",
    )
    args = parser.parse_args()

    cfg = BacktestConfig.from_env()
    if args.start:
        cfg.start_date = args.start
    if args.end:
        cfg.end_date = args.end

    base_kwargs = _build_base_kwargs(cfg)

    print("기준 설정(.env 기반)")
    print(f"- 기간: {base_kwargs.get('start_date')} ~ {base_kwargs.get('end_date')}")
    print(f"- 수수료: {float(base_kwargs.get('commission_fee_rate', 0.0))*100:.4f}%")
    print(f"- 세금: {float(base_kwargs.get('tax_rate', 0.0))*100:.4f}%")
    print(f"- 슬리피지: {base_kwargs.get('slippage_bps')} bps")
    print(f"- 체결기준(use_open_price): {base_kwargs.get('use_open_price')}")
    print("- 가변 파라미터: momentum_weight, momentum_months")

    results: list[dict] = []
    for momentum_weight, momentum_months in product(WEIGHTS, MONTHS):
        row = _run_single(
            base_kwargs=base_kwargs,
            momentum_weight=momentum_weight,
            momentum_months=momentum_months,
        )
        results.append(row)

    summary_df = pd.DataFrame(results)
    _print_summary(summary_df)

    out_path = args.csv
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    summary_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\nCSV 저장 완료: {out_path}")


if __name__ == "__main__":
    main()
