"""
민감도 분석 스크립트

다음 세 가지를 한 번에 검증합니다:
  1) momentum_weight sweep (m=0, 0.3, 0.5, 0.6)
  2) mixed_filter_profile 비교 (large_cap vs aggressive_mid)
  3) 연도별 MDD 분석

주의:
  기본적으로 feature/calc-fix 브랜치 코드를 임시 체크아웃 후 실행합니다.
  실행이 끝나면 원래 브랜치(HEAD) 상태로 자동 복원됩니다.
  --no-checkout 옵션을 사용하면 현재 작업 파일 그대로 실행합니다.

사용:
    uv run scripts/sensitivity_test.py
    uv run scripts/sensitivity_test.py --start 2017-01-01 --end 2026-03-31
    uv run scripts/sensitivity_test.py --no-checkout   # 현재 코드 그대로 실행
    uv run scripts/sensitivity_test.py --validate-cagr 42.37  # m=0.6 결과 검증

파라미터는 프로젝트 루트의 .env 파일을 자동으로 읽습니다.
"""

import argparse
import importlib
import itertools
import os
import subprocess
import sys

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)


def _load_env(key: str, default):
    env_path = os.path.join(PROJECT_ROOT, ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                if k.strip() == key:
                    try:
                        return type(default)(v.strip())
                    except (ValueError, TypeError):
                        return default
    return os.environ.get(key, default)


_COMMISSION_FEE_RATE = float(_load_env("COMMISSION_FEE_RATE", 0.0015))
_TAX_RATE = float(_load_env("TAX_RATE", 0.002))
_INITIAL_CAPITAL = int(_load_env("BACKTEST_INITIAL_CAPITAL", 10_000_000))
# main()과 동일 우선순위: REBALANCE_MONTHS > LIVE_REBALANCE_MONTHS > 기본값 3
_REBALANCE_MONTHS = int(
    _load_env("REBALANCE_MONTHS", None) or _load_env("LIVE_REBALANCE_MONTHS", 3)
)


def _import_backtest_class():
    mod_name = "greenblatt_korea_full_backtest"
    if mod_name in sys.modules:
        del sys.modules[mod_name]
    mod = importlib.import_module(mod_name)
    return mod.KoreaStockBacktest


_TARGET_FILES = ["greenblatt_korea_full_backtest.py", "stock_selector.py"]


def _git_checkout(branch: str, files: list) -> bool:
    result = subprocess.run(
        ["git", "checkout", branch, "--"] + files,
        cwd=PROJECT_ROOT, capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"[경고] git checkout {branch} 실패: {result.stderr.strip()}")
        return False
    return True


def _git_restore(files: list):
    subprocess.run(["git", "restore", "--"] + files, cwd=PROJECT_ROOT, capture_output=True)


def _build_base_params() -> dict:
    return dict(
        initial_capital=_INITIAL_CAPITAL,
        investment_ratio=0.95,
        num_stocks=40,
        commission_fee_rate=_COMMISSION_FEE_RATE,
        tax_rate=_TAX_RATE,
        rebalance_months=_REBALANCE_MONTHS,
        rebalance_days=None,
        strategy_mode="mixed",
        sell_losers_enabled=True,
        kosdaq_target_ratio=None,
        momentum_enabled=True,
        momentum_months=3,
        momentum_filter_enabled=True,
        large_cap_min_mcap=None,
    )


def yearly_mdd(portfolio_df: pd.DataFrame) -> dict:
    df = portfolio_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    results = {}
    for year, grp in df.groupby(df["date"].dt.year):
        vals = grp["portfolio_value"].values
        cummax = np.maximum.accumulate(vals)
        dd = (vals - cummax) / cummax
        results[int(year)] = float(dd.min() * 100)
    return results


def run_single(label: str, params: dict) -> dict:
    print(f"\n{'='*60}")
    print(f"[실행] {label}")
    print(f"  profile={params.get('mixed_filter_profile')}, momentum_weight={params.get('momentum_weight')}")
    print(f"{'='*60}")

    BacktestClass = _import_backtest_class()
    bt = BacktestClass(**params)
    results = bt.run_backtest()
    if results is None:
        print(f"[오류] {label} 백테스트 실패")
        return {}

    port_df = results["portfolio_df"].copy()
    port_df["date"] = pd.to_datetime(port_df["date"])

    yearly_vals = port_df.set_index("date")["portfolio_value"].resample("YE").last().dropna()
    yearly_rets = yearly_vals.pct_change() * 100
    yearly_rets.iloc[0] = (yearly_vals.iloc[0] / params["initial_capital"] - 1) * 100
    yearly_ret_dict = {int(k.year): round(float(v), 2) for k, v in yearly_rets.items()}

    return {
        "label": label,
        "profile": params["mixed_filter_profile"],
        "momentum_weight": params.get("momentum_weight", 0.0),
        "final_value": results["final_value"],
        "total_return_pct": round(results["total_return_pct"], 2),
        "cagr_pct": round(results["cagr_pct"], 2),
        "mdd_pct": round(results["mdd_pct"], 2),
        "win_rate_pct": round(results["win_rate_pct"], 2),
        "sharpe_ratio": round(results["sharpe_ratio"], 3),
        "num_trades": results["num_trades"],
        "yearly_returns": yearly_ret_dict,
        "yearly_mdd": yearly_mdd(port_df),
    }


def print_summary_table(all_results: list):
    if not all_results:
        print("\n결과 없음")
        return

    label_w = max(len(r["label"]) for r in all_results) + 2

    print("\n" + "=" * 100)
    print("▶ 종합 성과 비교")
    print("=" * 100)
    headers = ["시나리오", "프로파일", "mom_w", "CAGR%", "MDD%", "승률%", "샤프", "최종자산(만원)"]
    col_w = [label_w, 14, 6, 7, 7, 7, 6, 14]
    print("  ".join(h.ljust(w) for h, w in zip(headers, col_w)))
    print("-" * 100)
    for r in all_results:
        row = [
            r["label"],
            r["profile"],
            str(r["momentum_weight"]),
            f"{r['cagr_pct']:.2f}",
            f"{r['mdd_pct']:.2f}",
            f"{r['win_rate_pct']:.2f}",
            f"{r['sharpe_ratio']:.3f}",
            f"{r['final_value'] / 10_000:,.0f}",
        ]
        print("  ".join(v.ljust(w) for v, w in zip(row, col_w)))

    all_years = sorted({y for r in all_results for y in r["yearly_returns"]})
    print("\n" + "=" * 100)
    print("▶ 연도별 수익률(%) 비교")
    print("=" * 100)
    print("연도".ljust(6) + "  " + "  ".join(r["label"].ljust(label_w) for r in all_results))
    print("-" * (8 + (label_w + 2) * len(all_results)))
    for year in all_years:
        vals = [r["yearly_returns"].get(year, float("nan")) for r in all_results]
        print(str(year).ljust(6) + "  " + "  ".join(
            f"{v:>+7.2f}%".ljust(label_w) if not np.isnan(v) else "  N/A  ".ljust(label_w)
            for v in vals
        ))

    all_years_mdd = sorted({y for r in all_results for y in r["yearly_mdd"]})
    print("\n" + "=" * 100)
    print("▶ 연도별 MDD(%) 비교  [절대값이 클수록 낙폭 큼]")
    print("=" * 100)
    print("연도".ljust(6) + "  " + "  ".join(r["label"].ljust(label_w) for r in all_results))
    print("-" * (8 + (label_w + 2) * len(all_results)))
    for year in all_years_mdd:
        vals = [r["yearly_mdd"].get(year, float("nan")) for r in all_results]
        print(str(year).ljust(6) + "  " + "  ".join(
            f"{v:>+7.2f}%".ljust(label_w) if not np.isnan(v) else "  N/A  ".ljust(label_w)
            for v in vals
        ))


def save_csv(all_results: list, out_path: str):
    rows = []
    for r in all_results:
        base = {
            "label": r["label"], "profile": r["profile"],
            "momentum_weight": r["momentum_weight"],
            "cagr_pct": r["cagr_pct"], "mdd_pct": r["mdd_pct"],
            "win_rate_pct": r["win_rate_pct"], "sharpe_ratio": r["sharpe_ratio"],
            "final_value": r["final_value"], "num_trades": r["num_trades"],
        }
        for year, ret in r["yearly_returns"].items():
            base[f"ret_{year}"] = ret
        for year, mdd in r["yearly_mdd"].items():
            base[f"mdd_{year}"] = mdd
        rows.append(base)
    pd.DataFrame(rows).to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n결과 CSV 저장: {out_path}")


def main():
    parser = argparse.ArgumentParser(description="민감도 분석 스크립트")
    parser.add_argument("--start", default="2017-01-01")
    parser.add_argument("--end", default="2026-03-31")
    parser.add_argument("--profiles", nargs="+", default=["large_cap", "aggressive_mid"])
    parser.add_argument("--mom-weights", nargs="+", type=float, default=[0.0, 0.3, 0.5, 0.6])
    parser.add_argument("--out", default="results/sensitivity_results.csv")
    parser.add_argument("--branch", default="feature/calc-fix",
                        help="테스트할 브랜치 (기본: feature/calc-fix)")
    parser.add_argument("--no-checkout", action="store_true",
                        help="git checkout 없이 현재 작업 파일 그대로 실행")
    parser.add_argument("--validate-cagr", type=float, default=None,
                        help="검증용 기준 CAGR(%%). large_cap m=0.6 결과와 비교")
    args = parser.parse_args()

    BASE_PARAMS = _build_base_params()
    print(f"\n[ENV] commission={BASE_PARAMS['commission_fee_rate']*100:.4f}%  "
          f"tax={BASE_PARAMS['tax_rate']*100:.3f}%  "
          f"rebalance_months={BASE_PARAMS['rebalance_months']}  "
          f"initial_capital={BASE_PARAMS['initial_capital']:,}원")

    checked_out = False
    if not args.no_checkout:
        print(f"\n[GIT] {args.branch} 코드로 임시 체크아웃 중...")
        checked_out = _git_checkout(args.branch, _TARGET_FILES)
        if checked_out:
            print("[GIT] 체크아웃 성공. 실행 완료 후 HEAD 상태로 자동 복원됩니다.")
        else:
            print("[경고] 체크아웃 실패 — 현재 작업 파일로 계속 진행합니다.")
    else:
        print("[GIT] --no-checkout: 현재 작업 파일 사용.")

    all_results = []
    try:
        combos = list(itertools.product(args.profiles, args.mom_weights))
        print(f"\n총 {len(combos)}개 시나리오 실행 예정")
        print(f"  브랜치: {args.branch if checked_out else '(현재 파일)'}")
        print(f"  프로파일: {args.profiles}")
        print(f"  momentum_weight: {args.mom_weights}")
        print(f"  기간: {args.start} ~ {args.end}")

        for profile, mw in combos:
            label = f"{profile} | m={mw}"
            params = {
                **BASE_PARAMS,
                "start_date": args.start,
                "end_date": args.end,
                "mixed_filter_profile": profile,
                "momentum_weight": mw,
                "momentum_enabled": mw > 0,
                "momentum_filter_enabled": mw > 0,
            }
            result = run_single(label, params)
            if result:
                all_results.append(result)
    finally:
        if checked_out:
            print(f"\n[GIT] HEAD 상태로 복원 중...")
            _git_restore(_TARGET_FILES)
            print("[GIT] 복원 완료.")

    print_summary_table(all_results)

    if args.validate_cagr is not None and all_results:
        ref = [r for r in all_results
               if abs(r["momentum_weight"] - 0.6) < 0.01 and r["profile"] == "large_cap"]
        if ref:
            actual = ref[0]["cagr_pct"]
            expected = args.validate_cagr
            diff = abs(actual - expected)
            status = "✓ 정상 범위" if diff <= 2.0 else "⚠  2%p 초과 — 파라미터 불일치 확인 필요"
            print(f"\n[검증] large_cap | m=0.6  CAGR: {actual:.2f}%  "
                  f"(기준: {expected:.2f}%  차이: {diff:.2f}%p)  {status}")

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    save_csv(all_results, args.out)


if __name__ == "__main__":
    main()
