from __future__ import annotations

import asyncio
from datetime import datetime
import os

import pandas as pd

from live_trading.config import LiveTradingConfig
from live_trading.execution import build_order_intents
from live_trading.kiwoom_adapter import KiwoomBrokerAdapter
from live_trading.strategy_bridge import build_rebalance_signal
from live_trading.strategy_config import CostConfig, StrategyConfig
from live_trading.strategy_engine import LiveSignalEngine


def _limit_price(side: str, ref_price: float, bps: int) -> int:
    if ref_price <= 0:
        return 0
    factor = 1 + (bps / 10000) if side == "BUY" else 1 - (bps / 10000)
    return max(1, int(ref_price * factor))


async def _wait_until_market_open(config: LiveTradingConfig) -> None:
    if not config.open_wait_enabled:
        return

    now = datetime.now()
    hh, mm = config.market_open_hhmm.split(":")
    target = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
    if now >= target:
        return

    wait_seconds = int((target - now).total_seconds()) + max(0, config.market_open_grace_seconds)
    if wait_seconds > 0:
        print(f"개장 대기: {wait_seconds}초")
        await asyncio.sleep(wait_seconds)


def _append_report_rows(report_rows: list[dict], checks: list, round_name: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for check in checks:
        report_rows.append(
            {
                "timestamp": ts,
                "round": round_name,
                "ticker": check.submitted.ticker,
                "side": check.submitted.side,
                "order_no": check.submitted.order_no,
                "order_price": check.submitted.price,
                "requested_qty": check.submitted.requested_qty,
                "pending_qty": check.pending_qty,
                "is_filled": check.is_filled,
                "return_code": check.submitted.raw_response.get("return_code"),
            }
        )


def _save_daily_report(config: LiveTradingConfig, trading_date: str, report_rows: list[dict]) -> None:
    if not config.save_daily_report:
        return
    if not report_rows:
        return

    os.makedirs(config.report_dir, exist_ok=True)
    file_name = f"fills_{trading_date.replace('-', '')}.csv"
    path = os.path.join(config.report_dir, file_name)
    pd.DataFrame(report_rows).to_csv(path, index=False, encoding="utf-8-sig")
    print(f"일일 체결 리포트 저장: {path}")


async def run_once(signal_date: str | None = None) -> None:
    config = LiveTradingConfig.from_env()
    missing = config.validate()
    if missing:
        raise RuntimeError(
            "필수 환경변수가 비어 있습니다: "
            + ", ".join(missing)
            + " (.env 로드 또는 셸 export 상태를 확인하세요)"
        )

    print(f"[CONFIG] mode={config.mode}, account_no={'set' if bool(config.account_no) else 'empty'}")
    signal_date = signal_date or datetime.now().strftime("%Y-%m-%d")

    strategy_config = StrategyConfig(
        investment_ratio=config.investment_ratio,
        num_stocks=config.num_stocks,
        rebalance_months=config.rebalance_months,
        strategy_mode=config.strategy_mode,
        mixed_filter_profile=config.mixed_filter_profile,
        kosdaq_target_ratio=None,
        momentum_enabled=config.momentum_enabled,
        momentum_months=config.momentum_months,
        momentum_weight=config.momentum_weight,
        momentum_filter_enabled=config.momentum_filter_enabled,
        large_cap_min_mcap=config.large_cap_min_mcap,
    )
    cost_config = CostConfig(
        commission_fee_rate=config.commission_fee_rate,
        tax_rate=config.tax_rate,
    )
    signal_engine = LiveSignalEngine(strategy_config)

    signal = build_rebalance_signal(signal_engine, signal_date)
    if signal.selected.empty:
        print(f"[{signal.trading_date}] 선정 종목 없음")
        return

    async with KiwoomBrokerAdapter(config) as broker:
        await _wait_until_market_open(config)

        snapshot = await broker.get_account_snapshot()
        print(f"[DEBUG] 계정 스냅샷: 보유={snapshot.holdings}, 현금={snapshot.cash:,.0f}원")
        
        intents = build_order_intents(
            selected=signal.selected,
            holdings=snapshot.holdings,
            cash=snapshot.cash,
            investment_ratio=config.investment_ratio,
            commission_fee_rate=cost_config.commission_fee_rate,
        )

        if not intents:
            print(f"[DEBUG] 선정({len(signal.selected)}개)되었으나 주문 의도 생성 실패")
            total_asset = snapshot.cash + sum(
                snapshot.holdings.get(row.ticker, 0) * float(row.close)
                for row in signal.selected.itertuples(index=False)
            )
            invest_amount = total_asset * config.investment_ratio
            per_stock_amount = invest_amount / len(signal.selected)
            print(f"[DEBUG] 총자산={total_asset:,.0f}원, 투자금={invest_amount:,.0f}원, 주식당={per_stock_amount:,.0f}원")
            print(f"[{signal.trading_date}] 주문 대상 없음")
            return

        print(f"[{signal.trading_date}] 주문 생성: {len(intents)}건")
        submitted_orders = []
        report_rows: list[dict] = []
        for intent in intents:
            limit_price = _limit_price(intent.side, intent.reference_price, config.order_price_offset_bps)
            submitted = await broker.submit_order(
                ticker=intent.ticker,
                side=intent.side,
                quantity=intent.quantity,
                price=limit_price,
            )
            submitted_orders.append(submitted)
            print(
                f"order ticker={intent.ticker} side={intent.side} qty={intent.quantity} "
                f"price={limit_price} order_no={submitted.order_no} return_code={submitted.raw_response.get('return_code')}"
            )

        current_orders = submitted_orders
        for round_idx in range(1, max(0, config.max_retry_rounds) + 1):
            print(f"미체결 확인 대기: {config.order_timeout_minutes}분 (round={round_idx})")
            await broker.wait_for_fill_window(config.order_timeout_minutes)

            retried_orders, checks = await broker.run_retry_cycle(
                submitted_orders=current_orders,
            )

            _append_report_rows(report_rows, checks, round_name=f"retry_check_{round_idx}")

            filled = sum(1 for c in checks if c.is_filled)
            pending = len(checks) - filled
            print(f"{round_idx}차 체결 결과: filled={filled}, pending={pending}")

            if pending == 0:
                current_orders = []
                break

            for check in checks:
                if check.is_filled:
                    continue
                print(
                    f"retry ticker={check.submitted.ticker} side={check.submitted.side} "
                    f"pending_qty={check.pending_qty}"
                )

            if retried_orders:
                print(f"{round_idx}차 재주문 전송: {len(retried_orders)}건")

            current_orders = retried_orders

        final_checks = []
        if current_orders:
            print(f"최종 체결 확인 대기: {config.order_timeout_minutes}분")
            await broker.wait_for_fill_window(config.order_timeout_minutes)
            final_checks = await broker.check_orders(current_orders)
            _append_report_rows(report_rows, final_checks, round_name="final_check")

            final_filled = sum(1 for c in final_checks if c.is_filled)
            final_pending = len(final_checks) - final_filled
            print(f"최종 체결 확인: filled={final_filled}, pending={final_pending}")

        _save_daily_report(config, signal.trading_date, report_rows)


if __name__ == "__main__":
    asyncio.run(run_once())
