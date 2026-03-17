from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
import fcntl
import json
import os
from typing import Any, Literal

import pandas as pd

from live_trading.config import LiveTradingConfig
from live_trading.execution import build_order_intents
from live_trading.kiwoom_adapter import KiwoomBrokerAdapter, KiwoomAPIError
from live_trading.kiwoom_http_patch import apply_kiwoom_client_session_patch
from live_trading.strategy_bridge import build_rebalance_signal
from live_trading.strategy_config import CostConfig, StrategyConfig
from live_trading.strategy_engine import LiveSignalEngine


ExecutionState = Literal[
    "STARTED",
    "FAILED_BEFORE_ORDER",
    "ORDER_SUBMITTED",
    "PARTIAL_PENDING",
    "SUCCESS",
    "SKIPPED",
]

ExecutionAction = Literal["full_rebalance", "reconcile_only", "skip"]


def _map_legacy_result_to_execution_state(last_result: str) -> ExecutionState | None:
    normalized = (last_result or "").strip().lower()
    mapping: dict[str, ExecutionState] = {
        "executed": "SUCCESS",
        "empty_selection": "SKIPPED",
        "no_intent": "SKIPPED",
    }
    return mapping.get(normalized)


def _resolve_execution_state_from_state(state: dict[str, Any]) -> ExecutionState | None:
    raw = state.get("execution_state")
    if isinstance(raw, str):
        normalized = raw.strip().upper()
        if normalized in {
            "STARTED",
            "FAILED_BEFORE_ORDER",
            "ORDER_SUBMITTED",
            "PARTIAL_PENDING",
            "SUCCESS",
            "SKIPPED",
        }:
            return normalized  # type: ignore[return-value]

    legacy = state.get("last_result")
    if isinstance(legacy, str):
        return _map_legacy_result_to_execution_state(legacy)

    return None


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


def _print_selected_debug(selected: pd.DataFrame, max_rows: int) -> None:
    columns = [
        "ticker",
        "name",
        "market",
        "close",
        "per",
        "pbr",
        "roe",
        "score",
    ]
    available_cols = [col for col in columns if col in selected.columns]
    if not available_cols:
        available_cols = list(selected.columns)

    preview = selected[available_cols].copy()
    for col in preview.columns:
        if pd.api.types.is_numeric_dtype(preview[col]):
            preview[col] = preview[col].round(4)

    print(f"[DEBUG] 선정 종목 미리보기: total={len(selected)}")
    print(preview.head(max_rows).to_string(index=False))
    if len(preview) > max_rows:
        print(f"[DEBUG] 선정 종목 출력 생략: {len(preview) - max_rows}개")


def _print_intents_debug(intents: list, max_rows: int, order_price_offset_bps: int) -> None:
    if not intents:
        print("[DEBUG] 주문 의도 없음")
        return

    rows = []
    for intent in intents:
        rows.append(
            {
                "ticker": intent.ticker,
                "side": intent.side,
                "qty": intent.quantity,
                "current_qty": intent.current_quantity,
                "target_qty": intent.target_quantity,
                "ref_price": round(float(intent.reference_price), 2),
                "limit_price": _limit_price(intent.side, intent.reference_price, order_price_offset_bps),
                "reason": intent.reason,
            }
        )

    frame = pd.DataFrame(rows)
    buy_count = int((frame["side"] == "BUY").sum())
    sell_count = int((frame["side"] == "SELL").sum())
    print(f"[DEBUG] 주문 의도 미리보기: total={len(frame)} (BUY={buy_count}, SELL={sell_count})")
    print(frame.head(max_rows).to_string(index=False))
    if len(frame) > max_rows:
        print(f"[DEBUG] 주문 의도 출력 생략: {len(frame) - max_rows}개")


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


def _month_index(dt: datetime) -> int:
    return dt.year * 12 + (dt.month - 1)


def _period_key(trading_date: str, rebalance_months: int) -> str:
    if rebalance_months <= 0:
        raise ValueError("rebalance_months must be positive")

    dt = datetime.strptime(trading_date, "%Y-%m-%d")
    date_idx = _month_index(dt)
    period_idx = date_idx // rebalance_months
    period_start_idx = period_idx * rebalance_months
    start_year = period_start_idx // 12
    start_month = (period_start_idx % 12) + 1
    return f"{start_year:04d}-{start_month:02d}/{rebalance_months}m"


def _load_run_state(path: str) -> dict[str, Any]:
    try:
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception as exc:
        print(f"[WARN] 상태 파일 로드 실패: {exc}")
    return {}


def _save_run_state(path: str, state: dict[str, Any]) -> None:
    parent_dir = os.path.dirname(path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _acquire_run_lock(lock_path: str):
    parent_dir = os.path.dirname(lock_path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)
    lock_file = open(lock_path, "w", encoding="utf-8")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_file.close()
        raise RuntimeError(f"이미 실행 중입니다. lock={lock_path}")
    return lock_file


def _decide_execution_action(
    config: LiveTradingConfig,
    *,
    period_key: str,
    force: bool,
    dry_run: bool,
) -> tuple[ExecutionAction, dict[str, Any]]:
    state = _load_run_state(config.run_state_path)

    if dry_run:
        print(f"[DRY RUN] 주기 가드 상태와 무관하게 시뮬레이션 실행: period={period_key}")
        return "full_rebalance", state

    if not config.rebalance_guard_enabled:
        return "full_rebalance", state

    if force:
        print(f"[GUARD] force=true, 주기 가드 우회: period={period_key}")
        return "full_rebalance", state

    last_period_key = str(state.get("last_period_key") or "")
    if last_period_key != period_key:
        return "full_rebalance", state

    execution_state = _resolve_execution_state_from_state(state)
    if execution_state and state.get("execution_state") != execution_state:
        state["execution_state"] = execution_state
        _save_run_state(config.run_state_path, state)
        print(f"[GUARD] 구버전 상태 자동 마이그레이션: execution_state={execution_state}")

    if execution_state is None:
        print("[GUARD] execution_state를 판단할 수 없어 안전 모드로 스킵합니다.")
        return "skip", state

    if execution_state in {"SUCCESS", "SKIPPED"}:
        return "skip", state

    if execution_state in {"ORDER_SUBMITTED", "PARTIAL_PENDING"}:
        return "reconcile_only", state

    if execution_state == "FAILED_BEFORE_ORDER":
        return "full_rebalance", state

    return "skip", state


def _persist_execution_state(
    config: LiveTradingConfig,
    *,
    period_key: str,
    signal_date: str,
    trading_date: str,
    execution_state: ExecutionState,
    note: str | None = None,
) -> None:
    if not config.rebalance_guard_enabled:
        return

    current = _load_run_state(config.run_state_path)
    current.update(
        {
            "last_period_key": period_key,
            "last_signal_date": signal_date,
            "last_trading_date": trading_date,
            "execution_state": execution_state,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )
    if note:
        current["note"] = note
    _save_run_state(config.run_state_path, current)
    print(f"[GUARD] 상태 저장: period={period_key}, execution_state={execution_state}")


async def run_once(signal_date: str | None = None, *, force: bool = False, dry_run: bool = False) -> None:
    config = LiveTradingConfig.from_env()
    if dry_run:
        config.dry_run_enabled = True
    missing = config.validate()
    if missing:
        raise RuntimeError(
            "필수 환경변수가 비어 있습니다: "
            + ", ".join(missing)
            + " (.env 로드 또는 셸 export 상태를 확인하세요)"
        )

    apply_kiwoom_client_session_patch()

    print(
        f"[CONFIG] mode={config.mode}, account_no={'set' if bool(config.account_no) else 'empty'}, "
        f"dry_run={config.dry_run_enabled}"
    )
    signal_date = signal_date or datetime.now().strftime("%Y-%m-%d")

    lock_file = _acquire_run_lock(config.run_lock_path)
    period_key = ""
    trading_date = signal_date
    state_written = False
    state_before_order: ExecutionState = "FAILED_BEFORE_ORDER"
    try:
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
            fundamental_source=config.fundamental_source,
        )
        cost_config = CostConfig(
            commission_fee_rate=config.commission_fee_rate,
            tax_rate=config.tax_rate,
        )
        signal_engine = LiveSignalEngine(strategy_config)

        signal = build_rebalance_signal(signal_engine, signal_date)
        trading_date = signal.trading_date
        period_key = _period_key(signal.trading_date, config.rebalance_months)

        action, current_state = _decide_execution_action(
            config,
            period_key=period_key,
            force=force,
            dry_run=config.dry_run_enabled,
        )

        if action == "skip":
            print(
                f"[GUARD] 실행 스킵: trading_date={signal.trading_date}, period={period_key}, "
                f"execution_state={current_state.get('execution_state')}, state={config.run_state_path}"
            )
            return

        if action == "reconcile_only":
            print(
                f"[GUARD] 동일 주기 미완료 상태 감지. 신규 주문 없이 reconcile_only로 종료: "
                f"period={period_key}, execution_state={current_state.get('execution_state')}"
            )
            _persist_execution_state(
                config,
                period_key=period_key,
                signal_date=signal_date,
                trading_date=signal.trading_date,
                execution_state="PARTIAL_PENDING",
                note="reconcile_only: previous execution indicates submitted/pending orders",
            )
            state_written = True
            return

        if not config.dry_run_enabled:
            _persist_execution_state(
                config,
                period_key=period_key,
                signal_date=signal_date,
                trading_date=signal.trading_date,
                execution_state="STARTED",
            )

        if signal.selected.empty:
            print(f"[{signal.trading_date}] 선정 종목 없음")
            if not config.dry_run_enabled:
                _persist_execution_state(
                    config,
                    period_key=period_key,
                    signal_date=signal_date,
                    trading_date=signal.trading_date,
                    execution_state="SKIPPED",
                    note="empty_selection",
                )
            state_written = True
            return

        async with KiwoomBrokerAdapter(config) as broker:
            if config.dry_run_enabled:
                print("[DRY RUN] 개장 대기/실주문/체결확인/상태저장을 수행하지 않습니다.")
            else:
                await _wait_until_market_open(config)

            snapshot = await broker.get_account_snapshot()
            print(f"[DEBUG] 계정 스냅샷: 보유={snapshot.holdings}, 현금={snapshot.cash:,.0f}원")

            if config.debug_signal_enabled:
                _print_selected_debug(signal.selected, max(1, config.debug_max_rows))

            intents = build_order_intents(
                selected=signal.selected,
                holdings=snapshot.holdings,
                cash=snapshot.cash,
                investment_ratio=config.investment_ratio,
                commission_fee_rate=cost_config.commission_fee_rate,
            )

            if config.debug_signal_enabled:
                _print_intents_debug(
                    intents,
                    max_rows=max(1, config.debug_max_rows),
                    order_price_offset_bps=config.order_price_offset_bps,
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
                if not config.dry_run_enabled:
                    _persist_execution_state(
                        config,
                        period_key=period_key,
                        signal_date=signal_date,
                        trading_date=signal.trading_date,
                        execution_state="SKIPPED",
                        note="no_intent",
                    )
                state_written = True
                return

            print(f"[{signal.trading_date}] 주문 생성: {len(intents)}건")

            if config.dry_run_enabled:
                for intent in intents:
                    limit_price = _limit_price(intent.side, intent.reference_price, config.order_price_offset_bps)
                    print(
                        f"[DRY RUN] order ticker={intent.ticker} side={intent.side} qty={intent.quantity} "
                        f"price={limit_price} reason={intent.reason}"
                    )
                print(f"[DRY RUN] 시뮬레이션 완료: planned_orders={len(intents)}")
                state_written = True
                return

            submitted_orders = []
            report_rows: list[dict] = []
            for intent in intents:
                limit_price = _limit_price(intent.side, intent.reference_price, config.order_price_offset_bps)

                # 보조 가격 보완: 계산된 limit_price가 0인 경우 시세 조회로 대체
                if limit_price <= 0:
                    try:
                        quote = await broker.get_best_quote(intent.ticker)
                        fallback = quote.ask1 if intent.side == "BUY" else quote.bid1
                        if not fallback or fallback <= 0:
                            # 시세로 보완 불가
                            print(f"[WARN] 시세로 보완 불가(호가 없음): ticker={intent.ticker} side={intent.side}; 주문 스킵")
                            # 기록 후 스킵
                            report_rows.append(
                                {
                                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    "round": "pre_submit_skip",
                                    "ticker": intent.ticker,
                                    "side": intent.side,
                                    "order_no": "",
                                    "order_price": None,
                                    "requested_qty": intent.quantity,
                                    "pending_qty": intent.quantity,
                                    "is_filled": False,
                                    "return_code": None,
                                    "note": "missing_price",
                                }
                            )
                            print(f"[ORDER] skip ticker={intent.ticker} side={intent.side} qty={intent.quantity} reason=missing_price")
                            continue

                        print(f"[ORDER] limit_price==0 보완: ticker={intent.ticker} side={intent.side} fallback_price={fallback}")
                        limit_price = fallback
                    except Exception as exc:
                        print(f"[WARN] 시세 조회로 가격 보완 실패: {exc}; ticker={intent.ticker}; 주문 스킵")
                        report_rows.append(
                            {
                                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "round": "pre_submit_skip",
                                "ticker": intent.ticker,
                                "side": intent.side,
                                "order_no": "",
                                "order_price": None,
                                "requested_qty": intent.quantity,
                                "pending_qty": intent.quantity,
                                "is_filled": False,
                                "return_code": None,
                                "note": "quote_fetch_error",
                            }
                        )
                        print(f"[ORDER] skip ticker={intent.ticker} side={intent.side} qty={intent.quantity} reason=quote_fetch_error")
                        continue

                try:
                    submitted = await broker.submit_order(
                        ticker=intent.ticker,
                        side=intent.side,
                        quantity=intent.quantity,
                        price=limit_price,
                    )
                except KiwoomAPIError as exc:
                    # Log full API response for easier debugging / reproduction
                    try:
                        dumped = json.dumps(exc.body or {}, ensure_ascii=False)
                        print(f"[ORDER][KIWOOM_ERROR] api_id={exc.api_id} endpoint={exc.endpoint} body={dumped}")
                    except Exception:
                        print(f"[ORDER][KIWOOM_ERROR] api_id={exc.api_id} endpoint={exc.endpoint} body={exc.body}")

                    # If mock server signals 모의투자 장종료 (example return_code==20), skip order safely
                    try:
                        rc = int(exc.return_code) if exc.return_code is not None else None
                    except Exception:
                        rc = None

                    if rc == 20:
                        print(f"[ORDER] 모의투자 장종료 감지: ticker={intent.ticker} side={intent.side} return_code={rc}; 스킵 처리")
                        report_rows.append(
                            {
                                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "round": "submit_skip",
                                "ticker": intent.ticker,
                                "side": intent.side,
                                "order_no": "",
                                "order_price": limit_price,
                                "requested_qty": intent.quantity,
                                "pending_qty": intent.quantity,
                                "is_filled": False,
                                "return_code": rc,
                                "note": "mock_market_closed",
                            }
                        )
                        # move to next intent
                        continue

                    # For other Kiwoom errors, consult configured fallback codes and retry as market order if matched
                    fallback_codes = tuple(config.fallback_to_market_return_codes or ())
                    should_fallback = False
                    try:
                        if broker._kerr_matches_codes(exc, fallback_codes):
                            should_fallback = True
                    except Exception:
                        # fallback to simple heuristic matching on message/code
                        msg = str(exc.return_msg or "")
                        for c in fallback_codes:
                            try:
                                if rc == int(c):
                                    should_fallback = True
                                    break
                            except Exception:
                                pass
                            if f"RC{c}" in msg or str(c) in msg:
                                should_fallback = True
                                break

                    if should_fallback:
                        submitted = await broker.submit_order(
                            ticker=intent.ticker,
                            side=intent.side,
                            quantity=intent.quantity,
                            price=limit_price,
                            order_type="3",
                        )
                    else:
                        # re-raise to let outer handler (and crash) surface unexpected errors
                        raise
                submitted_orders.append(submitted)
                print(
                    f"order ticker={intent.ticker} side={intent.side} qty={intent.quantity} "
                    f"price={limit_price} order_no={submitted.order_no} return_code={submitted.raw_response.get('return_code')}"
                )

                # 주문 간 짧은 지연을 두어 레이트리밋 완화
                try:
                    await asyncio.sleep(max(0.0, float(config.order_submit_delay_seconds)))
                except Exception:
                    pass

            if not submitted_orders:
                print(f"[GUARD] 모든 주문이 스킵되었습니다. submitted_orders=0")
                if not config.dry_run_enabled:
                    _persist_execution_state(
                        config,
                        period_key=period_key,
                        signal_date=signal_date,
                        trading_date=signal.trading_date,
                        execution_state="SKIPPED",
                        note="all_orders_skipped_missing_price_or_quote_error",
                    )
                state_written = True
                _save_daily_report(config, signal.trading_date, report_rows)
                return

            _persist_execution_state(
                config,
                period_key=period_key,
                signal_date=signal_date,
                trading_date=signal.trading_date,
                execution_state="ORDER_SUBMITTED",
                note=f"submitted_orders={len(submitted_orders)}",
            )
            state_before_order = "PARTIAL_PENDING"

            current_orders = submitted_orders
            final_pending = 0
            for round_idx in range(1, max(0, config.max_retry_rounds) + 1):
                print(f"미체결 확인 대기: {config.order_timeout_minutes}분 (round={round_idx})")
                await broker.wait_for_fill_window(config.order_timeout_minutes)

                retried_orders, checks = await broker.run_retry_cycle(
                    submitted_orders=current_orders,
                )

                _append_report_rows(report_rows, checks, round_name=f"retry_check_{round_idx}")

                filled = sum(1 for c in checks if c.is_filled)
                pending = len(checks) - filled
                final_pending = pending
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
            if final_pending > 0:
                _persist_execution_state(
                    config,
                    period_key=period_key,
                    signal_date=signal_date,
                    trading_date=signal.trading_date,
                    execution_state="PARTIAL_PENDING",
                    note=f"pending_qty_exists_after_final_check={final_pending}",
                )
            else:
                _persist_execution_state(
                    config,
                    period_key=period_key,
                    signal_date=signal_date,
                    trading_date=signal.trading_date,
                    execution_state="SUCCESS",
                )
            state_written = True
    except Exception as exc:
        if config.rebalance_guard_enabled and period_key and not state_written:
            _persist_execution_state(
                config,
                period_key=period_key,
                signal_date=signal_date,
                trading_date=trading_date,
                execution_state=state_before_order,
                note=f"exception={type(exc).__name__}: {exc}",
            )
        raise
    finally:
        lock_file.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Live rebalance runner")
    parser.add_argument("--signal-date", type=str, default=None, help="신호 기준일 (YYYY-MM-DD)")
    parser.add_argument("--force", action="store_true", help="이미 실행한 주기라도 강제로 실행")
    parser.add_argument("--dry-run", action="store_true", help="주문 없이 시뮬레이션만 실행")
    args = parser.parse_args()

    asyncio.run(run_once(signal_date=args.signal_date, force=args.force, dry_run=args.dry_run))
