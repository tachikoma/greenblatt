from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd


Side = Literal["BUY", "SELL"]


@dataclass(slots=True)
class OrderIntent:
    ticker: str
    side: Side
    quantity: int
    reference_price: float
    current_quantity: int
    target_quantity: int
    reason: str


def build_order_intents(
    selected: pd.DataFrame,
    holdings: dict[str, int],
    cash: float,
    investment_ratio: float,
    commission_fee_rate: float,
    existing_positions_policy: str = "sell",
    holding_prices: dict[str, float] | None = None,
) -> list[OrderIntent]:
    """대끼 보유수량 기반 주문 의도를 계산한다."""
    if selected.empty:
        return []

    _hold_policy = existing_positions_policy == "hold"
    selected_price_map = {row.ticker: float(row.close) for row in selected.itertuples(index=False)}

    if _hold_policy:
        # hold 정책: 미선정 보유 종목은 유지 → selected 종목 보유분만 투자 풀에 포함
        total_asset = cash + sum(
            holdings.get(t, 0) * p for t, p in selected_price_map.items()
        )
    else:
        # sell 정책: 미선정 보유 종목도 매도 후 재투자 → 전체 보유 평가금액 포함
        # selected 가격이 API 데이터보다 신뢰도가 높으므로 selected를 우선 적용
        all_prices = {**(holding_prices or {}), **selected_price_map}
        total_asset = cash + sum(qty * all_prices.get(t, 0.0) for t, qty in holdings.items())
    invest_amount = total_asset * investment_ratio
    per_stock_amount = invest_amount / len(selected)

    target_shares: dict[str, int] = {}
    for row in selected.itertuples(index=False):
        price = float(row.close)
        if price <= 0:
            continue
        target_shares[row.ticker] = int(per_stock_amount / (price * (1 + commission_fee_rate)))

    # 잔액 순환 재배분: int() 절사로 사장된 예산을 랭킹 순서대로 1주씩 추가 배분
    _remaining = invest_amount - sum(
        target_shares.get(row.ticker, 0) * float(row.close) * (1 + commission_fee_rate)
        for row in selected.itertuples(index=False)
        if float(row.close) > 0
    )
    _changed = True
    while _changed:
        _changed = False
        for row in selected.itertuples(index=False):
            if row.ticker not in target_shares:
                continue
            _cost = float(row.close) * (1 + commission_fee_rate)
            if _remaining >= _cost:
                target_shares[row.ticker] += 1
                _remaining -= _cost
                _changed = True

    intents: list[OrderIntent] = []

    selected_tickers = set(target_shares.keys())
    for ticker, current_qty in holdings.items():
        if ticker not in selected_tickers and current_qty > 0:
            # 정책이 기존 포지션 유지를 요구하면 보유 종목 매도 건을 건너뜁니다
            if _hold_policy:
                continue
            ref_price = float(selected[selected["ticker"] == ticker]["close"].iloc[0]) if (selected["ticker"] == ticker).any() else 0.0
            intents.append(
                OrderIntent(
                    ticker=ticker,
                    side="SELL",
                    quantity=current_qty,
                    reference_price=ref_price,
                    current_quantity=current_qty,
                    target_quantity=0,
                    reason="not_selected",
                )
            )

    for row in selected.itertuples(index=False):
        ticker = row.ticker
        ref_price = float(row.close)
        target_qty = target_shares.get(ticker, 0)
        current_qty = int(holdings.get(ticker, 0))
        delta = target_qty - current_qty
        if delta > 0:
            intents.append(
                OrderIntent(
                    ticker=ticker,
                    side="BUY",
                    quantity=delta,
                    reference_price=ref_price,
                    current_quantity=current_qty,
                    target_quantity=target_qty,
                    reason="rebalance_to_target",
                )
            )
        elif delta < 0:
            # hold 정책이면 보유 초과분 매도 주문을 생성하지 않습니다
            if _hold_policy:
                continue
            intents.append(
                OrderIntent(
                    ticker=ticker,
                    side="SELL",
                    quantity=abs(delta),
                    reference_price=ref_price,
                    current_quantity=current_qty,
                    target_quantity=target_qty,
                    reason="rebalance_to_target",
                )
            )

    return [i for i in intents if i.quantity > 0]


def select_capital_constrained_stocks(
    selected: pd.DataFrame,
    holdings: dict[str, int],
    cash: float,
    investment_ratio: float,
    commission_fee_rate: float,
    max_stocks: int,
    min_stocks: int = 1,
    slippage_rate: float = 0.0,
    holding_prices: dict[str, float] | None = None,
    existing_positions_policy: str = "sell",
) -> tuple[pd.DataFrame, dict[str, float | int | bool]]:
    """자본 제약(최소 1주 매수 가능)을 만족하는 최종 선정 종목 수를 찾는다.

    입력 데이터프레임의 순서(랭킹 순서)를 유지하며, 최대 종목 수에서 시작해
    모든 종목을 1주 이상 매수 가능한 가장 큰 k를 탐색한다.
    """
    if selected.empty:
        return selected, {
            "applied": True,
            "selected_before": 0,
            "selected_after": 0,
            "k_chosen": 0,
            "invest_amount": 0.0,
            "per_stock_amount": 0.0,
        }

    # 종목코드/가격이 유효한 행만 남깁니다.
    frame = selected.copy()
    frame = frame[frame["ticker"].notna()]
    frame = frame[pd.to_numeric(frame["close"], errors="coerce") > 0]
    if frame.empty:
        return frame, {
            "applied": True,
            "selected_before": int(len(selected)),
            "selected_after": 0,
            "k_chosen": 0,
            "invest_amount": 0.0,
            "per_stock_amount": 0.0,
        }

    max_k = max(1, min(int(max_stocks), len(frame)))
    min_k = max(1, min(int(min_stocks), max_k))

    _hold_policy = existing_positions_policy == "hold"
    selected_price_map = {row.ticker: float(row.close) for row in frame.itertuples(index=False)}

    if _hold_policy:
        # hold 정책: 미선정 보유 종목 제외 → frame 내 보유분만 투자 풀에 포함
        total_asset = cash + sum(
            holdings.get(t, 0) * p for t, p in selected_price_map.items()
        )
    else:
        # sell 정책: 전체 보유 평가금액 포함
        all_prices = {**(holding_prices or {}), **selected_price_map}
        total_asset = cash + sum(qty * all_prices.get(t, 0.0) for t, qty in holdings.items())
    invest_amount = float(total_asset) * float(investment_ratio)

    def _is_feasible(candidate: pd.DataFrame, per_stock_amount: float) -> bool:
        denom_multiplier = (1 + float(commission_fee_rate)) * (1 + float(slippage_rate))
        for row in candidate.itertuples(index=False):
            price = float(row.close)
            target_qty = int(per_stock_amount / (price * denom_multiplier))
            if target_qty < 1:
                return False
        return True

    chosen = frame.head(min_k)
    chosen_k = len(chosen)
    chosen_per_stock_amount = invest_amount / chosen_k if chosen_k > 0 else 0.0

    for k in range(max_k, min_k - 1, -1):
        per_stock_amount = invest_amount / k
        candidate = frame.head(k)
        if _is_feasible(candidate, per_stock_amount):
            chosen = candidate
            chosen_k = k
            chosen_per_stock_amount = per_stock_amount
            break

    return chosen, {
        "applied": True,
        "selected_before": int(len(frame)),
        "selected_after": int(len(chosen)),
        "k_chosen": int(chosen_k),
        "invest_amount": float(invest_amount),
        "per_stock_amount": float(chosen_per_stock_amount),
    }
