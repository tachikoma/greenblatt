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
) -> list[OrderIntent]:
    """목표 보유수량 기반 주문 의도를 계산한다."""
    if selected.empty:
        return []

    total_asset = cash + sum(
        holdings.get(row.ticker, 0) * float(row.close)
        for row in selected.itertuples(index=False)
    )
    invest_amount = total_asset * investment_ratio
    per_stock_amount = invest_amount / len(selected)

    target_shares: dict[str, int] = {}
    for row in selected.itertuples(index=False):
        price = float(row.close)
        if price <= 0:
            continue
        target_shares[row.ticker] = int(per_stock_amount / (price * (1 + commission_fee_rate)))

    intents: list[OrderIntent] = []

    selected_tickers = set(target_shares.keys())
    for ticker, current_qty in holdings.items():
        if ticker not in selected_tickers and current_qty > 0:
            # if policy requests respecting existing positions, skip selling holdings
            if existing_positions_policy in {"respect_existing", "keep", "adopt", "retain"}:
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
            # If policy respects existing positions, do not issue sell orders to shrink positions
            if existing_positions_policy in {"respect_existing", "keep", "adopt", "retain"}:
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
