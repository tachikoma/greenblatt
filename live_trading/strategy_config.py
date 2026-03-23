from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class StrategyConfig:
    investment_ratio: float = 0.95
    num_stocks: int = 40
    rebalance_days: int | None = None
    rebalance_months: int = 3
    strategy_mode: str = "mixed"
    mixed_filter_profile: str = "large_cap"
    kosdaq_target_ratio: float | None = None
    momentum_enabled: bool = True
    momentum_months: int = 3
    momentum_weight: float = 0.60
    momentum_filter_enabled: bool = True
    large_cap_min_mcap: float | None = None
    fundamental_source: str = "kiwoom"


@dataclass(slots=True)
class CostConfig:
    commission_fee_rate: float = 0.0015
    tax_rate: float = 0.002
