"""변동성 타게팅(Volatility Targeting) 모듈

포트폴리오의 실현 변동성을 목표 변동성에 맞게 투자비율을 동적으로 조정한다.

공식:
    effective_ratio = min(base_ratio, base_ratio * (σ_target / σ_realized))

동작 방식:
- 실현 변동성(σ_realized)이 목표 변동성(σ_target)보다 낮으면: base_ratio 그대로 유지 (풀 투자)
- 실현 변동성이 목표보다 높으면: 비율을 낮춰 리스크 축소
- 데이터 부족/조회 실패 시: base_ratio 그대로 유지 (fail-open)
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(slots=True)
class VolTargetDecision:
    enabled: bool
    base_ratio: float
    effective_ratio: float
    sigma_target: float
    sigma_realized: float | None
    multiplier: float
    lookback_days: int
    data_points: int
    reason: str


def compute_vol_target_ratio(
    portfolio_history: list[dict],
    base_ratio: float,
    *,
    enabled: bool,
    sigma_target: float = 0.20,
    lookback_days: int = 20,
    min_ratio: float = 0.30,
) -> VolTargetDecision:
    """포트폴리오 히스토리 기반 변동성 타게팅 유효 투자비율을 계산한다.

    Parameters
    ----------
    portfolio_history:
        {'date': str, 'portfolio_value': float, ...} 딕셔너리 리스트.
        백테스트 `self.portfolio_history` 를 그대로 넘기면 된다.
    base_ratio:
        변동성 조건이 충족될 때 사용하는 최대 투자비율 (예: 0.95).
    enabled:
        False이면 즉시 base_ratio 반환 (기능 비활성화).
    sigma_target:
        연환산 목표 변동성 (예: 0.20 = 20%). 이 수준 이하면 풀 투자 유지.
    lookback_days:
        일별 수익률 계산에 사용할 거래일 수. 기본 20일(약 1개월).
    min_ratio:
        투자비율의 하한선. 극단적 하락 시에도 이 이하로 내려가지 않는다.
    """
    safe_base = float(max(0.0, min(1.0, base_ratio)))
    safe_sigma_target = float(max(0.01, sigma_target))
    safe_lookback = max(5, int(lookback_days))
    safe_min_ratio = float(max(0.0, min(safe_base, min_ratio)))

    if not enabled:
        return VolTargetDecision(
            enabled=False,
            base_ratio=safe_base,
            effective_ratio=safe_base,
            sigma_target=safe_sigma_target,
            sigma_realized=None,
            multiplier=1.0,
            lookback_days=safe_lookback,
            data_points=0,
            reason="disabled",
        )

    # 히스토리에서 일별 수익률 추출
    if not portfolio_history or len(portfolio_history) < safe_lookback + 1:
        return VolTargetDecision(
            enabled=True,
            base_ratio=safe_base,
            effective_ratio=safe_base,
            sigma_target=safe_sigma_target,
            sigma_realized=None,
            multiplier=1.0,
            lookback_days=safe_lookback,
            data_points=len(portfolio_history),
            reason="insufficient_history",
        )

    # 최근 lookback_days 기간의 일별 수익률
    recent = portfolio_history[-(safe_lookback + 1):]
    values = [r["portfolio_value"] for r in recent if r.get("portfolio_value") is not None]

    if len(values) < safe_lookback + 1:
        return VolTargetDecision(
            enabled=True,
            base_ratio=safe_base,
            effective_ratio=safe_base,
            sigma_target=safe_sigma_target,
            sigma_realized=None,
            multiplier=1.0,
            lookback_days=safe_lookback,
            data_points=len(values),
            reason="insufficient_history",
        )

    arr = np.array(values, dtype=float)
    daily_returns = arr[1:] / arr[:-1] - 1.0
    sigma_daily = float(np.std(daily_returns, ddof=1))
    sigma_realized = sigma_daily * np.sqrt(252)  # 연환산

    if sigma_realized <= 0:
        return VolTargetDecision(
            enabled=True,
            base_ratio=safe_base,
            effective_ratio=safe_base,
            sigma_target=safe_sigma_target,
            sigma_realized=float(sigma_realized),
            multiplier=1.0,
            lookback_days=safe_lookback,
            data_points=len(values) - 1,
            reason="zero_volatility",
        )

    # 목표 변동성 기반 투자비율 조정
    # sigma_realized <= sigma_target 이면 multiplier >= 1.0 → cap at 1.0 (풀 투자 유지)
    raw_multiplier = safe_sigma_target / sigma_realized
    multiplier = min(1.0, raw_multiplier)
    effective = max(safe_min_ratio, safe_base * multiplier)

    return VolTargetDecision(
        enabled=True,
        base_ratio=safe_base,
        effective_ratio=float(effective),
        sigma_target=safe_sigma_target,
        sigma_realized=float(sigma_realized),
        multiplier=float(multiplier),
        lookback_days=safe_lookback,
        data_points=len(values) - 1,
        reason="ok",
    )
