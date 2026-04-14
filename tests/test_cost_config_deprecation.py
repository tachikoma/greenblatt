from pathlib import Path
import sys

import pytest

# tests/ 실행 기준에서 프로젝트 루트를 import path에 추가
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def test_cost_config_removed_from_strategy_config():
    import live_trading.strategy_config as sc

    assert not hasattr(sc, "CostConfig")


def test_strategy_config_keeps_cost_fields():
    from live_trading.strategy_config import StrategyConfig

    cfg = StrategyConfig(commission_fee_rate=0.003, tax_rate=0.004)
    assert cfg.commission_fee_rate == 0.003
    assert cfg.tax_rate == 0.004


def test_live_trading_package_no_longer_exposes_cost_config():
    import live_trading

    # CostConfig 완전 제거 단계: 공개 API/속성 모두 미노출
    assert "CostConfig" not in live_trading.__all__
    with pytest.raises(AttributeError):
        _ = live_trading.CostConfig
