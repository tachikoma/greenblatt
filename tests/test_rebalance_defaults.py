import os
import importlib
import sys
from pathlib import Path

import pytest

# 테스트 실행 시 tests/가 작업 디렉터리로 설정되는 환경을 고려하여
# 프로젝트 루트(한 단계 위)를 PYTHONPATH에 추가합니다.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from defaults import DEFAULT_REBALANCE_MONTHS


def reload_module(module_name: str):
    if module_name in importlib.sys.modules:
        importlib.reload(importlib.import_module(module_name))
    else:
        importlib.import_module(module_name)


def test_backtest_uses_env(monkeypatch):
    monkeypatch.delenv('REBALANCE_MONTHS', raising=False)
    monkeypatch.setenv('REBALANCE_MONTHS', '6')
    # import here to ensure environment is read during object init
    from greenblatt_korea_full_backtest import KoreaStockBacktest

    bt = KoreaStockBacktest(rebalance_months=None)
    assert bt.rebalance_months == 6


def test_backtest_uses_default_when_no_env(monkeypatch):
    monkeypatch.delenv('REBALANCE_MONTHS', raising=False)
    monkeypatch.delenv('LIVE_REBALANCE_MONTHS', raising=False)
    from greenblatt_korea_full_backtest import KoreaStockBacktest

    bt = KoreaStockBacktest(rebalance_months=None)
    assert bt.rebalance_months == DEFAULT_REBALANCE_MONTHS


def test_backtest_constructor_overrides_env(monkeypatch):
    monkeypatch.setenv('REBALANCE_MONTHS', '6')
    from greenblatt_korea_full_backtest import KoreaStockBacktest

    bt = KoreaStockBacktest(rebalance_months=12)
    assert bt.rebalance_months == 12


def test_live_config_reads_env(monkeypatch):
    monkeypatch.setenv('REBALANCE_MONTHS', '9')
    from live_trading.config import LiveTradingConfig

    cfg = LiveTradingConfig.from_env()
    assert cfg.rebalance_months == 9


def test_live_config_default_when_no_env(monkeypatch):
    monkeypatch.delenv('REBALANCE_MONTHS', raising=False)
    monkeypatch.delenv('LIVE_REBALANCE_MONTHS', raising=False)
    from live_trading.config import LiveTradingConfig

    cfg = LiveTradingConfig.from_env()
    assert cfg.rebalance_months == DEFAULT_REBALANCE_MONTHS
