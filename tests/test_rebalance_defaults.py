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
    # dotenv가 로드되더라도 테스트 값을 우선 유지하도록 빈 문자열로 고정
    monkeypatch.setenv('REBALANCE_MONTHS', '')
    monkeypatch.setenv('LIVE_REBALANCE_MONTHS', '')
    monkeypatch.delenv('LIVE_MARKET_TIMING_ENABLED', raising=False)
    monkeypatch.delenv('LIVE_MARKET_TIMING_MODE', raising=False)
    monkeypatch.delenv('LIVE_MARKET_TIMING_MA_WINDOW', raising=False)
    monkeypatch.delenv('LIVE_MARKET_TIMING_MA_TREND_LOOKBACK', raising=False)
    monkeypatch.delenv('LIVE_MARKET_TIMING_RATIO_MULTIPLIER', raising=False)
    monkeypatch.delenv('LIVE_MARKET_TIMING_INDEX_CODE', raising=False)
    from live_trading.config import LiveTradingConfig

    cfg = LiveTradingConfig.from_env()
    assert cfg.rebalance_months == DEFAULT_REBALANCE_MONTHS
    assert cfg.market_timing_enabled is False
    assert cfg.market_timing_mode == 'below_ma_and_ma_falling'
    assert cfg.market_timing_ma_window == 200
    assert cfg.market_timing_ma_trend_lookback == 20
    assert cfg.market_timing_ratio_multiplier == 0.85
    assert cfg.market_timing_index_code == '1001'


def test_live_config_reads_market_timing_env(monkeypatch):
    monkeypatch.setenv('LIVE_MARKET_TIMING_ENABLED', 'true')
    monkeypatch.setenv('LIVE_MARKET_TIMING_MODE', 'below_ma')
    monkeypatch.setenv('LIVE_MARKET_TIMING_MA_WINDOW', '250')
    monkeypatch.setenv('LIVE_MARKET_TIMING_MA_TREND_LOOKBACK', '10')
    monkeypatch.setenv('LIVE_MARKET_TIMING_RATIO_MULTIPLIER', '0.6')
    monkeypatch.setenv('LIVE_MARKET_TIMING_INDEX_CODE', '1001')
    from live_trading.config import LiveTradingConfig

    cfg = LiveTradingConfig.from_env()
    assert cfg.market_timing_enabled is True
    assert cfg.market_timing_mode == 'below_ma'
    assert cfg.market_timing_ma_window == 250
    assert cfg.market_timing_ma_trend_lookback == 10
    assert cfg.market_timing_ratio_multiplier == 0.6
    assert cfg.market_timing_index_code == '1001'
