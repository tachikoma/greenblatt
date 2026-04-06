import os
import pytest

import greenblatt_korea_full_backtest as gbf
from greenblatt_korea_full_backtest import KoreaStockBacktest


class DummySelector:
    def __init__(self, *args, **kwargs):
        pass

    def nearest_trading_date(self, d):
        return d.replace('-', '')

    def previous_trading_date(self, d):
        return d.replace('-', '')

    def select_stocks(self, d):
        import pandas as pd
        return pd.DataFrame()

    def persist_caches(self):
        return None

    def get_market_tickers(self, d):
        return []


def _make_backtest_with_env(monkeypatch, env_vars: dict):
    # Clear relevant env keys first to avoid cross-test pollution
    for key in ['SELL_LOSERS_HOLD_DAYS', 'SELL_LOSERS_HOLD_REBALANCE_CYCLES', 'REBALANCE_DAYS', 'REBALANCE_MONTHS']:
        monkeypatch.delenv(key, raising=False)

    # set provided envs using monkeypatch for test isolation
    for k, v in env_vars.items():
        monkeypatch.setenv(k, str(v))

    # replace selector with dummy to avoid external API calls
    monkeypatch.setattr(gbf, 'KoreaStockSelector', DummySelector)

    return KoreaStockBacktest(start_date='2020-01-01', end_date='2020-12-31', sell_losers_enabled=True)


def test_hold_days_from_env_days(monkeypatch):
    b = _make_backtest_with_env(monkeypatch, {'SELL_LOSERS_HOLD_DAYS': '90'})
    assert int(b.sell_losers_hold_days) == 90


def test_hold_days_from_env_cycles_with_rebalance_days(monkeypatch):
    b = _make_backtest_with_env(monkeypatch, {'REBALANCE_DAYS': '7', 'SELL_LOSERS_HOLD_REBALANCE_CYCLES': '4'})
    assert int(b.sell_losers_hold_days) == 28


def test_sell_losers_executes_based_on_hold_days(monkeypatch):
    b = _make_backtest_with_env(monkeypatch, {'SELL_LOSERS_HOLD_DAYS': '10'})

    # 포트폴리오에 2020-01-01에 매수한 손실 포지션을 넣고 2020-01-12에 매도되어야 함
    b.portfolio = {
        '0001': {
            'ticker': '0001',
            'shares': 1,
            'buy_price': 100.0,
            'buy_date': '2020-01-01',
            'current_price': 90.0,
        }
    }
    b.cash = 0.0
    b.trade_history = []

    b.sell_losers('2020-01-12')

    assert '0001' not in b.portfolio
    assert any(t.get('action') == 'SELL_LOSS' and t.get('ticker') == '0001' for t in b.trade_history)
