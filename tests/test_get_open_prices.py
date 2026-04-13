import importlib
import sys
from pathlib import Path
import pandas as pd

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import stock_selector
from stock_selector import KoreaStockSelector


def _reload_stock_selector():
    if 'stock_selector' in sys.modules:
        importlib.reload(sys.modules['stock_selector'])
    return sys.modules['stock_selector']


def test_get_open_prices_uses_market_ohlcv_and_fallback(monkeypatch):
    module = _reload_stock_selector()
    monkeypatch.setattr(module, 'LIBRARIES_AVAILABLE', True)

    kospi_df = pd.DataFrame(
        {
            '시가': [1000.0, 0.0],
            '고가': [1100.0, 0.0],
            '저가': [900.0, 0.0],
            '종가': [1050.0, 0.0],
            '거래량': [100, 0],
            '거래대금': [100000.0, 0.0],
            '등락률': [5.0, 0.0],
        },
        index=pd.Index(['000100', '000200'], name='티커'),
    )
    kosdaq_df = pd.DataFrame(
        {
            '시가': [2000.0],
            '고가': [2100.0],
            '저가': [1950.0],
            '종가': [2050.0],
            '거래량': [150],
            '거래대금': [300000.0],
            '등락률': [2.5],
        },
        index=pd.Index(['100100'], name='티커'),
    )

    def fake_get_market_ohlcv_by_ticker(date, market='KOSPI'):
        if market == 'KOSPI':
            return kospi_df
        if market == 'KOSDAQ':
            return kosdaq_df
        return pd.DataFrame()

    monkeypatch.setattr(module.stock, 'get_market_ohlcv_by_ticker', fake_get_market_ohlcv_by_ticker)

    selector = KoreaStockSelector()
    tickers = ['000100', '000200', '100100', '100200']
    fallback_prices = {'000200': 950.0, '100200': 500.0}

    open_prices, fallback_used = selector.get_open_prices(tickers, '2024-04-10', fallback_prices=fallback_prices)

    assert open_prices == {
        '000100': 1000.0,
        '000200': 950.0,
        '100100': 2000.0,
        '100200': 500.0,
    }
    assert sorted(fallback_used) == ['000200', '100200']


def test_get_open_prices_returns_empty_fallback_when_library_unavailable(monkeypatch):
    module = _reload_stock_selector()
    monkeypatch.setattr(module, 'LIBRARIES_AVAILABLE', False)

    selector = KoreaStockSelector()
    tickers = ['000100', '100100']
    fallback_prices = {'000100': 1000.0}

    open_prices, fallback_used = selector.get_open_prices(tickers, '2024-04-10', fallback_prices=fallback_prices)

    assert open_prices == {'000100': 1000.0}
