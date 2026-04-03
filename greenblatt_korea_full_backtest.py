"""
그린블라트 응용 전략 - 한국 주식 백테스트 (실행 가능 버전)

필수 패키지 설치:
uv sync

실행 방법:
uv run greenblatt_korea_full_backtest.py

 데이터 출처:
 - FinanceDataReader: 주가 데이터
 - pykrx: 재무제표 및 시장 데이터

추가 기능:
 - `mixed_filter_profile='large_cap'` 옵션: 기존의 소형/중형 중심 스크리닝 대신 시가총액 상위 기업(상위 20% 기반) 및 하한선(`large_cap_min_mcap`)을 적용해 대형주 위주의 포트폴리오를 생성합니다.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
import time
from collections import OrderedDict
import warnings
warnings.filterwarnings('ignore')
import argparse
from defaults import DEFAULT_REBALANCE_MONTHS
from vol_targeting import compute_vol_target_ratio

try:
    from dotenv import find_dotenv, load_dotenv

    dotenv_path = find_dotenv(usecwd=True)
    if dotenv_path:
        load_dotenv(dotenv_path=dotenv_path, override=False)
    else:
        project_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
        load_dotenv(dotenv_path=project_env_path, override=False)
except ImportError:
    print("경고: python-dotenv가 설치되지 않았습니다.")
    print("설치 명령: uv sync")

try:
    import FinanceDataReader as fdr
    from pykrx import stock
    LIBRARIES_AVAILABLE = True
except ImportError:
    LIBRARIES_AVAILABLE = False
    print("경고: FinanceDataReader 또는 pykrx가 설치되지 않았습니다.")
    print("설치 명령: uv sync")

from stock_selector import KoreaStockSelector
from live_trading.execution import select_capital_constrained_stocks


class KoreaStockBacktest:
    """한국 주식 그린블라트 응용 전략 백테스트"""
    
    def __init__(self, start_date='2017-05-01', end_date='2025-04-30',
                 initial_capital=10000000, investment_ratio=0.95, num_stocks=30,
                 commission_fee_rate=0.0015, tax_rate=0.002, rebalance_months=None,
                 rebalance_days=None,
                 strategy_mode='mixed', mixed_filter_profile='aggressive_mid',
                 sell_losers_enabled=True, kosdaq_target_ratio=None,
                 momentum_enabled=True, momentum_months=6, momentum_weight=0.1,
                 momentum_filter_enabled=False,
                 large_cap_min_mcap=None,
                 fundamental_source=None,
                 capital_constrained_selection_enabled=True,
                 capital_constrained_min_stocks=20,
                 capital_constrained_max_stocks=None,
                 cache_dir='results/cache',
                 timing_enabled=True,
                 fundamental_cache_format='parquet',
                 fundamental_cache_max_entries=16,
                 slippage_bps: int = 10,
                 vol_target_enabled: bool = False,
                 vol_target_sigma: float = 0.20,
                 vol_target_lookback: int = 20,
                 vol_target_min_ratio: float = 0.30):
        """
        Parameters:
        -----------
        start_date : str
            백테스트 시작일 (YYYY-MM-DD)
        end_date : str
            백테스트 종료일 (YYYY-MM-DD)
        initial_capital : int
            초기 투자금액 (원)
        investment_ratio : float
            투자 비율 (0.6 = 60%)
        num_stocks : int
            보유 종목 수
        commission_fee_rate : float
            거래 수수료 비율 (기본 0.15% = 0.0015)
        tax_rate : float
            양도소득세 비율 (기본 0.2% = 0.002)
        rebalance_months : int
            리밸런싱 주기(개월). 12=연 1회, 6=반기, 3=분기
        strategy_mode : str
            종목 선정 모드 ('roe' 또는 'mixed')
                mixed_filter_profile : str
                        mixed 모드 필터 프로파일 ('relative', 'aggressive_mid', 'aggressive' 또는 'large_cap')
                        - 'large_cap' : 시가총액 상위 기업을 우선 선별 (상위 20% 기반)하며
                            `large_cap_min_mcap` 파라미터로 하한 시가총액을 설정할 수 있습니다.
        sell_losers_enabled : bool
            1년 보유 후 손실 종목 매도 사용 여부
        kosdaq_target_ratio : float | None
            KOSDAQ 목표 비중(0~1). None이면 강제 비중을 사용하지 않음
        """
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.investment_ratio = investment_ratio
        self.num_stocks = num_stocks
        self.commission_fee_rate = commission_fee_rate
        self.tax_rate = tax_rate
        # rebalance_days: 우선순위 -> 인자 > REBALANCE_DAYS env > LIVE_REBALANCE_DAYS env
        self.rebalance_days = None
        if rebalance_days is None:
            env_days = os.getenv('REBALANCE_DAYS') or os.getenv('LIVE_REBALANCE_DAYS')
            if env_days not in (None, ""):
                try:
                    parsed_days = int(env_days)
                    if parsed_days > 0:
                        self.rebalance_days = parsed_days
                except Exception:
                    self.rebalance_days = None
        else:
            try:
                parsed_days = int(rebalance_days)
                if parsed_days > 0:
                    self.rebalance_days = parsed_days
            except Exception:
                self.rebalance_days = None

        # rebalance_months: 우선순위 -> 인자 > REBALANCE_MONTHS env > LIVE_REBALANCE_MONTHS env > 기본값(DEFAULT_REBALANCE_MONTHS)
        if rebalance_months is None:
            env_reb = os.getenv('REBALANCE_MONTHS') or os.getenv('LIVE_REBALANCE_MONTHS')
            if env_reb not in (None, ""):
                try:
                    self.rebalance_months = int(env_reb)
                except Exception:
                    self.rebalance_months = DEFAULT_REBALANCE_MONTHS
            else:
                self.rebalance_months = DEFAULT_REBALANCE_MONTHS
        else:
            self.rebalance_months = int(rebalance_months)
        self.strategy_mode = strategy_mode
        self.mixed_filter_profile = mixed_filter_profile
        self.sell_losers_enabled = sell_losers_enabled
        self.kosdaq_target_ratio = kosdaq_target_ratio
        self.momentum_enabled = momentum_enabled
        self.momentum_months = momentum_months
        self.momentum_weight = momentum_weight
        self.momentum_filter_enabled = momentum_filter_enabled
        # large_cap_min_mcap: numeric (원 단위) or None - `mixed_filter_profile='large_cap'` 사용 시
        # None이면 기본 동작은 시가총액 상위 20% (top20 기반)로, 숫자를 주면 해당 하한을 추가로 적용합니다.
        self.large_cap_min_mcap = large_cap_min_mcap
        self.fundamental_source = str(fundamental_source or os.getenv('BACKTEST_FUNDAMENTAL_SOURCE', 'pykrx')).strip().lower()
        self.capital_constrained_selection_enabled = bool(capital_constrained_selection_enabled)
        self.capital_constrained_min_stocks = int(capital_constrained_min_stocks)
        self.capital_constrained_max_stocks = int(capital_constrained_max_stocks) if capital_constrained_max_stocks else int(num_stocks)
        self.cache_dir = cache_dir
        self.timing_enabled = timing_enabled
        self.fundamental_cache_format = fundamental_cache_format
        self.fundamental_cache_max_entries = max(1, int(fundamental_cache_max_entries))
        self.cache_version = {
            'fundamental_cache_v': 2,
            'momentum_cache_v': 1,
            'strategy_mode': self.strategy_mode,
            'momentum_months': self.momentum_months,
            'cache_format': self.fundamental_cache_format,
        }
        
        self.portfolio = {}
        self.cash = initial_capital
        self.portfolio_history = []
        self.trade_history = []

        # 슬리피지: 기본 단위는 basis points(bps). 매수 시는 가격에 슬리피지만큼 더 지불, 매도 시는 슬리피지만큼 덜 받음
        self.slippage_bps = int(slippage_bps or 0)
        self.slippage_rate = float(self.slippage_bps) / 10000.0

        # 변동성 타게팅: 포트폴리오 실현 변동성이 sigma_target을 초과하면 투자비율을 자동 축소
        self.vol_target_enabled = bool(vol_target_enabled)
        self.vol_target_sigma = float(vol_target_sigma)
        self.vol_target_lookback = int(vol_target_lookback)
        self.vol_target_min_ratio = float(vol_target_min_ratio)

        self.industry_cache = {}
        self.momentum_cache = {}
        self.price_cache = {}
        self.fundamental_cache = OrderedDict()  # {date|market: DataFrame} LRU
        self._load_caches()
        self.selector = KoreaStockSelector(
            num_stocks=self.num_stocks,
            strategy_mode=self.strategy_mode,
            mixed_filter_profile=self.mixed_filter_profile,
            kosdaq_target_ratio=self.kosdaq_target_ratio,
            momentum_enabled=self.momentum_enabled,
            momentum_months=self.momentum_months,
            momentum_weight=self.momentum_weight,
            momentum_filter_enabled=self.momentum_filter_enabled,
            large_cap_min_mcap=self.large_cap_min_mcap,
            fundamental_source=self.fundamental_source,
            cache_dir=self.cache_dir,
            timing_enabled=self.timing_enabled,
            fundamental_cache_format=self.fundamental_cache_format,
            fundamental_cache_max_entries=self.fundamental_cache_max_entries,
        )

    def _cache_paths(self):
        return {
            'industry': os.path.join(self.cache_dir, 'industry_cache.json'),
            'momentum': os.path.join(self.cache_dir, 'momentum_cache.json'),
            'price': os.path.join(self.cache_dir, 'price_cache.json'),
            'fundamentals': os.path.join(self.cache_dir, 'fundamentals'),
            'meta': os.path.join(self.cache_dir, 'cache_meta.json')
        }

    def _validate_cache_version(self, loaded_meta):
        if not isinstance(loaded_meta, dict):
            return False
        for key, value in self.cache_version.items():
            if loaded_meta.get(key) != value:
                return False
        return True

    def _purge_fundamental_disk_cache(self):
        paths = self._cache_paths()
        fundamentals_dir = paths['fundamentals']
        if not os.path.isdir(fundamentals_dir):
            return
        for file_name in os.listdir(fundamentals_dir):
            if file_name.endswith('.parquet') or file_name.endswith('.csv'):
                try:
                    os.remove(os.path.join(fundamentals_dir, file_name))
                except Exception:
                    pass

    def _fundamental_cache_file(self, date_str, market):
        paths = self._cache_paths()
        fundamentals_dir = paths['fundamentals']
        preferred_ext = '.parquet' if self.fundamental_cache_format == 'parquet' else '.csv'
        preferred = os.path.join(fundamentals_dir, f'{date_str}_{market}{preferred_ext}')
        alternate_ext = '.csv' if preferred_ext == '.parquet' else '.parquet'
        alternate = os.path.join(fundamentals_dir, f'{date_str}_{market}{alternate_ext}')
        return preferred, alternate

    def _load_fundamental_frame(self, date_str, market):
        preferred, alternate = self._fundamental_cache_file(date_str, market)
        for path in [preferred, alternate]:
            if not os.path.exists(path):
                continue
            try:
                if path.endswith('.parquet'):
                    return pd.read_parquet(path)
                return pd.read_csv(path)
            except Exception:
                continue
        return None

    def _save_fundamental_frame(self, date_str, market, df):
        preferred, _ = self._fundamental_cache_file(date_str, market)
        try:
            if preferred.endswith('.parquet'):
                df.to_parquet(preferred, index=False)
            else:
                df.to_csv(preferred, index=False, encoding='utf-8-sig')
            return
        except Exception:
            pass

        fallback = preferred.replace('.parquet', '.csv')
        try:
            df.to_csv(fallback, index=False, encoding='utf-8-sig')
        except Exception:
            pass

    def _set_fundamental_cache_lru(self, cache_key, frame):
        if cache_key in self.fundamental_cache:
            self.fundamental_cache.move_to_end(cache_key)
            self.fundamental_cache[cache_key] = frame
            return
        self.fundamental_cache[cache_key] = frame
        if len(self.fundamental_cache) > self.fundamental_cache_max_entries:
            self.fundamental_cache.popitem(last=False)

    def _load_json_cache(self, path):
        try:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        return data
        except Exception:
            pass
        return {}

    def _save_json_cache(self, path, data):
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
        except Exception as e:
            print(f"캐시 저장 실패 ({path}): {e}")

    def _load_caches(self):
        try:
            os.makedirs(self.cache_dir, exist_ok=True)
            paths = self._cache_paths()
            os.makedirs(paths['fundamentals'], exist_ok=True)

            loaded_meta = self._load_json_cache(paths['meta'])
            if not self._validate_cache_version(loaded_meta):
                print("[CACHE] cache version mismatch detected, invalidating incompatible caches")
                self.momentum_cache = {}
                self.fundamental_cache = OrderedDict()
                self._purge_fundamental_disk_cache()

            self.industry_cache = self._load_json_cache(paths['industry'])
            if len(self.momentum_cache) == 0:
                self.momentum_cache = self._load_json_cache(paths['momentum'])
            self.price_cache = self._load_json_cache(paths['price'])
            print(
                f"[CACHE] loaded: industry={len(self.industry_cache):,}, "
                f"momentum={len(self.momentum_cache):,}, price={len(self.price_cache):,}"
            )
        except Exception as e:
            print(f"[CACHE] load failed: {e}")
            self.industry_cache = {}
            self.momentum_cache = {}
            self.price_cache = {}
            self.fundamental_cache = {}

    def _persist_caches(self):
        try:
            os.makedirs(self.cache_dir, exist_ok=True)
            paths = self._cache_paths()
            os.makedirs(paths['fundamentals'], exist_ok=True)
            self._save_json_cache(paths['industry'], self.industry_cache)
            self._save_json_cache(paths['momentum'], self.momentum_cache)
            self._save_json_cache(paths['price'], self.price_cache)
            self._save_json_cache(paths['meta'], self.cache_version)

            # Fundamental 캐시를 Parquet/CSV로 저장
            for cache_key, df in self.fundamental_cache.items():
                try:
                    parts = cache_key.split('|')
                    if len(parts) == 2:
                        date_str, market = parts
                        self._save_fundamental_frame(date_str, market, df)
                except Exception:
                    pass
            print(
                f"[CACHE] saved: industry={len(self.industry_cache):,}, "
                f"momentum={len(self.momentum_cache):,}, "
                f"fundamental={len(self.fundamental_cache):,}"
            )
        except Exception as e:
            print(f"[CACHE] save failed: {e}")

    def _get_fundamental_and_cap(self, date_str, markets=['KOSPI', 'KOSDAQ']):
        """
        펀더멘탈/시총 통합 데이터 조회 (캐시 우선)
        반환: merged DataFrame (ticker, PER, PBR, EPS, BPS, close, market_cap, market)
        """
        if hasattr(self, 'selector') and self.selector is not None:
            try:
                return self.selector._get_fundamental_and_cap(date_str, markets)
            except Exception:
                pass

        t_start = time.perf_counter()
        cache_hits = 0
        cache_misses = 0
        dfs_merged = []
        
        for market in markets:
            cache_key = f"{date_str}|{market}"
            
            # 메모리 캐시에서 먼저 검색
            if cache_key in self.fundamental_cache:
                df_cached = self.fundamental_cache[cache_key].copy()
                self.fundamental_cache.move_to_end(cache_key)
                if 'market' not in df_cached.columns:
                    df_cached['market'] = market
                dfs_merged.append(df_cached)
                cache_hits += 1
                continue
            
            # 디스크 캐시 검색
            df_disk = self._load_fundamental_frame(date_str, market)
            if df_disk is not None:
                if 'market' not in df_disk.columns:
                    df_disk['market'] = market
                self._set_fundamental_cache_lru(cache_key, df_disk)
                dfs_merged.append(df_disk)
                cache_hits += 1
                continue
            
            # API 호출 (캐시 미스)
            try:
                df_fund_mkt = stock.get_market_fundamental_by_ticker(date_str, market=market)
                df_cap_mkt = stock.get_market_cap_by_ticker(date_str, market=market)
                if not df_fund_mkt.empty and not df_cap_mkt.empty:
                    df_fund_mkt = df_fund_mkt.reset_index().rename(columns={'티커': 'ticker'})
                    df_cap_mkt = df_cap_mkt.reset_index().rename(columns={'티커': 'ticker', '종가': 'close', '시가총액': 'market_cap'})
                    merged = pd.merge(df_fund_mkt, df_cap_mkt[['ticker', 'close', 'market_cap']], on='ticker', how='inner')
                    merged['market'] = market
                    self._set_fundamental_cache_lru(cache_key, merged)
                    dfs_merged.append(merged)
                    cache_misses += 1
            except Exception:
                pass
        
        self._log_timing(
            'fetch.fundamental_cap',
            time.perf_counter() - t_start,
            extra=f"hit={cache_hits}, miss={cache_misses}, markets={len(markets)}"
        )
        
        if len(dfs_merged) == 0:
            return pd.DataFrame()
        
        return pd.concat(dfs_merged, ignore_index=True)

    def _log_timing(self, label, elapsed_sec, extra=''):
        if not self.timing_enabled:
            return
        if extra:
            print(f"  [TIME] {label}: {elapsed_sec:.3f}s ({extra})")
        else:
            print(f"  [TIME] {label}: {elapsed_sec:.3f}s")
        
    def get_market_tickers(self, date=None):
        """KOSPI + KOSDAQ 상장 종목 리스트 가져오기"""
        if hasattr(self, 'selector') and self.selector is not None:
            try:
                return self.selector.get_market_tickers(date)
            except Exception:
                pass

        if not LIBRARIES_AVAILABLE:
            return []
        
        try:
            if date is None:
                date = datetime.now().strftime('%Y%m%d')
            else:
                date = date.replace('-', '')
            
            tickers_kospi = stock.get_market_ticker_list(date=date, market="KOSPI")
            tickers_kosdaq = stock.get_market_ticker_list(date=date, market="KOSDAQ")
            return list(set(tickers_kospi + tickers_kosdaq))  # 중복 제거
        except Exception as e:
            print(f"종목 리스트 조회 실패: {e}")
            return []
    
    def _get_nearest_trading_date(self, date_str):
        """공휴일/주말이면 가장 가까운 다음 영업일 반환 (yyyymmdd 형식)"""
        try:
            return stock.get_nearest_business_day_in_a_week(date=date_str, prev=False)
        except Exception:
            # fallback: 최대 7일 앞으로 탐색 (KOSPI/KOSDAQ 모두 시도)
            from datetime import datetime, timedelta
            dt = datetime.strptime(date_str, '%Y%m%d')
            for i in range(7):
                candidate = (dt + timedelta(days=i)).strftime('%Y%m%d')
                for market in ['KOSPI', 'KOSDAQ']:
                    try:
                        df_test = stock.get_market_fundamental_by_ticker(candidate, market=market)
                        if not df_test.empty and df_test.iloc[:, 0].sum() != 0:
                            return candidate
                    except:
                        pass
            return date_str

    def _get_previous_trading_date(self, date_str):
        """공휴일/주말이면 가장 가까운 이전 영업일 반환 (yyyymmdd 형식)"""
        try:
            return stock.get_nearest_business_day_in_a_week(date=date_str, prev=True)
        except Exception:
            dt = datetime.strptime(date_str, '%Y%m%d')
            for i in range(7):
                candidate = (dt - timedelta(days=i)).strftime('%Y%m%d')
                for market in ['KOSPI', 'KOSDAQ']:
                    try:
                        df_test = stock.get_market_fundamental_by_ticker(candidate, market=market)
                        if not df_test.empty and df_test.iloc[:, 0].sum() != 0:
                            return candidate
                    except:
                        pass
            return date_str




    


    def _get_industry_info(self, tickers, date_str):
        """종목별 업종/이름 정보 조회 (금융주 필터링용)"""
        try:
            industry_map = {}
            cache_hits = 0
            cache_misses = 0
            for ticker in set(tickers):
                if ticker in self.industry_cache:
                    industry_map[ticker] = self.industry_cache[ticker]
                    cache_hits += 1
                    continue
                try:
                    info = stock.get_market_ticker_info(ticker)
                    if info:
                        industry = info.get('업종', '기타')
                        industry_map[ticker] = industry
                        self.industry_cache[ticker] = industry
                    else:
                        industry_map[ticker] = '기타'
                        self.industry_cache[ticker] = '기타'
                    cache_misses += 1
                except:
                    industry_map[ticker] = '기타'
                    self.industry_cache[ticker] = '기타'
                    cache_misses += 1

            if self.timing_enabled:
                print(f"    [CACHE] industry hit={cache_hits}, miss={cache_misses}")
            return industry_map
        except Exception:
            return {}

    def _pick_dividend_column(self, df):
        """배당수익률 컬럼명 자동 탐지"""
        if 'DIV' in df.columns:
            return 'DIV'
        if '배당수익률' in df.columns:
            return '배당수익률'
        return None

    def screen_stocks_pykrx_roe(self, target_date, markets=['KOSPI', 'KOSDAQ']):
        """
        pykrx ROE 기반 종목 스크리닝 (GP/A 대신 ROE 사용)
        
        매개변수:
        -----------
        target_date : str
            조회 날짜 ('YYYY-MM-DD' 형식)
        markets : list
            조회 시장 리스트 (기본값: ['KOSPI', 'KOSDAQ'])
        
        반환값:
        -------
        DataFrame: 선정된 종목 (ticker, PER, PBR, ROE, total_rank, close)
        """
        if not LIBRARIES_AVAILABLE:
            return pd.DataFrame()
        
        try:
            date_str = target_date.replace('-', '')
            df = self._get_fundamental_and_cap(date_str, markets)

            if df.empty:
                print("    ROE 스크리닝: 펀더멘탈 데이터 없음")
                return pd.DataFrame()
            
            # 3. 금융주 업종 정보 추가 및 필터링
            industry_map = self._get_industry_info(df['ticker'].tolist(), date_str)
            df['industry'] = df['ticker'].map(industry_map).fillna('기타')
            # 금융주 필터 제거 (금융주도 괜찮은 종목 있음)
            
            # 4. 밸류/성장 공존 필터 (Value trap 완화)
            df = df[(df['PER'] > 0) & (df['PER'] < 20)]
            df = df[(df['PBR'] > 0.2) & (df['PBR'] < 10)]

            # 5. 유동성 확보를 위한 시총 하한
            df = df[df['market_cap'] >= 5e10]  # 500억원 이상
            
            if len(df) == 0:
                print("    ROE 스크리닝: 필터링 후 종목 없음")
                return pd.DataFrame()
            
            # 6. ROE 계산 및 하한선 적용 (퀄리티 강화)
            df['ROE'] = np.where(
                (df['EPS'] > 0) & (df['BPS'] > 0),
                (df['EPS'] / df['BPS']) * 100,
                np.nan
            )
            df = df[df['ROE'] >= 10]
            
            # 7. 그린블라트식 등수 합산 (PER + PBR + ROE 단순 합산)
            df['rank_per'] = df['PER'].rank(ascending=True, method='average', na_option='bottom')
            df['rank_pbr'] = df['PBR'].rank(ascending=True, method='average', na_option='bottom')
            df['rank_roe'] = df['ROE'].rank(ascending=False, method='average', na_option='bottom')
            df['total_rank'] = df['rank_per'] + df['rank_pbr'] + df['rank_roe']
            
            # 8. 상위 N개 종목 선정
            result = df.sort_values('total_rank', ascending=True).head(self.num_stocks)
            
            # 선정 과정 통계 출력
            if len(result) > 0:
                print(f"      필터 후: {len(df)}개 → 선정: {len(result)}개")
                print(f"      PER {result['PER'].min():.1f}~{result['PER'].max():.1f} (평균 {result['PER'].mean():.2f})")
                print(f"      PBR {result['PBR'].min():.2f}~{result['PBR'].max():.2f} (평균 {result['PBR'].mean():.2f})")
                print(f"      ROE {result['ROE'].min():.1f}~{result['ROE'].max():.1f}% (평균 {result['ROE'].mean():.2f}%)")
            
            return result[['ticker', 'PER', 'PBR', 'ROE', 'total_rank', 'close', 'market_cap']].copy()
            
        except Exception as e:
            print(f"    ROE 스크리닝 오류: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def screen_stocks_mixed(self, target_date, markets=['KOSPI', 'KOSDAQ']):
        """
        혼합 스크리닝: 가치(35%) + 품질(65%)
        - 가치: PER/PBR 저평가 순위
        - 품질: ROE + 배당수익률 순위
        """
        if not LIBRARIES_AVAILABLE:
            return pd.DataFrame()

        try:
            t_screen_start = time.perf_counter()
            date_str = target_date.replace('-', '')
            df = self._get_fundamental_and_cap(date_str, markets)

            if df.empty:
                print("    MIXED 스크리닝: 펀더멘탈 데이터 없음")
                return pd.DataFrame()

            # 1) 공통 최소 필터 (프로파일에 따라 시가총액 기준 조정)
            if self.mixed_filter_profile == 'large_cap':
                # large_cap_min_mcap이 None일 때는 시가총액 하한을 적용하지 않음
                if self.large_cap_min_mcap is None:
                    df = df[(df['PER'] > 0) & (df['PBR'] > 0)]
                else:
                    try:
                        min_mcap = float(self.large_cap_min_mcap)
                    except Exception:
                        min_mcap = None

                    if min_mcap is None:
                        df = df[(df['PER'] > 0) & (df['PBR'] > 0)]
                    else:
                        df = df[(df['PER'] > 0) & (df['PBR'] > 0) & (df['market_cap'] >= min_mcap)]
            else:
                df = df[(df['PER'] > 0) & (df['PBR'] > 0) & (df['market_cap'] >= 5e10)]

            # 2) 품질 지표 계산
            df['ROE'] = np.where(
                (df['EPS'] > 0) & (df['BPS'] > 0),
                (df['EPS'] / df['BPS']) * 100,
                np.nan
            )
            df = df[df['ROE'] >= 10]

            dividend_col = self._pick_dividend_column(df)
            if dividend_col is None:
                df['DIV_YIELD'] = 0.0
            else:
                df['DIV_YIELD'] = pd.to_numeric(df[dividend_col], errors='coerce').fillna(0.0)

            if len(df) == 0:
                print("    MIXED 스크리닝: 품질 필터 후 종목 없음")
                return pd.DataFrame()

            # 3) 필터 프로파일별 분기
            if self.mixed_filter_profile == 'large_cap':
                # 대형주 모드: 기본은 시장 상위 20%를 적용
                # 추가로 `large_cap_min_mcap`이 None이 아니면 하한선을 적용
                cap_lower_limit = df['market_cap'].quantile(0.80)
                df = df[df['market_cap'] >= cap_lower_limit]
                if self.large_cap_min_mcap is not None:
                    try:
                        min_mcap = float(self.large_cap_min_mcap)
                        df = df[df['market_cap'] >= min_mcap]
                    except Exception:
                        pass
            elif self.mixed_filter_profile == 'aggressive':
                # 공격형: 절대 PBR 상한 + 시장별 시총 하위 20% 집중
                df = df[df['PBR'] < 10]
                df['mcap_cut'] = df.groupby('market')['market_cap'].transform(lambda s: s.quantile(0.20))
                df = df[df['market_cap'] <= df['mcap_cut']]
            elif self.mixed_filter_profile == 'aggressive_mid':
                # 중간안: 절대 PBR 상한 + 시장별 시총 하위 30% 집중
                df = df[df['PBR'] < 10]
                df['mcap_cut'] = df.groupby('market')['market_cap'].transform(lambda s: s.quantile(0.30))
                df = df[df['market_cap'] <= df['mcap_cut']]
            else:
                # 상대분위수형(기본): 시장별 분포 기반 컷
                df['per_cut'] = df.groupby('market')['PER'].transform(lambda s: s.quantile(0.40))
                df['pbr_cut'] = df.groupby('market')['PBR'].transform(lambda s: s.quantile(0.40))
                df['roe_cut'] = df.groupby('market')['ROE'].transform(lambda s: s.quantile(0.60))
                df['mcap_cut'] = df.groupby('market')['market_cap'].transform(
                    lambda s: s.quantile(0.50 if s.name == 'KOSPI' else 0.70)
                )

                df = df[
                    (df['PER'] <= df['per_cut'])
                    & (df['PBR'] <= df['pbr_cut'])
                    & (df['ROE'] >= df['roe_cut'])
                    & (df['market_cap'] <= df['mcap_cut'])
                ]

            if len(df) == 0:
                print("    MIXED 스크리닝: 시장별 분위수 필터 후 종목 없음")
                return pd.DataFrame()

            # (대형주 필터는 `mixed_filter_profile=='large_cap'` 분기에서 처리됨)

            # 4) 시장 내 지표별 절대 등수 (그린블라트 원형: 지표별 등수 합산)
            df['rank_per'] = df.groupby('market')['PER'].rank(ascending=True, method='average', na_option='bottom')
            df['rank_pbr'] = df.groupby('market')['PBR'].rank(ascending=True, method='average', na_option='bottom')
            df['rank_roe'] = df.groupby('market')['ROE'].rank(ascending=False, method='average', na_option='bottom')
            df['rank_div'] = df.groupby('market')['DIV_YIELD'].rank(ascending=False, method='average', na_option='bottom')

            value_rank = df['rank_per'] + df['rank_pbr']

            # 공통: 모멘텀 계산 (프로파일과 무관하게 계산하여 이후 가중치에 포함)
            if self.momentum_enabled:
                t_mom_start = time.perf_counter()
                moms = []
                cache_hit = 0
                cache_miss = 0
                try:
                    end_dt = pd.to_datetime(date_str)
                    start_dt = (end_dt - pd.DateOffset(months=self.momentum_months)).strftime('%Y%m%d')
                    end_dt_str = end_dt.strftime('%Y%m%d')
                except:
                    start_dt = date_str
                    end_dt_str = date_str

                for ticker in df['ticker']:
                    if len(moms) % 10 == 0:
                        print(f"    [MIXED] computing momentum: processed {len(moms)} tickers...")
                    cache_key = f"{ticker}|{start_dt}|{end_dt_str}"
                    if cache_key in self.momentum_cache:
                        moms.append(self.momentum_cache[cache_key])
                        cache_hit += 1
                        continue
                    try:
                        ohlc = stock.get_market_ohlcv(start_dt, end_dt_str, ticker)
                        if ohlc is not None and not ohlc.empty:
                            first = ohlc['종가'].iloc[0]
                            last = ohlc['종가'].iloc[-1]
                            mom = (last / first) - 1 if first > 0 else 0.0
                        else:
                            mom = 0.0
                    except:
                        mom = 0.0
                    self.momentum_cache[cache_key] = float(mom)
                    cache_miss += 1
                    moms.append(mom)

                self._log_timing(
                    'mixed.momentum',
                    time.perf_counter() - t_mom_start,
                    extra=f"hit={cache_hit}, miss={cache_miss}, universe={len(df)}"
                )

                df['mom'] = moms
                if self.momentum_filter_enabled:
                    df = df[df['mom'] > 0]
                df['rank_mom_pct'] = df.groupby('market')['mom'].rank(ascending=False, pct=True, method='average')
            else:
                # 모멘텀이 비활성화된 경우 안전하게 열을 만들어 둠
                df['rank_mom_pct'] = 0.0

            # 2단계 혼합 방식
            # Stage 1: 프로파일별 순수 그린블라트 등수 합산 (단위 무관, 임의 가중 없음)
            if self.mixed_filter_profile == 'large_cap':
                df['greenblatt_rank'] = value_rank + df['rank_roe']
            else:
                df['greenblatt_rank'] = value_rank + df['rank_roe'] + df['rank_div']

            # Stage 2: 그린블라트 합산을 [0,1] 정규화 후 momentum_weight 비율로 모멘텀과 혼합
            df['rank_greenblatt_pct'] = df.groupby('market')['greenblatt_rank'].rank(ascending=True, pct=True, method='average')
            m = float(self.momentum_weight) if self.momentum_enabled else 0.0
            df['total_rank'] = (1.0 - m) * df['rank_greenblatt_pct'] + m * df['rank_mom_pct']

            df_sorted = df.sort_values('total_rank', ascending=True)

            if self.kosdaq_target_ratio is not None:
                kosdaq_target = int(self.num_stocks * self.kosdaq_target_ratio)
                kospi_target = self.num_stocks - kosdaq_target

                kosdaq_pool = df_sorted[df_sorted['market'] == 'KOSDAQ']
                kospi_pool = df_sorted[df_sorted['market'] == 'KOSPI']

                selected = []
                if kosdaq_target > 0:
                    selected.append(kosdaq_pool.head(kosdaq_target))
                if kospi_target > 0:
                    selected.append(kospi_pool.head(kospi_target))

                if len(selected) > 0:
                    result = pd.concat(selected, ignore_index=True)
                else:
                    result = df_sorted.head(self.num_stocks)

                # 부족분 보충
                if len(result) < self.num_stocks:
                    remaining = self.num_stocks - len(result)
                    chosen = set(result['ticker'])
                    extra = df_sorted[~df_sorted['ticker'].isin(chosen)].head(remaining)
                    if not extra.empty:
                        result = pd.concat([result, extra], ignore_index=True)

                result = result.head(self.num_stocks)
            else:
                result = df_sorted.head(self.num_stocks)

            if len(result) > 0:
                market_counts = result['market'].value_counts().to_dict()
                print(f"      [MIXED-{self.mixed_filter_profile}] 필터 후: {len(df)}개 → 선정: {len(result)}개")
                print(f"      [MIXED] 시장구성: {market_counts}")
                print(f"      [MIXED] PER 평균 {result['PER'].mean():.2f}, PBR 평균 {result['PBR'].mean():.2f}")
                print(f"      [MIXED] ROE 평균 {result['ROE'].mean():.2f}%, 배당수익률 평균 {result['DIV_YIELD'].mean():.2f}%")

            self._log_timing('mixed.total', time.perf_counter() - t_screen_start)

            return result[['ticker', 'market', 'PER', 'PBR', 'ROE', 'DIV_YIELD', 'total_rank', 'close', 'market_cap']].copy()

        except Exception as e:
            print(f"    MIXED 스크리닝 오류: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    

    
    def rebalance(self, selected_stocks, rebalance_date, investment_ratio=None):
        """포트폴리오 리밸런싱"""
        commission_rate = self.commission_fee_rate
        tax_rate = self.tax_rate
        ratio_to_use = self.investment_ratio if investment_ratio is None else float(investment_ratio)
        # 부분 리밸런싱: 기존 보유 중 선정된 종목은 유지/조정, 제외 종목만 매도
        if len(selected_stocks) == 0:
            return

        # 가격 맵 (선정된 종목의 기준가격)
        price_map = selected_stocks.set_index('ticker')['close'].to_dict()

        # 1) 매도: 포트폴리오에 있으나 이번에 선정되지 않은 종목은 전량 매도
        sell_total = 0
        to_remove = []
        for ticker, position in list(self.portfolio.items()):
            if ticker not in price_map:
                sell_price = position.get('current_price', position['buy_price'])
                # 슬리피지 반영 실행가 (판매자는 실수령액이 소폭 감소함)
                exec_sell_price = sell_price * (1 - self.slippage_rate)
                gross_sell_amount = position['shares'] * exec_sell_price
                commission = gross_sell_amount * commission_rate
                tax = gross_sell_amount * tax_rate
                total_costs = commission + tax
                net_sell_amount = gross_sell_amount - total_costs
                self.cash += net_sell_amount
                sell_total += net_sell_amount

                self.trade_history.append({
                    'date': rebalance_date,
                    'ticker': ticker,
                    'action': 'SELL',
                    'shares': position['shares'],
                    'price': sell_price,
                    'exec_price': exec_sell_price,
                    'amount': net_sell_amount,
                    'gross_amount': gross_sell_amount,
                    'commission': commission,
                    'tax': tax,
                    'fee': total_costs
                })

                to_remove.append(ticker)

        for t in to_remove:
            del self.portfolio[t]

        # 2) 목표 자산 할당 기반 계산 (현재 포트폴리오 가치 기준)
        total_value = self.get_portfolio_value()
        invest_amount = total_value * ratio_to_use
        per_stock_amount = invest_amount / len(selected_stocks) if len(selected_stocks) > 0 else 0

        # 3) 목표 수량 산정 및 매매 (보유 종목은 조정, 신규는 매수)
        for _, stock in selected_stocks.iterrows():
            ticker = stock['ticker']
            price = stock['close']
            # 가격 정보가 없거나 비정상이면 해당 종목 건너뜀
            try:
                if pd.isna(price) or float(price) <= 0:
                    continue
            except Exception:
                continue
            # 목표 수량 (수수료+슬리피지 고려)
            target_shares = int(per_stock_amount / (price * (1 + commission_rate) * (1 + self.slippage_rate)))

            if ticker in self.portfolio:
                position = self.portfolio[ticker]
                current_shares = position['shares']
                # 조정: 과다 보유면 일부 매도, 부족하면 추가 매수
                if target_shares < current_shares:
                    sell_shares = current_shares - target_shares
                    sell_price = position.get('current_price', position['buy_price'])
                    gross_sell_amount = sell_shares * sell_price
                    commission = gross_sell_amount * commission_rate
                    tax = gross_sell_amount * tax_rate
                    total_costs = commission + tax
                    net_sell_amount = gross_sell_amount - total_costs
                    self.cash += net_sell_amount

                    position['shares'] = current_shares - sell_shares
                    self.trade_history.append({
                        'date': rebalance_date,
                        'ticker': ticker,
                        'action': 'SELL',
                        'shares': sell_shares,
                        'price': sell_price,
                        'amount': net_sell_amount,
                        'gross_amount': gross_sell_amount,
                        'commission': commission,
                        'tax': tax,
                        'fee': total_costs
                    })
                    # 포지션이 0이 되면 제거
                    if position['shares'] <= 0:
                        del self.portfolio[ticker]
                elif target_shares > current_shares:
                    buy_shares = target_shares - current_shares
                    # 슬리피지 반영 실행가 (구매자는 실제로 소폭 더 지불함)
                    exec_buy_price = price * (1 + self.slippage_rate)
                    gross_buy_amount = buy_shares * exec_buy_price
                    buy_commission = gross_buy_amount * commission_rate
                    total_buy_cost = gross_buy_amount + buy_commission
                    # 현금 부족 시 구매수량 조정 (실행가격+수수료 반영)
                    if total_buy_cost > self.cash:
                        denom = price * (1 + self.slippage_rate) * (1 + commission_rate)
                        affordable_shares = int(self.cash / denom)
                        buy_shares = max(0, affordable_shares)
                        exec_buy_price = price * (1 + self.slippage_rate)
                        gross_buy_amount = buy_shares * exec_buy_price
                        buy_commission = gross_buy_amount * commission_rate
                        total_buy_cost = gross_buy_amount + buy_commission

                    if buy_shares > 0:
                        self.cash -= total_buy_cost
                        # 가중 평균 매입 단가 적용
                        prev_shares = position['shares']
                        prev_price = position.get('buy_price', price)
                        new_total_shares = prev_shares + buy_shares
                        new_buy_price = ((prev_shares * prev_price) + (buy_shares * price)) / new_total_shares
                        position['shares'] = new_total_shares
                        position['buy_price'] = new_buy_price
                        position['current_price'] = price

                        self.trade_history.append({
                            'date': rebalance_date,
                            'ticker': ticker,
                            'action': 'BUY',
                            'shares': buy_shares,
                            'price': price,
                            'exec_price': exec_buy_price,
                            'amount': total_buy_cost,
                            'gross_amount': gross_buy_amount,
                            'commission': buy_commission,
                            'tax': 0,
                            'fee': buy_commission
                        })
                else:
                    # 목표수량과 동일하면 가격만 업데이트
                    position['current_price'] = price
            else:
                # 신규 매수
                buy_shares = target_shares
                if buy_shares <= 0:
                    continue
                exec_buy_price = price * (1 + self.slippage_rate)
                gross_buy_amount = buy_shares * exec_buy_price
                buy_commission = gross_buy_amount * commission_rate
                total_buy_cost = gross_buy_amount + buy_commission
                if total_buy_cost > self.cash:
                    denom = price * (1 + self.slippage_rate) * (1 + commission_rate)
                    affordable_shares = int(self.cash / denom)
                    buy_shares = max(0, affordable_shares)
                    exec_buy_price = price * (1 + self.slippage_rate)
                    gross_buy_amount = buy_shares * exec_buy_price
                    buy_commission = gross_buy_amount * commission_rate
                    total_buy_cost = gross_buy_amount + buy_commission

                if buy_shares > 0:
                    self.cash -= total_buy_cost
                    self.portfolio[ticker] = {
                        'ticker': ticker,
                        'shares': buy_shares,
                        'buy_price': price,
                        'buy_date': rebalance_date,
                        'current_price': price
                    }

                    self.trade_history.append({
                        'date': rebalance_date,
                        'ticker': ticker,
                        'action': 'BUY',
                        'shares': buy_shares,
                        'price': price,
                        'exec_price': exec_buy_price,
                        'amount': total_buy_cost,
                        'gross_amount': gross_buy_amount,
                        'commission': buy_commission,
                        'tax': 0,
                        'fee': buy_commission
                    })
    
    def update_portfolio_prices(self, date):
        """포트폴리오 보유 종목 가격 업데이트"""
        if not LIBRARIES_AVAILABLE:
            return
        
        date_str = date.replace('-', '')
        cache_hit = 0
        cache_miss = 0
        t_start = time.perf_counter()
        
        for ticker in list(self.portfolio.keys()):
            cache_key = f"{date_str}|{ticker}"
            if cache_key in self.price_cache:
                self.portfolio[ticker]['current_price'] = self.price_cache[cache_key]
                cache_hit += 1
                continue
            try:
                df = stock.get_market_ohlcv(date_str, date_str, ticker)
                if not df.empty:
                    close_price = float(df['종가'].iloc[0])
                    self.portfolio[ticker]['current_price'] = close_price
                    self.price_cache[cache_key] = close_price
                    cache_miss += 1
            except:
                pass

        self._log_timing(
            'portfolio.price_update',
            time.perf_counter() - t_start,
            extra=f"hit={cache_hit}, miss={cache_miss}, holdings={len(self.portfolio)}"
        )
    
    def sell_losers(self, current_date):
        """1년 보유 후 손실 종목 매도"""
        commission_rate = self.commission_fee_rate
        tax_rate = self.tax_rate
        current_date_obj = datetime.strptime(current_date, '%Y-%m-%d')
        tickers_to_sell = []
        
        for ticker, position in self.portfolio.items():
            buy_date_obj = datetime.strptime(position['buy_date'], '%Y-%m-%d')
            holding_days = (current_date_obj - buy_date_obj).days
            
            # 1년 이상 보유
            if holding_days >= 365:
                current_price = position['current_price']
                buy_price = position['buy_price']
                return_rate = (current_price - buy_price) / buy_price
                
                # 손실이면 매도
                if return_rate < 0:
                    tickers_to_sell.append(ticker)
        
        # 매도 실행
        for ticker in tickers_to_sell:
            position = self.portfolio[ticker]
            # 슬리피지 반영 실행가 (판매자는 실수령액이 소폭 감소함)
            exec_sell_price = position['current_price'] * (1 - self.slippage_rate)
            gross_sell_amount = position['shares'] * exec_sell_price
            commission = gross_sell_amount * commission_rate
            tax = gross_sell_amount * tax_rate
            total_costs = commission + tax
            net_sell_amount = gross_sell_amount - total_costs
            self.cash += net_sell_amount
            
            # 매도 기록
            self.trade_history.append({
                'date': current_date,
                'ticker': ticker,
                'action': 'SELL_LOSS',
                'shares': position['shares'],
                'price': position['current_price'],
                'exec_price': exec_sell_price,
                'amount': net_sell_amount,
                'gross_amount': gross_sell_amount,
                'commission': commission,
                'tax': tax,
                'fee': total_costs,
                'return': (position['current_price'] - position['buy_price']) / position['buy_price']
            })
            
            del self.portfolio[ticker]
    
    def get_portfolio_value(self):
        """현재 포트폴리오 총 가치"""
        stock_value = sum(
            pos['shares'] * pos['current_price'] 
            for pos in self.portfolio.values()
        )
        return self.cash + stock_value
    
    def _fetch_period_close_prices(self, tickers: list, from_date: str, to_date: str) -> 'pd.DataFrame':
        """리밸런싱 구간 내 보유 종목 일별 종가 DataFrame 반환 (index=날짜, columns=티커)"""
        if not tickers:
            return pd.DataFrame()
        from_str = from_date.replace('-', '')
        to_str = to_date.replace('-', '')
        price_data: dict = {}
        for ticker in tickers:
            try:
                df = stock.get_market_ohlcv(from_str, to_str, ticker)
                if df is not None and not df.empty and '종가' in df.columns:
                    series = df['종가'].astype(float)
                    price_data[ticker] = series
                    # price_cache 업데이트 (재실행 시 재조회 방지)
                    for dt_idx, val in series.items():
                        ck = f"{dt_idx.strftime('%Y%m%d')}|{ticker}"
                        if ck not in self.price_cache:
                            self.price_cache[ck] = float(val)
            except Exception:
                pass
        if not price_data:
            return pd.DataFrame()
        result = pd.DataFrame(price_data)
        result = result.ffill()  # 거래 정지/결측치는 직전 종가로 채움
        return result

    def run_backtest(self):
        """백테스트 실행"""
        
        if not LIBRARIES_AVAILABLE:
            print("필요한 라이브러리가 설치되지 않았습니다.")
            return None
        
        print("\n" + "="*80)
        print("그린블라트 응용 전략 백테스트 시작")
        print("="*80)
        print(f"기간: {self.start_date} ~ {self.end_date}")
        print(f"초기자본: {self.initial_capital:,}원")
        print(f"투자비율: {self.investment_ratio*100}%")
        print(f"거래수수료: {self.commission_fee_rate*100:.2f}%")
        print(f"세금: {self.tax_rate*100:.2f}%")
        print(f"슬리피지: {self.slippage_bps} bps")
        print(f"보유종목수: {self.num_stocks}개")
        if self.rebalance_days is not None and self.rebalance_days > 0:
            print(f"리밸런싱주기: {self.rebalance_days}일")
        else:
            print(f"리밸런싱주기: {self.rebalance_months}개월")
        print(f"선정모드: {self.strategy_mode}")
        print(f"펀더멘털 소스: {self.fundamental_source}")
        if self.strategy_mode == 'mixed':
            print(f"필터프로파일: {self.mixed_filter_profile}")
        if self.kosdaq_target_ratio is not None:
            print(f"KOSDAQ 목표 비중: {self.kosdaq_target_ratio*100:.0f}%")
        print(f"손실매도: {'ON' if self.sell_losers_enabled else 'OFF'}")
        print("="*80)
        total_start = time.perf_counter()
        
        # 리밸런싱 날짜 생성 (일 단위가 설정되면 일 단위 우선)
        start = datetime.strptime(self.start_date, '%Y-%m-%d')
        end = datetime.strptime(self.end_date, '%Y-%m-%d')
        
        rebalance_dates = []
        current_date = pd.Timestamp(start)
        while current_date <= pd.Timestamp(end):
            rebalance_dates.append(current_date.strftime('%Y-%m-%d'))
            if self.rebalance_days is not None and self.rebalance_days > 0:
                current_date = current_date + pd.DateOffset(days=self.rebalance_days)
            else:
                current_date = current_date + pd.DateOffset(months=self.rebalance_months)
        
        # 백테스트 실행
        for i, rebal_date in enumerate(rebalance_dates):
            t_rebal_start = time.perf_counter()
            scheduled_date = rebal_date
            trading_date = self.selector.nearest_trading_date(scheduled_date)
            trading_date_fmt = datetime.strptime(trading_date, '%Y%m%d').strftime('%Y-%m-%d')

            if scheduled_date != trading_date_fmt:
                print(f"\n[{i+1}/{len(rebalance_dates)}] {scheduled_date} 리밸런싱 (실행일: {trading_date_fmt})")
            else:
                print(f"\n[{i+1}/{len(rebalance_dates)}] {scheduled_date} 리밸런싱")
            
            t_select_start = time.perf_counter()
            selected_stocks = self.selector.select_stocks(trading_date_fmt)
            self._log_timing('rebalance.select_stocks', time.perf_counter() - t_select_start)
            effective_date = trading_date_fmt
            
            if selected_stocks.empty:
                print("  선정 종목이 없습니다.")
                continue

            if self.capital_constrained_selection_enabled:
                holdings_map = {ticker: int(pos.get('shares', 0)) for ticker, pos in self.portfolio.items()}
                selected_stocks, alloc_meta = select_capital_constrained_stocks(
                    selected=selected_stocks,
                    holdings=holdings_map,
                    cash=self.cash,
                    investment_ratio=self.investment_ratio,
                    commission_fee_rate=self.commission_fee_rate,
                    max_stocks=self.capital_constrained_max_stocks,
                    min_stocks=self.capital_constrained_min_stocks,
                    slippage_rate=self.slippage_rate,
                )
                print(
                    "  [ALLOC] 자본제약 적용: "
                    f"before={int(alloc_meta['selected_before'])}, "
                    f"after={int(alloc_meta['selected_after'])}, "
                    f"k={int(alloc_meta['k_chosen'])}, "
                    f"주식당={float(alloc_meta['per_stock_amount']):,.0f}원"
                )
                if selected_stocks.empty:
                    print("  자본 제약으로 매수 가능한 종목이 없습니다.")
                    continue
            
            print(f"  선정 종목: {len(selected_stocks)}개")
            
            # 보유 종목 현재가 업데이트 (selected_stocks의 close 활용)
            if i > 0 and len(self.portfolio) > 0:
                price_map = selected_stocks.set_index('ticker')['close'].to_dict()
                for ticker, position in self.portfolio.items():
                    if ticker in price_map and price_map[ticker] > 0:
                        position['current_price'] = price_map[ticker]
            
            # 손실 종목 매도 (첫 리밸런싱 제외)
            if i > 0 and self.sell_losers_enabled:
                t_sell_start = time.perf_counter()
                self.sell_losers(effective_date)
                self._log_timing('rebalance.sell_losers', time.perf_counter() - t_sell_start)
            
            # 변동성 타게팅: 실현 변동성 기반으로 유효 투자비율 동적 조정
            vol_decision = compute_vol_target_ratio(
                portfolio_history=self.portfolio_history,
                base_ratio=self.investment_ratio,
                enabled=self.vol_target_enabled,
                sigma_target=self.vol_target_sigma,
                lookback_days=self.vol_target_lookback,
                min_ratio=self.vol_target_min_ratio,
            )
            if self.vol_target_enabled:
                print(
                    f"  [VOL-TARGET] reason={vol_decision.reason}, "
                    f"σ_realized={f'{vol_decision.sigma_realized*100:.1f}%' if vol_decision.sigma_realized is not None else 'N/A'}, "
                    f"σ_target={vol_decision.sigma_target*100:.0f}%, "
                    f"multiplier={vol_decision.multiplier:.3f}, "
                    f"base={vol_decision.base_ratio:.4f} → effective={vol_decision.effective_ratio:.4f}"
                )
            effective_investment_ratio = vol_decision.effective_ratio

            # 리밸런싱
            t_exec_start = time.perf_counter()
            self.rebalance(selected_stocks, effective_date, investment_ratio=effective_investment_ratio)
            self._log_timing('rebalance.execute', time.perf_counter() - t_exec_start)
            
            # 포트폴리오 가치 기록
            portfolio_value = self.get_portfolio_value()
            self.portfolio_history.append({
                'date': effective_date,
                'portfolio_value': portfolio_value,
                'cash': self.cash,
                'stock_value': portfolio_value - self.cash,
                'num_holdings': len(self.portfolio),
                'return': (portfolio_value - self.initial_capital) / self.initial_capital
            })
            
            print(f"  포트폴리오 가치: {portfolio_value:,.0f}원 ({(portfolio_value/self.initial_capital-1)*100:.2f}%)")

            # 리밸런싱 구간 내 일별 포트폴리오 가치 기록 (MDD 계산 정확도 향상)
            if len(self.portfolio) > 0:
                next_boundary = rebalance_dates[i + 1] if i + 1 < len(rebalance_dates) else self.end_date
                tickers_held = list(self.portfolio.keys())
                shares_map = {t: self.portfolio[t]['shares'] for t in tickers_held}
                fallback_prices = {t: self.portfolio[t].get('current_price', 0.0) for t in tickers_held}

                prices_df = self._fetch_period_close_prices(tickers_held, effective_date, next_boundary)
                if not prices_df.empty:
                    already_recorded = {effective_date}
                    for dt_idx, row in prices_df.iterrows():
                        date_str = dt_idx.strftime('%Y-%m-%d') if hasattr(dt_idx, 'strftime') else str(dt_idx)[:10]
                        if date_str in already_recorded:
                            continue
                        already_recorded.add(date_str)
                        stock_val = sum(
                            shares_map.get(t, 0) * (
                                float(row[t]) if t in row.index and not pd.isna(row[t])
                                else fallback_prices.get(t, 0.0)
                            )
                            for t in tickers_held
                        )
                        total_val = self.cash + stock_val
                        self.portfolio_history.append({
                            'date': date_str,
                            'portfolio_value': total_val,
                            'cash': self.cash,
                            'stock_value': stock_val,
                            'num_holdings': len(self.portfolio),
                            'return': (total_val - self.initial_capital) / self.initial_capital
                        })

            self._log_timing('rebalance.total', time.perf_counter() - t_rebal_start)

        # 종료일 기준 최종 평가 추가 (리밸런싱일과 다를 수 있음)
        if len(self.portfolio_history) > 0:
            end_trading_date = self.selector.previous_trading_date(self.end_date)
            end_trading_date_fmt = datetime.strptime(end_trading_date, '%Y%m%d').strftime('%Y-%m-%d')
            last_recorded_date = self.portfolio_history[-1]['date']

            if end_trading_date_fmt != last_recorded_date:
                end_tickers = self.selector.get_market_tickers(end_trading_date_fmt)
                if len(end_tickers) > 0 and len(self.portfolio) > 0:
                    try:
                        # 종료일 가격만 업데이트 (종목 선정 아님) - selector의 Kiwoom 우선 경로 사용
                        end_df = self.selector._get_fundamental_and_cap(
                            end_trading_date_fmt.replace('-', ''),
                            markets=['KOSPI', 'KOSDAQ'],
                        )

                        if end_df is not None and not end_df.empty and 'ticker' in end_df.columns and 'close' in end_df.columns:
                            end_price_map = end_df.set_index('ticker')['close'].to_dict()
                            for ticker, position in self.portfolio.items():
                                if ticker in end_price_map and end_price_map[ticker] > 0:
                                    position['current_price'] = end_price_map[ticker]
                    except Exception:
                        pass

                final_value = self.get_portfolio_value()
                self.portfolio_history.append({
                    'date': end_trading_date_fmt,
                    'portfolio_value': final_value,
                    'cash': self.cash,
                    'stock_value': final_value - self.cash,
                    'num_holdings': len(self.portfolio),
                    'return': (final_value - self.initial_capital) / self.initial_capital
                })

            self.selector.persist_caches()
            self._log_timing('backtest.total', time.perf_counter() - total_start)
        
        return self.calculate_performance()
    
    def calculate_performance(self):
        """성과 분석"""
        
        if len(self.portfolio_history) == 0:
            return None
        
        df = pd.DataFrame(self.portfolio_history)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)  # 일별 기록 삽입 후 순서 보장

        # 수익률 계산
        final_value = df['portfolio_value'].iloc[-1]
        total_return = (final_value - self.initial_capital) / self.initial_capital
        
        # 연수 계산
        years = (df['date'].iloc[-1] - df['date'].iloc[0]).days / 365.25
        
        # 연평균성장률(CAGR) 계산
        cagr = (final_value / self.initial_capital) ** (1/years) - 1 if years > 0 else 0
        
        # MDD 계산
        cummax = df['portfolio_value'].cummax()
        drawdown = (df['portfolio_value'] - cummax) / cummax
        mdd = drawdown.min()
        
        # 승률 계산 (월별 기준 — 일별 노이즈 제거)
        _monthly_vals = df.set_index('date')['portfolio_value'].resample('ME').last().dropna()
        _monthly_rets = _monthly_vals.pct_change().dropna()
        win_rate = ((_monthly_rets > 0).sum() / len(_monthly_rets)
                    if len(_monthly_rets) > 0 else 0)

        # 샤프 비율 (연환산, 무위험수익률 3.5%)
        # 공식: (CAGR - Rf) / (일별 변동성 × √252)
        _rf = 0.035
        _daily_rets = df.set_index('date')['portfolio_value'].pct_change().dropna()
        _annual_vol = _daily_rets.std() * np.sqrt(252)
        sharpe = (cagr - _rf) / _annual_vol if _annual_vol > 0 else 0

        results = {
            'initial_capital': self.initial_capital,
            'final_value': final_value,
            'total_return_pct': total_return * 100,
            'cagr_pct': cagr * 100,
            'mdd_pct': mdd * 100,
            'win_rate_pct': win_rate * 100,
            'sharpe_ratio': sharpe,
            'years': years,
            'requested_start_date': self.start_date,
            'requested_end_date': self.end_date,
            'actual_start_date': df['date'].iloc[0].strftime('%Y-%m-%d'),
            'actual_end_date': df['date'].iloc[-1].strftime('%Y-%m-%d'),
            'num_trades': len(self.trade_history),
            'portfolio_df': df,
            'trades_df': pd.DataFrame(self.trade_history)
        }
        
        return results
    
    def print_results(self, results):
        """결과 출력"""
        
        if results is None:
            print("백테스트 결과가 없습니다.")
            return
        
        print("\n" + "="*80)
        print("백테스트 결과")
        print("="*80)
        print(f"초기 자본:    {results['initial_capital']:>15,}원")
        print(f"최종 자산:    {results['final_value']:>15,.0f}원")
        print(f"총 수익률:    {results['total_return_pct']:>15.2f}%")
        print(f"CAGR:         {results['cagr_pct']:>15.2f}%")
        print(f"MDD:          {results['mdd_pct']:>15.2f}%")
        print(f"승률:         {results['win_rate_pct']:>15.2f}%")
        print(f"샤프 비율:    {results['sharpe_ratio']:>15.2f}")
        print(f"요청 기간:    {results['requested_start_date']} ~ {results['requested_end_date']}")
        print(f"실행 기간:    {results['actual_start_date']} ~ {results['actual_end_date']}")
        print(f"백테스트 기간: {results['years']:>14.2f}년")
        print(f"총 거래 횟수:  {results['num_trades']:>15}회")
        print("="*80)
        
        # 연도별 수익률 (전년 말 포트폴리오 가치 대비 해당 연도 수익률)
        df = results['portfolio_df'].copy()
        df['year'] = df['date'].dt.year
        _yearly_vals = df.groupby('year')['portfolio_value'].last()
        _yearly_rets = _yearly_vals.pct_change() * 100
        # 첫 해는 초기자본 대비 계산
        _yearly_rets.iloc[0] = (_yearly_vals.iloc[0] / results['initial_capital'] - 1) * 100

        print("\n연도별 수익률:")
        print("-"*40)
        for year, ret in _yearly_rets.items():
            print(f"{year}: {ret:>10.2f}%")
        print("-"*40)


def main():
    """메인 실행 함수"""
    # 단일 실행: 사용자가 선택한 기준 적용
    import os
    os.makedirs('results', exist_ok=True)
    
    # 환경 변수에서 설정 로드
    try:
        commission_fee_rate = float(os.getenv('COMMISSION_FEE_RATE', '0.0015'))
        tax_rate = float(os.getenv('TAX_RATE', '0.002'))
        backtest_start_date = os.getenv('BACKTEST_START_DATE', '2025-01-01')
        backtest_end_date = os.getenv('BACKTEST_END_DATE', '2025-12-31')
        backtest_initial_capital = int(os.getenv('BACKTEST_INITIAL_CAPITAL', '5000000'))
    except ValueError:
        print("경고: .env 파일의 설정이 유효하지 않습니다. 기본값을 사용합니다.")
        commission_fee_rate = 0.0015
        tax_rate = 0.002
        backtest_start_date = '2025-01-01'
        backtest_end_date = '2025-12-31'
        backtest_initial_capital = 5000000

    print(f"로드된 설정: commission_fee_rate={commission_fee_rate*100:.2f}%, tax_rate={tax_rate*100:.2f}%")
    print(f"백테스트 기간: {backtest_start_date} ~ {backtest_end_date}")
    print(f"초기자본: {backtest_initial_capital:,}원")

    # CLI 파서: CLI 인자 > 환경변수(.env 포함) > 기본값
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--rebalance-months', '-r', type=int, help='리밸런싱 주기(개월). CLI가 우선 적용됩니다')
    parser.add_argument('--rebalance-days', type=int, help='리밸런싱 주기(일). 설정되면 월 단위보다 우선 적용됩니다')
    args, _ = parser.parse_known_args()

    if args.rebalance_months is not None:
        backtest_rebalance_months = int(args.rebalance_months)
    else:
        reb_env = os.getenv('REBALANCE_MONTHS') or os.getenv('LIVE_REBALANCE_MONTHS')
        if reb_env not in (None, ""):
            try:
                backtest_rebalance_months = int(reb_env)
            except Exception:
                backtest_rebalance_months = 3
        else:
            backtest_rebalance_months = 3

    if args.rebalance_days is not None:
        backtest_rebalance_days = int(args.rebalance_days)
    else:
        reb_days_env = os.getenv('REBALANCE_DAYS') or os.getenv('LIVE_REBALANCE_DAYS')
        if reb_days_env not in (None, ""):
            try:
                parsed_days = int(reb_days_env)
                backtest_rebalance_days = parsed_days if parsed_days > 0 else None
            except Exception:
                backtest_rebalance_days = None
        else:
            backtest_rebalance_days = None

    if backtest_rebalance_days is not None and backtest_rebalance_days > 0:
        rebalance_desc = f"{backtest_rebalance_days}d"
    else:
        rebalance_desc = f"{backtest_rebalance_months}m"
    print(f"Running single backtest with chosen baseline: momentum_weight=0.60, rebalance={rebalance_desc}, num_stocks=40")

    # 변동성 타게팅 환경변수
    backtest_vol_target_enabled = os.getenv('BACKTEST_VOL_TARGET_ENABLED', 'false').lower() in {'1', 'true', 'yes', 'y'}
    backtest_vol_target_sigma = float(os.getenv('BACKTEST_VOL_TARGET_SIGMA', '0.20'))
    backtest_vol_target_lookback = int(os.getenv('BACKTEST_VOL_TARGET_LOOKBACK', '20'))
    backtest_vol_target_min_ratio = float(os.getenv('BACKTEST_VOL_TARGET_MIN_RATIO', '0.30'))
    if backtest_vol_target_enabled:
        print(
            f"[VOL-TARGET] 활성화: σ_target={backtest_vol_target_sigma*100:.0f}%, "
            f"lookback={backtest_vol_target_lookback}일, min_ratio={backtest_vol_target_min_ratio:.0%}"
        )

    backtest = KoreaStockBacktest(
        start_date=backtest_start_date,
        end_date=backtest_end_date,
        initial_capital=backtest_initial_capital,
        investment_ratio=0.95,
        num_stocks=40,
        commission_fee_rate=commission_fee_rate,
        tax_rate=tax_rate,
        rebalance_months=backtest_rebalance_months,
        rebalance_days=backtest_rebalance_days,
        strategy_mode='mixed',
        mixed_filter_profile='large_cap',
        sell_losers_enabled=True,
        kosdaq_target_ratio=None,
        momentum_enabled=True,
        momentum_months=3,
        momentum_weight=0.60,
        momentum_filter_enabled=True,
        large_cap_min_mcap=None,
        vol_target_enabled=backtest_vol_target_enabled,
        vol_target_sigma=backtest_vol_target_sigma,
        vol_target_lookback=backtest_vol_target_lookback,
        vol_target_min_ratio=backtest_vol_target_min_ratio,
    )

    results = backtest.run_backtest()
    if results:
        backtest.print_results(results)
        results['portfolio_df'].to_csv('results/backtest_portfolio.csv', index=False, encoding='utf-8-sig')
        results['trades_df'].to_csv('results/backtest_trades.csv', index=False, encoding='utf-8-sig')
        print("\nSaved: results/backtest_portfolio.csv, results/backtest_trades.csv")


if __name__ == "__main__":
    main()
