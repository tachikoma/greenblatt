from __future__ import annotations

from collections import OrderedDict
from datetime import datetime, timedelta
import json
import os
import time

import numpy as np
import pandas as pd

try:
    from pykrx import stock
    LIBRARIES_AVAILABLE = True
except ImportError:
    LIBRARIES_AVAILABLE = False


class KoreaStockSelector:
    """백테스트/실전 공용 종목선정 엔진."""

    def __init__(
        self,
        *,
        num_stocks: int = 30,
        strategy_mode: str = "mixed",
        mixed_filter_profile: str = "aggressive_mid",
        kosdaq_target_ratio: float | None = None,
        momentum_enabled: bool = True,
        momentum_months: int = 6,
        momentum_weight: float = 0.1,
        momentum_filter_enabled: bool = False,
        large_cap_min_mcap: float | None = None,
        cache_dir: str = "results/cache",
        timing_enabled: bool = True,
        fundamental_cache_format: str = "parquet",
        fundamental_cache_max_entries: int = 16,
    ) -> None:
        self.num_stocks = num_stocks
        self.strategy_mode = strategy_mode
        self.mixed_filter_profile = mixed_filter_profile
        self.kosdaq_target_ratio = kosdaq_target_ratio
        self.momentum_enabled = momentum_enabled
        self.momentum_months = momentum_months
        self.momentum_weight = momentum_weight
        self.momentum_filter_enabled = momentum_filter_enabled
        self.large_cap_min_mcap = large_cap_min_mcap
        self.cache_dir = cache_dir
        self.timing_enabled = timing_enabled
        self.fundamental_cache_format = fundamental_cache_format
        self.fundamental_cache_max_entries = max(1, int(fundamental_cache_max_entries))
        self.cache_version = {
            "fundamental_cache_v": 2,
            "momentum_cache_v": 1,
            "strategy_mode": self.strategy_mode,
            "momentum_months": self.momentum_months,
            "cache_format": self.fundamental_cache_format,
        }

        self.industry_cache: dict[str, str] = {}
        self.momentum_cache: dict[str, float] = {}
        self.price_cache: dict[str, float] = {}
        self.fundamental_cache: OrderedDict[str, pd.DataFrame] = OrderedDict()

        self._load_caches()

    def _cache_paths(self):
        return {
            "industry": os.path.join(self.cache_dir, "industry_cache.json"),
            "momentum": os.path.join(self.cache_dir, "momentum_cache.json"),
            "price": os.path.join(self.cache_dir, "price_cache.json"),
            "fundamentals": os.path.join(self.cache_dir, "fundamentals"),
            "meta": os.path.join(self.cache_dir, "cache_meta.json"),
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
        fundamentals_dir = paths["fundamentals"]
        if not os.path.isdir(fundamentals_dir):
            return
        for file_name in os.listdir(fundamentals_dir):
            if file_name.endswith(".parquet") or file_name.endswith(".csv"):
                try:
                    os.remove(os.path.join(fundamentals_dir, file_name))
                except Exception:
                    pass

    def _fundamental_cache_file(self, date_str, market):
        paths = self._cache_paths()
        fundamentals_dir = paths["fundamentals"]
        preferred_ext = ".parquet" if self.fundamental_cache_format == "parquet" else ".csv"
        preferred = os.path.join(fundamentals_dir, f"{date_str}_{market}{preferred_ext}")
        alternate_ext = ".csv" if preferred_ext == ".parquet" else ".parquet"
        alternate = os.path.join(fundamentals_dir, f"{date_str}_{market}{alternate_ext}")
        return preferred, alternate

    def _load_fundamental_frame(self, date_str, market):
        preferred, alternate = self._fundamental_cache_file(date_str, market)
        for path in [preferred, alternate]:
            if not os.path.exists(path):
                continue
            try:
                if path.endswith(".parquet"):
                    return pd.read_parquet(path)
                return pd.read_csv(path)
            except Exception:
                continue
        return None

    def _save_fundamental_frame(self, date_str, market, df):
        preferred, _ = self._fundamental_cache_file(date_str, market)
        try:
            if preferred.endswith(".parquet"):
                df.to_parquet(preferred, index=False)
            else:
                df.to_csv(preferred, index=False, encoding="utf-8-sig")
            return
        except Exception:
            pass

        fallback = preferred.replace(".parquet", ".csv")
        try:
            df.to_csv(fallback, index=False, encoding="utf-8-sig")
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
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        return data
        except Exception:
            pass
        return {}

    def _save_json_cache(self, path, data):
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
        except Exception as e:
            print(f"캐시 저장 실패 ({path}): {e}")

    def _load_caches(self):
        try:
            os.makedirs(self.cache_dir, exist_ok=True)
            paths = self._cache_paths()
            os.makedirs(paths["fundamentals"], exist_ok=True)

            loaded_meta = self._load_json_cache(paths["meta"])
            if not self._validate_cache_version(loaded_meta):
                print("[CACHE] cache version mismatch detected, invalidating incompatible caches")
                self.momentum_cache = {}
                self.fundamental_cache = OrderedDict()
                self._purge_fundamental_disk_cache()

            self.industry_cache = self._load_json_cache(paths["industry"])
            if len(self.momentum_cache) == 0:
                self.momentum_cache = self._load_json_cache(paths["momentum"])
            self.price_cache = self._load_json_cache(paths["price"])
            print(
                f"[CACHE] loaded: industry={len(self.industry_cache):,}, "
                f"momentum={len(self.momentum_cache):,}, price={len(self.price_cache):,}"
            )
        except Exception as e:
            print(f"[CACHE] load failed: {e}")
            self.industry_cache = {}
            self.momentum_cache = {}
            self.price_cache = {}
            self.fundamental_cache = OrderedDict()

    def persist_caches(self):
        try:
            os.makedirs(self.cache_dir, exist_ok=True)
            paths = self._cache_paths()
            os.makedirs(paths["fundamentals"], exist_ok=True)
            self._save_json_cache(paths["industry"], self.industry_cache)
            self._save_json_cache(paths["momentum"], self.momentum_cache)
            self._save_json_cache(paths["price"], self.price_cache)
            self._save_json_cache(paths["meta"], self.cache_version)

            for cache_key, df in self.fundamental_cache.items():
                try:
                    parts = cache_key.split("|")
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

    def _get_fundamental_and_cap(self, date_str, markets=["KOSPI", "KOSDAQ"]):
        t_start = time.perf_counter()
        cache_hits = 0
        cache_misses = 0
        dfs_merged = []

        for market in markets:
            cache_key = f"{date_str}|{market}"

            if cache_key in self.fundamental_cache:
                df_cached = self.fundamental_cache[cache_key].copy()
                self.fundamental_cache.move_to_end(cache_key)
                if "market" not in df_cached.columns:
                    df_cached["market"] = market
                dfs_merged.append(df_cached)
                cache_hits += 1
                continue

            df_disk = self._load_fundamental_frame(date_str, market)
            if df_disk is not None:
                if "market" not in df_disk.columns:
                    df_disk["market"] = market
                self._set_fundamental_cache_lru(cache_key, df_disk)
                dfs_merged.append(df_disk)
                cache_hits += 1
                continue

            try:
                df_fund_mkt = stock.get_market_fundamental_by_ticker(date_str, market=market)
                df_cap_mkt = stock.get_market_cap_by_ticker(date_str, market=market)
                if not df_fund_mkt.empty and not df_cap_mkt.empty:
                    df_fund_mkt = df_fund_mkt.reset_index().rename(columns={"티커": "ticker"})
                    df_cap_mkt = df_cap_mkt.reset_index().rename(columns={"티커": "ticker", "종가": "close", "시가총액": "market_cap"})
                    merged = pd.merge(df_fund_mkt, df_cap_mkt[["ticker", "close", "market_cap"]], on="ticker", how="inner")
                    merged["market"] = market
                    self._set_fundamental_cache_lru(cache_key, merged)
                    dfs_merged.append(merged)
                    cache_misses += 1
            except Exception:
                pass

        self._log_timing(
            "fetch.fundamental_cap",
            time.perf_counter() - t_start,
            extra=f"hit={cache_hits}, miss={cache_misses}, markets={len(markets)}",
        )

        if len(dfs_merged) == 0:
            return pd.DataFrame()

        return pd.concat(dfs_merged, ignore_index=True)

    def _log_timing(self, label, elapsed_sec, extra=""):
        if not self.timing_enabled:
            return
        if extra:
            print(f"  [TIME] {label}: {elapsed_sec:.3f}s ({extra})")
        else:
            print(f"  [TIME] {label}: {elapsed_sec:.3f}s")

    def get_market_tickers(self, date=None):
        if not LIBRARIES_AVAILABLE:
            return []

        try:
            if date is None:
                date = datetime.now().strftime("%Y%m%d")
            else:
                date = date.replace("-", "")

            tickers_kospi = stock.get_market_ticker_list(date=date, market="KOSPI")
            tickers_kosdaq = stock.get_market_ticker_list(date=date, market="KOSDAQ")
            return list(set(tickers_kospi + tickers_kosdaq))
        except Exception as e:
            print(f"종목 리스트 조회 실패: {e}")
            return []

    def nearest_trading_date(self, date_str):
        try:
            return stock.get_nearest_business_day_in_a_week(date=date_str.replace("-", ""), prev=False)
        except Exception:
            dt = datetime.strptime(date_str.replace("-", ""), "%Y%m%d")
            for i in range(7):
                candidate = (dt + timedelta(days=i)).strftime("%Y%m%d")
                for market in ["KOSPI", "KOSDAQ"]:
                    try:
                        df_test = stock.get_market_fundamental_by_ticker(candidate, market=market)
                        if not df_test.empty and df_test.iloc[:, 0].sum() != 0:
                            return candidate
                    except Exception:
                        pass
            return date_str.replace("-", "")

    def previous_trading_date(self, date_str):
        try:
            return stock.get_nearest_business_day_in_a_week(date=date_str.replace("-", ""), prev=True)
        except Exception:
            dt = datetime.strptime(date_str.replace("-", ""), "%Y%m%d")
            for i in range(7):
                candidate = (dt - timedelta(days=i)).strftime("%Y%m%d")
                for market in ["KOSPI", "KOSDAQ"]:
                    try:
                        df_test = stock.get_market_fundamental_by_ticker(candidate, market=market)
                        if not df_test.empty and df_test.iloc[:, 0].sum() != 0:
                            return candidate
                    except Exception:
                        pass
            return date_str.replace("-", "")

    def _get_industry_info(self, tickers, date_str):
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
                        industry = info.get("업종", "기타")
                        industry_map[ticker] = industry
                        self.industry_cache[ticker] = industry
                    else:
                        industry_map[ticker] = "기타"
                        self.industry_cache[ticker] = "기타"
                    cache_misses += 1
                except Exception:
                    industry_map[ticker] = "기타"
                    self.industry_cache[ticker] = "기타"
                    cache_misses += 1

            if self.timing_enabled:
                print(f"    [CACHE] industry hit={cache_hits}, miss={cache_misses}")
            return industry_map
        except Exception:
            return {}

    def _pick_dividend_column(self, df):
        if "DIV" in df.columns:
            return "DIV"
        if "배당수익률" in df.columns:
            return "배당수익률"
        return None

    def screen_stocks_pykrx_roe(self, target_date, markets=["KOSPI", "KOSDAQ"]):
        if not LIBRARIES_AVAILABLE:
            return pd.DataFrame()

        try:
            date_str = target_date.replace("-", "")
            df = self._get_fundamental_and_cap(date_str, markets)

            if df.empty:
                print("    ROE 스크리닝: 펀더멘탈 데이터 없음")
                return pd.DataFrame()

            industry_map = self._get_industry_info(df["ticker"].tolist(), date_str)
            df["industry"] = df["ticker"].map(industry_map).fillna("기타")

            df = df[(df["PER"] > 0) & (df["PER"] < 20)]
            df = df[(df["PBR"] > 0.2) & (df["PBR"] < 10)]
            df = df[df["market_cap"] >= 5e10]

            if len(df) == 0:
                print("    ROE 스크리닝: 필터링 후 종목 없음")
                return pd.DataFrame()

            df["ROE"] = np.where(
                (df["EPS"] > 0) & (df["BPS"] > 0),
                (df["EPS"] / df["BPS"]) * 100,
                np.nan,
            )
            df = df[df["ROE"] >= 10]

            df["rank_per"] = df["PER"].rank(ascending=True, method="average", na_option="bottom")
            df["rank_pbr"] = df["PBR"].rank(ascending=True, method="average", na_option="bottom")
            df["rank_roe"] = df["ROE"].rank(ascending=False, method="average", na_option="bottom")
            df["total_rank"] = df["rank_per"] + df["rank_pbr"] + (df["rank_roe"] * 1.5)

            result = df.sort_values("total_rank", ascending=True).head(self.num_stocks)

            if len(result) > 0:
                print(f"      필터 후: {len(df)}개 → 선정: {len(result)}개")
                print(f"      PER {result['PER'].min():.1f}~{result['PER'].max():.1f} (평균 {result['PER'].mean():.2f})")
                print(f"      PBR {result['PBR'].min():.2f}~{result['PBR'].max():.2f} (평균 {result['PBR'].mean():.2f})")
                print(f"      ROE {result['ROE'].min():.1f}~{result['ROE'].max():.1f}% (평균 {result['ROE'].mean():.2f}%)")

            return result[["ticker", "PER", "PBR", "ROE", "total_rank", "close", "market_cap"]].copy()

        except Exception as e:
            print(f"    ROE 스크리닝 오류: {e}")
            return pd.DataFrame()

    def screen_stocks_mixed(self, target_date, markets=["KOSPI", "KOSDAQ"]):
        if not LIBRARIES_AVAILABLE:
            return pd.DataFrame()

        try:
            t_screen_start = time.perf_counter()
            date_str = target_date.replace("-", "")
            df = self._get_fundamental_and_cap(date_str, markets)

            if df.empty:
                print("    MIXED 스크리닝: 펀더멘탈 데이터 없음")
                return pd.DataFrame()

            if self.mixed_filter_profile == "large_cap":
                if self.large_cap_min_mcap is None:
                    df = df[(df["PER"] > 0) & (df["PBR"] > 0)]
                else:
                    try:
                        min_mcap = float(self.large_cap_min_mcap)
                    except Exception:
                        min_mcap = None

                    if min_mcap is None:
                        df = df[(df["PER"] > 0) & (df["PBR"] > 0)]
                    else:
                        df = df[(df["PER"] > 0) & (df["PBR"] > 0) & (df["market_cap"] >= min_mcap)]
            else:
                df = df[(df["PER"] > 0) & (df["PBR"] > 0) & (df["market_cap"] >= 5e10)]

            df["ROE"] = np.where(
                (df["EPS"] > 0) & (df["BPS"] > 0),
                (df["EPS"] / df["BPS"]) * 100,
                np.nan,
            )
            df = df[df["ROE"] >= 10]

            dividend_col = self._pick_dividend_column(df)
            if dividend_col is None:
                df["DIV_YIELD"] = 0.0
            else:
                df["DIV_YIELD"] = pd.to_numeric(df[dividend_col], errors="coerce").fillna(0.0)

            if len(df) == 0:
                print("    MIXED 스크리닝: 품질 필터 후 종목 없음")
                return pd.DataFrame()

            if self.mixed_filter_profile == "large_cap":
                cap_lower_limit = df["market_cap"].quantile(0.80)
                df = df[df["market_cap"] >= cap_lower_limit]
                if self.large_cap_min_mcap is not None:
                    try:
                        min_mcap = float(self.large_cap_min_mcap)
                        df = df[df["market_cap"] >= min_mcap]
                    except Exception:
                        pass
            elif self.mixed_filter_profile == "aggressive":
                df = df[df["PBR"] < 10]
                df["mcap_cut"] = df.groupby("market")["market_cap"].transform(lambda s: s.quantile(0.20))
                df = df[df["market_cap"] <= df["mcap_cut"]]
            elif self.mixed_filter_profile == "aggressive_mid":
                df = df[df["PBR"] < 10]
                df["mcap_cut"] = df.groupby("market")["market_cap"].transform(lambda s: s.quantile(0.30))
                df = df[df["market_cap"] <= df["mcap_cut"]]
            else:
                df["per_cut"] = df.groupby("market")["PER"].transform(lambda s: s.quantile(0.40))
                df["pbr_cut"] = df.groupby("market")["PBR"].transform(lambda s: s.quantile(0.40))
                df["roe_cut"] = df.groupby("market")["ROE"].transform(lambda s: s.quantile(0.60))
                df["mcap_cut"] = df.groupby("market")["market_cap"].transform(
                    lambda s: s.quantile(0.50 if s.name == "KOSPI" else 0.70)
                )

                df = df[
                    (df["PER"] <= df["per_cut"])
                    & (df["PBR"] <= df["pbr_cut"])
                    & (df["ROE"] >= df["roe_cut"])
                    & (df["market_cap"] <= df["mcap_cut"])
                ]

            if len(df) == 0:
                print("    MIXED 스크리닝: 시장별 분위수 필터 후 종목 없음")
                return pd.DataFrame()

            df["rank_per_norm"] = df.groupby("market")["PER"].rank(ascending=True, pct=True, method="average")
            df["rank_pbr_norm"] = df.groupby("market")["PBR"].rank(ascending=True, pct=True, method="average")
            df["rank_roe_norm"] = df.groupby("market")["ROE"].rank(ascending=False, pct=True, method="average")
            df["rank_div_norm"] = df.groupby("market")["DIV_YIELD"].rank(ascending=False, pct=True, method="average")

            value_score = (df["rank_per_norm"] + df["rank_pbr_norm"]) / 2

            if self.momentum_enabled:
                t_mom_start = time.perf_counter()
                moms = []
                cache_hit = 0
                cache_miss = 0
                try:
                    end_dt = pd.to_datetime(date_str)
                    start_dt = (end_dt - pd.DateOffset(months=self.momentum_months)).strftime("%Y%m%d")
                    end_dt_str = end_dt.strftime("%Y%m%d")
                except Exception:
                    start_dt = date_str
                    end_dt_str = date_str

                for ticker in df["ticker"]:
                    cache_key = f"{ticker}|{start_dt}|{end_dt_str}"
                    if cache_key in self.momentum_cache:
                        moms.append(self.momentum_cache[cache_key])
                        cache_hit += 1
                        continue
                    try:
                        ohlc = stock.get_market_ohlcv(start_dt, end_dt_str, ticker)
                        if ohlc is not None and not ohlc.empty:
                            first = ohlc["종가"].iloc[0]
                            last = ohlc["종가"].iloc[-1]
                            mom = (last / first) - 1 if first > 0 else 0.0
                        else:
                            mom = 0.0
                    except Exception:
                        mom = 0.0
                    self.momentum_cache[cache_key] = float(mom)
                    cache_miss += 1
                    moms.append(mom)

                self._log_timing(
                    "mixed.momentum",
                    time.perf_counter() - t_mom_start,
                    extra=f"hit={cache_hit}, miss={cache_miss}, universe={len(df)}",
                )

                df["mom"] = moms
                if self.momentum_filter_enabled:
                    df = df[df["mom"] > 0]
                df["rank_mom_norm"] = df.groupby("market")["mom"].rank(ascending=False, pct=True, method="average")
            else:
                df["rank_mom_norm"] = 0.0

            if self.mixed_filter_profile == "large_cap":
                if self.momentum_enabled:
                    m = float(self.momentum_weight)
                    value_w = 0.20
                    mom_w = m
                    roe_w = 1.0 - value_w - mom_w
                    if roe_w < 0:
                        roe_w = 0.0
                        value_w = max(0.0, 1.0 - mom_w)

                    df["total_rank"] = (
                        value_w * value_score
                        + roe_w * df["rank_roe_norm"]
                        + mom_w * df["rank_mom_norm"]
                    )
                else:
                    df["total_rank"] = 0.40 * value_score + 0.60 * df["rank_roe_norm"]
            else:
                if self.momentum_enabled:
                    m = float(self.momentum_weight)
                    roe_w = 0.7 * (1 - m)
                    div_w = 0.3 * (1 - m)
                    mom_w = m
                    quality_score = (
                        (roe_w * df["rank_roe_norm"])
                        + (div_w * df["rank_div_norm"])
                        + (mom_w * df["rank_mom_norm"])
                    )
                else:
                    quality_score = (0.7 * df["rank_roe_norm"]) + (0.3 * df["rank_div_norm"])

                df["total_rank"] = (0.35 * value_score) + (0.65 * quality_score)

            df_sorted = df.sort_values("total_rank", ascending=True)

            if self.kosdaq_target_ratio is not None:
                kosdaq_target = int(self.num_stocks * self.kosdaq_target_ratio)
                kospi_target = self.num_stocks - kosdaq_target

                kosdaq_pool = df_sorted[df_sorted["market"] == "KOSDAQ"]
                kospi_pool = df_sorted[df_sorted["market"] == "KOSPI"]

                selected = []
                if kosdaq_target > 0:
                    selected.append(kosdaq_pool.head(kosdaq_target))
                if kospi_target > 0:
                    selected.append(kospi_pool.head(kospi_target))

                if len(selected) > 0:
                    result = pd.concat(selected, ignore_index=True)
                else:
                    result = df_sorted.head(self.num_stocks)

                if len(result) < self.num_stocks:
                    remaining = self.num_stocks - len(result)
                    chosen = set(result["ticker"])
                    extra = df_sorted[~df_sorted["ticker"].isin(chosen)].head(remaining)
                    if not extra.empty:
                        result = pd.concat([result, extra], ignore_index=True)

                result = result.head(self.num_stocks)
            else:
                result = df_sorted.head(self.num_stocks)

            if len(result) > 0:
                market_counts = result["market"].value_counts().to_dict()
                print(f"      [MIXED-{self.mixed_filter_profile}] 필터 후: {len(df)}개 → 선정: {len(result)}개")
                print(f"      [MIXED] 시장구성: {market_counts}")
                print(f"      [MIXED] PER 평균 {result['PER'].mean():.2f}, PBR 평균 {result['PBR'].mean():.2f}")
                print(f"      [MIXED] ROE 평균 {result['ROE'].mean():.2f}%, 배당수익률 평균 {result['DIV_YIELD'].mean():.2f}%")

            self._log_timing("mixed.total", time.perf_counter() - t_screen_start)

            return result[["ticker", "market", "PER", "PBR", "ROE", "DIV_YIELD", "total_rank", "close", "market_cap"]].copy()

        except Exception as e:
            print(f"    MIXED 스크리닝 오류: {e}")
            return pd.DataFrame()

    def select_stocks(self, trading_date: str) -> pd.DataFrame:
        if self.strategy_mode == "mixed":
            return self.screen_stocks_mixed(trading_date)
        return self.screen_stocks_pykrx_roe(trading_date)
