from __future__ import annotations

from dataclasses import dataclass
import os
from defaults import DEFAULT_REBALANCE_MONTHS

try:
    from dotenv import find_dotenv, load_dotenv
    from utils.env import env_get
except ImportError:  # pragma: no cover
    find_dotenv = None
    load_dotenv = None


@dataclass(slots=True)
class LiveTradingConfig:
    mode: str = "mock"
    appkey: str = ""
    secretkey: str = ""
    account_no: str = ""
    investment_ratio: float = 0.95
    num_stocks: int = 40
    rebalance_days: int | None = None
    rebalance_months: int = DEFAULT_REBALANCE_MONTHS
    strategy_mode: str = "mixed"
    mixed_filter_profile: str = "large_cap"
    momentum_enabled: bool = True
    momentum_months: int = 3
    momentum_weight: float = 0.60
    momentum_filter_enabled: bool = True
    large_cap_min_mcap: float | None = None
    fundamental_source: str = "pykrx"
    commission_fee_rate: float = 0.00015
    tax_rate: float = 0.002
    order_timeout_minutes: int = 3
    order_price_offset_bps: int = 10
    order_endpoint: str = "/api/dostk/ordr"
    # 레거시 단일 API ID (하위 호환성 유지를 위해 유지)
    order_api_id: str = "kt10000"
    # 증권사에서 매수/매도에 서로 다른 TR이 필요한 경우를 위한 별도 API ID
    order_buy_api_id: str = "kt10000"
    order_sell_api_id: str = "kt10001"
    # Separate API ID for 정정(수정) TR
    order_modify_api_id: str = "kt10002"
    order_status_endpoint: str = "/api/dostk/acnt"
    order_status_api_id: str = "kt00007"
    order_cancel_endpoint: str = "/api/dostk/ordr"
    # 취소 TR 기본값은 kt10003
    order_cancel_api_id: str = "kt10003"
    quote_endpoint: str = "/api/dostk/mrkcond"
    quote_api_id: str = "ka10004"
    quote_market_type: str = "0"
    use_hoga_retry_price: bool = True
    log_quote_response: bool = False
    retry_price_offset_bps: int = 25
    balance_endpoint: str = "/api/dostk/acnt"
    balance_api_id: str = "kt00018"
    # kt00001 예수금상세현황요청 API ID (주문가능금액/예수금 정밀 조회)
    deposit_api_id: str = "kt00001"
    retry_order_type: str = "03"
    # 공통 처리용 Kiwoom return_code 설정
    # - 예: LIVE_FALLBACK_TO_MARKET_CODES='4027,4080' : 해당 코드 발생 시 호출자에서 시장가 재시도 권장
    # - 예: LIVE_IGNORABLE_RETURN_CODES='4033' : 해당 코드는 무시(비치명적)하도록 테스트/통합에서 사용
    fallback_to_market_return_codes: tuple[int, ...] = (4027,)
    ignorable_return_codes: tuple[int, ...] = (4033,)
    # 주문유형(`trde_tp`) 참고(일반적인 값 - 실제 값은 증권사/키움 문서를 확인하세요):
    # - '0' or '00' : 보통 / 지정가 (지정가 주문)
    # - '3' or '03' : 시장가
    # - 특수지시(IOC, FAK 등)는 증권사마다 코드가 다를 수 있음 (예: '05', '07' 등)
    # 주의: 이 프로젝트에서는 실거래 모드에서 `order_type` 인자를 그대로 `trde_tp`로 전달합니다.
    # 실제 운영 전에는 반드시 증권사(또는 키움) API 문서를 확인해 정확한 코드를 설정하세요.
    max_retry_rounds: int = 5
    # True이면 주문 제출 전 가격 반올림 시 API에서 제공하는 틱 사이즈(사용 가능 시)를 사용하려고 시도합니다.
    # 기본값: False (내부 밴드 사용)
    use_api_tick_when_available: bool = False
    order_time_wait_enabled: bool = True
    order_time_hhmm: str = "15:20"  # 주문 제출 목표 시각 (KST, 예: 장시작 09:00 / 동시호가 15:20)
    order_time_grace_seconds: int = 0  # 목표 시각 이후 추가 대기 (초)
    save_daily_report: bool = True
    report_dir: str = "results/live_reports"
    rebalance_guard_enabled: bool = True
    run_state_path: str = "results/live_state/rebalance_state.json"
    run_lock_path: str = "results/live_state/rebalance.lock"
    debug_signal_enabled: bool = False
    debug_max_rows: int = 50
    dry_run_enabled: bool = False
    # True면 선정 종목 중 자본 제약(최소 1주 매수 가능)을 만족하는 최대 종목 수를 자동 선택합니다.
    capital_constrained_selection_enabled: bool = True
    # 자본 제약 자동 선택 시 최소/최대 종목 수 범위
    capital_constrained_min_stocks: int = 20
    capital_constrained_max_stocks: int = 40
    # 기존 보유 포지션 처리 정책
    # 'sell' (기본): 미선정 종목 전량 매도 후 재투자
    # 'hold': 미선정 종목은 그대로 유지, 선정 종목만 목표 수량으로 리밸런싱
    # 'adopt' (미구현): 기존 보유 종목을 이 전략의 포지션으로 간주하여 편입
    # 'rebalance' (미구현): 점진적 재조정 (한 번에 전량 교체하지 않음)
    existing_positions_policy: str = "sell"
    # 주문 제출 관련 설정
    order_submit_delay_seconds: float = 0.1
    # OrderWatch 관련 설정
    order_fill_poll_interval: float = 0.5
    order_fill_timeout_seconds: float = 60.0
    order_fill_max_amend: int = 2
    order_fill_amend_strategy: str = "reduce_price"
    order_fill_initial_wait_seconds: float = 5.0
    order_watch_start_retries: int = 3
    order_watch_start_backoff_seconds: float = 0.5
    # 공통 요청 재시도 설정 (기본값 하나로 통합)
    common_request_retries: int = 3
    common_request_retry_backoff_seconds: float = 0.5
    # Kiwoom additional endpoints for fundamentals
    fund_endpoint: str = "/api/dostk/stkinfo"
    fund_api_id: str = "ka10001"
    dotenv_path: str = ""
    # 변동성 타게팅: 포트폴리오 실현 변동성 기반 투자비율 동적 조정
    vol_target_enabled: bool = True
    vol_target_sigma: float = 0.28
    vol_target_lookback: int = 60
    vol_target_min_ratio: float = 0.65
    # True이면 fills 히스토리가 lookback에 미달할 때 pykrx 과거 주가로 워밍업 히스토리를 보완한다.
    # 실거래 첫날부터 vol-targeting이 정상 발동된다.
    vol_target_warmup_enabled: bool = True    # 종목 선정 시 몇 거래일 전 종가를 기준으로 할지 지정한다. (기본값 0 = 당일)
    # 1로 설정하면 T-1 확정 종가를 사용하므로 장중 현재가 오염을 차단한다.
    signal_date_lag: int = 0
    @property
    def is_mock(self) -> bool:
        return self.mode.lower() == "mock"

    def validate(self) -> list[str]:
        missing: list[str] = []
        if not self.appkey.strip():
            missing.append("KIWOOM_APPKEY")
        if not self.secretkey.strip():
            missing.append("KIWOOM_SECRETKEY")
        return missing

    @classmethod
    def from_env(cls) -> "LiveTradingConfig":
        dotenv_path = ""
        if load_dotenv is not None:
            if find_dotenv is not None:
                dotenv_path = find_dotenv(usecwd=True) or find_dotenv()
            if dotenv_path:
                load_dotenv(dotenv_path=dotenv_path, override=False)
            else:
                load_dotenv(override=False)

        # 환경 변수에서 공통 재시도 횟수/백오프 값을 계산합니다.
        # 통합 키(`COMMON_REQUEST_RETRIES`, `COMMON_REQUEST_RETRY_BACKOFF_SECONDS`)를 우선 사용하고,
        # 없으면 기존 LIVE_* 레거시 키를 폴백으로 사용합니다.
        common_req_retries = int(env_get("COMMON_REQUEST_RETRIES", fallback_keys=["LIVE_COMMON_REQUEST_RETRIES"], default="3"))
        common_req_backoff = float(env_get("COMMON_REQUEST_RETRY_BACKOFF_SECONDS", fallback_keys=["LIVE_COMMON_REQUEST_RETRY_BACKOFF_SECONDS"], default="0.5"))
        # REBALANCE_DAYS가 있으면 일 단위 리밸런싱을 우선 적용합니다.
        reb_days_env = env_get("REBALANCE_DAYS", fallback_keys=["REBALANCE_DAYS", "LIVE_REBALANCE_DAYS"])
        rebalance_days_val: int | None = None
        if reb_days_env not in (None, ""):
            try:
                parsed_days = int(reb_days_env)
                if parsed_days > 0:
                    rebalance_days_val = parsed_days
            except Exception:
                rebalance_days_val = None

        # REBALANCE_MONTHS 환경변수 우선 지원 (백테스트/라이브 공통)
        reb_env = env_get("REBALANCE_MONTHS", fallback_keys=["REBALANCE_MONTHS", "LIVE_REBALANCE_MONTHS"], default=str(DEFAULT_REBALANCE_MONTHS))
        try:
            rebalance_months_val = int(reb_env)
        except Exception:
            rebalance_months_val = int(str(DEFAULT_REBALANCE_MONTHS))

        return cls(
            mode=env_get("KIWOOM_MODE", fallback_keys=["KIWOOM_MODE"], default="mock").lower(),
            appkey=env_get("KIWOOM_APPKEY", default=""),
            secretkey=env_get("KIWOOM_SECRETKEY", default=""),
            account_no=env_get("KIWOOM_ACCOUNT_NO", default=""),
            investment_ratio=float(env_get("INVESTMENT_RATIO", fallback_keys=["LIVE_INVESTMENT_RATIO"], default="0.95")),
            num_stocks=int(env_get("NUM_STOCKS", fallback_keys=["LIVE_NUM_STOCKS"], default="40")),
            rebalance_days=rebalance_days_val,
            rebalance_months=rebalance_months_val,
            strategy_mode=env_get("STRATEGY_MODE", fallback_keys=["LIVE_STRATEGY_MODE"], default="mixed"),
            mixed_filter_profile=env_get("MIXED_FILTER_PROFILE", fallback_keys=["LIVE_MIXED_FILTER_PROFILE"], default="large_cap"),
            momentum_enabled=str(env_get("MOMENTUM_ENABLED", fallback_keys=["LIVE_MOMENTUM_ENABLED"], default="true")).lower() in {"1", "true", "yes", "y"},
            momentum_months=int(env_get("MOMENTUM_MONTHS", fallback_keys=["LIVE_MOMENTUM_MONTHS"], default="3")),
            momentum_weight=float(env_get("MOMENTUM_WEIGHT", fallback_keys=["LIVE_MOMENTUM_WEIGHT"], default="0.60")),
            momentum_filter_enabled=str(env_get("MOMENTUM_FILTER_ENABLED", fallback_keys=["LIVE_MOMENTUM_FILTER_ENABLED"], default="true")).lower() in {"1", "true", "yes", "y"},
            large_cap_min_mcap=float(env_get("LARGE_CAP_MIN_MCAP", fallback_keys=["LIVE_LARGE_CAP_MIN_MCAP"], default="")) if env_get("LARGE_CAP_MIN_MCAP", fallback_keys=["LIVE_LARGE_CAP_MIN_MCAP"], default="") else None,
            fundamental_source=env_get("FUNDAMENTAL_SOURCE", fallback_keys=["LIVE_FUNDAMENTAL_SOURCE"], default="pykrx").strip().lower(),
            commission_fee_rate=float(env_get("COMMISSION_FEE_RATE", fallback_keys=["LIVE_COMMISSION_FEE_RATE"], default="0.0015")),
            tax_rate=float(env_get("TAX_RATE", fallback_keys=["LIVE_TAX_RATE"], default="0.002")),
            order_timeout_minutes=int(env_get("ORDER_TIMEOUT_MINUTES", fallback_keys=["LIVE_ORDER_TIMEOUT_MINUTES"], default="3")),
            order_price_offset_bps=int(env_get("ORDER_PRICE_OFFSET_BPS", fallback_keys=["LIVE_ORDER_PRICE_OFFSET_BPS"], default="10")),
            order_endpoint=env_get("KIWOOM_ORDER_ENDPOINT", default="/api/dostk/ordr"),
            order_api_id=env_get("KIWOOM_ORDER_API_ID", default="kt10000"),
            order_buy_api_id=env_get("KIWOOM_ORDER_BUY_API_ID", fallback_keys=["KIWOOM_ORDER_API_ID"], default=env_get("KIWOOM_ORDER_API_ID", default="kt10000")),
            order_sell_api_id=env_get("KIWOOM_ORDER_SELL_API_ID", default="kt10001"),
            order_status_endpoint=env_get("KIWOOM_ORDER_STATUS_ENDPOINT", default="/api/dostk/acnt"),
            order_status_api_id=env_get("KIWOOM_ORDER_STATUS_API_ID", default="kt00007"),
            order_cancel_endpoint=env_get("KIWOOM_ORDER_CANCEL_ENDPOINT", default="/api/dostk/ordr"),
            order_modify_api_id=env_get("KIWOOM_ORDER_MODIFY_API_ID", default="kt10002"),
            order_cancel_api_id=env_get("KIWOOM_ORDER_CANCEL_API_ID", default="kt10003"),
            quote_endpoint=env_get("KIWOOM_QUOTE_ENDPOINT", default="/api/dostk/mrkcond"),
            quote_api_id=env_get("KIWOOM_QUOTE_API_ID", default="ka10004"),
            quote_market_type=env_get("KIWOOM_QUOTE_MARKET_TYPE", default="0"),
            use_hoga_retry_price=env_get("USE_HOGA_RETRY_PRICE", fallback_keys=["LIVE_USE_HOGA_RETRY_PRICE"], default="true").lower() in {"1", "true", "yes", "y"},
            log_quote_response=env_get("LOG_QUOTE_RESPONSE", fallback_keys=["LIVE_LOG_QUOTE_RESPONSE"], default="false").lower() in {"1", "true", "yes", "y"},
            retry_price_offset_bps=int(env_get("RETRY_PRICE_OFFSET_BPS", fallback_keys=["LIVE_RETRY_PRICE_OFFSET_BPS"], default="25")),
            retry_order_type=env_get("RETRY_ORDER_TYPE", fallback_keys=["LIVE_RETRY_ORDER_TYPE"], default="03"),
            use_api_tick_when_available=env_get("USE_API_TICK", fallback_keys=["LIVE_USE_API_TICK"], default="false").lower() in {"1", "true", "yes", "y"},
            max_retry_rounds=int(env_get("MAX_RETRY_ROUNDS", fallback_keys=["LIVE_MAX_RETRY_ROUNDS"], default="5")),
            balance_endpoint=env_get("KIWOOM_BALANCE_ENDPOINT", default="/api/dostk/acnt"),
            balance_api_id=env_get("KIWOOM_BALANCE_API_ID", default="kt00018"),
            deposit_api_id=env_get("KIWOOM_DEPOSIT_API_ID", default="kt00001"),
            order_time_wait_enabled=env_get("ORDER_TIME_WAIT_ENABLED", fallback_keys=["OPEN_WAIT_ENABLED", "LIVE_OPEN_WAIT_ENABLED"], default="true").lower() in {"1", "true", "yes", "y"},
            order_time_hhmm=env_get("ORDER_TIME_HHMM", fallback_keys=["MARKET_OPEN_HHMM", "LIVE_MARKET_OPEN_HHMM"], default="15:20"),
            order_time_grace_seconds=int(env_get("ORDER_TIME_GRACE_SECONDS", fallback_keys=["MARKET_OPEN_GRACE_SECONDS", "LIVE_MARKET_OPEN_GRACE_SECONDS"], default="0")),
            save_daily_report=env_get("SAVE_DAILY_REPORT", fallback_keys=["LIVE_SAVE_DAILY_REPORT"], default="true").lower() in {"1", "true", "yes", "y"},
            report_dir=env_get("REPORT_DIR", fallback_keys=["LIVE_REPORT_DIR"], default="results/live_reports"),
            rebalance_guard_enabled=env_get("REBALANCE_GUARD_ENABLED", fallback_keys=["LIVE_REBALANCE_GUARD_ENABLED"], default="true").lower() in {"1", "true", "yes", "y"},
            run_state_path=env_get("RUN_STATE_PATH", fallback_keys=["LIVE_RUN_STATE_PATH"], default="results/live_state/rebalance_state.json"),
            run_lock_path=env_get("RUN_LOCK_PATH", fallback_keys=["LIVE_RUN_LOCK_PATH"], default="results/live_state/rebalance.lock"),
            debug_signal_enabled=env_get("DEBUG_SIGNAL_ENABLED", fallback_keys=["LIVE_DEBUG_SIGNAL_ENABLED"], default="false").lower() in {"1", "true", "yes", "y"},
            debug_max_rows=int(env_get("DEBUG_MAX_ROWS", fallback_keys=["LIVE_DEBUG_MAX_ROWS"], default="50")),
            dry_run_enabled=env_get("DRY_RUN_ENABLED", fallback_keys=["LIVE_DRY_RUN_ENABLED"], default="false").lower() in {"1", "true", "yes", "y"},
            capital_constrained_selection_enabled=env_get("CAPITAL_CONSTRAINED_SELECTION_ENABLED", fallback_keys=["LIVE_CAPITAL_CONSTRAINED_SELECTION_ENABLED"], default="true").lower() in {"1", "true", "yes", "y"},
            capital_constrained_min_stocks=int(env_get("CAPITAL_CONSTRAINED_MIN_STOCKS", fallback_keys=["LIVE_CAPITAL_CONSTRAINED_MIN_STOCKS"], default="20")),
            capital_constrained_max_stocks=int(env_get("CAPITAL_CONSTRAINED_MAX_STOCKS", fallback_keys=["LIVE_CAPITAL_CONSTRAINED_MAX_STOCKS", "LIVE_NUM_STOCKS"], default="40")),
            existing_positions_policy=env_get("EXISTING_POSITIONS_POLICY", fallback_keys=["LIVE_EXISTING_POSITIONS_POLICY"], default="sell").strip().lower(),
            # common retries/backoff (computed above)
            common_request_retries=common_req_retries,
            common_request_retry_backoff_seconds=common_req_backoff,
            order_submit_delay_seconds=float(env_get("ORDER_SUBMIT_DELAY_SECONDS", fallback_keys=["LIVE_ORDER_SUBMIT_DELAY_SECONDS"], default="0.1")),
            order_fill_poll_interval=float(env_get("ORDER_FILL_POLL_INTERVAL", fallback_keys=["LIVE_ORDER_FILL_POLL_INTERVAL"], default="0.5")),
            order_fill_timeout_seconds=float(env_get("ORDER_FILL_TIMEOUT_SECONDS", fallback_keys=["LIVE_ORDER_FILL_TIMEOUT_SECONDS"], default="60.0")),
            order_fill_max_amend=int(env_get("ORDER_FILL_MAX_AMEND", fallback_keys=["LIVE_ORDER_FILL_MAX_AMEND"], default="2")),
            order_fill_amend_strategy=env_get("ORDER_FILL_AMEND_STRATEGY", fallback_keys=["LIVE_ORDER_FILL_AMEND_STRATEGY"], default="reduce_price"),
            order_fill_initial_wait_seconds=float(env_get("ORDER_FILL_INITIAL_WAIT_SECONDS", fallback_keys=["LIVE_ORDER_FILL_INITIAL_WAIT_SECONDS"], default="5.0")),
            order_watch_start_retries=int(env_get("ORDER_WATCH_START_RETRIES", fallback_keys=["LIVE_ORDER_WATCH_START_RETRIES"], default="3")),
            order_watch_start_backoff_seconds=float(env_get("ORDER_WATCH_START_BACKOFF_SECONDS", fallback_keys=["LIVE_ORDER_WATCH_START_BACKOFF_SECONDS"], default="0.5")),
            fund_endpoint=env_get("KIWOOM_FUND_ENDPOINT", default="/api/dostk/stkinfo"),
            fund_api_id=env_get("KIWOOM_FUND_API_ID", default="ka10001"),
            dotenv_path=dotenv_path,
            # 변동성 타게팅 설정
            vol_target_enabled=env_get("VOL_TARGET_ENABLED", fallback_keys=["LIVE_VOL_TARGET_ENABLED"], default="true").lower() in {"1", "true", "yes", "y"},
            vol_target_sigma=float(env_get("VOL_TARGET_SIGMA", fallback_keys=["LIVE_VOL_TARGET_SIGMA"], default="0.28")),
            vol_target_lookback=int(env_get("VOL_TARGET_LOOKBACK", fallback_keys=["LIVE_VOL_TARGET_LOOKBACK"], default="60")),
            vol_target_min_ratio=float(env_get("VOL_TARGET_MIN_RATIO", fallback_keys=["LIVE_VOL_TARGET_MIN_RATIO"], default="0.65")),
            vol_target_warmup_enabled=env_get("VOL_TARGET_WARMUP_ENABLED", fallback_keys=["LIVE_VOL_TARGET_WARMUP_ENABLED"], default="true").lower() in {"1", "true", "yes", "y"},
            signal_date_lag=int(env_get("SIGNAL_DATE_LAG", fallback_keys=["LIVE_SIGNAL_DATE_LAG"], default="1")),
            # 쉼표로 구분된 return_code 리스트를 파싱합니다
            fallback_to_market_return_codes=tuple(
                int(x.strip()) for x in (env_get("FALLBACK_TO_MARKET_CODES", fallback_keys=["LIVE_FALLBACK_TO_MARKET_CODES"], default="4027").split(",")) if x.strip()
            ),
            ignorable_return_codes=tuple(
                int(x.strip()) for x in (env_get("IGNORABLE_RETURN_CODES", fallback_keys=["LIVE_IGNORABLE_RETURN_CODES"], default="4033").split(",")) if x.strip()
            ),
        )
