from __future__ import annotations

from dataclasses import dataclass
import os
from defaults import DEFAULT_REBALANCE_MONTHS

try:
    from dotenv import find_dotenv, load_dotenv
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
    fundamental_source: str = "kiwoom"
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
    open_wait_enabled: bool = True
    market_open_hhmm: str = "09:00"
    market_open_grace_seconds: int = 30
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
    vol_target_enabled: bool = False
    vol_target_sigma: float = 0.20
    vol_target_lookback: int = 20
    vol_target_min_ratio: float = 0.30

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
        # 명시적 LIVE_COMMON_REQUEST_* 환경변수를 우선 사용하고, 없으면 기본값으로 대체합니다.
        cr_retries_env = os.getenv("LIVE_COMMON_REQUEST_RETRIES")
        common_req_retries = int(cr_retries_env) if cr_retries_env not in (None, "") else int(os.getenv("LIVE_COMMON_REQUEST_RETRIES", "3"))

        cr_backoff_env = os.getenv("LIVE_COMMON_REQUEST_RETRY_BACKOFF_SECONDS")
        common_req_backoff = float(cr_backoff_env) if cr_backoff_env not in (None, "") else float(os.getenv("LIVE_COMMON_REQUEST_RETRY_BACKOFF_SECONDS", "0.5"))
        # REBALANCE_DAYS가 있으면 일 단위 리밸런싱을 우선 적용합니다.
        reb_days_env = os.getenv("REBALANCE_DAYS")
        if reb_days_env is None or reb_days_env == "":
            reb_days_env = os.getenv("LIVE_REBALANCE_DAYS")
        rebalance_days_val: int | None = None
        if reb_days_env not in (None, ""):
            try:
                parsed_days = int(reb_days_env)
                if parsed_days > 0:
                    rebalance_days_val = parsed_days
            except Exception:
                rebalance_days_val = None

        # REBALANCE_MONTHS 환경변수 우선 지원 (백테스트/라이브 공통)
        reb_env = os.getenv("REBALANCE_MONTHS")
        if reb_env is None or reb_env == "":
            reb_env = os.getenv("LIVE_REBALANCE_MONTHS", str(DEFAULT_REBALANCE_MONTHS))
        try:
            rebalance_months_val = int(reb_env)
        except Exception:
            rebalance_months_val = int(os.getenv("LIVE_REBALANCE_MONTHS", str(DEFAULT_REBALANCE_MONTHS)))

        return cls(
            mode=os.getenv("KIWOOM_MODE", "mock").lower(),
            appkey=os.getenv("KIWOOM_APPKEY", ""),
            secretkey=os.getenv("KIWOOM_SECRETKEY", ""),
            account_no=os.getenv("KIWOOM_ACCOUNT_NO", ""),
            investment_ratio=float(os.getenv("LIVE_INVESTMENT_RATIO", "0.95")),
            num_stocks=int(os.getenv("LIVE_NUM_STOCKS", "40")),
            rebalance_days=rebalance_days_val,
            rebalance_months=rebalance_months_val,
            strategy_mode=os.getenv("LIVE_STRATEGY_MODE", "mixed"),
            mixed_filter_profile=os.getenv("LIVE_MIXED_FILTER_PROFILE", "large_cap"),
            momentum_enabled=os.getenv("LIVE_MOMENTUM_ENABLED", "true").lower() in {"1", "true", "yes", "y"},
            momentum_months=int(os.getenv("LIVE_MOMENTUM_MONTHS", "3")),
            momentum_weight=float(os.getenv("LIVE_MOMENTUM_WEIGHT", "0.60")),
            momentum_filter_enabled=os.getenv("LIVE_MOMENTUM_FILTER_ENABLED", "true").lower() in {"1", "true", "yes", "y"},
            large_cap_min_mcap=float(os.getenv("LIVE_LARGE_CAP_MIN_MCAP")) if os.getenv("LIVE_LARGE_CAP_MIN_MCAP") else None,
            fundamental_source=os.getenv("LIVE_FUNDAMENTAL_SOURCE", "pykrx").strip().lower(),
            commission_fee_rate=float(os.getenv("LIVE_COMMISSION_FEE_RATE", "0.0015")),
            tax_rate=float(os.getenv("LIVE_TAX_RATE", "0.002")),
            order_timeout_minutes=int(os.getenv("LIVE_ORDER_TIMEOUT_MINUTES", "3")),
            order_price_offset_bps=int(os.getenv("LIVE_ORDER_PRICE_OFFSET_BPS", "10")),
            order_endpoint=os.getenv("KIWOOM_ORDER_ENDPOINT", "/api/dostk/ordr"),
            order_api_id=os.getenv("KIWOOM_ORDER_API_ID", "kt10000"),
            order_buy_api_id=os.getenv("KIWOOM_ORDER_BUY_API_ID", os.getenv("KIWOOM_ORDER_API_ID", "kt10000")),
            order_sell_api_id=os.getenv("KIWOOM_ORDER_SELL_API_ID", "kt10001"),
            order_status_endpoint=os.getenv("KIWOOM_ORDER_STATUS_ENDPOINT", "/api/dostk/acnt"),
            order_status_api_id=os.getenv("KIWOOM_ORDER_STATUS_API_ID", "kt00007"),
            order_cancel_endpoint=os.getenv("KIWOOM_ORDER_CANCEL_ENDPOINT", "/api/dostk/ordr"),
            order_modify_api_id=os.getenv("KIWOOM_ORDER_MODIFY_API_ID", "kt10002"),
            order_cancel_api_id=os.getenv("KIWOOM_ORDER_CANCEL_API_ID", "kt10003"),
            quote_endpoint=os.getenv("KIWOOM_QUOTE_ENDPOINT", "/api/dostk/mrkcond"),
            quote_api_id=os.getenv("KIWOOM_QUOTE_API_ID", "ka10004"),
            quote_market_type=os.getenv("KIWOOM_QUOTE_MARKET_TYPE", "0"),
            use_hoga_retry_price=os.getenv("LIVE_USE_HOGA_RETRY_PRICE", "true").lower() in {"1", "true", "yes", "y"},
            log_quote_response=os.getenv("LIVE_LOG_QUOTE_RESPONSE", "false").lower() in {"1", "true", "yes", "y"},
            retry_price_offset_bps=int(os.getenv("LIVE_RETRY_PRICE_OFFSET_BPS", "25")),
            retry_order_type=os.getenv("LIVE_RETRY_ORDER_TYPE", "03"),
            use_api_tick_when_available=os.getenv("LIVE_USE_API_TICK", "false").lower() in {"1", "true", "yes", "y"},
            max_retry_rounds=int(os.getenv("LIVE_MAX_RETRY_ROUNDS", "5")),
            balance_endpoint=os.getenv("KIWOOM_BALANCE_ENDPOINT", "/api/dostk/acnt"),
            balance_api_id=os.getenv("KIWOOM_BALANCE_API_ID", "kt00018"),
            deposit_api_id=os.getenv("KIWOOM_DEPOSIT_API_ID", "kt00001"),
            open_wait_enabled=os.getenv("LIVE_OPEN_WAIT_ENABLED", "true").lower() in {"1", "true", "yes", "y"},
            market_open_hhmm=os.getenv("LIVE_MARKET_OPEN_HHMM", "09:00"),
            market_open_grace_seconds=int(os.getenv("LIVE_MARKET_OPEN_GRACE_SECONDS", "30")),
            save_daily_report=os.getenv("LIVE_SAVE_DAILY_REPORT", "true").lower() in {"1", "true", "yes", "y"},
            report_dir=os.getenv("LIVE_REPORT_DIR", "results/live_reports"),
            rebalance_guard_enabled=os.getenv("LIVE_REBALANCE_GUARD_ENABLED", "true").lower() in {"1", "true", "yes", "y"},
            run_state_path=os.getenv("LIVE_RUN_STATE_PATH", "results/live_state/rebalance_state.json"),
            run_lock_path=os.getenv("LIVE_RUN_LOCK_PATH", "results/live_state/rebalance.lock"),
            debug_signal_enabled=os.getenv("LIVE_DEBUG_SIGNAL_ENABLED", "false").lower() in {"1", "true", "yes", "y"},
            debug_max_rows=int(os.getenv("LIVE_DEBUG_MAX_ROWS", "50")),
            dry_run_enabled=os.getenv("LIVE_DRY_RUN_ENABLED", "false").lower() in {"1", "true", "yes", "y"},
            capital_constrained_selection_enabled=os.getenv("LIVE_CAPITAL_CONSTRAINED_SELECTION_ENABLED", "true").lower() in {"1", "true", "yes", "y"},
            capital_constrained_min_stocks=int(os.getenv("LIVE_CAPITAL_CONSTRAINED_MIN_STOCKS", "20")),
            capital_constrained_max_stocks=int(os.getenv("LIVE_CAPITAL_CONSTRAINED_MAX_STOCKS", os.getenv("LIVE_NUM_STOCKS", "40"))),
            existing_positions_policy=os.getenv("LIVE_EXISTING_POSITIONS_POLICY", "sell").strip().lower(),
            # common retries/backoff (computed above)
            common_request_retries=common_req_retries,
            common_request_retry_backoff_seconds=common_req_backoff,
            order_submit_delay_seconds=float(os.getenv("LIVE_ORDER_SUBMIT_DELAY_SECONDS", "0.1")),
            order_fill_poll_interval=float(os.getenv("LIVE_ORDER_FILL_POLL_INTERVAL", "0.5")),
            order_fill_timeout_seconds=float(os.getenv("LIVE_ORDER_FILL_TIMEOUT_SECONDS", "60.0")),
            order_fill_max_amend=int(os.getenv("LIVE_ORDER_FILL_MAX_AMEND", "2")),
            order_fill_amend_strategy=os.getenv("LIVE_ORDER_FILL_AMEND_STRATEGY", "reduce_price"),
            order_fill_initial_wait_seconds=float(os.getenv("LIVE_ORDER_FILL_INITIAL_WAIT_SECONDS", "5.0")),
            order_watch_start_retries=int(os.getenv("LIVE_ORDER_WATCH_START_RETRIES", "3")),
            order_watch_start_backoff_seconds=float(os.getenv("LIVE_ORDER_WATCH_START_BACKOFF_SECONDS", "0.5")),
            fund_endpoint=os.getenv("KIWOOM_FUND_ENDPOINT", "/api/dostk/stkinfo"),
            fund_api_id=os.getenv("KIWOOM_FUND_API_ID", "ka10001"),
            dotenv_path=dotenv_path,
            # 변동성 타게팅 설정
            vol_target_enabled=os.getenv("LIVE_VOL_TARGET_ENABLED", "false").lower() in {"1", "true", "yes", "y"},
            vol_target_sigma=float(os.getenv("LIVE_VOL_TARGET_SIGMA", "0.20")),
            vol_target_lookback=int(os.getenv("LIVE_VOL_TARGET_LOOKBACK", "20")),
            vol_target_min_ratio=float(os.getenv("LIVE_VOL_TARGET_MIN_RATIO", "0.30")),
            # 쉼표로 구분된 return_code 리스트를 파싱합니다
            fallback_to_market_return_codes=tuple(
                int(x.strip()) for x in (os.getenv("LIVE_FALLBACK_TO_MARKET_CODES", "4027").split(",")) if x.strip()
            ),
            ignorable_return_codes=tuple(
                int(x.strip()) for x in (os.getenv("LIVE_IGNORABLE_RETURN_CODES", "4033").split(",")) if x.strip()
            ),
        )
