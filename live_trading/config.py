from __future__ import annotations

from dataclasses import dataclass
import os

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
    rebalance_months: int = 3
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
    # legacy single api id (kept for backward compatibility)
    order_api_id: str = "kt10000"
    # Separate API IDs for buy/sell if broker requires different TRs
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
    # NOTE: 이 프로젝트에서는 실거래 모드에서 `order_type` 인자를 그대로 `trde_tp`로 전달합니다.
    # 실제 운영 전에는 반드시 증권사(또는 키움) API 문서를 확인해 정확한 코드를 설정하세요.
    max_retry_rounds: int = 2
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
    # existing positions handling policy when strategy starts against accounts
    # supported values: 'sell' (default, current behavior), 'respect_existing' (do not sell existing),
    # 'adopt' (treat existing as belonging to this strategy), 'rebalance' (gradual)
    existing_positions_policy: str = "sell"
    # 주문 제출 관련 설정
    order_submit_delay_seconds: float = 0.1
    # 공통 요청 재시도 설정 (기본값 하나로 통합)
    common_request_retries: int = 3
    common_request_retry_backoff_seconds: float = 0.5
    # Kiwoom additional endpoints for fundamentals
    fund_endpoint: str = "/api/dostk/stkinfo"
    fund_api_id: str = "ka10001"
    dotenv_path: str = ""

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

        # compute common retries/backoff from environment:
        # prefer explicit LIVE_COMMON_REQUEST_* env vars, fall back to defaults
        cr_retries_env = os.getenv("LIVE_COMMON_REQUEST_RETRIES")
        common_req_retries = int(cr_retries_env) if cr_retries_env not in (None, "") else int(os.getenv("LIVE_COMMON_REQUEST_RETRIES", "3"))

        cr_backoff_env = os.getenv("LIVE_COMMON_REQUEST_RETRY_BACKOFF_SECONDS")
        common_req_backoff = float(cr_backoff_env) if cr_backoff_env not in (None, "") else float(os.getenv("LIVE_COMMON_REQUEST_RETRY_BACKOFF_SECONDS", "0.5"))

        return cls(
            mode=os.getenv("KIWOOM_MODE", "mock").lower(),
            appkey=os.getenv("KIWOOM_APPKEY", ""),
            secretkey=os.getenv("KIWOOM_SECRETKEY", ""),
            account_no=os.getenv("KIWOOM_ACCOUNT_NO", ""),
            investment_ratio=float(os.getenv("LIVE_INVESTMENT_RATIO", "0.95")),
            num_stocks=int(os.getenv("LIVE_NUM_STOCKS", "40")),
            rebalance_months=int(os.getenv("LIVE_REBALANCE_MONTHS", "3")),
            strategy_mode=os.getenv("LIVE_STRATEGY_MODE", "mixed"),
            mixed_filter_profile=os.getenv("LIVE_MIXED_FILTER_PROFILE", "large_cap"),
            momentum_enabled=os.getenv("LIVE_MOMENTUM_ENABLED", "true").lower() in {"1", "true", "yes", "y"},
            momentum_months=int(os.getenv("LIVE_MOMENTUM_MONTHS", "3")),
            momentum_weight=float(os.getenv("LIVE_MOMENTUM_WEIGHT", "0.60")),
            momentum_filter_enabled=os.getenv("LIVE_MOMENTUM_FILTER_ENABLED", "true").lower() in {"1", "true", "yes", "y"},
            large_cap_min_mcap=float(os.getenv("LIVE_LARGE_CAP_MIN_MCAP")) if os.getenv("LIVE_LARGE_CAP_MIN_MCAP") else None,
            fundamental_source=os.getenv("LIVE_FUNDAMENTAL_SOURCE", "kiwoom").strip().lower(),
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
            max_retry_rounds=int(os.getenv("LIVE_MAX_RETRY_ROUNDS", "2")),
            balance_endpoint=os.getenv("KIWOOM_BALANCE_ENDPOINT", "/api/dostk/acnt"),
            balance_api_id=os.getenv("KIWOOM_BALANCE_API_ID", "kt00018"),
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
            existing_positions_policy=os.getenv("LIVE_EXISTING_POSITIONS_POLICY", "sell").strip().lower(),
            # common retries/backoff (computed above)
            common_request_retries=common_req_retries,
            common_request_retry_backoff_seconds=common_req_backoff,
            order_submit_delay_seconds=float(os.getenv("LIVE_ORDER_SUBMIT_DELAY_SECONDS", "0.1")),
            fund_endpoint=os.getenv("KIWOOM_FUND_ENDPOINT", "/api/dostk/stkinfo"),
            fund_api_id=os.getenv("KIWOOM_FUND_API_ID", "ka10001"),
            dotenv_path=dotenv_path,
            # parse comma-separated return_code lists
            fallback_to_market_return_codes=tuple(
                int(x.strip()) for x in (os.getenv("LIVE_FALLBACK_TO_MARKET_CODES", "4027").split(",")) if x.strip()
            ),
            ignorable_return_codes=tuple(
                int(x.strip()) for x in (os.getenv("LIVE_IGNORABLE_RETURN_CODES", "4033").split(",")) if x.strip()
            ),
        )
