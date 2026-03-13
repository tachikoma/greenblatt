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
    order_api_id: str = "kt10000"
    order_status_endpoint: str = "/api/dostk/acnt"
    order_status_api_id: str = "kt00007"
    order_cancel_endpoint: str = "/api/dostk/ordr"
    order_cancel_api_id: str = "kt10002"
    quote_endpoint: str = "/api/dostk/mrkcond"
    quote_api_id: str = "ka10004"
    quote_market_type: str = "0"
    use_hoga_retry_price: bool = True
    log_quote_response: bool = False
    retry_price_offset_bps: int = 25
    balance_endpoint: str = "/api/dostk/acnt"
    balance_api_id: str = "kt00018"
    retry_order_type: str = "03"
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
    # 주문 제출 관련 설정
    order_submit_delay_seconds: float = 0.1
    order_submit_retries: int = 3
    order_submit_retry_backoff_seconds: float = 0.5
    # Kiwoom additional endpoints for fundamentals/cap
    fund_endpoint: str = ""
    fund_api_id: str = ""
    fund_cap_endpoint: str = ""
    fund_cap_api_id: str = ""
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
            order_status_endpoint=os.getenv("KIWOOM_ORDER_STATUS_ENDPOINT", "/api/dostk/acnt"),
            order_status_api_id=os.getenv("KIWOOM_ORDER_STATUS_API_ID", "kt00007"),
            order_cancel_endpoint=os.getenv("KIWOOM_ORDER_CANCEL_ENDPOINT", "/api/dostk/ordr"),
            order_cancel_api_id=os.getenv("KIWOOM_ORDER_CANCEL_API_ID", "kt10002"),
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
            order_submit_delay_seconds=float(os.getenv("LIVE_ORDER_SUBMIT_DELAY_SECONDS", "0.1")),
            order_submit_retries=int(os.getenv("LIVE_ORDER_SUBMIT_RETRIES", "3")),
            order_submit_retry_backoff_seconds=float(os.getenv("LIVE_ORDER_SUBMIT_RETRY_BACKOFF_SECONDS", "0.5")),
            fund_endpoint=os.getenv("KIWOOM_FUND_ENDPOINT", ""),
            fund_api_id=os.getenv("KIWOOM_FUND_API_ID", ""),
            fund_cap_endpoint=os.getenv("KIWOOM_FUND_CAP_ENDPOINT", ""),
            fund_cap_api_id=os.getenv("KIWOOM_FUND_CAP_API_ID", ""),
            dotenv_path=dotenv_path,
        )
