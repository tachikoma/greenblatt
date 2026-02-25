from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(slots=True)
class LiveTradingConfig:
    mode: str = "mock"
    appkey: str = ""
    secretkey: str = ""
    account_no: str = ""
    investment_ratio: float = 0.95
    num_stocks: int = 40
    rebalance_months: int = 3
    order_timeout_minutes: int = 3
    order_price_offset_bps: int = 10
    order_endpoint: str = "/api/dostk/ordr"
    order_api_id: str = "kt10000"
    order_status_endpoint: str = "/api/dostk/acnt"
    order_status_api_id: str = "kt00007"
    order_cancel_endpoint: str = "/api/dostk/ordr"
    order_cancel_api_id: str = "kt10002"
    quote_endpoint: str = "/api/dostk/stkinfo"
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

    @property
    def is_mock(self) -> bool:
        return self.mode.lower() == "mock"

    @classmethod
    def from_env(cls) -> "LiveTradingConfig":
        return cls(
            mode=os.getenv("KIWOOM_MODE", "mock").lower(),
            appkey=os.getenv("KIWOOM_APPKEY", ""),
            secretkey=os.getenv("KIWOOM_SECRETKEY", ""),
            account_no=os.getenv("KIWOOM_ACCOUNT_NO", ""),
            investment_ratio=float(os.getenv("LIVE_INVESTMENT_RATIO", "0.95")),
            num_stocks=int(os.getenv("LIVE_NUM_STOCKS", "40")),
            rebalance_months=int(os.getenv("LIVE_REBALANCE_MONTHS", "3")),
            order_timeout_minutes=int(os.getenv("LIVE_ORDER_TIMEOUT_MINUTES", "3")),
            order_price_offset_bps=int(os.getenv("LIVE_ORDER_PRICE_OFFSET_BPS", "10")),
            order_endpoint=os.getenv("KIWOOM_ORDER_ENDPOINT", "/api/dostk/ordr"),
            order_api_id=os.getenv("KIWOOM_ORDER_API_ID", "kt10000"),
            order_status_endpoint=os.getenv("KIWOOM_ORDER_STATUS_ENDPOINT", "/api/dostk/acnt"),
            order_status_api_id=os.getenv("KIWOOM_ORDER_STATUS_API_ID", "kt00007"),
            order_cancel_endpoint=os.getenv("KIWOOM_ORDER_CANCEL_ENDPOINT", "/api/dostk/ordr"),
            order_cancel_api_id=os.getenv("KIWOOM_ORDER_CANCEL_API_ID", "kt10002"),
            quote_endpoint=os.getenv("KIWOOM_QUOTE_ENDPOINT", "/api/dostk/stkinfo"),
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
        )
