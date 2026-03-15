from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
import aiohttp
import random

from kiwoom import API, MOCK, REAL

from .config import LiveTradingConfig


@dataclass(slots=True)
class AccountSnapshot:
    cash: float
    holdings: dict[str, int]


@dataclass(slots=True)
class SubmittedOrder:
    ticker: str
    side: str
    requested_qty: int
    price: int
    order_no: str
    raw_response: dict[str, Any]


@dataclass(slots=True)
class OrderCheck:
    submitted: SubmittedOrder
    pending_qty: int
    is_filled: bool


@dataclass(slots=True)
class BestQuote:
    ask1: int | None
    bid1: int | None


class KiwoomAPIError(Exception):
    """Raised when Kiwoom REST API returns a non-success return_code in the body."""

    def __init__(self, endpoint: str | None, api_id: str | None, body: dict | None):
        # store raw attributes
        self.endpoint = endpoint
        self.api_id = api_id
        self.body = body or {}

        # try to extract standardized fields if present
        rc = None
        msg = None
        if isinstance(self.body, dict):
            rc = self.body.get("return_code") if "return_code" in self.body else self.body.get("code")
            msg = self.body.get("return_msg") if "return_msg" in self.body else self.body.get("message")

        try:
            self.return_code = int(rc) if rc is not None and str(rc).strip() != "" else None
        except Exception:
            self.return_code = None

        self.return_msg = str(msg) if msg is not None else None

        super().__init__(f"Kiwoom API error api_id={api_id} endpoint={endpoint} return_code={self.return_code} return_msg={self.return_msg} body={self.body}")


class KiwoomBrokerAdapter:
    """kiwoom-restful 기반 브로커 어댑터.

    주문 API 스펙이 계좌/상품별로 달라질 수 있어 endpoint/api-id는 환경변수로 주입한다.
    """

    def __init__(self, config: LiveTradingConfig, api_client: object | None = None):
        self.config = config
        host = MOCK if config.is_mock else REAL
        self.host = host
        # allow injecting a mock/alternative API client for testing
        if api_client is not None:
            self.api = api_client
        else:
            self.api = API(host=host, appkey=config.appkey, secretkey=config.secretkey)
        # runtime flag to enable HTTP logging even in real mode
        self._force_http_log = os.getenv("LIVE_LOG_HTTP", "false").lower() in {"1", "true", "yes", "y"}
        # diagnostics/log flags
        self._quote_response_logged = False
        self._connect_diag_logged = False

    async def _request(self, endpoint: str, api_id: str | None = None, data: dict | None = None):
        """Central request wrapper that optionally logs request/response for debugging.

        Logs when running in mock mode or when `LIVE_LOG_HTTP` is set.
        """
        should_log = self.config.is_mock or self._force_http_log
        if should_log:
            try:
                print(f"[HTTP][REQUEST] endpoint={endpoint}, api_id={api_id}, data={data}")
            except Exception:
                print("[HTTP][REQUEST] (print failed)")

        response = await self.api.request(endpoint=endpoint, api_id=api_id, data=data)

        try:
            body = response.json()
        except Exception:
            body = None

        if should_log:
            try:
                print(f"[HTTP][RESPONSE] endpoint={endpoint}, api_id={api_id}, body={body}")
            except Exception:
                print("[HTTP][RESPONSE] (print failed)")

        # If the Kiwoom mock/real API surfaces a `return_code` in the body, treat non-zero as an error.
        try:
            if isinstance(body, dict) and "return_code" in body:
                rc = body.get("return_code")
                # normalize numeric-like strings
                try:
                    ival = int(rc)
                except Exception:
                    ival = 0 if rc in (0, "0", None) else 1
                if ival != 0:
                    raise KiwoomAPIError(endpoint=endpoint, api_id=api_id, body=body)
        except KiwoomAPIError:
            # re-raise to preserve KiwoomAPIError type
            raise
        except Exception:
            # ignore parsing issues and continue
            pass

        return response

    @staticmethod
    def _mask_secret(value: str, *, head: int = 4, tail: int = 4) -> str:
        value = str(value or "")
        if not value:
            return "<empty>"
        if len(value) <= head + tail:
            return f"{value[:1]}***{value[-1:]}"
        return f"{value[:head]}...{value[-tail:]}"

    @staticmethod
    def _edge_whitespace_flags(value: str) -> tuple[bool, bool, bool]:
        value = str(value or "")
        has_leading = bool(value[:1] and value[:1].isspace())
        has_trailing = bool(value[-1:] and value[-1:].isspace())
        has_control = any(ch in value for ch in ("\r", "\n", "\t"))
        return has_leading, has_trailing, has_control

    def _log_connect_diagnostics(self) -> None:
        if self._connect_diag_logged:
            return
        self._connect_diag_logged = True

        app_lead, app_trail, app_ctrl = self._edge_whitespace_flags(self.config.appkey)
        sec_lead, sec_trail, sec_ctrl = self._edge_whitespace_flags(self.config.secretkey)
        dotenv_path = self.config.dotenv_path if self.config.dotenv_path else "<not-found-or-env-only>"

        print(
            "[KIWOOM][DIAG] "
            f"mode={self.config.mode}, host={self.host}, "
            f"dotenv_path={dotenv_path}, "
            f"appkey(masked)={self._mask_secret(self.config.appkey)}, appkey_len={len(self.config.appkey)}, "
            f"appkey_leading_ws={app_lead}, appkey_trailing_ws={app_trail}, appkey_ctrl_char={app_ctrl}, "
            f"secret(masked)={self._mask_secret(self.config.secretkey)}, secret_len={len(self.config.secretkey)}, "
            f"secret_leading_ws={sec_lead}, secret_trailing_ws={sec_trail}, secret_ctrl_char={sec_ctrl}"
        )

    def normalize_ticker(self, ticker: str) -> str:
        """Normalize ticker codes to the numeric form expected by downstream APIs.

        Examples:
        - 'A003670' -> '003670'
        - '003670'  -> '003670'
        """
        if ticker is None:
            return ""
        t = str(ticker).strip()
        if not t:
            return t
        norm = re.sub(r'^\D+', '', t)
        return norm if norm else t

    async def __aenter__(self) -> "KiwoomBrokerAdapter":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    def _kerr_matches_codes(self, kerr: "KiwoomAPIError", codes: tuple[int, ...]) -> bool:
        try:
            rc = int(kerr.return_code) if kerr.return_code is not None else None
        except Exception:
            rc = None

        msg = str(kerr.return_msg or "")
        if not codes:
            return False

        for c in codes:
            try:
                if rc == int(c):
                    return True
            except Exception:
                pass
            if f"RC{c}" in msg or str(c) in msg:
                return True
        return False

    async def connect(self) -> None:
        self._log_connect_diagnostics()
        # Enable low-level library debugging when adapter-level HTTP logging
        # is desired (mock mode or LIVE_LOG_HTTP). This lets the underlying
        # kiwoom-restful client emit its detailed request/response dumps.
        should_log = self.config.is_mock or self._force_http_log
        try:
            setattr(self.api, "debugging", bool(should_log))
        except Exception:
            pass

        await self.api.connect()

    async def close(self) -> None:
        await self.api.close()

    async def get_recent_trades(self, days: int = 5) -> list[dict[str, Any]]:
        end = datetime.today()
        start = end - timedelta(days=days)
        records = await self.api.trade(start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
        return records

    async def get_account_snapshot(self) -> AccountSnapshot:
        """계정 잔액 및 보유 종목 조회 (Mock/Real 모두 동일한 API 호출)"""
        try:
            balance_response = await self._get_account_balance()
        except Exception as exc:
            print(f"[WARN] 잔액 조회 실패: {exc}")
            return AccountSnapshot(cash=0.0, holdings={})
        
        cash = float(balance_response.get("prsm_dpst_aset_amt", 0) or 0)
        
        holdings: dict[str, int] = {}
        holdings_list = balance_response.get("acnt_evlt_remn_indv_tot", [])
        if isinstance(holdings_list, list):
            for item in holdings_list:
                ticker = item.get("stk_cd")
                qty_str = item.get("rmnd_qty", "0")
                if ticker:
                    try:
                        norm_ticker = self.normalize_ticker(ticker)
                        qty = int(float(qty_str or 0))
                        if qty > 0:
                            holdings[norm_ticker] = qty
                    except (ValueError, TypeError):
                        pass
        
        return AccountSnapshot(cash=cash, holdings=holdings)
    
    async def _get_account_balance(self) -> dict[str, any]:
        """키움 kt00018 API로 계좌평가잔고 조회"""
        data = {
            "qry_tp": "1",  # 조회구분: 1=합산, 2=개별
            "dmst_stex_tp": "KRX",  # 국내거래소구분
        }
        
        response = await self._request(
            endpoint=self.config.balance_endpoint,
            api_id=self.config.balance_api_id,
            data=data,
        )
        body = response.json()
        print(f"[BALANCE] prsm_dpst_aset_amt={body.get('prsm_dpst_aset_amt')}, holdings_count={len(body.get('acnt_evlt_remn_indv_tot', []))}")
        return body

    async def submit_order(
        self,
        ticker: str,
        side: str,
        quantity: int,
        price: int,
        order_type: str = "00",
    ) -> SubmittedOrder:
        # Normalize ticker centrally and log mapping for easier debugging
        norm_ticker = self.normalize_ticker(ticker)
        if norm_ticker != str(ticker).strip():
            print(f"[ORDER] ticker normalized: original={ticker} -> normalized={norm_ticker}")

        # 기본 필드
        data = {
            "dmst_stex_tp": "KRX",
            "stk_cd": norm_ticker,
            "ord_qty": str(quantity),
            "ord_cond": order_type,
        }

        # 모드별 주문 유형 처리
        # - 모의: 모의서버는 보통(0) 또는 시장가(3)만 허용되는 경우가 있으므로
        #   단, 호출자가 명시적으로 `order_type`을 전달한 경우 이를 우선 사용합니다.
        #   시장가(order_type=='3')일 경우 단가(`ord_uv`)는 전송하지 않습니다.
        if self.config.is_mock:
            if order_type and str(order_type).strip():
                data["trde_tp"] = str(order_type)
                # 시장가는 단가를 전송하지 않음
                if str(order_type).strip() != "3":
                    try:
                        if price and int(price) > 0:
                            data["ord_uv"] = str(price)
                    except Exception:
                        pass
            else:
                if price and int(price) > 0:
                    data["trde_tp"] = "0"  # 보통
                    data["ord_uv"] = str(price)
                else:
                    data["trde_tp"] = "3"  # 시장가
                    # 시장가는 단가를 전송하지 않음
        else:
            # 실거래 모드: `trde_tp`는 매수/매도 구분용 코드가 아니라 '주문유형'을 나타내야 합니다.
            # API ID는 매수/매도에 따라 변경되므로 trde_tp는 order_type 파라미터 또는
            # 지정가/시장가 판단으로 설정합니다.
            data["ord_uv"] = str(price)
            # 우선 인자로 전달된 order_type을 사용 (있으면 그대로 전달)
            if order_type and str(order_type).strip():
                data["trde_tp"] = str(order_type)
            else:
                # 기본 폴백: 가격이 주어지면 보통(0), 가격이 없으면 시장가(3)
                try:
                    if price and int(price) > 0:
                        data["trde_tp"] = "0"
                    else:
                        data["trde_tp"] = "3"
                except Exception:
                    data["trde_tp"] = "0"
        if self.config.account_no:
            data["acnt_no"] = self.config.account_no

        retries = max(0, int(self.config.order_submit_retries))
        backoff = float(self.config.order_submit_retry_backoff_seconds or 0.5)
        last_exc: Exception | None = None

        for attempt in range(1, retries + 1):
            try:
                # choose API ID by side (buy/sell); fall back to legacy order_api_id
                api_id = (
                    self.config.order_buy_api_id
                    if side.upper() == "BUY"
                    else self.config.order_sell_api_id
                ) or self.config.order_api_id

                response = await self._request(
                    endpoint=self.config.order_endpoint,
                    api_id=api_id,
                    data=data,
                )
                body = response.json()
                order_no = self._extract_order_no(body)
                # Store normalized ticker in the submitted order for consistency
                return SubmittedOrder(
                    ticker=norm_ticker,
                    side=side,
                    requested_qty=quantity,
                    price=price,
                    order_no=order_no,
                    raw_response=body,
                )
            # Let KiwoomAPIError bubble up to caller for handling (caller may decide to fallback to market)
            except Exception as exc:
                last_exc = exc
                status = getattr(exc, 'status', None)
                # aiohttp ClientResponseError has .status attribute
                if isinstance(exc, aiohttp.ClientResponseError) or status == 429:
                    if attempt < retries:
                        sleep_for = backoff * attempt
                        print(f"[ORDER] received 429, retrying after {sleep_for}s (attempt={attempt}/{retries})")
                        await asyncio.sleep(sleep_for)
                        continue
                # non-retriable or exhausted retries: re-raise
                raise

        # if loop exits without return, raise last exception
        if last_exc:
            raise last_exc
        raise RuntimeError("submit_order failed without exception")

    async def query_open_order_pending_qty(self, order: SubmittedOrder) -> int:
        data = {
            "qry_tp": "0",
            "sell_tp": "0",
            "stk_bond_tp": "1",
            "mrkt_tp": "0",
            # dmst_stex_tp is required by kt00007
            "dmst_stex_tp": "KRX",
        }
        if self.config.account_no:
            data["acnt_no"] = self.config.account_no

        response = await self._request(
            endpoint=self.config.order_status_endpoint,
            api_id=self.config.order_status_api_id,
            data=data,
        )
        body = response.json()
        records = self._extract_records(body)

        for rec in records:
            rec_order_no = str(rec.get("ord_no") or rec.get("주문번호") or "").strip()
            rec_ticker = str(rec.get("stk_cd") or rec.get("종목번호") or "").strip()
            if rec_order_no and rec_order_no == order.order_no:
                return self._extract_pending_qty(rec)
            if not order.order_no and rec_ticker == order.ticker:
                return self._extract_pending_qty(rec)

        return 0

    async def cancel_order(self, order: SubmittedOrder, pending_qty: int) -> dict[str, Any]:
        data = {
            "dmst_stex_tp": "KRX",
            "stk_cd": order.ticker,
            "orig_ord_no": order.order_no,
            "ord_qty": str(max(0, int(pending_qty))),
            "trde_tp": "3",
        }
        if self.config.account_no:
            data["acnt_no"] = self.config.account_no

        response = await self._request(
            endpoint=self.config.order_cancel_endpoint,
            api_id=self.config.order_cancel_api_id,
            data=data,
        )
        return response.json()

    async def modify_order(self, order: SubmittedOrder, new_qty: int, new_price: int, cond_price: int | None = None) -> dict[str, Any]:
        """정정(수정) 주문을 보냅니다. API ID는 `order_modify_api_id`를 사용합니다."""
        data = {
            "dmst_stex_tp": "KRX",
            "orig_ord_no": order.order_no,
            "stk_cd": order.ticker,
            "mdfy_qty": str(int(new_qty)),
            "mdfy_uv": str(int(new_price)),
        }
        if cond_price is not None:
            data["mdfy_cond_uv"] = str(int(cond_price))
        if self.config.account_no:
            data["acnt_no"] = self.config.account_no

        response = await self._request(
            endpoint=self.config.order_cancel_endpoint,
            api_id=self.config.order_modify_api_id,
            data=data,
        )
        return response.json()

    async def run_retry_cycle(
        self,
        submitted_orders: list[SubmittedOrder],
    ) -> tuple[list[SubmittedOrder], list[OrderCheck]]:
        checks: list[OrderCheck] = []
        retries: list[SubmittedOrder] = []

        for order in submitted_orders:
            pending_qty = await self.query_open_order_pending_qty(order)
            is_filled = pending_qty <= 0
            checks.append(OrderCheck(submitted=order, pending_qty=pending_qty, is_filled=is_filled))

            if is_filled:
                continue

            # Attempt modify(정정) first using order_modify_api_id. If modify fails, fall back to cancel+resubmit.
            retry_price = await self.get_retry_price(
                side=order.side,
                ticker=order.ticker,
                base_price=order.price,
            )

            try:
                mod_resp = await self.modify_order(
                    order=order,
                    new_qty=pending_qty,
                    new_price=max(1, retry_price),
                )
                order_no = str(mod_resp.get("ord_no") or mod_resp.get("base_orig_ord_no") or order.order_no)
                retried = SubmittedOrder(
                    ticker=order.ticker,
                    side=order.side,
                    requested_qty=int(pending_qty),
                    price=int(max(1, retry_price)),
                    order_no=order_no,
                    raw_response=mod_resp,
                )
                retries.append(retried)
                continue
            except Exception as exc:  # pragma: no cover - best-effort fallback
                print(f"[ORDER] modify failed, falling back to cancel+resubmit: {exc}")

            await self.cancel_order(order, pending_qty)
            # submit_order may raise KiwoomAPIError (e.g. RC4027). Let caller-level policy decide
            # whether to retry as market order. If caller wants automatic fallback, it must catch
            # KiwoomAPIError and re-invoke submit_order with order_type="3".
            try:
                retried = await self.submit_order(
                    ticker=order.ticker,
                    side=order.side,
                    quantity=pending_qty,
                    price=max(1, retry_price),
                    order_type=self.config.retry_order_type,
                )
            except KiwoomAPIError as exc:
                # If Kiwoom signals 상/하한가 (RC4027), perform a market-order retry here.
                msg = str(exc.return_msg or "")
                try:
                    rc = int(exc.return_code) if exc.return_code is not None else None
                except Exception:
                    rc = None

                # decide fallback using configured codes
                if self._kerr_matches_codes(exc, tuple(self.config.fallback_to_market_return_codes or ())):
                    retried = await self.submit_order(
                        ticker=order.ticker,
                        side=order.side,
                        quantity=pending_qty,
                        price=max(1, retry_price),
                        order_type="3",
                    )
                else:
                    # re-raise other Kiwoom errors
                    raise
            retries.append(retried)

        return retries, checks

    async def check_orders(self, submitted_orders: list[SubmittedOrder]) -> list[OrderCheck]:
        checks: list[OrderCheck] = []
        for order in submitted_orders:
            pending_qty = await self.query_open_order_pending_qty(order)
            checks.append(
                OrderCheck(
                    submitted=order,
                    pending_qty=pending_qty,
                    is_filled=pending_qty <= 0,
                )
            )
        return checks

    async def get_retry_price(self, side: str, ticker: str, base_price: int) -> int:
        if self.config.use_hoga_retry_price:
            quote = await self.get_best_quote(ticker)
            if side.upper() == "BUY" and quote.ask1 and quote.ask1 > 0:
                return int(quote.ask1)
            if side.upper() == "SELL" and quote.bid1 and quote.bid1 > 0:
                return int(quote.bid1)

        if base_price <= 0:
            return 1
        factor = 1 + (self.config.retry_price_offset_bps / 10000) if side.upper() == "BUY" else 1 - (self.config.retry_price_offset_bps / 10000)
        return max(1, int(base_price * factor))

    async def get_best_quote(self, ticker: str) -> BestQuote:
        norm_ticker = self.normalize_ticker(ticker)
        if norm_ticker != str(ticker).strip():
            print(f"[QUOTE] ticker normalized: original={ticker} -> normalized={norm_ticker}")

        data = {
            "stk_cd": norm_ticker,
            "mrkt_tp": self.config.quote_market_type,
        }
        retries = max(0, int(self.config.quote_request_retries or 0))
        backoff = float(self.config.quote_request_retry_backoff_seconds or 0.5)
        last_exc: Exception | None = None

        for attempt in range(1, max(1, retries) + 1):
            try:
                response = await self._request(
                    endpoint=self.config.quote_endpoint,
                    api_id=self.config.quote_api_id,
                    data=data,
                )
                body = response.json()
                self._log_quote_response_once(ticker=ticker, body=body)

                # Expand candidate keys to include ka10004 (주식호가요청) field names
                ask = self._extract_quote_value(
                    body,
                    [
                        # ka10004 primary sell (ask) fields
                        "sel_fpr_bid",
                        "sel_1th_pre_bid",
                        "sel_2th_pre_bid",
                        "sel_3th_pre_bid",
                        "sel_4th_pre_bid",
                        "sel_5th_pre_bid",
                        "sel_6th_pre_bid",
                        "sel_7th_pre_bid",
                        "sel_8th_pre_bid",
                        "sel_9th_pre_bid",
                        "sel_10th_pre_bid",
                        # legacy / other names
                        "ask1",
                        "ask_pri_1",
                        "offerho1",
                        "매도호가1",
                        "41",
                    ],
                )
                bid = self._extract_quote_value(
                    body,
                    [
                        # ka10004 primary buy (bid) fields
                        "buy_fpr_bid",
                        "buy_1th_pre_bid",
                        "buy_2th_pre_bid",
                        "buy_3th_pre_bid",
                        "buy_4th_pre_bid",
                        "buy_5th_pre_bid",
                        "buy_6th_pre_bid",
                        "buy_7th_pre_bid",
                        "buy_8th_pre_bid",
                        "buy_9th_pre_bid",
                        "buy_10th_pre_bid",
                        # legacy / other names
                        "bid1",
                        "bid_pri_1",
                        "bidho1",
                        "매수호가1",
                        "51",
                    ],
                )
                return BestQuote(ask1=ask, bid1=bid)

            except Exception as exc:
                last_exc = exc
                status = getattr(exc, "status", None)
                headers = getattr(exc, "headers", {}) or {}

                is_429 = False
                try:
                    if isinstance(exc, aiohttp.ClientResponseError) or status == 429:
                        is_429 = True
                except Exception:
                    is_429 = status == 429

                if is_429 and attempt < retries:
                    # honor Retry-After if provided
                    ra = headers.get("Retry-After") or headers.get("retry-after")
                    try:
                        wait = float(ra) if ra is not None else (backoff * (2 ** (attempt - 1)))
                    except Exception:
                        wait = backoff * (2 ** (attempt - 1))
                    print(f"[QUOTE] received 429, retrying after {wait}s (attempt={attempt}/{retries})")
                    await asyncio.sleep(wait)
                    continue

                # Non-retriable or exhausted retries: re-raise
                raise

        if last_exc:
            raise last_exc
        return BestQuote(ask1=None, bid1=None)

    # NOTE: `request_endpoint` was removed because it bypassed the adapter's
    # centralized `_request` wrapper (which handles HTTP logging and normalized
    # Kiwoom error handling). Use `_request()` or `request_endpoint_paginated()`
    # below instead.

    async def request_endpoint_paginated(
        self,
        endpoint: str,
        api_id: str,
        data: dict[str, Any],
        *,
        list_key: str = "list",
        max_pages: int = 50,
    ) -> dict[str, Any]:
        """Call Kiwoom endpoint with continuation headers(cont-yn/next-key) support.

        Uses underlying request_until provided by kiwoom client.
        """
        if not endpoint or not api_id:
            raise RuntimeError("Kiwoom endpoint or api_id not configured")
        should_log = self.config.is_mock or self._force_http_log

        if should_log:
            try:
                print(f"[HTTP][REQUEST] endpoint={endpoint}, api_id={api_id}, data={data}")
            except Exception:
                print("[HTTP][REQUEST] (print failed)")

        pages = {"count": 0}

        def _should_continue(body: dict[str, Any]) -> bool:
            pages["count"] += 1
            if pages["count"] >= max(1, int(max_pages)):
                return False
            values = body.get(list_key)
            return isinstance(values, list) and len(values) > 0

        body = await self.api.request_until(
            should_continue=_should_continue,
            endpoint=endpoint,
            api_id=api_id,
            data=data,
        )

        if should_log:
            try:
                print(f"[HTTP][RESPONSE] endpoint={endpoint}, api_id={api_id}, body={body}")
            except Exception:
                print("[HTTP][RESPONSE] (print failed)")

        if isinstance(body, dict):
            return body
        return {}

    async def get_fundamental_by_ticker(self, ticker: str) -> dict[str, any]:
        """Call ka10001-like endpoint to fetch fundamentals for a single ticker.

        Returns a dict with normalized keys: open_pric, mac, per, eps, roe, pbr, bps, div, dps
        """
        norm_ticker = self.normalize_ticker(ticker)
        if norm_ticker != str(ticker).strip():
            print(f"[FUND] ticker normalized: original={ticker} -> normalized={norm_ticker}")

        data = {"stk_cd": norm_ticker}
        # prefer explicit fund endpoint; DO NOT fallback to quote_endpoint (quote API differs)
        endpoint = self.config.fund_endpoint
        api_id = self.config.fund_api_id or "ka10001"

        if not endpoint:
            # If no fund endpoint configured, do not call quote endpoint with fund api_id
            print(f"  [FUND] fund_endpoint not configured; skipping Kiwoom fund call for {ticker}")
            return {}

        # Retry/backoff configuration: fallback to quote retry settings if specific not provided
        retries = max(1, int(getattr(self.config, "quote_request_retries", 3) or 3))
        backoff_base = float(getattr(self.config, "quote_request_retry_backoff_seconds", 0.5) or 0.5)
        last_exc: Exception | None = None

        body = None
        for attempt in range(1, retries + 1):
            try:
                response = await self._request(endpoint=endpoint, api_id=api_id, data=data)
                try:
                    body = response.json()
                except Exception:
                    body = None
                # successful low-level response; break retry loop
                break
            except Exception as exc:
                last_exc = exc
                status = getattr(exc, "status", None)
                is_429 = False
                try:
                    if isinstance(exc, aiohttp.ClientResponseError) or status == 429:
                        is_429 = True
                except Exception:
                    is_429 = status == 429

                if is_429 and attempt < retries:
                    # honor Retry-After header if available on exception (some clients attach headers)
                    ra = None
                    headers = getattr(exc, "headers", {}) or {}
                    ra = headers.get("Retry-After") or headers.get("retry-after")
                    try:
                        wait = float(ra) if ra is not None else (backoff_base * (2 ** (attempt - 1)))
                    except Exception:
                        wait = backoff_base * (2 ** (attempt - 1))
                    # add small jitter to avoid thundering herd
                    jitter = random.uniform(0, min(0.5, wait))
                    wait = wait + jitter
                    print(f"[FUND] received 429, retrying after {wait:.2f}s (attempt={attempt}/{retries})")
                    await asyncio.sleep(wait)
                    continue

                # For KiwoomAPIError (non-zero return_code) do not blindly retry unless HTTP 429
                break

        if body is None and last_exc is not None:
            # surface the last exception to caller
            raise last_exc

        # body may contain data under various keys -- try to find numeric fields
        def _get(k):
            v = body.get(k)
            if v is None:
                # try lowercase
                return body.get(k.lower())
            return v

        # collect raw values first
        raw_open = _get("open_pric") or _get("open") or _get("open_pri") or None
        raw_mac = _get("mac") or _get("market_cap") or _get("시가총액") or None
        raw_per = _get("per") or None
        raw_eps = _get("eps") or None
        raw_roe = _get("roe") or None
        raw_cur_prc = _get("cur_prc") or _get("curPrc") or _get("cur_pri") or None
        raw_pbr = _get("pbr") or None
        raw_bps = _get("bps") or None
        raw_div = _get("div") or None
        raw_dps = _get("dps") or None

        # Normalize numeric strings to numbers where possible
        open_pric = self._to_number(raw_open)
        mac_num = self._to_number(raw_mac)
        per = self._to_number(raw_per)
        eps = self._to_number(raw_eps)
        roe = self._to_number(raw_roe)
        cur_prc = self._to_number(raw_cur_prc)
        # If current price uses sign for direction, normalize to absolute value
        try:
            if cur_prc is not None and cur_prc < 0:
                cur_prc = abs(cur_prc)
                print(f"  [KIWOOM] {ticker} cur_prc negative detected, converted to {cur_prc}")
        except Exception:
            pass
        pbr = self._to_number(raw_pbr)
        bps = self._to_number(raw_bps)
        div = self._to_number(raw_div)
        dps = self._to_number(raw_dps)

        # Conditional unit conversion for mac (시가총액)
        market_cap = None
        try:
            if mac_num is not None:
                # Heuristic: if value seems small (< 1e9), it's likely in '백만' 단위
                if abs(mac_num) < 1e9:
                    market_cap = float(mac_num) * 1_000_000
                    print(f"  [KIWOOM] {ticker} mac detected small ({mac_num}); assuming millions, converted market_cap={market_cap}")
                else:
                    market_cap = float(mac_num)
                    print(f"  [KIWOOM] {ticker} mac detected large ({mac_num}); assuming KRW, market_cap={market_cap}")
        except Exception:
            market_cap = None

        # Fix/normalize open_pric if negative (Kiwoom may return negative sign erroneously)
        open_pric_normalized = open_pric
        try:
            if open_pric_normalized is not None and open_pric_normalized < 0:
                # most likely sign issue; use absolute value and log
                open_pric_normalized = abs(open_pric_normalized)
                print(f"  [KIWOOM] {ticker} open_pric negative detected, converted to {open_pric_normalized}")
        except Exception:
            open_pric_normalized = open_pric

        result = {
            "ticker": ticker,
            "open_pric": open_pric_normalized,
            "mac": mac_num,
            "market_cap": market_cap,
            "cur_prc": cur_prc,
            "per": per,
            "eps": eps,
            "roe": roe,
            "pbr": pbr,
            "bps": bps,
            "div": div,
            "dps": dps,
        }

        return result

    def _to_number(self, value: any) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            raw = value.strip().replace(",", "")
            # remove percent sign
            if raw.endswith("%"):
                raw = raw[:-1]
            if raw == "":
                return None
            try:
                return float(raw)
            except Exception:
                return None
        return None

    def _log_quote_response_once(self, ticker: str, body: dict[str, Any]) -> None:
        if not self.config.log_quote_response:
            return
        if self._quote_response_logged:
            return

        payload = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ticker": ticker,
            "body": body,
        }
        try:
            dumped = json.dumps(body, ensure_ascii=False)
            print(f"[QUOTE_SAMPLE] ticker={ticker} body={dumped}")
        except Exception:
            print(f"[QUOTE_SAMPLE] ticker={ticker} body={body}")

        try:
            report_dir = self.config.report_dir or "results/live_reports"
            os.makedirs(report_dir, exist_ok=True)
            file_name = f"quote_sample_{datetime.now().strftime('%Y%m%d')}.jsonl"
            path = os.path.join(report_dir, file_name)
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
            print(f"[QUOTE_SAMPLE] saved={path}")
        except Exception as exc:
            print(f"[QUOTE_SAMPLE] file_save_failed={exc}")

        self._quote_response_logged = True

    async def wait_for_fill_window(self, minutes: int) -> None:
        await asyncio.sleep(max(0, int(minutes)) * 60)

    def _extract_order_no(self, body: dict[str, Any]) -> str:
        keys = ["ord_no", "order_no", "주문번호", "ordNo"]
        for key in keys:
            value = body.get(key)
            if value:
                return str(value)

        for key in ["output", "data", "list", "acnt_ord_cntr_prst_array", "acnt_ord_cntr_prps_dtl"]:
            node = body.get(key)
            if isinstance(node, list) and node:
                first = node[0]
                if isinstance(first, dict):
                    for k in keys:
                        v = first.get(k)
                        if v:
                            return str(v)
        return ""

    def _extract_records(self, body: dict[str, Any]) -> list[dict[str, Any]]:
        candidate_keys = [
            # older/other APIs
            "acnt_ord_cntr_prst_array",
            # kt00007 (계좌별주문체결내역상세요청) primary list key
            "acnt_ord_cntr_prps_dtl",
            "list",
            "output",
            "data",
        ]
        for key in candidate_keys:
            records = body.get(key)
            if isinstance(records, list):
                return [r for r in records if isinstance(r, dict)]
        return []

    def _extract_pending_qty(self, rec: dict[str, Any]) -> int:
        keys = ["rmn_qty", "unfill_qty", "미체결수량", "미체결잔량", "cncl_qty", "ord_remnq", "주문잔량"]
        for key in keys:
            value = rec.get(key)
            if value is None:
                continue
            try:
                return max(0, int(float(value)))
            except Exception:
                continue

        requested = rec.get("ord_qty") or rec.get("주문수량")
        filled = rec.get("cntr_qty") or rec.get("체결수량")
        try:
            req = int(float(requested or 0))
            fil = int(float(filled or 0))
            return max(0, req - fil)
        except Exception:
            return 0

    def _extract_quote_value(self, body: dict[str, Any], keys: list[str]) -> int | None:
        queue: list[Any] = [body]
        while queue:
            node = queue.pop(0)
            if isinstance(node, dict):
                for key in keys:
                    if key in node:
                        parsed = self._to_int(node.get(key))
                        if parsed is not None and parsed > 0:
                            return parsed
                for value in node.values():
                    if isinstance(value, (dict, list)):
                        queue.append(value)
            elif isinstance(node, list):
                for value in node:
                    if isinstance(value, (dict, list)):
                        queue.append(value)
        return None

    def _to_int(self, value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return int(abs(value))
        if isinstance(value, str):
            raw = value.strip().replace(",", "")
            if not raw:
                return None
            if raw.startswith("+"):
                raw = raw[1:]
            try:
                return int(abs(float(raw)))
            except Exception:
                return None
        return None
