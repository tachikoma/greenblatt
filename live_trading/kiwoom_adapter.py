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
import time

from kiwoom import API, MOCK, REAL

from .config import LiveTradingConfig


# Simple async token bucket for rate limiting
class TokenBucket:
    def __init__(self, rate: float, capacity: float):
        self.rate = float(rate)
        self.capacity = float(capacity)
        self._tokens = float(capacity)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()
        self._pause_until = 0.0

    async def consume(self, amount: float = 1.0):
        async with self._lock:
            now = time.monotonic()
            # honor temporary pause
            if now < self._pause_until:
                wait = self._pause_until - now
                print(f"[TOKEN] paused, waiting {wait:.3f}s until resume")
                # release lock and sleep outside
                pass
            # refill tokens
            self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
            self._last = now
            if self._tokens >= amount:
                self._tokens -= amount
                print(f"[TOKEN] consume: ok amount={amount}, tokens_left={self._tokens:.3f}")
                return
            needed = amount - self._tokens
            wait = needed / self.rate if self.rate > 0 else 0.1
            print(f"[TOKEN] consume: need={amount}, tokens={self._tokens:.3f}, waiting {wait:.3f}s")

        await asyncio.sleep(wait)
        await self.consume(amount)

    async def pause(self, seconds: float, reason: str | None = None):
        async with self._lock:
            prev = self._pause_until
            self._pause_until = max(self._pause_until, time.monotonic() + float(seconds))
            now = time.monotonic()
            resume_in = max(0.0, self._pause_until - now)
            reason_str = f" reason={reason}" if reason else ""
            print(f"[TOKEN] pause: paused for {seconds}s (resume_in={resume_in:.3f}s){reason_str}")


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
    tick_size: int | None = None


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
        # concurrency and rate-limiting defaults
        try:
            self._func_concurrency = int(os.getenv("KIWOOM_FUNC_CONCURRENCY", "3"))
        except Exception:
            self._func_concurrency = 3
        # create adapter-level semaphore and token bucket lazily on first use
        self._sem: asyncio.Semaphore | None = None
        self._bucket: TokenBucket | None = None
        # sequential lock to enforce one-request-at-a-time semantics when desired
        self._seq_lock: asyncio.Lock | None = None
        # websocket / order-event callbacks
        self._order_event_callbacks: set = set()
        # dict-based dispatch: order_no -> list[asyncio.Event] for targeted WS routing
        self._order_fill_events: dict[str, asyncio.Event] = {}
        self._order_fill_payloads: dict[str, dict] = {}
        # Note: websocket events are received via the installed `kiwoom.API`
        # callbacks; adapter-level _ws_* fields/listener were unused and
        # have been removed to avoid confusion.

    async def _request(
        self,
        endpoint: str,
        api_id: str | None = None,
        data: dict | None = None,
        *,
        headers: dict[str, str] | None = None,
    ):
        """Central request wrapper with rate limiting, timeout, 429 handling, and logging.

        ``headers`` can be used to pass continuation headers (cont-yn/next-key) for
        pagination.  When None the kiwoom client generates default auth headers.
        Logs when running in mock mode or when ``LIVE_LOG_HTTP`` is set.
        """
        should_log = self.config.is_mock or self._force_http_log

        # lazy init semaphore and token bucket
        if self._sem is None:
            self._sem = asyncio.Semaphore(self._func_concurrency)
        if self._bucket is None:
            try:
                rate = float(os.getenv("KIWOOM_RATE", "5"))
            except Exception:
                rate = 5.0
            try:
                cap = float(os.getenv("KIWOOM_CAPACITY", "10"))
            except Exception:
                cap = 10.0
            self._bucket = TokenBucket(rate=rate, capacity=cap)
        # lazy init sequential lock
        if self._seq_lock is None:
            # use a lock to serialize requests so callers observing per-request delays
            # see strictly sequential behavior even when caller coroutines run concurrently
            self._seq_lock = asyncio.Lock()

        if should_log:
            try:
                print(f"[HTTP][REQUEST] endpoint={endpoint}, api_id={api_id}, data={data}")
            except Exception:
                print("[HTTP][REQUEST] (print failed)")

        # enforce rate limit and concurrency; also serialize requests via _seq_lock
        try:
            await self._bucket.consume()
        except Exception:
            pass

        async with self._sem:
            # ensure sequential ordering of requests to avoid rate-limit storms
            async with self._seq_lock:
                # perform the underlying API request but enforce a configurable
                # per-request timeout to avoid indefinite hangs. Timeout can be
                # configured via LiveTradingConfig.request_timeout_seconds or
                # env KIWOOM_REQUEST_TIMEOUT (seconds). Default: 30s.
                try:
                    timeout = float(getattr(self.config, "request_timeout_seconds", os.getenv("KIWOOM_REQUEST_TIMEOUT", "30") or 30))
                except Exception:
                    timeout = 30.0

                try:
                    coro = self.api.request(endpoint=endpoint, api_id=api_id, headers=headers, data=data)
                    response = await asyncio.wait_for(coro, timeout)
                except asyncio.TimeoutError:
                    try:
                        await self._bucket.pause(timeout, reason=f"request timeout endpoint={endpoint} api_id={api_id}")
                    except Exception:
                        pass
                    exc = Exception("request timeout")
                    setattr(exc, "status", 408)
                    try:
                        setattr(exc, "headers", {})
                    except Exception:
                        pass
                    raise exc

        # handle HTTP 429 by pausing bucket if possible and raising an exception with status
        status = getattr(response, "status", None)
        if status == 429:
            ra = None
            try:
                ra = response.headers.get("Retry-After") or response.headers.get("retry-after")
            except Exception:
                ra = None
            try:
                wait = float(ra) if ra is not None else 5.0
            except Exception:
                wait = 5.0
            try:
                await self._bucket.pause(wait, reason=f"HTTP 429 endpoint={endpoint} api_id={api_id}")
            except Exception:
                pass
            exc = Exception("HTTP 429")
            setattr(exc, "status", 429)
            try:
                setattr(exc, "headers", dict(getattr(response, "headers", {}) or {}))
            except Exception:
                pass
            raise exc

        try:
            body = response.json()
        except Exception:
            body = None

        if should_log:
            try:
                print(f"[HTTP][RESPONSE] endpoint={endpoint}, api_id={api_id}, body={body}")
            except Exception:
                print("[HTTP][RESPONSE] (print failed)")

        # Enforce a short per-request delay to avoid hitting per-unit-time quotas.
        # Mock mode: 0.3s, Real mode: 0.1s (configurable via env or LiveTradingConfig)
        try:
            if self.config.is_mock:
                per_req = float(getattr(self.config, "mock_request_delay_seconds", 0.3))
            else:
                per_req = float(getattr(self.config, "live_request_delay_seconds", 0.1))
            # only sleep if a positive small delay is configured
            if per_req and per_req > 0:
                await asyncio.sleep(per_req)
        except Exception:
            pass

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
                    # If the API body signals a 429, also pause the token bucket
                    if ival == 429:
                        try:
                            # prefer an explicit retry_after in body if present
                            raw_ra = body.get("retry_after") or body.get("Retry-After")
                            wait = float(raw_ra) if raw_ra is not None else float(os.getenv("KIWOOM_429_PAUSE", "1.0"))
                        except Exception:
                            wait = 1.0
                        try:
                            await self._bucket.pause(wait, reason=f"body return_code=429 endpoint={endpoint} api_id={api_id}")
                        except Exception:
                            pass
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

    def normalize_order_no(self, order_no: str) -> str:
        """Normalize order numbers for robust comparison (strip leading zeros).

        Returns the stripped representation if possible, else the original string.
        """
        if order_no is None:
            return ""
        s = str(order_no).strip()
        if not s:
            return ""
        # strip leading zeros but keep '0' if that's the whole value
        stripped = s.lstrip("0")
        return stripped if stripped != "" else s

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

        # Register adapter dispatcher onto API-level websocket callbacks.
        # The API already connected its Socket in `self.api.connect()`; instead
        # of creating a separate aiohttp session, register callbacks so that
        # incoming websocket messages are forwarded to adapter consumers.
        try:
            trnms = (
                "REAL",
                "LOGIN",
                "SYSTEM",
                "PING",
                "REG",
                "REMOVE",
            )
            self._api_prev_callbacks = {}

            async def _adapter_api_cb(payload):
                # The API may call this callback with either:
                # - a dict (non-REAL messages), or
                # - a RealData instance (for trnm=='REAL', where API passes RealData)
                try:
                    # detect RealData-like object by attributes
                    if hasattr(payload, "type") and hasattr(payload, "values"):
                        try:
                            raw = payload.values
                            if isinstance(raw, (bytes, bytearray)):
                                s = raw.decode("utf-8")
                            else:
                                s = str(raw)
                            try:
                                import orjson as _orjson

                                values = _orjson.loads(s)
                            except Exception:
                                import json as _json

                                values = _json.loads(s)
                        except Exception:
                            values = {}

                        doc = {
                            "trnm": "REAL",
                            "data": [
                                {
                                    "type": getattr(payload, "type", None),
                                    "name": getattr(payload, "name", None),
                                    "item": getattr(payload, "item", None),
                                    "values": values,
                                }
                            ],
                        }
                        await self._dispatch_order_event(doc)
                        return

                    # otherwise forward as-is
                    await self._dispatch_order_event(payload)
                except Exception:
                    pass

            for t in trnms:
                try:
                    prev = None
                    try:
                        prev = self.api._callbacks.get(t)
                    except Exception:
                        prev = None
                    self._api_prev_callbacks[t] = prev
                    self.api.add_callback_on_real_data(real_type=t, callback=_adapter_api_cb)
                except Exception:
                    pass
            print(f"[WS] adapter registered API callbacks for trnms: {', '.join(trnms)}")
        except Exception:
            pass

    async def close(self) -> None:
        # unregister any API callbacks we added during connect
        try:
            if hasattr(self, "_api_prev_callbacks") and isinstance(self._api_prev_callbacks, dict):
                for t, prev in list(self._api_prev_callbacks.items()):
                    try:
                        if prev is None:
                            try:
                                del self.api._callbacks[t]
                            except Exception:
                                pass
                        else:
                            self.api._callbacks[t] = prev
                    except Exception:
                        pass
                self._api_prev_callbacks = {}
        except Exception:
            pass

        await self.api.close()

    def register_order_event_callback(self, cb):
        """Register a callback to receive websocket order events.

        Callback can be a sync function (callable) or an async coroutine function.
        """
        try:
            self._order_event_callbacks.add(cb)
        except Exception:
            pass

    def unregister_order_event_callback(self, cb):
        try:
            self._order_event_callbacks.discard(cb)
        except Exception:
            pass

    # ── Batch fill-event management ──────────────────────────────────────

    def register_fill_event(self, order_no: str) -> asyncio.Event:
        """Register an asyncio.Event for an order_no. Returns the Event."""
        evt = asyncio.Event()
        self._order_fill_events[self.normalize_order_no(order_no)] = evt
        return evt

    def unregister_fill_event(self, order_no: str) -> None:
        self._order_fill_events.pop(self.normalize_order_no(order_no), None)
        self._order_fill_payloads.pop(self.normalize_order_no(order_no), None)

    def get_fill_payload(self, order_no: str) -> dict | None:
        return self._order_fill_payloads.get(self.normalize_order_no(order_no))

    def _notify_fill_event(self, order_no: str, payload: dict | None = None) -> None:
        """Signal the fill event for a given order_no (called from WS dispatch)."""
        key = self.normalize_order_no(order_no)
        evt = self._order_fill_events.get(key)
        if evt is not None:
            if payload:
                self._order_fill_payloads[key] = payload
            evt.set()

    async def register_real_for_orders(self, grp_no: str, codes: list[str], refresh: str = "1") -> None:
        """Register given tickers for order-execution real data (type '00').

        This does not modify the installed `kiwoom` package; it sends the
        same `REG` websocket payload that the library's `register_tick`
        uses but requests type '00' (주문체결).
        """
        try:
            if not hasattr(self.api, "socket"):
                return
            assert isinstance(codes, list)
            assert len(codes) <= 100, "Max 100 codes per group"
            await self.api.socket.send(
                {
                    "trnm": "REG",
                    "grp_no": grp_no,
                    "refresh": refresh,
                    "data": [
                        {
                            "item": codes,
                            "type": ["00"],
                        }
                    ],
                }
            )
        except Exception:
            # best-effort: do not raise to avoid breaking caller flows
            return

    async def remove_real_registration(self, grp_no: str, codes: list[str], types: str | list[str] = "00") -> None:
        """Remove previously registered real data for a group (wrapper).

        Delegates to `api.remove_register` if available, otherwise sends
        a REMOVE payload over the socket.
        """
        try:
            if hasattr(self.api, "remove_register"):
                await self.api.remove_register(grp_no=grp_no, codes=codes, type=types)
                return
        except Exception:
            pass

        # fallback: send REMOVE payload directly
        try:
            t = types if isinstance(types, list) else [types]
            if not hasattr(self.api, "socket"):
                return
            await self.api.socket.send(
                {
                    "trnm": "REMOVE",
                    "grp_no": grp_no,
                    "refresh": "",
                    "data": [{"item": codes, "type": t}],
                }
            )
        except Exception:
            return

    async def _dispatch_order_event(self, payload: object) -> None:
        # ── Fast dict-based dispatch for batch monitoring ────────────────
        if isinstance(payload, dict) and self._order_fill_events:
            try:
                self._try_dispatch_fill_event(payload)
            except Exception:
                pass

        # ── Legacy broadcast to registered callbacks ────────────────────
        for cb in list(self._order_event_callbacks):
            try:
                if asyncio.iscoroutinefunction(cb):
                    await cb(payload)
                else:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, lambda: cb(payload))
            except Exception:
                pass

    # Keys from Kiwoom websocket schema
    _WS_ORD_KEYS = {"ord_no", "order_no", "주문번호", "9203", "904"}
    _WS_REM_KEYS = {"ord_remnq", "unfill_qty", "미체결수량", "902"}
    _WS_CNTR_KEYS = {"cntr_qty", "체결수량", "911"}

    def _try_dispatch_fill_event(self, payload: dict) -> None:
        """Check WS payload for order completion and signal the matching fill event."""
        def _find(node, keys):
            if isinstance(node, dict):
                for k in keys:
                    if k in node:
                        return node[k]
                for v in node.values():
                    if isinstance(v, (dict, list)):
                        r = _find(v, keys)
                        if r is not None:
                            return r
            elif isinstance(node, list):
                for item in node:
                    r = _find(item, keys)
                    if r is not None:
                        return r
            return None

        ord_no_raw = _find(payload, self._WS_ORD_KEYS)
        if not ord_no_raw:
            return
        ord_no = str(ord_no_raw).strip()
        if not ord_no:
            return

        key = self.normalize_order_no(ord_no)
        if key not in self._order_fill_events:
            return

        rem_raw = _find(payload, self._WS_REM_KEYS)
        try:
            rem = int(float(rem_raw or 0)) if rem_raw is not None else None
        except Exception:
            rem = None

        if rem is not None and rem <= 0:
            self._notify_fill_event(ord_no, payload)
            return

        cntr_raw = _find(payload, self._WS_CNTR_KEYS)
        try:
            cntr = int(float(cntr_raw or 0)) if cntr_raw is not None else None
        except Exception:
            cntr = None

        # We don't have requested_qty here, so only mark filled if remaining==0
        # The cntr_qty check requires knowing total; leave it for batch poll
        if cntr is not None and rem is None:
            # cannot determine fill without remaining qty; skip
            pass

    # Note: an adapter-level websocket listener was removed because the
    # installed `kiwoom.API` already manages websocket connections and
    # dispatches real-time messages via its callback hooks.

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
        explicit_tick: int | None = None,
    ) -> SubmittedOrder:
        # Normalize ticker centrally and log mapping for easier debugging
        norm_ticker = self.normalize_ticker(ticker)
        if norm_ticker != str(ticker).strip():
            print(f"[ORDER] ticker normalized: original={ticker} -> normalized={norm_ticker}")

        # 가격 사전 라운딩: explicit_tick 우선, 없으면 내부 밴드 사용
        try:
            adj_price = int(price or 0)
        except Exception:
            adj_price = 0

        try:
            if explicit_tick and int(explicit_tick) > 0:
                # Use provided tick to round to nearest valid unit
                tick = int(explicit_tick)
                # nearest rounding
                rem = adj_price % tick
                if rem * 2 < tick:
                    adj_price = adj_price - rem
                else:
                    adj_price = adj_price + (tick - rem)
            else:
                adj_price = self.round_price_to_tick(adj_price, mode="nearest")
        except Exception:
            pass

        # Log what tick and adjusted price will be used for submission
        try:
            used_tick = int(explicit_tick) if explicit_tick and int(explicit_tick) > 0 else self.get_tick_size(adj_price)
        except Exception:
            used_tick = None
        try:
            print(f"[ORDER] submit_prepare ticker={norm_ticker} side={side} requested_price={price} adj_price={adj_price} used_tick={used_tick} explicit_tick={explicit_tick}")
        except Exception:
            pass

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
        # ensure ord_uv uses adjusted price for non-market orders in mock mode
        if self.config.is_mock:
            if order_type and str(order_type).strip():
                data["trde_tp"] = str(order_type)
                # 시장가는 단가를 전송하지 않음
                if str(order_type).strip() != "3":
                    try:
                        if adj_price and int(adj_price) > 0:
                            data["ord_uv"] = str(adj_price)
                    except Exception:
                        pass
            else:
                if adj_price and int(adj_price) > 0:
                    data["trde_tp"] = "0"  # 보통
                    data["ord_uv"] = str(adj_price)
                else:
                    data["trde_tp"] = "3"  # 시장가
                    # 시장가는 단가를 전송하지 않음
        else:
            # 실거래 모드: `trde_tp`는 매수/매도 구분용 코드가 아니라 '주문유형'을 나타내야 합니다.
            # API ID는 매수/매도에 따라 변경되므로 trde_tp는 order_type 파라미터 또는
            # 지정가/시장가 판단으로 설정합니다.
            data["ord_uv"] = str(adj_price)
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

        retries = max(0, int(getattr(self.config, "common_request_retries", 3)))
        backoff = float(getattr(self.config, "common_request_retry_backoff_seconds", 0.5) or 0.5)
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
                    price=int(adj_price),
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
                        # Use a fixed small delay between retries instead of exponential backoff.
                        try:
                            if self.config.is_mock:
                                sleep_for = float(getattr(self.config, "mock_request_delay_seconds", 0.3))
                            else:
                                sleep_for = float(getattr(self.config, "live_request_delay_seconds", 0.1))
                        except Exception:
                            sleep_for = 0.3 if self.config.is_mock else 0.1
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

        # Try primary account-order TR (kt00007) first using paginated helper
        try:
            body = await self.request_endpoint_paginated(
                endpoint=self.config.order_status_endpoint,
                api_id=self.config.order_status_api_id,
                data=data,
                list_key="acnt_ord_cntr_prps_dtl",
                max_pages=int(getattr(self.config, "order_status_max_pages", 50) or 50),
            )
        except Exception:
            body = None

        records = self._extract_records(body) if body else []

        # 1) ord_no로 우선 정확 매칭 (정규화된 비교 포함)
        for rec in records:
            rec_order_no = self._extract_order_no(rec)
            if rec_order_no and order.order_no:
                try:
                    if self.normalize_order_no(rec_order_no) == self.normalize_order_no(order.order_no) or rec_order_no.endswith(order.order_no) or order.order_no.endswith(rec_order_no):
                        pending = self._extract_pending_qty(rec)
                        print(f"[MATCH] ord_no matched: submitted={order.order_no} record={rec_order_no} pending={pending} rec_sample={json.dumps({k: rec.get(k) for k in ('stk_cd','ord_qty','ord_uv','ord_remnq','cntr_qty','cnfm_qty')}, ensure_ascii=False)}")
                        return pending
                except Exception:
                    # fallback to raw equality
                    if rec_order_no == order.order_no:
                        pending = self._extract_pending_qty(rec)
                        return pending

        # 2) ord_no 매칭 실패시 폴백: 티커 기준으로 수량/단가 매칭 시도
        for rec in records:
            rec_ticker = rec.get("stk_cd") or rec.get("티커") or rec.get("종목코드")
            if not rec_ticker:
                continue
            if not str(rec_ticker).endswith(order.ticker):
                continue

            # 주문수량 추출
            rec_ord_qty = 0
            try:
                rec_ord_qty = int(rec.get("ord_qty") or rec.get("주문수량") or rec.get("ordQty") or 0)
            except Exception:
                rec_ord_qty = 0

            # 주문단가 추출
            rec_price = 0
            try:
                rec_price = int(rec.get("ord_uv") or rec.get("주문단가") or rec.get("ordPrice") or 0)
            except Exception:
                rec_price = 0

            if order.requested_qty and rec_ord_qty and rec_ord_qty == order.requested_qty:
                pending = self._extract_pending_qty(rec)
                print(f"[MATCH-FALLBACK] ticker+qty matched: ticker={order.ticker} rec_ord_qty={rec_ord_qty} pending={pending} rec_ord_no={self._extract_order_no(rec)}")
                return pending

            if order.price and rec_price and rec_price == order.price:
                pending = self._extract_pending_qty(rec)
                print(f"[MATCH-FALLBACK] ticker+price matched: ticker={order.ticker} rec_price={rec_price} pending={pending} rec_ord_no={self._extract_order_no(rec)}")
                return pending

        # 아직 매칭 실패: unfilled TR (ka10075)로 추가 조회하여 시도
        try:
            unfilled = await self.query_unfilled_orders(order.ticker)
        except Exception:
            unfilled = []

        for rec in unfilled:
            rec_order_no = self._extract_order_no(rec)
            if rec_order_no and order.order_no:
                if self.normalize_order_no(rec_order_no) == self.normalize_order_no(order.order_no) or rec_order_no.endswith(order.order_no) or order.order_no.endswith(rec_order_no):
                    pending = self._extract_pending_qty(rec)
                    print(f"[UNFILLED-FALLBACK] ord_no matched on ka10075: submitted={order.order_no} record={rec_order_no} pending={pending}")
                    return pending

        for rec in unfilled:
            rec_ticker = rec.get("stk_cd") or rec.get("티커") or rec.get("종목코드")
            if not rec_ticker:
                continue
            if not str(rec_ticker).endswith(order.ticker):
                continue

            try:
                rec_ord_qty = int(rec.get("ord_qty") or rec.get("oso_qty") or rec.get("주문수량") or 0)
            except Exception:
                rec_ord_qty = 0
            try:
                rec_price = int(rec.get("ord_uv") or rec.get("ord_pric") or rec.get("주문단가") or 0)
            except Exception:
                rec_price = 0

            if order.requested_qty and rec_ord_qty and rec_ord_qty == order.requested_qty:
                pending = self._extract_pending_qty(rec)
                print(f"[UNFILLED-FALLBACK] ticker+qty matched on ka10075: ticker={order.ticker} rec_ord_qty={rec_ord_qty} pending={pending}")
                return pending
            if order.price and rec_price and rec_price == order.price:
                pending = self._extract_pending_qty(rec)
                print(f"[UNFILLED-FALLBACK] ticker+price matched on ka10075: ticker={order.ticker} rec_price={rec_price} pending={pending}")
                return pending

        # 최종적으로 매칭 실패: 0(미확인) 반환
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

    async def _fetch_all_open_orders(self) -> list[dict[str, Any]]:
        """Fetch the full list of open orders from kt00007 in a single paginated call."""
        data = {
            "qry_tp": "0",
            "sell_tp": "0",
            "stk_bond_tp": "1",
            "mrkt_tp": "0",
            "dmst_stex_tp": "KRX",
        }
        if self.config.account_no:
            data["acnt_no"] = self.config.account_no

        try:
            body = await self.request_endpoint_paginated(
                endpoint=self.config.order_status_endpoint,
                api_id=self.config.order_status_api_id,
                data=data,
                list_key="acnt_ord_cntr_prps_dtl",
                max_pages=int(getattr(self.config, "order_status_max_pages", 50) or 50),
            )
        except Exception:
            body = None

        return self._extract_records(body) if body else []

    def _match_order_in_records(
        self, order: SubmittedOrder, records: list[dict[str, Any]]
    ) -> int:
        """Match a SubmittedOrder against pre-fetched records and return pending qty."""
        # 1) ord_no exact match
        for rec in records:
            rec_order_no = self._extract_order_no(rec)
            if rec_order_no and order.order_no:
                try:
                    if (
                        self.normalize_order_no(rec_order_no) == self.normalize_order_no(order.order_no)
                        or rec_order_no.endswith(order.order_no)
                        or order.order_no.endswith(rec_order_no)
                    ):
                        return self._extract_pending_qty(rec)
                except Exception:
                    if rec_order_no == order.order_no:
                        return self._extract_pending_qty(rec)

        # 2) ticker + qty/price fallback
        for rec in records:
            rec_ticker = rec.get("stk_cd") or rec.get("티커") or rec.get("종목코드")
            if not rec_ticker or not str(rec_ticker).endswith(order.ticker):
                continue
            try:
                rec_ord_qty = int(rec.get("ord_qty") or rec.get("주문수량") or 0)
            except Exception:
                rec_ord_qty = 0
            try:
                rec_price = int(rec.get("ord_uv") or rec.get("주문단가") or 0)
            except Exception:
                rec_price = 0
            if order.requested_qty and rec_ord_qty and rec_ord_qty == order.requested_qty:
                return self._extract_pending_qty(rec)
            if order.price and rec_price and rec_price == order.price:
                return self._extract_pending_qty(rec)

        return 0

    async def check_orders_batch(self, submitted_orders: list[SubmittedOrder]) -> list[OrderCheck]:
        """Check all orders with a single kt00007 call + optional ka10075 fallback."""
        records = await self._fetch_all_open_orders()
        print(f"[BATCH] fetched {len(records)} open-order records for {len(submitted_orders)} orders")

        checks: list[OrderCheck] = []
        unmatched: list[SubmittedOrder] = []

        for order in submitted_orders:
            pending = self._match_order_in_records(order, records)
            if pending > 0:
                checks.append(OrderCheck(submitted=order, pending_qty=pending, is_filled=False))
            elif pending == 0:
                # Could be filled OR simply not in the open-order list (also filled)
                checks.append(OrderCheck(submitted=order, pending_qty=0, is_filled=True))
            else:
                unmatched.append(order)
                checks.append(OrderCheck(submitted=order, pending_qty=0, is_filled=True))

        return checks

    async def run_retry_cycle(
        self,
        submitted_orders: list[SubmittedOrder],
    ) -> tuple[list[SubmittedOrder], list[OrderCheck]]:
        checks = await self.check_orders_batch(submitted_orders)
        retries: list[SubmittedOrder] = []

        for check in checks:
            if check.is_filled:
                continue

            order = check.submitted
            pending_qty = check.pending_qty

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
        """Check orders — delegates to batch version for efficiency."""
        return await self.check_orders_batch(submitted_orders)

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
        retries = max(0, int(getattr(self.config, "common_request_retries", 0)))
        backoff = float(getattr(self.config, "common_request_retry_backoff_seconds", 0.5) or 0.5)
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
                # Try to extract tick-size from various possible fields returned by API
                tick = None
                try:
                    # common keys that might indicate tick size
                    for k in ("tick", "tick_size", "hoga_unit", "hoga", "ho_ga", "호가단위", "호가_단위", "가격단위"):
                        v = body.get(k)
                        if v is None:
                            # search nested structures
                            def _find_in(node):
                                if isinstance(node, dict):
                                    if k in node and node[k] is not None:
                                        return node[k]
                                    for val in node.values():
                                        r = _find_in(val)
                                        if r is not None:
                                            return r
                                elif isinstance(node, list):
                                    for item in node:
                                        r = _find_in(item)
                                        if r is not None:
                                            return r
                                return None

                            v = _find_in(body)
                        if v is not None:
                            try:
                                tick = int(float(str(v).strip().replace(',', '')))
                                if tick > 0:
                                    break
                            except Exception:
                                tick = None
                except Exception:
                    tick = None

                return BestQuote(ask1=ask, bid1=bid, tick_size=tick)

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

    async def _request_paginated(
        self,
        endpoint: str,
        api_id: str,
        data: dict[str, Any],
        *,
        list_key: str = "list",
        max_pages: int = 50,
        max_retries: int = 3,
    ) -> dict[str, Any]:
        """Paginate through a Kiwoom endpoint using cont-yn/next-key continuation headers.

        Each page is fetched via ``_request()``, inheriting rate-limiting, token-bucket,
        per-request delay, and logging.  Per-page 429 / timeout errors trigger a retry
        with exponential back-off before giving up and re-raising.

        List values found under ``list_key`` (and any other list fields) are accumulated
        across pages.  Scalar fields from the first page are kept as-is.
        """
        if not endpoint or not api_id:
            raise RuntimeError("Kiwoom endpoint or api_id not configured")

        accumulated: dict[str, Any] = {}
        page_headers: dict[str, str] | None = None

        for page_num in range(1, max(1, max_pages) + 1):
            # --- per-page retry loop ---
            response = None
            for attempt in range(1, max_retries + 1):
                try:
                    response = await self._request(endpoint, api_id, data, headers=page_headers)
                    break  # success
                except Exception as exc:
                    status = getattr(exc, "status", None)
                    # KiwoomAPIError with return_code 5 is the body-level rate limit
                    is_retriable = status in (429, 408)
                    if not is_retriable and isinstance(exc, KiwoomAPIError):
                        try:
                            is_retriable = int(exc.return_code or 0) in (5, 429)
                        except Exception:
                            pass
                    if is_retriable and attempt < max_retries:
                        try:
                            base = float(
                                getattr(self.config, "mock_request_delay_seconds", 0.3)
                                if self.config.is_mock
                                else getattr(self.config, "live_request_delay_seconds", 0.1)
                            )
                        except Exception:
                            base = 0.3
                        wait = base * (2 ** (attempt - 1))
                        print(
                            f"[PAGINATE] p{page_num} attempt={attempt}/{max_retries} "
                            f"retriable error, retrying after {wait:.2f}s "
                            f"endpoint={endpoint} api_id={api_id}"
                        )
                        await asyncio.sleep(wait)
                        continue
                    raise  # non-retriable or retries exhausted

            if response is None:  # safety guard — should not happen
                break

            # --- accumulate page body ---
            body = response.json()
            if not isinstance(body, dict):
                break
            for key, val in body.items():
                if isinstance(val, list):
                    existing = accumulated.get(key)
                    if isinstance(existing, list):
                        existing.extend(val)
                    else:
                        accumulated[key] = list(val)
                elif key not in accumulated:
                    # keep scalar fields from first page only
                    accumulated[key] = val

            # --- check continuation ---
            resp_headers = response.headers or {}
            if resp_headers.get("cont-yn") != "Y" or not resp_headers.get("next-key"):
                break
            # Build continuation headers for the next page
            page_headers = self.api.headers(
                api_id, cont_yn="Y", next_key=resp_headers["next-key"]
            )

        return accumulated

    async def request_endpoint_paginated(
        self,
        endpoint: str,
        api_id: str,
        data: dict[str, Any],
        *,
        list_key: str = "list",
        max_pages: int = 50,
    ) -> dict[str, Any]:
        """Call Kiwoom endpoint with cont-yn/next-key pagination.  Delegates to
        ``_request_paginated()`` so every page is rate-limited and retry-protected.
        """
        return await self._request_paginated(
            endpoint, api_id, data, list_key=list_key, max_pages=max_pages
        )

    async def query_unfilled_orders(self, stk_cd: str | None = None, *, trde_tp: str = "0") -> list[dict[str, Any]]:
        """Fetch unfilled orders using ka10075 (미체결요청) and return a list of records.

        This follows cont-yn/next-key via `request_endpoint_paginated` and
        returns the combined list under the 'oso' key.
        """
        endpoint = getattr(self.config, "order_unfilled_endpoint", None) or getattr(self.config, "order_status_endpoint", None)
        api_id = getattr(self.config, "order_unfilled_api_id", None) or "ka10075"

        data = {
            "all_stk_tp": "0",
            "trde_tp": str(trde_tp or "0"),
            # default to KRX-only unless caller specifies otherwise
            "stex_tp": "1",
        }
        if stk_cd:
            # ka10075 expects 6-digit code often, but accept normalized or raw
            data["stk_cd"] = str(stk_cd)

        try:
            body = await self.request_endpoint_paginated(
                endpoint=endpoint,
                api_id=api_id,
                data=data,
                list_key="oso",
                max_pages=int(getattr(self.config, "order_unfilled_max_pages", 50) or 50),
            )
        except Exception:
            return []

        if not isinstance(body, dict):
            return []

        vals = body.get("oso") or body.get("list") or []
        if isinstance(vals, list):
            return [v for v in vals if isinstance(v, dict)]
        return []

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
        retries = max(1, int(getattr(self.config, "common_request_retries", 3) or 3))
        backoff_base = float(getattr(self.config, "common_request_retry_backoff_seconds", 0.5) or 0.5)
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
        # Prefer explicit remaining quantity fields when present
        rem_keys = ["ord_remnq", "unfill_qty", "미체결수량", "미체결잔량", "cncl_qty", "주문잔량"]
        for key in rem_keys:
            value = rec.get(key)
            if value is None:
                continue
            try:
                val = int(float(value))
                print(f"[PENDING-EXTRACT] found rem_key={key} raw={value} parsed={val} rec_ord_no={self._extract_order_no(rec)}")
                return max(0, val)
            except Exception:
                continue

        # If remaining/ord_remnq not provided, infer using best available filled quantity.
        # Some APIs include 'cntr_qty' (execution quantity) and also 'cnfm_qty' (confirmed qty).
        requested = rec.get("ord_qty") or rec.get("주문수량")
        cntr = rec.get("cntr_qty") or rec.get("체결수량")
        cnfm = rec.get("cnfm_qty") or rec.get("확인수량")

        try:
            req = int(float(requested or 0))
        except Exception:
            req = 0

        # treat confirmed and executed quantities as the effective filled quantity
        try:
            fil_cntr = int(float(cntr or 0))
        except Exception:
            fil_cntr = 0
        try:
            fil_cnfm = int(float(cnfm or 0))
        except Exception:
            fil_cnfm = 0

        filled = max(fil_cntr, fil_cnfm)
        # Log inference for debugging (when explicit remnant not provided)
        try:
            req_str = requested or rec.get("ord_qty")
            print(f"[PENDING-EXTRACT] inferred: requested={req_str} filled_cntr={fil_cntr} filled_cnfm={fil_cnfm} inferred_pending={max(0, req - filled)} rec_ord_no={self._extract_order_no(rec)}")
        except Exception:
            pass
        return max(0, req - filled)

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

    def get_tick_size(self, price: int) -> int:
        """Return KRX tick size for a given price.

        Implements the 2023-01 KRX tick-size bands (same for KOSPI/KOSDAQ/KONEX):

            - < 1,000원: 1원
            - 1,000원 ~ 2,000원 미만: 1원
            - 2,000원 ~ 5,000원 미만: 5원
            - 5,000원 ~ 10,000원 미만: 10원
            - 10,000원 ~ 20,000원 미만: 10원
            - 20,000원 ~ 50,000원 미만: 50원
            - 50,000원 ~ 100,000원 미만: 100원
            - 100,000원 ~ 200,000원 미만: 100원
            - 200,000원 ~ 500,000원 미만: 500원
            - >= 500,000원: 1,000원
        """
        try:
            p = int(price)
        except Exception:
            return 1

        if p < 1000:
            return 1
        if p < 2000:
            return 1
        if p < 5000:
            return 5
        if p < 10000:
            return 10
        if p < 20000:
            return 10
        if p < 50000:
            return 50
        if p < 100000:
            return 100
        if p < 200000:
            return 100
        if p < 500000:
            return 500
        return 1000

    def round_price_to_tick(self, price: int, mode: str = "nearest") -> int:
        """Round given price to tick unit.

        mode: 'nearest'|'down'|'up'
        """
        try:
            p = int(price)
        except Exception:
            return int(price or 0)

        tick = max(1, int(self.get_tick_size(p)))
        if tick <= 1:
            return p

        if mode == "down":
            return (p // tick) * tick
        if mode == "up":
            return ((p + tick - 1) // tick) * tick
        # nearest
        rem = p % tick
        if rem * 2 < tick:
            return p - rem
        return p + (tick - rem)
