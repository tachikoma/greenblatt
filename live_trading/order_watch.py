import asyncio
import time
from typing import Optional

from .kiwoom_adapter import KiwoomBrokerAdapter, SubmittedOrder


class OrderWatch:
    """Watch a submitted order until final state using websocket events + polling.

    Usage:
        ow = OrderWatch(adapter, submitted_order)
        result = await ow.wait_for_fill()

    Result: dict with keys: 'state' ('FILLED'|'PENDING'|'AMENDED'|'TIMEOUT'),
    'pending_qty', 'attempts', 'last_event'
    """

    def __init__(
        self,
        adapter: KiwoomBrokerAdapter,
        order: SubmittedOrder,
        *,
        poll_interval: float | None = None,
        timeout_seconds: float | None = None,
        max_amend: int | None = None,
        amend_strategy: str | None = None,
    ) -> None:
        self.adapter = adapter
        self.order = order
        self.poll_interval = float(poll_interval or float(getattr(adapter.config, 'order_fill_poll_interval', None) or 0.5))
        self.timeout_seconds = float(timeout_seconds or float(getattr(adapter.config, 'order_fill_timeout_seconds', None) or 60))
        self.max_amend = int(max_amend if max_amend is not None else (getattr(adapter.config, 'order_fill_max_amend', None) or 2))
        self.amend_strategy = amend_strategy or getattr(adapter.config, "order_fill_amend_strategy", "reduce_price")

        self._done_event = asyncio.Event()
        self._last_event = None
        self._attempts = 0
        self._poll_task: Optional[asyncio.Task] = None
        self._listeners_registered = False

    async def _ws_callback(self, payload):
        # payload may be JSON dict or other shapes; attempt to detect matching order
        if not isinstance(payload, dict):
            return

        def _find_key(node, targets):
            """Recursively search dict/list for any of the target keys. Returns first found value."""
            if node is None:
                return None
            if isinstance(node, dict):
                for k in list(node.keys()):
                    if str(k) in targets:
                        return node.get(k)
                for v in node.values():
                    if isinstance(v, (dict, list)):
                        r = _find_key(v, targets)
                        if r is not None:
                            return r
            elif isinstance(node, list):
                for item in node:
                    r = _find_key(item, targets)
                    if r is not None:
                        return r
            return None

        # possible keys for fields from Kiwoom websocket schema
        ord_keys = {"ord_no", "order_no", "주문번호", "9203", "904"}
        ticker_keys = {"stk_cd", "ticker", "종목번호", "9001"}
        rem_keys = {"ord_remnq", "unfill_qty", "미체결수량", "902"}
        cntr_keys = {"cntr_qty", "체결수량", "911"}

        try:
            ord_no = str(_find_key(payload, ord_keys) or "").strip()
            stk = str(_find_key(payload, ticker_keys) or "").strip()

            # Log important status codes for debugging (e.g., 912=주문상태코드, 913=상태명)
            try:
                s_912 = _find_key(payload, {"912"})
                s_913 = _find_key(payload, {"913"})
                if s_912 is not None or s_913 is not None:
                    print(f"[WS-EVENT] trnm={payload.get('trnm') or 'REAL'} type={_find_key(payload, {'type'})} item={_find_key(payload, {'item','9001','stk_cd'})} ord_no={ord_no or _find_key(payload, {'9203','904','ord_no','order_no'})} status_912={s_912} status_913={s_913}")
            except Exception:
                pass

            # if order_no available and matches, mark filled when cntr_qty or rem indicates full
            if ord_no and self.order.order_no and ord_no == self.order.order_no:
                rem = _find_key(payload, rem_keys)
                cntr = _find_key(payload, cntr_keys)
                try:
                    # Detailed logging for WS events that indicate remaining quantity
                    try:
                        rem_val = int(float(rem or 0)) if rem is not None else None
                    except Exception:
                        rem_val = None
                    try:
                        cntr_val = int(float(cntr or 0)) if cntr is not None else None
                    except Exception:
                        cntr_val = None

                    print(f"[WS-EVENT-DETAILED] ord_no={ord_no} rem={rem_val} cntr={cntr_val} ticker={stk} trnm={payload.get('trnm') or 'REAL'}")

                    if rem_val is not None and rem_val <= 0:
                        self._last_event = payload
                        self._done_event.set()
                        return
                    if cntr_val is not None:
                        req = int(float(self.order.requested_qty or 0))
                        if cntr_val >= req:
                            self._last_event = payload
                            self._done_event.set()
                            return
                except Exception:
                    pass
                # fallback: any update for this ord_no mark progress
                self._last_event = payload
                return

            # fallback when order_no missing: match by ticker
            if stk and stk == self.order.ticker:
                rem = _find_key(payload, rem_keys)
                try:
                    if rem is not None and int(float(rem or 0)) <= 0:
                        self._last_event = payload
                        self._done_event.set()
                        return
                except Exception:
                    pass
                self._last_event = payload
        except Exception:
            return

    async def _poll_loop(self):
        """Legacy per-order poll loop. Kept for backward compatibility with
        run_orderwatch_mock.py but no longer started by default in batch mode.
        """
        try:
            while not self._done_event.is_set():
                try:
                    pending = await self.adapter.query_open_order_pending_qty(self.order)
                except Exception:
                    pending = None
                if pending is not None and pending <= 0:
                    self._last_event = {"source": "poll", "pending": pending}
                    self._done_event.set()
                    return
                await asyncio.sleep(self.poll_interval)
        except asyncio.CancelledError:
            return

    def notify_filled(self, payload: dict | None = None) -> None:
        """Called externally (e.g. by batch monitor) to signal this order is filled."""
        if payload:
            self._last_event = payload
        self._done_event.set()

    async def register_listeners(self):
        """Register websocket callbacks.

        This only registers callbacks (no network polling). It logs detailed
        diagnostics if registration fails.
        Safe to call multiple times; subsequent calls are no-ops.
        """
        if not getattr(self, "_listeners_registered", False):
            try:
                self.adapter.register_order_event_callback(self._ws_callback)
                self._listeners_registered = True
            except Exception as exc:
                # Detailed debug log to help diagnose why callback registration failed
                try:
                    api_present = hasattr(self.adapter, "api")
                    socket_present = api_present and hasattr(self.adapter.api, "socket")
                    print(f"[WARN] OrderWatch.register_listeners: register_order_event_callback failed: {exc}")
                    print(f"[WARN] OrderWatch.register_listeners: adapter_info api_present={api_present} socket_present={socket_present}")
                except Exception:
                    print(f"[WARN] OrderWatch.register_listeners: register failed: {exc}")
                self._listeners_registered = False

    async def unregister_listeners(self):
        """Unregister websocket callback only (no poll loop control)."""
        if getattr(self, "_listeners_registered", False):
            try:
                self.adapter.unregister_order_event_callback(self._ws_callback)
            except Exception as exc:
                try:
                    print(f"[WARN] OrderWatch.unregister_listeners: unregister failed: {exc}")
                except Exception:
                    pass
            self._listeners_registered = False

    async def stop_poll_loop(self):
        """Stop and await the internal poll loop task (if running).

        This is separated from `unregister_listeners` so callers can decide
        whether to stop polling independently of removing websocket callbacks.
        """
        try:
            if self._poll_task:
                self._poll_task.cancel()
                try:
                    await self._poll_task
                except asyncio.CancelledError:
                    pass
        except Exception:
            pass
        finally:
            self._poll_task = None

    async def wait_for_fill(self, *, enable_poll_loop: bool = False):
        """Wait until filled or timeout; attempt amend policy on timeout.

        Args:
            enable_poll_loop: If True, starts a per-order poll loop (legacy behavior).
                In batch mode this should be False (default) since the caller
                runs a single batch poll instead.
        """
        # ensure listener/polling started (caller may have pre-started)
        try:
            await self.register_listeners()
        except Exception:
            pass

        # register order-level real data (type '00') for this ticker if adapter supports it
        self._real_registered = False
        self._grp_no = None
        try:
            try:
                norm = self.adapter.normalize_ticker(self.order.ticker)
            except Exception:
                norm = str(self.order.ticker)
            self._grp_no = f"ow_{(self.order.order_no or norm)}_{int(time.time() * 1000)}"
            await self.adapter.register_real_for_orders(self._grp_no, [norm], refresh="1")
            self._real_registered = True
        except Exception:
            self._real_registered = False

        # poll loop: only start in legacy (non-batch) mode
        if enable_poll_loop and self._poll_task is None:
            try:
                self._poll_task = asyncio.create_task(self._poll_loop())
            except Exception:
                pass

        start = time.time()
        try:
            await asyncio.wait_for(self._done_event.wait(), timeout=self.timeout_seconds)
            state = "FILLED"
            pending = 0
        except asyncio.TimeoutError:
            # Timeout: attempt amend/cancel policy up to max_amend
            state = "TIMEOUT"
            pending = None
            for attempt in range(1, max(1, self.max_amend) + 1):
                try:
                    pending = await self.adapter.query_open_order_pending_qty(self.order)
                except Exception:
                    pending = None
                if pending is None or pending <= 0:
                    state = "FILLED"
                    break

                # compute retry price and attempt modify
                try:
                    retry_price = await self.adapter.get_retry_price(side=self.order.side, ticker=self.order.ticker, base_price=self.order.price)
                    await self.adapter.modify_order(order=self.order, new_qty=pending, new_price=max(1, int(retry_price)))
                    self._attempts += 1
                    # wait a short window after amend
                    try:
                        await asyncio.wait_for(self._done_event.wait(), timeout=max(5, self.poll_interval * 10))
                        state = "FILLED"
                        break
                    except asyncio.TimeoutError:
                        state = "AMENDED"
                        continue
                except Exception as exc:
                    # modification failed; attempt cancel+resubmit is complex -> leave as pending
                    state = "PENDING"
                    continue

        finally:
            # cleanup poll loop first, then unregister callbacks
            try:
                await self.stop_poll_loop()
            except Exception:
                pass
            try:
                await self.unregister_listeners()
            except Exception:
                pass

            # remove real registration if we created one
            try:
                if getattr(self, "_real_registered", False) and getattr(self, "_grp_no", None):
                    try:
                        norm = self.adapter.normalize_ticker(self.order.ticker)
                    except Exception:
                        norm = str(self.order.ticker)
                    try:
                        await self.adapter.remove_real_registration(self._grp_no, [norm], types="00")
                    except Exception:
                        pass
            except Exception:
                pass

        try:
            pending_final = await self.adapter.query_open_order_pending_qty(self.order)
        except Exception:
            pending_final = None

        return {
            "state": state,
            "pending_qty": pending_final,
            "attempts": self._attempts,
            "last_event": self._last_event,
        }
