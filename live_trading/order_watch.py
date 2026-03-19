import asyncio
import time
from typing import Optional

from .kiwoom_adapter import KiwoomBrokerAdapter, SubmittedOrder


class OrderWatch:
    """웹소켓 이벤트와 폴링을 사용하여 제출된 주문의 최종 상태를 모니터링합니다.

    사용법:
        ow = OrderWatch(adapter, submitted_order)
        result = await ow.wait_for_fill()

    반환값: 다음 키를 가진 dict
      - 'state': 'FILLED'|'PENDING'|'AMENDED'|'TIMEOUT'
      - 'pending_qty'
      - 'attempts'
      - 'last_event'
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
        # payload는 JSON dict 또는 다른 형태일 수 있으므로 일치하는 주문을 탐지하려 시도합니다
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

        # Kiwoom 웹소켓 스키마에서 올 수 있는 필드 키들
        ord_keys = {"ord_no", "order_no", "주문번호", "9203", "904"}
        ticker_keys = {"stk_cd", "ticker", "종목번호", "9001"}
        rem_keys = {"ord_remnq", "unfill_qty", "미체결수량", "902"}
        cntr_keys = {"cntr_qty", "체결수량", "911"}

        try:
            ord_no = str(_find_key(payload, ord_keys) or "").strip()
            stk = str(_find_key(payload, ticker_keys) or "").strip()

            # 디버깅을 위해 중요한 상태 코드를 로그합니다 (예: 912=주문상태코드, 913=상태명)
            try:
                s_912 = _find_key(payload, {"912"})
                s_913 = _find_key(payload, {"913"})
                if s_912 is not None or s_913 is not None:
                    print(f"[WS-EVENT] trnm={payload.get('trnm') or 'REAL'} type={_find_key(payload, {'type'})} item={_find_key(payload, {'item','9001','stk_cd'})} ord_no={ord_no or _find_key(payload, {'9203','904','ord_no','order_no'})} status_912={s_912} status_913={s_913}")
            except Exception:
                pass

            # order_no가 있고 일치하면 cntr_qty 또는 rem이 전체 체결을 가리킬 때 체결로 표시합니다
            if ord_no and self.order.order_no and ord_no == self.order.order_no:
                rem = _find_key(payload, rem_keys)
                cntr = _find_key(payload, cntr_keys)
                try:
                    # 남은 수량을 나타내는 WS 이벤트에 대한 상세 로그
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
                # 폴백: 해당 ord_no에 대한 어떤 업데이트든 진행 상황으로 간주합니다
                self._last_event = payload
                return

            # order_no가 없을 때의 폴백: 티커 기준으로 매칭 시도
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
        """레거시 주문별 폴링 루프입니다.

        `run_orderwatch_mock.py`와의 하위 호환을 위해 유지하지만
        배치 모드에서는 기본적으로 시작되지 않습니다.
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
        """웹소켓 콜백을 등록합니다.

        콜백 등록만 수행하며 네트워크 폴링은 실행하지 않습니다.
        등록 실패 시 진단 로그를 출력합니다.
        여러 번 호출해도 안전하며 이후 호출은 무시됩니다.
        """
        if not getattr(self, "_listeners_registered", False):
            try:
                self.adapter.register_order_event_callback(self._ws_callback)
                self._listeners_registered = True
            except Exception as exc:
                # 콜백 등록 실패 원인 진단을 돕는 상세 디버그 로그
                try:
                    api_present = hasattr(self.adapter, "api")
                    socket_present = api_present and hasattr(self.adapter.api, "socket")
                    print(f"[WARN] OrderWatch.register_listeners: register_order_event_callback failed: {exc}")
                    print(f"[WARN] OrderWatch.register_listeners: adapter_info api_present={api_present} socket_present={socket_present}")
                except Exception:
                    print(f"[WARN] OrderWatch.register_listeners: register failed: {exc}")
                self._listeners_registered = False

    async def unregister_listeners(self):
        """웹소켓 콜백 등록 해제만 수행합니다 (폴링 루프 제어는 하지 않음)."""
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
        """내부 폴링 루프 태스크를 중지하고 대기합니다(실행 중인 경우).

        `unregister_listeners`와 분리되어 있어 콜링 측이
        웹소켓 콜백 제거와 폴링 중지를 독립적으로 선택할 수 있습니다.
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
        """체결될 때까지 또는 타임아웃까지 대기합니다; 타임아웃 시 정정(amend) 정책을 시도합니다.

        인자:
            enable_poll_loop: True면 주문별 폴링 루프를 시작합니다(레거시 동작).
                배치 모드에서는 호출자가 전체 배치 폴링을 수행하므로 기본값(False)을 권장합니다.
        """
        # 리스너/폴링이 시작되었는지 확인합니다 (호출자가 미리 시작했을 수 있음)
        try:
            await self.register_listeners()
        except Exception:
            pass

        # 어댑터가 지원하면 이 티커에 대해 주문 레벨 실시간 데이터(type '00')를 등록합니다
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

        # 폴링 루프: 레거시(비배치) 모드에서만 시작합니다
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
            # 타임아웃: 최대 max_amend까지 정정/취소 정책을 시도합니다
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

                # 재시도 가격을 계산하고 정정을 시도합니다
                try:
                    retry_price = await self.adapter.get_retry_price(side=self.order.side, ticker=self.order.ticker, base_price=self.order.price)
                    await self.adapter.modify_order(order=self.order, new_qty=pending, new_price=max(1, int(retry_price)))
                    self._attempts += 1
                    # 정정 후 짧은 대기 시간을 둡니다
                    try:
                        await asyncio.wait_for(self._done_event.wait(), timeout=max(5, self.poll_interval * 10))
                        state = "FILLED"
                        break
                    except asyncio.TimeoutError:
                        state = "AMENDED"
                        continue
                except Exception as exc:
                    # 정정 실패: 취소+재등록 시도는 복잡하므로 보류 상태로 둡니다
                    state = "PENDING"
                    continue

        finally:
            # 먼저 폴링 루프를 정리한 후 콜백 등록을 해제합니다
            try:
                await self.stop_poll_loop()
            except Exception:
                pass
            try:
                await self.unregister_listeners()
            except Exception:
                pass

            # 생성한 실시간 등록이 있으면 제거합니다
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
