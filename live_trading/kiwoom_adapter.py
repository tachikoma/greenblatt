from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

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


class KiwoomBrokerAdapter:
    """kiwoom-restful 기반 브로커 어댑터.

    주문 API 스펙이 계좌/상품별로 달라질 수 있어 endpoint/api-id는 환경변수로 주입한다.
    """

    def __init__(self, config: LiveTradingConfig):
        self.config = config
        host = MOCK if config.is_mock else REAL
        self.api = API(host=host, appkey=config.appkey, secretkey=config.secretkey)
        self._quote_response_logged = False

    async def __aenter__(self) -> "KiwoomBrokerAdapter":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def connect(self) -> None:
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
                        qty = int(float(qty_str or 0))
                        if qty > 0:
                            holdings[ticker] = qty
                    except (ValueError, TypeError):
                        pass
        
        return AccountSnapshot(cash=cash, holdings=holdings)
    
    async def _get_account_balance(self) -> dict[str, any]:
        """키움 kt00018 API로 계좌평가잔고 조회"""
        data = {
            "qry_tp": "1",  # 조회구분: 1=합산, 2=개별
            "dmst_stex_tp": "KRX",  # 국내거래소구분
        }
        
        response = await self.api.request(
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
        data = {
            "dmst_stex_tp": "KRX",
            "stk_cd": ticker,
            "ord_qty": str(quantity),
            "ord_uv": str(price),
            "trde_tp": "1" if side.upper() == "BUY" else "2",
            "ord_cond": order_type,
        }
        if self.config.account_no:
            data["acnt_no"] = self.config.account_no

        response = await self.api.request(
            endpoint=self.config.order_endpoint,
            api_id=self.config.order_api_id,
            data=data,
        )
        body = response.json()
        order_no = self._extract_order_no(body)
        return SubmittedOrder(
            ticker=ticker,
            side=side,
            requested_qty=quantity,
            price=price,
            order_no=order_no,
            raw_response=body,
        )

    async def query_open_order_pending_qty(self, order: SubmittedOrder) -> int:
        data = {
            "qry_tp": "0",
            "sell_tp": "0",
            "stk_bond_tp": "1",
            "mrkt_tp": "0",
        }
        if self.config.account_no:
            data["acnt_no"] = self.config.account_no

        response = await self.api.request(
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

        response = await self.api.request(
            endpoint=self.config.order_cancel_endpoint,
            api_id=self.config.order_cancel_api_id,
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

            await self.cancel_order(order, pending_qty)
            retry_price = await self.get_retry_price(
                side=order.side,
                ticker=order.ticker,
                base_price=order.price,
            )
            retried = await self.submit_order(
                ticker=order.ticker,
                side=order.side,
                quantity=pending_qty,
                price=max(1, retry_price),
                order_type=self.config.retry_order_type,
            )
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
        data = {
            "stk_cd": ticker,
            "mrkt_tp": self.config.quote_market_type,
        }
        response = await self.api.request(
            endpoint=self.config.quote_endpoint,
            api_id=self.config.quote_api_id,
            data=data,
        )
        body = response.json()
        self._log_quote_response_once(ticker=ticker, body=body)

        ask = self._extract_quote_value(
            body,
            ["ask1", "ask_pri_1", "offerho1", "매도호가1", "41"],
        )
        bid = self._extract_quote_value(
            body,
            ["bid1", "bid_pri_1", "bidho1", "매수호가1", "51"],
        )
        return BestQuote(ask1=ask, bid1=bid)

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

        for key in ["output", "data", "list", "acnt_ord_cntr_prst_array"]:
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
            "acnt_ord_cntr_prst_array",
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
        keys = ["rmn_qty", "unfill_qty", "미체결수량", "미체결잔량", "cncl_qty"]
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
