"""
Стакан CLOB (docs/API_NOTES.md, §3).

WebSocket: wss://ws-subscriptions-clob.polymarket.com/ws/market, без авторизации.
  подписка  {"assets_ids":[…],"type":"market","custom_feature_enabled":true}
  на лету   {"assets_ids":[…],"operation":"subscribe"|"unsubscribe"}
  heartbeat текстовый PING каждые 10 с, ответ PONG.

Проверено на живом потоке (2026-10-03):
- в price_change side=BUY меняет bids, SELL - asks; size = новый объём уровня, 0 = уровень удалён;
- порядок уровней в book не гарантирован (в живых данных лучшая цена в конце), поэтому
  лучшие цены всегда считаются как max(bids) / min(asks);
- пустая сторона стакана в best_bid/best_ask приходит как "0" (нет bids) и "1" (нет asks);
- ~0.6% price_change расходятся с best_bid/best_ask сервера (уровень съеден сделкой без
  отдельного price_change). Уровни лучше серверной цены убираем, стакан помечаем
  несогласованным; согласованность возвращает следующий совпавший price_change или снимок
  book. На записи 16 токенов: 95% эпизодов короче 7 мс, самый длинный 0.86 с.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections import Counter
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable

from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus

log = logging.getLogger(__name__)

CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
USER_AGENT = "updown-twap-bot/0.1"


def _d(x: Any) -> Decimal | None:
    if x is None or x == "":
        return None
    try:
        v = Decimal(str(x))
    except InvalidOperation:
        return None
    return v if v.is_finite() else None


def _ms(x: Any) -> float | None:
    try:
        return float(x) / 1000.0
    except (TypeError, ValueError):
        return None


class OrderBook:
    def __init__(self, token_id: str):
        self.token_id = token_id
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.initialized = False
        self.consistent = False
        self.version = 0                 # растёт с каждым изменением (для прореженной записи)
        self.last_update: float | None = None   # время получения последнего изменения
        self.server_ts: float | None = None
        self.tick_size: Decimal | None = None
        self.server_best_bid: Decimal | None = None
        self.server_best_ask: Decimal | None = None
        self.mismatches = 0

    # ---------- изменения ----------

    def apply_snapshot(self, bids: Iterable[dict], asks: Iterable[dict], *, recv_ts: float,
                       server_ts: float | None = None, tick_size: Any = None) -> None:
        self.bids = self._levels(bids)
        self.asks = self._levels(asks)
        self.initialized = True
        self.consistent = True
        self._touch(recv_ts, server_ts)
        if _d(tick_size) is not None:
            self.tick_size = _d(tick_size)

    def apply_change(self, side: str, price: Any, size: Any, *, recv_ts: float,
                     server_ts: float | None = None, best_bid: Any = None, best_ask: Any = None) -> bool:
        """Применить изменение уровня. Возвращает False, если стакан разошёлся с сервером."""
        book = self.bids if side == "BUY" else self.asks if side == "SELL" else None
        p, s = _d(price), _d(size)
        if book is None or p is None or s is None or s < 0:
            raise ValueError(f"плохой price_change: side={side} price={price} size={size}")
        if s == 0:
            book.pop(p, None)
        else:
            book[p] = s
        self._touch(recv_ts, server_ts)
        return self._check_server_bbo(_d(best_bid), _d(best_ask))

    def apply_server_bbo(self, best_bid: Any, best_ask: Any, *, recv_ts: float) -> None:
        self.server_best_bid, self.server_best_ask = _d(best_bid), _d(best_ask)

    def _check_server_bbo(self, sb: Decimal | None, sa: Decimal | None) -> bool:
        if sb is None and sa is None:
            return True
        self.server_best_bid, self.server_best_ask = sb, sa
        if not self._matches_server(sb, sa):
            self.mismatches += 1
            # уровни лучше серверной цены уже съедены сделкой - убираем их
            if sb is not None:
                for p in [p for p in self.bids if p > sb]:
                    del self.bids[p]
            if sa is not None:
                for p in [p for p in self.asks if p < sa]:
                    del self.asks[p]
            self.consistent = False
            return False
        self.consistent = True
        return True

    def _matches_server(self, sb: Decimal | None, sa: Decimal | None) -> bool:
        """Сервер пишет пустую сторону как best_bid "0" / best_ask "1"."""
        bb, ba = self.best_bid(), self.best_ask()
        bid_ok = sb is None or (bb[0] == sb if bb is not None else sb == 0)
        ask_ok = sa is None or (ba[0] == sa if ba is not None else sa == 1)
        return bid_ok and ask_ok

    def _touch(self, recv_ts: float, server_ts: float | None) -> None:
        self.version += 1
        self.last_update = recv_ts
        if server_ts is not None:
            self.server_ts = server_ts

    @staticmethod
    def _levels(levels: Iterable[dict]) -> dict[Decimal, Decimal]:
        out = {}
        for lv in levels:
            p, s = _d(lv.get("price")), _d(lv.get("size"))
            if p is not None and s is not None and s > 0:
                out[p] = s
        return out

    # ---------- чтение ----------

    def best_bid(self) -> tuple[Decimal, Decimal] | None:
        if not self.bids:
            return None
        p = max(self.bids)
        return p, self.bids[p]

    def best_ask(self) -> tuple[Decimal, Decimal] | None:
        if not self.asks:
            return None
        p = min(self.asks)
        return p, self.asks[p]

    def spread(self) -> Decimal | None:
        bb, ba = self.best_bid(), self.best_ask()
        return ba[0] - bb[0] if bb and ba else None

    def mid(self) -> Decimal | None:
        bb, ba = self.best_bid(), self.best_ask()
        return (ba[0] + bb[0]) / 2 if bb and ba else None

    def levels(self, side: str, n: int | None = None) -> list[tuple[Decimal, Decimal]]:
        """side: 'bid' или 'ask'; от лучшей цены к худшей."""
        if side == "bid":
            items = sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)
        elif side == "ask":
            items = sorted(self.asks.items(), key=lambda kv: kv[0])
        else:
            raise ValueError(side)
        return items[:n] if n is not None else items

    def depth_usd(self, side: str, max_levels: int | None = None) -> Decimal:
        return sum((p * s for p, s in self.levels(side, max_levels)), Decimal(0))

    def walk(self, side: str, shares: Decimal, limit_price: Decimal | None = None) -> tuple[Decimal, Decimal]:
        """
        Сколько акций можно исполнить по стакану и на какую сумму.
        side='ask' - покупка (идём по asks вверх), 'bid' - продажа (по bids вниз).
        limit_price ограничивает худшую допустимую цену. Возвращает (исполнено, сумма USD без комиссии).
        """
        left, filled, notional = Decimal(shares), Decimal(0), Decimal(0)
        for p, s in self.levels(side):
            if left <= 0:
                break
            if limit_price is not None and ((side == "ask" and p > limit_price) or (side == "bid" and p < limit_price)):
                break
            q = min(left, s)
            filled += q
            notional += q * p
            left -= q
        return filled, notional

    def is_crossed(self) -> bool:
        bb, ba = self.best_bid(), self.best_ask()
        return bool(bb and ba and bb[0] >= ba[0])

    def record(self, n_levels: int, recv_ts: float) -> dict:
        bb, ba = self.best_bid(), self.best_ask()
        return {"t": "book", "tok": self.token_id, "recv_ts": recv_ts, "server_ts": self.server_ts,
                "bb": bb[0] if bb else None, "ba": ba[0] if ba else None, "ok": self.consistent,
                "bids": [[p, s] for p, s in self.levels("bid", n_levels)],
                "asks": [[p, s] for p, s in self.levels("ask", n_levels)]}


class ClobBookFeed:
    def __init__(
        self,
        url: str = CLOB_WS_URL,
        *,
        ping_every_sec: float = 10.0,
        stale_after_sec: float = 30.0,
        snapshot_timeout_sec: float = 15.0,
        backoff_sec: Iterable[float] = (1, 2, 5, 10, 30),
        on_event: Callable[[dict], None] | None = None,
        on_trade: Callable[[dict], None] | None = None,
        on_bbo: Callable[[dict], None] | None = None,
        clock: Callable[[], float] = time.time,
        rng: random.Random | None = None,
        connect: Callable[..., Any] = ws_connect,
        connect_kwargs: dict | None = None,
    ):
        self.url = url
        self.ping_every_sec = ping_every_sec
        self.stale_after_sec = stale_after_sec
        self.snapshot_timeout_sec = snapshot_timeout_sec
        self.backoff_sec = [float(x) for x in backoff_sec]
        self.on_event = on_event
        self.on_trade = on_trade
        self.on_bbo = on_bbo
        self.clock = clock
        self.rng = rng or random.Random()
        self._connect = connect
        self._connect_kwargs = connect_kwargs or {}

        self.books: dict[str, OrderBook] = {}
        self.desired: set[str] = set()
        self.subscribed: set[str] = set()
        self._subscribed_at: dict[str, float] = {}
        self.connected = False
        self.connections = 0
        self.reconnects = 0
        self.last_data: float | None = None
        self.last_pong: float | None = None
        self.stats: Counter[str] = Counter()
        self._tokens_changed = asyncio.Event()

    # ---------- публичное ----------

    def set_tokens(self, tokens: Iterable[str]) -> None:
        new = set(tokens)
        if new != self.desired:
            self.desired = new
            self._tokens_changed.set()

    def book(self, token_id: str) -> OrderBook | None:
        return self.books.get(token_id)

    def book_fresh(self, token_id: str, now: float | None = None) -> bool:
        b = self.books.get(token_id)
        now = self.clock() if now is None else now
        return bool(self.connected and b is not None and b.initialized and b.consistent
                    and self.last_data is not None and now - self.last_data <= self.stale_after_sec)

    async def run(self, stop: asyncio.Event) -> None:
        attempt = 0
        while not stop.is_set():
            if not self.desired:
                self._tokens_changed.clear()
                waiter = asyncio.ensure_future(self._tokens_changed.wait())
                stopper = asyncio.ensure_future(stop.wait())
                await asyncio.wait({waiter, stopper}, return_when=asyncio.FIRST_COMPLETED)
                waiter.cancel()
                stopper.cancel()
                continue
            reason, got_data = "", False
            try:
                got_data = await self._session(stop)
                if stop.is_set():
                    break
                reason = "соединение завершилось"
            except _Reconnect as e:
                reason, got_data = e.reason, e.got_data
            except ConnectionClosed as e:
                frame = e.rcvd or e.sent
                reason = f"закрыто сервером, код {frame.code if frame else None}"
                got_data = self.last_data is not None
            except InvalidStatus as e:
                reason = f"HTTP {e.response.status_code} при подключении"
            except (OSError, asyncio.TimeoutError, InvalidHandshake) as e:
                reason = f"{type(e).__name__}: {e}"
            finally:
                self.connected = False
                self.subscribed = set()
                for b in self.books.values():
                    b.consistent = False
            if stop.is_set():
                break
            if got_data:
                attempt = 0
            base = self.backoff_sec[min(attempt, len(self.backoff_sec) - 1)]
            delay = self.rng.uniform(0, base)
            attempt += 1
            self.reconnects += 1
            self._emit("reconnect_wait", reason=reason, delay_sec=round(delay, 3), attempt=attempt)
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    # ---------- соединение ----------

    async def _session(self, stop: asyncio.Event) -> bool:
        self.last_data = None
        self.books = {}          # новое соединение - новые снимки; старые уровни не используем
        kwargs = {"open_timeout": 10, "close_timeout": 2, "user_agent_header": USER_AGENT,
                  "max_size": 8 * 2 ** 20, **self._connect_kwargs}
        async with self._connect(self.url, **kwargs) as ws:
            self.connections += 1
            now = self.clock()
            tokens = sorted(self.desired)
            await ws.send(json.dumps({"assets_ids": tokens, "type": "market", "custom_feature_enabled": True}))
            self.subscribed = set(tokens)
            self._subscribed_at = {t: now for t in tokens}
            self.connected = True
            self.last_pong = now
            last_ping = now
            sub_time = now
            self._emit("connected", tokens=len(tokens), connection=self.connections)

            while not stop.is_set():
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=min(1.0, self.ping_every_sec))
                except asyncio.TimeoutError:
                    raw = None
                now = self.clock()
                if raw is not None:
                    self._handle(raw, now)
                if now - last_ping >= self.ping_every_sec:
                    await ws.send("PING")
                    last_ping = now
                if now - (self.last_pong or sub_time) > 3 * self.ping_every_sec:
                    raise _Reconnect("нет PONG", self.last_data is not None)
                if self.subscribed and now - (self.last_data or sub_time) > self.stale_after_sec:
                    self._emit("stale", reason=f"нет данных {now - (self.last_data or sub_time):.1f} с")
                    raise _Reconnect("нет данных стакана", self.last_data is not None)
                await self._sync_tokens(ws, now)
                missing = [t for t in self.subscribed
                           if not (self.books.get(t) and self.books[t].initialized)
                           and now - self._subscribed_at.get(t, now) > self.snapshot_timeout_sec]
                if missing:
                    # без начального снимка стакан не собрать: переподключение даёт полный снимок
                    raise _Reconnect(f"нет снимка book для {len(missing)} токенов", self.last_data is not None)
        return self.last_data is not None

    async def _sync_tokens(self, ws, now: float) -> None:
        add = sorted(self.desired - self.subscribed)
        remove = sorted(self.subscribed - self.desired)
        if add:
            await ws.send(json.dumps({"assets_ids": add, "operation": "subscribe"}))
            self.subscribed.update(add)
            for t in add:
                self._subscribed_at[t] = now
            self._emit("subscribe", tokens=len(add))
        if remove:
            await ws.send(json.dumps({"assets_ids": remove, "operation": "unsubscribe"}))
            self.subscribed.difference_update(remove)
            for t in remove:
                self.books.pop(t, None)
                self._subscribed_at.pop(t, None)
            self._emit("unsubscribe", tokens=len(remove))

    # ---------- разбор ----------

    def _handle(self, raw: str | bytes, now: float) -> None:
        if raw in ("PONG", b"PONG"):
            self.last_pong = now
            return
        try:
            msg = json.loads(raw)
        except (ValueError, TypeError):
            self.stats["bad_frame"] += 1
            return
        self.last_pong = now     # любой кадр - признак живого соединения
        for ev in (msg if isinstance(msg, list) else [msg]):
            if isinstance(ev, dict):
                self._handle_event(ev, now)

    def _handle_event(self, ev: dict, now: float) -> None:
        kind = ev.get("event_type")
        self.stats[kind or "unknown"] += 1
        server_ts = _ms(ev.get("timestamp"))
        if kind == "book":
            tok = ev.get("asset_id")
            if tok in self.subscribed:
                self.books.setdefault(tok, OrderBook(tok)).apply_snapshot(
                    ev.get("bids") or [], ev.get("asks") or [], recv_ts=now, server_ts=server_ts,
                    tick_size=ev.get("tick_size"))
                self.last_data = now
        elif kind == "price_change":
            for pc in ev.get("price_changes") or []:
                b = self.books.get(pc.get("asset_id"))
                if b is None or not b.initialized:
                    continue
                try:
                    ok = b.apply_change(pc.get("side"), pc.get("price"), pc.get("size"), recv_ts=now,
                                        server_ts=server_ts, best_bid=pc.get("best_bid"),
                                        best_ask=pc.get("best_ask"))
                except ValueError:
                    self.stats["bad_price_change"] += 1
                    continue
                if not ok:
                    self.stats["bbo_mismatch"] += 1
            self.last_data = now
        elif kind == "best_bid_ask":
            tok = ev.get("asset_id")
            if tok in self.books:
                self.books[tok].apply_server_bbo(ev.get("best_bid"), ev.get("best_ask"), recv_ts=now)
            if self.on_bbo and tok in self.subscribed:
                self.on_bbo({"t": "bbo", "tok": tok, "bb": _d(ev.get("best_bid")), "ba": _d(ev.get("best_ask")),
                             "server_ts": server_ts, "recv_ts": now})
            self.last_data = now
        elif kind == "last_trade_price":
            tok = ev.get("asset_id")
            if self.on_trade and tok in self.subscribed:
                self.on_trade({"t": "trade", "tok": tok, "price": _d(ev.get("price")), "size": _d(ev.get("size")),
                               "side": ev.get("side"), "fee_rate_bps": ev.get("fee_rate_bps"),
                               "server_ts": server_ts, "recv_ts": now})
            self.last_data = now
        elif kind == "tick_size_change":
            tok = ev.get("asset_id")
            if tok in self.books:
                self.books[tok].tick_size = _d(ev.get("new_tick_size"))
            self._emit("tick_size_change", tok=tok, old=ev.get("old_tick_size"), new=ev.get("new_tick_size"))
        elif kind == "market_resolved":
            if set(ev.get("assets_ids") or []) & self.subscribed:
                self._emit("market_resolved", market=ev.get("market"), winning=ev.get("winning_asset_id"),
                           outcome=ev.get("winning_outcome"))
        # new_market и прочее только считаем

    def _emit(self, kind: str, **fields: Any) -> None:
        level = logging.WARNING if kind in ("reconnect_wait", "stale") else logging.INFO
        log.log(level, "book %s %s", kind, fields)
        if self.on_event:
            self.on_event({"kind": kind, "src": "book", "recv_ts": self.clock(), **fields})


class _Reconnect(Exception):
    def __init__(self, reason: str, got_data: bool):
        super().__init__(reason)
        self.reason = reason
        self.got_data = got_data
