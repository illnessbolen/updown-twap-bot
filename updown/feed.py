"""
Цены PolyBolt: Chainlink TWAP-60 и спот (docs/API_NOTES.md, §1).

Протокол: https://docs.polymarket.com/api-reference/live-data/overview.md
  connect wss://ws-live-v2.polymarket.com/ws -> {"op":"auth"} -> ждём {"op":"authed"}
  -> {"op":"subscribe","subscriptions":[{"channel","filter"}]} -> снапшот + обновления.

Нет молчаливых сбоев (CLAUDE.md, правило 5): сокет может «зависнуть» без разрыва,
поэтому для каждого потока храним время последних данных (last_recv) и время цены
(last_price_ts). Если что-то из этого старше stale_after_sec, переподключаемся,
а is_fresh() возвращает False, пока цена снова не станет свежей.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable

from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus

from .config import ApiCreds

log = logging.getLogger(__name__)

DEFAULT_URL = "wss://ws-live-v2.polymarket.com/ws"
TWAP = "price.crypto.twap"
SPOT = "price.crypto"
TWAP_WINDOW_SEC = 60
USER_AGENT = "updown-twap-bot/0.1"

# Коды закрытия PolyBolt
CLOSE_AUTH_FAILED = 4001
CLOSE_SLOW_CONSUMER = 4002
CLOSE_DRAINING = 4003
CLOSE_POLICY = 4008

# op=error: какие коды переподключением не лечатся (это ошибка ключей или наш баг)
FATAL_ERRORS = {"auth_invalid", "bad_filter", "sub_limit", "rate_limited", "auth_attempts"}


class FeedFatal(Exception):
    """Ошибка, которую переподключение не исправит: неверные ключи или нарушение протокола."""


class _Reconnect(Exception):
    def __init__(self, reason: str, delay: float | None = None):
        super().__init__(reason)
        self.reason = reason
        self.delay = delay   # None - обычный backoff


@dataclass(frozen=True)
class PriceTick:
    channel: str          # TWAP или SPOT
    symbol: str           # "btcusd"
    ts: float             # время цены (payload.timestamp), unix-секунды
    value: Decimal        # full_accuracy_value
    recv_ts: float        # когда получили
    source: str | None = None
    snapshot: bool = False
    seq: int | None = None


@dataclass
class StreamState:
    last_recv: float | None = None       # последний кадр с данными (любое соединение)
    session_recv: float | None = None    # последний кадр с данными в текущем соединении
    last_price_ts: float | None = None   # время самой свежей цены
    last_value: Decimal | None = None
    source: str | None = None


def _close_code(exc: ConnectionClosed) -> int | None:
    frame = exc.rcvd or exc.sent
    return frame.code if frame is not None else None


def _decimal(point: dict) -> Decimal | None:
    raw = point.get("full_accuracy_value")
    if raw is None:
        raw = point.get("value")
    if raw is None:
        return None
    try:
        v = Decimal(str(raw))
    except InvalidOperation:
        return None
    return v if v.is_finite() and v > 0 else None


class PolyBoltFeed:
    def __init__(
        self,
        creds: ApiCreds,
        symbols: Iterable[str],
        *,
        url: str = DEFAULT_URL,
        spot: bool = True,
        spot_provider: str = "",
        stale_after_sec: float = 45.0,
        backoff_sec: Iterable[float] = (1, 2, 5, 10, 30),
        auth_timeout_sec: float = 10.0,
        drain_max_delay_sec: float = 10.0,
        on_tick: Callable[[PriceTick], None] | None = None,
        on_event: Callable[[dict], None] | None = None,
        clock: Callable[[], float] = time.time,
        rng: random.Random | None = None,
        connect: Callable[..., Any] = ws_connect,
        connect_kwargs: dict | None = None,
    ):
        self.creds = creds
        self.symbols = list(symbols)
        self.url = url
        self.spot = spot
        self.spot_provider = spot_provider
        self.stale_after_sec = float(stale_after_sec)
        self.backoff_sec = [float(x) for x in backoff_sec]
        self.auth_timeout_sec = auth_timeout_sec
        self.drain_max_delay_sec = drain_max_delay_sec
        self.on_tick = on_tick
        self.on_event = on_event
        self.clock = clock
        self.rng = rng or random.Random()
        self._connect = connect
        self._connect_kwargs = connect_kwargs or {}
        self.check_interval = min(1.0, self.stale_after_sec / 4)

        self.streams: dict[tuple[str, str], StreamState] = {
            key: StreamState() for key in self._stream_keys()}
        self.connected = False      # авторизованы и подписаны
        self._session_had_data = False
        self.connections = 0
        self.reconnects = 0
        self._sub_time: float | None = None

    # ---------- публичное ----------

    @property
    def subscriptions(self) -> list[dict]:
        subs = []
        for sym in self.symbols:
            subs.append({"channel": TWAP, "filter": {"symbol": sym, "window_seconds": TWAP_WINDOW_SEC}})
            if self.spot:
                flt: dict[str, Any] = {"symbol": sym}
                if self.spot_provider:
                    flt["provider"] = self.spot_provider
                subs.append({"channel": SPOT, "filter": flt})
        return subs

    def stream_fresh(self, channel: str, symbol: str, now: float | None = None) -> bool:
        st = self.streams.get((channel, symbol))
        if not self.connected or st is None or st.session_recv is None or st.last_price_ts is None:
            return False
        now = self.clock() if now is None else now
        return (now - st.session_recv <= self.stale_after_sec
                and now - st.last_price_ts <= self.stale_after_sec)

    def is_fresh(self, symbol: str, now: float | None = None) -> bool:
        """Можно ли открывать позиции по символу: все его потоки свежие."""
        return all(self.stream_fresh(ch, sym, now) for ch, sym in self.streams if sym == symbol)

    async def run(self, stop: asyncio.Event) -> None:
        """Работает до stop. FeedFatal пробрасывается наружу."""
        attempt = 0
        while not stop.is_set():
            delay: float | None = None
            reason = ""
            got_data = False
            try:
                got_data = await self._session(stop)
                if stop.is_set():
                    break
                reason = "соединение завершилось"
            except FeedFatal as e:
                self._emit("fatal", reason=str(e))
                raise
            except _Reconnect as e:
                reason, delay = e.reason, e.delay
                got_data = self._session_had_data
            except ConnectionClosed as e:
                code = _close_code(e)
                got_data = self._session_had_data
                reason = f"закрыто сервером, код {code}"
                if code == CLOSE_AUTH_FAILED:
                    self._emit("fatal", reason="4001: ошибка авторизации, проверьте ключи в .env")
                    raise FeedFatal("PolyBolt закрыл соединение с кодом 4001 (ошибка авторизации)") from e
                if code == CLOSE_POLICY:
                    self._emit("fatal", reason="4008: нарушение политики PolyBolt (ошибка в боте)")
                    raise FeedFatal("PolyBolt закрыл соединение с кодом 4008 (нарушение политики)") from e
                if code == CLOSE_DRAINING:
                    delay = self.rng.uniform(0, self.drain_max_delay_sec)
            except InvalidStatus as e:
                status = e.response.status_code
                reason = f"HTTP {status} при подключении"
                if status in (429, 503):
                    retry_after = e.response.headers.get("Retry-After")
                    try:
                        ra = float(retry_after) if retry_after else 0.0
                    except ValueError:
                        ra = 0.0
                    delay = max(ra, self._backoff(attempt)) + self.rng.uniform(0, 1)
            except (OSError, asyncio.TimeoutError, InvalidHandshake) as e:
                reason = f"{type(e).__name__}: {e}"
            finally:
                self.connected = False

            if stop.is_set():
                break
            if got_data:
                attempt = 0
            if delay is None:
                delay = self._backoff(attempt)
            attempt += 1
            self.reconnects += 1
            self._emit("reconnect_wait", reason=reason, delay_sec=round(delay, 3), attempt=attempt)
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    # ---------- одно соединение ----------

    async def _session(self, stop: asyncio.Event) -> bool:
        self._session_had_data = False
        for st in self.streams.values():
            st.session_recv = None
        seq_last: dict[str, int] = {}

        kwargs = {"open_timeout": 10, "close_timeout": 2, "user_agent_header": USER_AGENT,
                  **self._connect_kwargs}
        async with self._connect(self.url, **kwargs) as ws:
            self.connections += 1
            self._emit("connected", url=self.url, connection=self.connections)
            await ws.send(json.dumps({"op": "auth", "rid": "auth", "auth": {
                "apiKey": self.creds.api_key,
                "secret": self.creds.secret,
                "passphrase": self.creds.passphrase,
            }}))
            await self._await_authed(ws)
            await ws.send(json.dumps({"op": "subscribe", "rid": "sub",
                                      "subscriptions": self.subscriptions}))
            self._sub_time = self.clock()
            self.connected = True
            self._emit("subscribed_sent", streams=len(self.streams))

            while not stop.is_set():
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=self.check_interval)
                except asyncio.TimeoutError:
                    raw = None
                if raw is not None:
                    self._handle(raw, seq_last)
                stale = self._stale_reason()
                if stale:
                    self._emit("stale", reason=stale)
                    raise _Reconnect(f"нет свежих данных: {stale}")
        return self._session_had_data

    async def _await_authed(self, ws) -> None:
        deadline = self.clock() + self.auth_timeout_sec
        while True:
            left = deadline - self.clock()
            if left <= 0:
                raise _Reconnect("нет ответа на auth")
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=left)
            except asyncio.TimeoutError:
                raise _Reconnect("нет ответа на auth") from None
            msg = self._parse(raw)
            if msg is None:
                continue
            op = msg.get("op")
            if op == "authed":
                self._emit("authed")
                return
            if op == "error":
                code = msg.get("code")
                if code == "auth_invalid":
                    raise FeedFatal("auth_invalid: PolyBolt не принял ключи, проверьте .env")
                if code in FATAL_ERRORS:
                    raise FeedFatal(f"ошибка PolyBolt при авторизации: {code}")
                raise _Reconnect(f"ошибка при авторизации: {code}")
            # до authed ничего другого не ждём, но и не теряем молча
            self._emit("unexpected_before_auth", op=op, channel=msg.get("channel"))

    # ---------- разбор сообщений ----------

    def _parse(self, raw: str | bytes) -> dict | None:
        try:
            msg = json.loads(raw)
        except (ValueError, TypeError):
            self._emit("bad_frame", size=len(raw))
            return None
        if not isinstance(msg, dict):
            self._emit("bad_frame", size=len(raw))
            return None
        return msg

    def _handle(self, raw: str | bytes, seq_last: dict[str, int]) -> None:
        msg = self._parse(raw)
        if msg is None:
            return
        op = msg.get("op")
        if op is not None:
            self._handle_op(msg)
            return

        channel = msg.get("channel")
        payload = msg.get("payload")
        if not isinstance(payload, dict):
            self._emit("bad_frame", channel=channel)
            return

        seq = msg.get("seq")
        if isinstance(seq, int):
            prev = seq_last.get(channel)
            if prev is not None and seq != prev + 1:
                self._emit("seq_gap", channel=channel, expected=prev + 1, got=seq)
            seq_last[channel] = seq
        if msg.get("dropped"):
            self._emit("dropped", channel=channel, count=msg.get("dropped"))

        symbol = payload.get("symbol")
        st = self.streams.get((channel, symbol))
        if st is None:
            self._emit("unexpected_stream", channel=channel, symbol=symbol)
            return
        if channel == TWAP:
            window = payload.get("window_seconds")
            if window is not None and window != TWAP_WINDOW_SEC:
                self._emit("bad_window", symbol=symbol, window_seconds=window)
                return

        source = payload.get("source")
        if source and st.source and source != st.source:
            self._emit("source_changed", channel=channel, symbol=symbol, old=st.source, new=source)
        if source:
            st.source = source

        now = self.clock()
        snapshot = bool(msg.get("snapshot"))
        points = (payload.get("data") or []) if snapshot else [payload]
        for point in points:
            value = _decimal(point)
            ts_ms = point.get("timestamp")
            if value is None or not isinstance(ts_ms, (int, float)):
                self._emit("bad_value", channel=channel, symbol=symbol)
                continue
            tick = PriceTick(channel=channel, symbol=symbol, ts=ts_ms / 1000.0, value=value,
                             recv_ts=now, source=st.source, snapshot=snapshot, seq=seq)
            if st.last_price_ts is None or tick.ts >= st.last_price_ts:
                st.last_price_ts = tick.ts
                st.last_value = value
            if self.on_tick:
                self.on_tick(tick)
        # пустой снапшот - это не данные: свежесть не обновляем
        if points:
            st.last_recv = st.session_recv = now
            self._session_had_data = True

    def _handle_op(self, msg: dict) -> None:
        op = msg.get("op")
        if op == "error":
            code = msg.get("code")
            if code in FATAL_ERRORS:
                raise FeedFatal(f"ошибка PolyBolt: {code} (channel={msg.get('channel')})")
            if code in ("auth_required", "auth_unavailable"):
                raise _Reconnect(f"ошибка PolyBolt: {code}")
            self._emit("server_error", code=code, channel=msg.get("channel"), rid=msg.get("rid"))
        elif op == "subscribed":
            ack = {k: v for k, v in msg.items() if k != "op"}
            self._emit("subscribed", **ack)
        elif op in ("unsubscribed", "pong", "authed"):
            self._emit(op)
        else:
            self._emit("unknown_op", op=op)

    def _stale_reason(self) -> str | None:
        if self._sub_time is None:
            return None
        now = self.clock()
        limit = self.stale_after_sec
        for (channel, symbol), st in self.streams.items():
            ref = st.session_recv if st.session_recv is not None else self._sub_time
            if now - ref > limit:
                return f"{channel}/{symbol}: нет данных {now - ref:.1f} с"
            # возраст цены проверяем после того, как прошёл порог с момента подписки
            if (st.last_price_ts is not None and now - self._sub_time > limit
                    and now - st.last_price_ts > limit):
                return f"{channel}/{symbol}: цене {now - st.last_price_ts:.1f} с"
        return None

    # ---------- служебное ----------

    def _stream_keys(self) -> list[tuple[str, str]]:
        keys = [(TWAP, s) for s in self.symbols]
        if self.spot:
            keys += [(SPOT, s) for s in self.symbols]
        return keys

    def _backoff(self, attempt: int) -> float:
        base = self.backoff_sec[min(attempt, len(self.backoff_sec) - 1)]
        return self.rng.uniform(0, base)   # full jitter, как советует документация PolyBolt

    def _emit(self, kind: str, **fields: Any) -> None:
        event = {"kind": kind, "recv_ts": self.clock(), **fields}
        level = logging.WARNING if kind in (
            "stale", "reconnect_wait", "seq_gap", "dropped", "server_error", "bad_frame",
            "bad_value", "bad_window", "source_changed", "unexpected_stream",
            "unexpected_before_auth") else logging.ERROR if kind == "fatal" else logging.INFO
        log.log(level, "feed %s %s", kind, {k: v for k, v in fields.items()})
        if self.on_event:
            self.on_event(event)
