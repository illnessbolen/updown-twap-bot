"""
Поток цен PolyBolt: 60-секундный Chainlink TWAP (price.crypto.twap) и спот для sigma
(price.crypto). Протокол и ограничения: docs/API_NOTES.md, раздел 2.1.

Что делает модуль:
  * подключение, auth (ключи из окружения), подписка, разбор кадров в PriceTick;
  * heartbeat: прикладной {"op":"ping"}; pong НЕ считается данными;
  * детектор зависания (правило 5): если по какому-то обязательному потоку нет НОВЫХ
    данных дольше stale_after_sec, соединение закрывается и открывается заново;
  * is_fresh(): False, пока цена не свежая. Новые позиции открывать можно только при True;
  * реконнект с backoff и jitter, коды закрытия 4001/4002/4003/4008 и HTTP 429/503
    обрабатываются по документации;
  * все входящие кадры и служебные события пишутся в JSONL (recorder.py).

Фатальные ошибки (неверные ключи, нарушение протокола, отказ в подписке) не
скрываются и не гоняются по кругу: run() выбрасывает FeedFatalError.

Исходящий кадр auth в журнал и в лог не попадает.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import time
from collections import Counter
from dataclasses import dataclass
from typing import Callable

import websockets
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus

log = logging.getLogger("feed")


class _NoSecretsFilter(logging.Filter):
    """Страховка: ни одна запись логгера websockets не должна содержать кадр auth."""

    def filter(self, record: logging.LogRecord) -> bool:
        text = record.getMessage()
        return not any(w in text for w in ('"auth"', "apiKey", "passphrase"))


# websockets на уровне DEBUG печатает КАЖДЫЙ кадр, в том числе исходящий auth с ключами.
# Поэтому клиент получает отдельный логгер, зафиксированный на WARNING и с фильтром.
_WS_LOG = logging.getLogger("feed.ws")
_WS_LOG.setLevel(logging.WARNING)
_WS_LOG.addFilter(_NoSecretsFilter())
_WS_LOG.propagate = True

CH_SPOT = "price.crypto"
CH_TWAP = "price.crypto.twap"

# ошибки подписки, после которых имеет смысл просто переподключиться
_RETRYABLE_ERRORS = {"auth_required", "auth_unavailable"}

# собственный код закрытия клиента (4000-4999 разрешены для приложений)
_WATCHDOG_CLOSE_CODE = 4000


class FeedFatalError(Exception):
    """Дальнейшие попытки бессмысленны, нужно вмешательство человека."""


class FeedAuthError(FeedFatalError):
    pass


class FeedProtocolError(FeedFatalError):
    pass


class FeedSubscriptionError(FeedFatalError):
    pass


class _Reconnect(Exception):
    def __init__(self, reason: str, delay: float | None = None, min_delay: float = 0.0):
        super().__init__(reason)
        self.reason = reason
        self.delay = delay          # фиксированная задержка (4003), иначе backoff
        self.min_delay = min_delay  # нижняя граница (Retry-After)


@dataclass(frozen=True)
class FeedSettings:
    url: str
    symbols: tuple[str, ...]
    twap_window_sec: int = 60
    use_spot: bool = True
    spot_provider: str = ""
    stale_after_sec: float = 45.0
    backoff_sec: tuple[float, ...] = (1, 2, 5, 10, 30)
    heartbeat_sec: float = 15.0
    pong_timeout_sec: float = 10.0
    auth_timeout_sec: float = 10.0
    subscribe_timeout_sec: float = 15.0
    connect_timeout_sec: float = 15.0
    drain_delay_max_sec: float = 10.0   # 4003: реконнект через случайные 0..10 с

    @classmethod
    def from_config(cls, feed_cfg, symbols) -> "FeedSettings":
        return cls(
            url=feed_cfg.url, symbols=tuple(symbols),
            twap_window_sec=feed_cfg.twap_window_sec, use_spot=feed_cfg.use_spot_for_sigma,
            spot_provider=feed_cfg.spot_provider, stale_after_sec=feed_cfg.stale_after_sec,
            backoff_sec=tuple(feed_cfg.reconnect_backoff_sec),
            heartbeat_sec=feed_cfg.heartbeat_sec, pong_timeout_sec=feed_cfg.pong_timeout_sec,
            auth_timeout_sec=feed_cfg.auth_timeout_sec,
            subscribe_timeout_sec=feed_cfg.subscribe_timeout_sec,
            connect_timeout_sec=feed_cfg.connect_timeout_sec)


@dataclass(frozen=True, slots=True)
class PriceTick:
    channel: str            # CH_TWAP или CH_SPOT
    symbol: str             # "btcusd"
    ts_ms: int              # время производителя (мс)
    value: float
    raw_value: str          # full_accuracy_value как пришло (строка decimal)
    source: str | None      # "chainlink", "pyth", ...
    seq: int | None
    recv_ts: float          # локальное время приёма (unix, с)
    snapshot: bool          # точка из снапшота истории, а не живой апдейт

    @property
    def kind(self) -> str:
        return "twap" if self.channel == CH_TWAP else "spot"


Key = tuple[str, str]  # (channel, symbol)


class Feed:
    def __init__(self, settings: FeedSettings, creds, recorder=None, *,
                 clock: Callable[[], float] = time.time,
                 mono: Callable[[], float] = time.monotonic,
                 rng: Callable[[], float] = random.random):
        self.s = settings
        self._creds = creds
        self._rec = recorder
        self._wall = clock
        self._mono = mono
        self._rng = rng

        channels = [CH_TWAP] + ([CH_SPOT] if settings.use_spot else [])
        self.required: tuple[Key, ...] = tuple(
            (ch, sym) for sym in settings.symbols for ch in channels)
        self._required_set = set(self.required)

        self.stats: Counter[str] = Counter()
        self.last_data_ts: float | None = None   # unix-время последнего НОВОГО тика
        self._latest: dict[Key, PriceTick] = {}
        self._last_emitted_ms: dict[Key, int] = {}
        self._source: dict[Key, str] = {}
        self._callbacks: list[Callable[[PriceTick], None]] = []

        self._stop = asyncio.Event()
        self._ws = None
        self._was_fresh = False
        self._reset_connection_state()

    # ------------------------------------------------------------ публичный API
    def on_tick(self, callback: Callable[[PriceTick], None]) -> None:
        self._callbacks.append(callback)

    def latest(self, channel: str, symbol: str) -> PriceTick | None:
        return self._latest.get((channel, symbol))

    @property
    def ready(self) -> bool:
        """Соединение аутентифицировано и все подписки подтверждены."""
        return self._phase == "live"

    def is_fresh(self, symbol: str | None = None) -> bool:
        """
        True, только если соединение живо и по КАЖДОМУ обязательному потоку (для symbol или
        для всех символов) в этом соединении пришёл новый тик не позже stale_after_sec назад
        и время самого тика (по часам производителя) тоже не старше порога.
        Открывать новые позиции можно только при True.
        """
        if not self.ready:
            return False
        mono, wall = self._mono(), self._wall()
        limit = self.s.stale_after_sec
        for key in self.required:
            if symbol is not None and key[1] != symbol:
                continue
            t = self._key_mono.get(key)
            e = self._latest_event_ms.get(key)
            if t is None or e is None:
                return False
            if mono - t > limit or wall - e / 1000.0 > limit:
                return False
        return True

    async def wait_fresh(self, timeout: float, symbol: str | None = None) -> bool:
        deadline = self._mono() + timeout
        while self._mono() < deadline:
            if self.is_fresh(symbol):
                return True
            await asyncio.sleep(0.05)
        return self.is_fresh(symbol)

    def status(self) -> dict:
        age = None if self.last_data_ts is None else round(self._wall() - self.last_data_ts, 1)
        return {"ready": self.ready, "fresh": self.is_fresh(), "last_data_age_sec": age,
                **{k: v for k, v in sorted(self.stats.items())}}

    async def stop(self) -> None:
        self._stop.set()
        ws = self._ws
        if ws is not None:
            try:
                await ws.close()
            except Exception:  # noqa: BLE001 - при остановке причина не важна
                pass

    # ------------------------------------------------------------ главный цикл
    async def run(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            self._reset_connection_state()
            reason, delay, min_delay = "", None, 0.0
            try:
                await self._session()
                reason = "соединение закрыто"
            except _Reconnect as r:
                reason, delay, min_delay = r.reason, r.delay, r.min_delay
            except InvalidStatus as e:
                status = e.response.status_code
                retry_after = _parse_retry_after(e.response.headers.get("Retry-After"))
                reason = f"HTTP {status} при подключении"
                if status in (429, 503):
                    min_delay = retry_after
                # прочие статусы (в т.ч. 403 от WAF или прокси) считаем временными: ретрай
                # с backoff виден в логе и в журнале событий, а is_fresh() остаётся False
            except (OSError, asyncio.TimeoutError, ConnectionClosed, InvalidHandshake) as e:
                reason = f"{type(e).__name__}: {e}"
            finally:
                self._phase = "closed"
                self._ws = None

            if self._stop.is_set():
                break
            if self._conn_got_data:
                attempt = 0
            wait = delay if delay is not None else self._backoff(attempt)
            wait = max(wait, min_delay)
            self.stats["reconnects"] += 1
            self._event("reconnect", reason=reason, delay_sec=round(wait, 3), attempt=attempt)
            log.warning("переподключение через %.1f с (%s)", wait, reason)
            attempt += 1
            await self._sleep_or_stop(wait)
        self._event("stopped")

    def _backoff(self, attempt: int) -> float:
        cap = self.s.backoff_sec[min(attempt, len(self.s.backoff_sec) - 1)]
        # "equal jitter": не ниже половины шага, чтобы не долбить сервер почти без паузы
        return cap * (0.5 + 0.5 * self._rng())

    async def _sleep_or_stop(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass

    def _fatal(self, exc: FeedFatalError) -> FeedFatalError:
        self._event("fatal", error=type(exc).__name__, detail=str(exc))
        log.error("фатальная ошибка: %s", exc)
        return exc

    # ------------------------------------------------------------ одно соединение
    def _reset_connection_state(self) -> None:
        self._phase = "closed"                  # closed -> auth -> subscribing -> live
        self._deadline = 0.0
        self._acks = 0
        self._ready_mono = 0.0
        self._last_pong = 0.0
        self._key_mono: dict[Key, float] = {}   # локальное (monotonic) время последнего нового тика
        self._latest_event_ms: dict[Key, int] = {}
        self._last_seq: dict[str, int] = {}
        self._watchdog_reason: str | None = None
        self._conn_got_data = False
        self._was_fresh = False

    async def _session(self) -> None:
        s = self.s
        async with websockets.connect(
                s.url, open_timeout=s.connect_timeout_sec, ping_interval=None,
                close_timeout=3, max_size=2 ** 20, logger=_WS_LOG) as ws:
            self._ws = ws
            self._event("connected", url=s.url)
            self._phase = "auth"
            self._deadline = self._mono() + s.auth_timeout_sec
            # ВАЖНО: этот кадр содержит секреты, его нельзя ни писать, ни логировать
            await ws.send(json.dumps({"op": "auth", "rid": "a1", "auth": {
                "apiKey": self._creds.api_key, "secret": self._creds.api_secret,
                "passphrase": self._creds.api_passphrase}}))
            tasks = [asyncio.create_task(self._watchdog(ws), name="feed-watchdog"),
                     asyncio.create_task(self._heartbeat(ws), name="feed-heartbeat")]
            try:
                try:
                    async for raw in ws:
                        await self._on_frame(raw)
                except ConnectionClosed:
                    pass
            finally:
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self._phase = "closed"

            code, reason = ws.close_code, ws.close_reason
            self._event("closed", code=code, reason=reason,
                        watchdog=self._watchdog_reason)
            if self._stop.is_set():
                return
            if self._watchdog_reason:
                raise _Reconnect(self._watchdog_reason)
            if code == 4001:
                raise self._fatal(FeedAuthError("сервер закрыл соединение с кодом 4001: ключи отклонены"))
            if code == 4008:
                raise self._fatal(FeedProtocolError(
                    f"код 4008 (нарушение политики): {reason or 'лимит или неверный запрос'}"))
            if code == 4003:
                raise _Reconnect("сервер перезапускается (4003)",
                                 delay=self._rng() * s.drain_delay_max_sec)
            raise _Reconnect(f"закрыто сервером, код {code} {reason}".strip())

    # ------------------------------------------------------------ сторожа
    async def _watchdog(self, ws) -> None:
        s = self.s
        interval = max(0.05, min(1.0, s.stale_after_sec / 4, s.heartbeat_sec / 2))
        while True:
            await asyncio.sleep(interval)
            mono = self._mono()
            reason = None
            if self._phase in ("auth", "subscribing") and mono > self._deadline:
                reason = f"таймаут фазы {self._phase}"
            elif self._phase == "live":
                for key in self.required:
                    ref = self._key_mono.get(key, self._ready_mono)
                    if mono - ref > s.stale_after_sec:
                        reason = (f"нет новых данных по {key[0]}/{key[1]} "
                                  f"дольше {s.stale_after_sec:g} с")
                        self.stats["stale_events"] += 1
                        break
                fresh = self.is_fresh()
                if self._was_fresh and not fresh and reason is None:
                    self._was_fresh = False
                    self._event("not_fresh", detail="устарели данные по часам производителя")
                    log.warning("цена не свежая (время тиков устарело)")
            if reason:
                self._was_fresh = False
                self._event("watchdog", reason=reason)
                log.warning("%s, закрываю соединение", reason)
                await self._close_by_watchdog(ws, reason)
                return

    async def _close_by_watchdog(self, ws, reason: str) -> None:
        self._watchdog_reason = reason
        try:
            await ws.close(code=_WATCHDOG_CLOSE_CODE, reason="watchdog")
        except Exception:  # noqa: BLE001
            pass

    async def _heartbeat(self, ws) -> None:
        s = self.s
        n = 0
        try:
            while True:
                await asyncio.sleep(s.heartbeat_sec)
                if self._phase != "live":
                    continue
                n += 1
                sent = self._mono()
                await ws.send(json.dumps({"op": "ping", "rid": f"p{n}"}))
                await asyncio.sleep(s.pong_timeout_sec)
                if self._last_pong < sent:
                    self.stats["pong_timeouts"] += 1
                    reason = f"нет pong за {s.pong_timeout_sec:g} с"
                    self._was_fresh = False
                    self._event("watchdog", reason=reason)
                    log.warning("%s, закрываю соединение", reason)
                    await self._close_by_watchdog(ws, reason)
                    return
        except ConnectionClosed:
            return

    # ------------------------------------------------------------ разбор кадров
    async def _on_frame(self, raw) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            msg = json.loads(raw)
        except ValueError:
            self.stats["bad_json"] += 1
            self._record("rx", raw=raw[:2000])
            return
        if not isinstance(msg, dict):
            self.stats["unknown_frames"] += 1
            self._record("rx", msg=msg)
            return
        self.stats["frames"] += 1
        self._record("rx", msg=msg)
        if "op" in msg:
            await self._on_op(msg)
        elif "channel" in msg and "payload" in msg:
            self._on_data(msg)
        else:
            self.stats["unknown_frames"] += 1

    async def _on_op(self, msg: dict) -> None:
        op = msg.get("op")
        if op == "authed":
            self._phase = "subscribing"
            self._deadline = self._mono() + self.s.subscribe_timeout_sec
            await self._ws.send(json.dumps(
                {"op": "subscribe", "rid": "s1", "subscriptions": self._subscriptions()}))
            self._event("subscribe_sent", count=len(self.required))
        elif op == "subscribed":
            self._acks += 1
            served = msg.get("provider")   # TODO(verify): имя поля с фактическим поставщиком
            if served:
                self._event("provider_served", channel=msg.get("channel"), provider=served)
            if self._phase == "subscribing" and self._acks >= len(self.required):
                self._go_live("acks")
        elif op == "pong":
            self._last_pong = self._mono()
        elif op == "unsubscribed":
            pass
        elif op == "error":
            code = msg.get("code")
            detail = f"{code} (channel={msg.get('channel')})"
            if code == "auth_invalid":
                raise self._fatal(FeedAuthError("ключи отклонены (auth_invalid)"))
            if code in _RETRYABLE_ERRORS:
                raise _Reconnect(f"ошибка сервера {detail}")
            raise self._fatal(FeedSubscriptionError(f"сервер отклонил запрос: {detail}"))
        else:
            self.stats["unknown_ops"] += 1

    def _go_live(self, via: str) -> None:
        self._phase = "live"
        self._ready_mono = self._mono()
        self._last_pong = self._ready_mono
        self._event("ready", acks=self._acks, via=via)
        log.info("подписки активны (%s)", via)

    def _subscriptions(self) -> list[dict]:
        subs = []
        for ch, sym in self.required:
            if ch == CH_TWAP:
                flt = {"symbol": sym, "window_seconds": self.s.twap_window_sec}
            else:
                flt = {"symbol": sym}
                if self.s.spot_provider:
                    flt["provider"] = self.s.spot_provider
            subs.append({"channel": ch, "filter": flt})
        return subs

    def _on_data(self, msg: dict) -> None:
        ch = msg["channel"]
        if ch not in (CH_TWAP, CH_SPOT):
            self.stats["unknown_frames"] += 1
            return
        seq = msg.get("seq")
        if isinstance(seq, int):
            last = self._last_seq.get(ch)
            if last is not None and seq != last + 1:
                self.stats["seq_gaps"] += 1
                self._event("seq_gap", channel=ch, expected=last + 1, got=seq)
                log.warning("разрыв seq на %s: ждали %d, пришло %d", ch, last + 1, seq)
            self._last_seq[ch] = seq
        dropped = msg.get("dropped")
        if dropped:
            self.stats["dropped_frames"] += int(dropped)
            self._event("dropped", channel=ch, count=int(dropped))
            log.warning("сервер отбросил %s кадров на %s (клиент не успевает)", dropped, ch)

        payload = msg["payload"]
        if not isinstance(payload, dict):
            self.stats["unknown_frames"] += 1
            return
        symbol = payload.get("symbol")
        key = (ch, symbol)
        if key not in self._required_set:
            self.stats["unexpected_symbol"] += 1
            return

        source = payload.get("source")
        if source is not None:
            prev = self._source.get(key)
            if prev is not None and prev != source:
                self._event("source_changed", channel=ch, symbol=symbol, old=prev, new=source)
                log.warning("поставщик %s/%s сменился: %s -> %s", ch, symbol, prev, source)
            self._source[key] = source

        snapshot = bool(msg.get("snapshot"))
        points = (payload.get("data") or []) if snapshot else [payload]
        if snapshot and not points:
            self._event("empty_snapshot", channel=ch, symbol=symbol)
        if snapshot:
            points = sorted((p for p in points if isinstance(p, dict)),
                            key=lambda p: p.get("timestamp") or 0)
        for p in points:
            self._accept_point(key, p, source, seq if isinstance(seq, int) else None,
                               snapshot, msg.get("ts"))

        # Запасной путь: если все потоки уже отдали данные, подписки точно работают, даже если
        # число ack отличается от ожидаемого (TODO(verify): один ack на подписку или на пакет)
        if self._phase == "subscribing" and all(k in self._latest_event_ms for k in self.required):
            self._go_live("data")

        if not self._was_fresh and self.is_fresh():
            self._was_fresh = True
            self._event("fresh")
            log.info("цена свежая")

    def _accept_point(self, key: Key, p: dict, source, seq, snapshot: bool, env_ts) -> None:
        ch, symbol = key
        raw_v = p.get("full_accuracy_value")
        if raw_v is None:
            raw_v = p.get("value")
        try:
            value = float(raw_v)
            ts_ms = int(p.get("timestamp") if p.get("timestamp") is not None else env_ts)
        except (TypeError, ValueError):
            value, ts_ms = math.nan, 0
        if not math.isfinite(value) or value <= 0 or ts_ms <= 0:
            self.stats["bad_values"] += 1
            return
        last = self._last_emitted_ms.get(key)
        if last is not None and ts_ms <= last:
            # повтор, пересечение снапшота с уже виденным или откат времени: НЕ данные
            self.stats["old_ticks"] += 1
            return
        recv = self._wall()
        tick = PriceTick(ch, symbol, ts_ms, value, str(raw_v), source, seq, recv, snapshot)
        self._last_emitted_ms[key] = ts_ms
        self._latest[key] = tick
        self._latest_event_ms[key] = ts_ms
        self._key_mono[key] = self._mono()
        self.last_data_ts = recv
        self._conn_got_data = True
        self.stats["ticks"] += 1
        for cb in self._callbacks:
            try:
                cb(tick)
            except Exception:  # noqa: BLE001 - ошибка подписчика не должна ронять поток
                self.stats["callback_errors"] += 1
                log.exception("ошибка в обработчике тиков")

    # ------------------------------------------------------------ журнал
    def _record(self, kind: str, **fields) -> None:
        if self._rec is not None:
            self._rec.write(kind, **fields)

    def _event(self, name: str, **fields) -> None:
        self._record("event", name=name, **fields)


def _parse_retry_after(value) -> float:
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 0.0
