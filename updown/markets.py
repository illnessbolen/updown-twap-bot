"""
Рынки Up/Down 5m и 15m (docs/API_NOTES.md, §2).

Поиск: Gamma API, документированный фильтр
  GET /events?tag_slug=up-or-down&closed=false&end_date_min=…&end_date_max=…
и отбор по event.seriesSlug ("btc-up-or-down-5m" и т.п.).

Strike (price to beat) во время окна API не отдаёт, поэтому берём его из своей
записи Chainlink TWAP-60: значение потока в момент начала окна. После закрытия
Polymarket публикует eventMetadata.priceToBeat/finalPrice (поле не документировано),
с ними сверяемся командой `py -m updown strike-check`.
TODO(verify): какой именно принт становится strike (см. API_NOTES, V1). Принты идут
раз в секунду с целыми секундами; основное правило - принт ровно на границе.
"""
from __future__ import annotations

import asyncio
import bisect
import json
import logging
import math
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Iterable

from .fees import FeeSchedule, UnsupportedFeeSchedule

log = logging.getLogger(__name__)

GAMMA_URL = "https://gamma-api.polymarket.com"
TAG_SLUG = "up-or-down"
USER_AGENT = "updown-twap-bot/0.1"
EXPECTED_TWAP_SEC = 60


class MarketParseError(ValueError):
    pass


def asset_of(symbol: str) -> str:
    """btcusd -> btc"""
    return symbol[:-3] if symbol.endswith("usd") else symbol


def series_slug(symbol: str, duration_min: int) -> str:
    return f"{asset_of(symbol)}-up-or-down-{duration_min}m"


def parse_iso(s: str) -> float:
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class UpDownMarket:
    slug: str
    event_id: str
    market_id: str
    condition_id: str
    symbol: str            # "btcusd"
    duration_min: int      # 5 или 15
    start_ts: float        # начало окна = момент strike
    end_ts: float          # конец окна = время расчёта
    token_up: str
    token_down: str
    tick_size: Decimal
    min_order_size: Decimal
    fees: FeeSchedule
    twap_window_sec: int
    active: bool
    closed: bool
    accepting_orders: bool
    question: str = ""

    @property
    def tokens(self) -> tuple[str, str]:
        return self.token_up, self.token_down

    def side_of(self, token_id: str) -> str | None:
        return "up" if token_id == self.token_up else "down" if token_id == self.token_down else None

    def is_live(self, now: float) -> bool:
        return self.start_ts <= now < self.end_ts

    def secs_left(self, now: float) -> float:
        return self.end_ts - now

    def record(self) -> dict:
        return {"t": "market", "slug": self.slug, "event_id": self.event_id, "market_id": self.market_id,
                "condition_id": self.condition_id, "sym": self.symbol, "dur": self.duration_min,
                "start_ts": self.start_ts, "end_ts": self.end_ts, "token_up": self.token_up,
                "token_down": self.token_down, "tick": self.tick_size, "min_size": self.min_order_size,
                "fee_rate": self.fees.rate, "fee_exp": self.fees.exponent, "rebate": self.fees.rebate_rate,
                "fees_enabled": self.fees.enabled, "twap_sec": self.twap_window_sec,
                "accepting": self.accepting_orders, "question": self.question}


def _json_list(v: Any, name: str) -> list:
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        try:
            out = json.loads(v)
        except ValueError as e:
            raise MarketParseError(f"{name}: не JSON") from e
        if isinstance(out, list):
            return out
    raise MarketParseError(f"{name}: ожидается список")


def parse_event(event: dict, wanted: dict[str, tuple[str, int]]) -> UpDownMarket | None:
    """
    wanted: {seriesSlug: (symbol, duration_min)}. Возвращает None, если серия не наша.
    Бросает MarketParseError, если рынок наш, но данные не годятся для торговли.
    """
    series = event.get("seriesSlug")
    if series not in wanted:
        return None
    symbol, duration = wanted[series]
    markets = event.get("markets") or []
    if len(markets) != 1:
        raise MarketParseError(f"в событии {len(markets)} рынков, ожидался 1")
    m = markets[0]

    labels = _json_list(m.get("outcomes"), "outcomes")
    tokens = _json_list(m.get("clobTokenIds"), "clobTokenIds")
    if len(labels) != 2 or len(tokens) != 2:
        raise MarketParseError("ожидалось два исхода")
    by_label = {str(lbl).strip().lower(): str(tok) for lbl, tok in zip(labels, tokens)}
    if set(by_label) != {"up", "down"}:
        raise MarketParseError(f"исходы {labels}, ожидались Up/Down")

    start_raw = m.get("eventStartTime") or event.get("startTime")
    end_raw = m.get("endDate") or event.get("endDate")
    if not start_raw or not end_raw:
        raise MarketParseError("нет eventStartTime/endDate")
    start_ts, end_ts = parse_iso(start_raw), parse_iso(end_raw)
    if abs((end_ts - start_ts) - duration * 60) > 1:
        raise MarketParseError(f"длина окна {end_ts - start_ts:.0f} с, ожидалось {duration * 60}")

    cmc = m.get("cryptoMarketConfig") or {}
    twap_sec = int(cmc.get("twapLookbackSeconds", EXPECTED_TWAP_SEC))   # TODO(verify): поле не документировано
    if twap_sec != EXPECTED_TWAP_SEC:
        raise MarketParseError(f"TWAP-окно {twap_sec} с, бот поддерживает только {EXPECTED_TWAP_SEC}")

    try:
        fees = FeeSchedule.from_gamma_market(m)
        if fees.enabled:
            fees.check()
    except UnsupportedFeeSchedule as e:
        raise MarketParseError(f"комиссия: {e}") from e

    try:
        tick = Decimal(str(m["orderPriceMinTickSize"]))
        min_size = Decimal(str(m["orderMinSize"]))
    except (KeyError, ArithmeticError, ValueError) as e:
        raise MarketParseError("нет orderPriceMinTickSize/orderMinSize") from e

    return UpDownMarket(
        slug=event.get("slug") or m.get("slug") or "",
        event_id=str(event.get("id", "")),
        market_id=str(m.get("id", "")),
        condition_id=str(m.get("conditionId", "")),
        symbol=symbol,
        duration_min=duration,
        start_ts=start_ts,
        end_ts=end_ts,
        token_up=by_label["up"],
        token_down=by_label["down"],
        tick_size=tick,
        min_order_size=min_size,
        fees=fees,
        twap_window_sec=twap_sec,
        active=bool(m.get("active")),
        closed=bool(m.get("closed")),
        accepting_orders=bool(m.get("acceptingOrders")),
        question=m.get("question") or event.get("title") or "",
    )


# ---------- Gamma API ----------

def _urllib_fetch(url: str, timeout: float) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


class GammaClient:
    PAGE = 100

    def __init__(self, base_url: str = GAMMA_URL, *, timeout: float = 10.0,
                 fetch: Callable[[str, float], Any] | None = None):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._fetch = fetch or _urllib_fetch

    async def get(self, path: str, params: Iterable[tuple[str, str]] = ()) -> Any:
        qs = urllib.parse.urlencode(list(params))
        url = f"{self.base_url}{path}" + (f"?{qs}" if qs else "")
        return await asyncio.to_thread(self._fetch, url, self.timeout)

    async def list_updown_events(self, end_min: float, end_max: float, *, closed: bool) -> list[dict]:
        """События tag_slug=up-or-down с endDate в [end_min, end_max]. Если упёрлись в лимит - делим интервал."""
        out = await self.get("/events", [
            ("tag_slug", TAG_SLUG), ("closed", "true" if closed else "false"),
            ("end_date_min", iso(end_min)), ("end_date_max", iso(end_max)), ("limit", str(self.PAGE))])
        if not isinstance(out, list):
            raise ValueError("Gamma /events: ожидался список")
        if len(out) >= self.PAGE and end_max - end_min > 120:
            mid = math.floor((end_min + end_max) / 2)
            left = await self.list_updown_events(end_min, mid, closed=closed)
            right = await self.list_updown_events(mid + 1, end_max, closed=closed)
            seen, merged = set(), []
            for e in left + right:
                if e.get("id") not in seen:
                    seen.add(e.get("id"))
                    merged.append(e)
            return merged
        return out

    async def get_event_by_slug(self, slug: str) -> dict | None:
        return await self.get(f"/events/slug/{urllib.parse.quote(slug)}")


# ---------- реестр рынков ----------

class MarketRegistry:
    def __init__(self, gamma: GammaClient, symbols: Iterable[str], durations: Iterable[int], *,
                 lookahead_sec: float = 1200.0, keep_after_end_sec: float = 3600.0,
                 on_market: Callable[[UpDownMarket], None] | None = None,
                 on_event: Callable[[dict], None] | None = None,
                 clock: Callable[[], float] = time.time):
        self.gamma = gamma
        self.wanted = {series_slug(s, d): (s, d) for s in symbols for d in durations}
        self.lookahead_sec = lookahead_sec
        self.keep_after_end_sec = keep_after_end_sec
        self.on_market = on_market
        self.on_event = on_event
        self.clock = clock
        self.markets: dict[str, UpDownMarket] = {}
        self.last_ok_ts: float | None = None
        self.errors = 0
        self._rejected: set[str] = set()
        self._missing: set[str] = set()

    async def refresh(self) -> list[UpDownMarket]:
        now = self.clock()
        events = await self.gamma.list_updown_events(now - 60, now + self.lookahead_sec, closed=False)
        new = []
        for ev in events:
            try:
                m = parse_event(ev, self.wanted)
            except MarketParseError as e:
                slug = ev.get("slug", "?")
                if slug not in self._rejected:
                    self._rejected.add(slug)
                    self._emit("market_rejected", slug=slug, reason=str(e))
                continue
            if m is None:
                continue
            if m.slug not in self.markets:
                new.append(m)
            self.markets[m.slug] = m
        for m in new:
            if self.on_market:
                self.on_market(m)
        for slug in [s for s, m in self.markets.items() if m.end_ts < now - self.keep_after_end_sec]:
            del self.markets[slug]
        for series, (sym, dur) in self.wanted.items():
            live = any(m.symbol == sym and m.duration_min == dur and m.is_live(now) for m in self.markets.values())
            if not live and series not in self._missing:
                self._missing.add(series)
                self._emit("market_missing", series=series)
            elif live and series in self._missing:
                self._missing.discard(series)
                self._emit("market_back", series=series)
        self.last_ok_ts = now
        return new

    def live(self, now: float | None = None) -> list[UpDownMarket]:
        now = self.clock() if now is None else now
        return sorted((m for m in self.markets.values() if m.is_live(now)), key=lambda m: (m.symbol, m.duration_min))

    def by_token(self, token_id: str) -> UpDownMarket | None:
        for m in self.markets.values():
            if token_id in m.tokens:
                return m
        return None

    def tokens_to_watch(self, now: float, before_start_sec: float, after_end_sec: float) -> set[str]:
        out: set[str] = set()
        for m in self.markets.values():
            if m.start_ts - before_start_sec <= now < m.end_ts + after_end_sec:
                out.update(m.tokens)
        return out

    async def run(self, stop: asyncio.Event, every_sec: float = 30.0) -> None:
        while not stop.is_set():
            try:
                await self.refresh()
            except Exception as e:   # сеть, Cloudflare, неожиданный ответ: пишем и пробуем снова
                self.errors += 1
                self._emit("gamma_error", error=f"{type(e).__name__}: {e}")
            try:
                await asyncio.wait_for(stop.wait(), timeout=every_sec)
            except asyncio.TimeoutError:
                pass

    def _emit(self, kind: str, **fields: Any) -> None:
        log.warning("markets %s %s", kind, fields)
        if self.on_event:
            self.on_event({"kind": kind, "recv_ts": self.clock(), **fields})


# ---------- strike по своей записи TWAP ----------

class TwapHistory:
    """Принты TWAP-60 по символам (ts -> value), с дедупликацией по времени."""

    def __init__(self, keep_sec: float = 3 * 3600):
        self.keep_sec = keep_sec
        self._ts: dict[str, list[float]] = {}
        self._val: dict[str, dict[float, Decimal]] = {}

    def add(self, symbol: str, ts: float, value: Decimal) -> None:
        tss = self._ts.setdefault(symbol, [])
        vals = self._val.setdefault(symbol, {})
        if ts not in vals:
            bisect.insort(tss, ts)
        vals[ts] = value
        cutoff = tss[-1] - self.keep_sec
        if tss[0] < cutoff:
            k = bisect.bisect_left(tss, cutoff)
            for old in tss[:k]:
                del vals[old]
            del tss[:k]

    def candidates(self, symbol: str, at: float) -> "StrikeCandidates":
        tss = self._ts.get(symbol, [])
        vals = self._val.get(symbol, {})
        i = bisect.bisect_left(tss, at)
        exact = vals[at] if i < len(tss) and tss[i] == at else None
        before = (tss[i - 1], vals[tss[i - 1]]) if i > 0 else None
        j = i + 1 if exact is not None else i
        after = (tss[j], vals[tss[j]]) if j < len(tss) else None
        return StrikeCandidates(at=at, exact=exact, before=before, after=after)

    def span(self, symbol: str) -> tuple[float, float] | None:
        tss = self._ts.get(symbol)
        return (tss[0], tss[-1]) if tss else None


@dataclass(frozen=True)
class StrikeCandidates:
    at: float
    exact: Decimal | None
    before: tuple[float, Decimal] | None   # последний принт строго до at
    after: tuple[float, Decimal] | None    # первый принт строго после at

    def chosen(self, max_gap_sec: float = 2.0) -> Decimal | None:
        """Основное правило: принт ровно на границе; иначе последний до неё, если он не старше max_gap_sec."""
        if self.exact is not None:
            return self.exact
        if self.before is not None and self.at - self.before[0] <= max_gap_sec:
            return self.before[1]
        return None

    def record(self, max_gap_sec: float = 2.0) -> dict:
        return {"at": self.at, "exact": self.exact,
                "before_ts": self.before[0] if self.before else None,
                "before": self.before[1] if self.before else None,
                "after_ts": self.after[0] if self.after else None,
                "after": self.after[1] if self.after else None,
                "chosen": self.chosen(max_gap_sec)}


@dataclass
class StrikeTracker:
    """Фиксирует strike (TWAP на начало окна) и итог (TWAP на конец) для рынков реестра."""
    history: TwapHistory
    registry: MarketRegistry
    on_record: Callable[[dict], None] | None = None
    settle_sec: float = 3.0          # подождать принт на границе (задержка потока ~1.5 с)
    max_gap_sec: float = 2.0
    strikes: dict[str, StrikeCandidates] = field(default_factory=dict)
    finals: dict[str, StrikeCandidates] = field(default_factory=dict)

    def strike(self, slug: str) -> Decimal | None:
        c = self.strikes.get(slug)
        return c.chosen(self.max_gap_sec) if c else None

    def poll(self, now: float) -> None:
        for m in list(self.registry.markets.values()):
            if m.slug not in self.strikes and now >= m.start_ts + self.settle_sec:
                self._fix(m, "strike", m.start_ts, self.strikes, now)
            if m.slug not in self.finals and now >= m.end_ts + self.settle_sec:
                self._fix(m, "final", m.end_ts, self.finals, now)

    def _fix(self, m: UpDownMarket, kind: str, at: float, store: dict, now: float) -> None:
        c = self.history.candidates(m.symbol, at)
        store[m.slug] = c
        if self.on_record:
            self.on_record({"t": kind, "slug": m.slug, "sym": m.symbol, "dur": m.duration_min,
                            "recv_ts": now, **c.record(self.max_gap_sec)})
        if c.chosen(self.max_gap_sec) is None:
            log.warning("%s %s: нет принта TWAP у границы %s - рынок не торгуем", kind, m.slug, iso(at))


# ---------- сверка с Polymarket ----------

def same_price(ours: Decimal | None, theirs: Any, rel_tol: float = 1e-9) -> bool | None:
    if ours is None or theirs is None:
        return None
    t = float(theirs)
    return abs(float(ours) - t) <= rel_tol * max(abs(t), 1.0)


def check_against_gamma(event: dict, strike: StrikeCandidates, final: StrikeCandidates,
                        max_gap_sec: float = 2.0) -> dict:
    """Сравнение наших кандидатов strike/итога с eventMetadata и исходом рынка."""
    meta = event.get("eventMetadata") or {}
    ptb, fin = meta.get("priceToBeat"), meta.get("finalPrice")
    m = (event.get("markets") or [{}])[0]
    outcome = None
    try:
        prices = _json_list(m.get("outcomePrices"), "outcomePrices")
        labels = _json_list(m.get("outcomes"), "outcomes")
        won = [str(lbl).lower() for lbl, p in zip(labels, prices) if str(p) in ("1", "1.0")]
        outcome = won[0] if len(won) == 1 else None
    except MarketParseError:
        pass
    our_strike, our_final = strike.chosen(max_gap_sec), final.chosen(max_gap_sec)
    our_outcome = None
    if our_strike is not None and our_final is not None:
        our_outcome = "up" if our_final >= our_strike else "down"
    return {
        "slug": event.get("slug"), "price_to_beat": ptb, "final_price": fin, "outcome": outcome,
        "strike_exact": same_price(strike.exact, ptb),
        "strike_before": same_price(strike.before[1] if strike.before else None, ptb),
        "strike_after": same_price(strike.after[1] if strike.after else None, ptb),
        "strike_chosen": same_price(our_strike, ptb),
        "final_exact": same_price(final.exact, fin),
        "final_chosen": same_price(our_final, fin),
        "outcome_match": (our_outcome == outcome) if (our_outcome and outcome) else None,
        "strike_diff": (float(our_strike) - float(ptb)) if (our_strike is not None and ptb is not None) else None,
    }
