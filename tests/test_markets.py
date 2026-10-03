import asyncio
import copy
import json
from decimal import Decimal
from pathlib import Path

import pytest

from updown.fees import CRYPTO
from updown.markets import (
    GammaClient, MarketParseError, MarketRegistry, StrikeCandidates, StrikeTracker, TwapHistory,
    check_against_gamma, iso, parse_event, parse_iso, same_price, series_slug,
)

FIX = Path(__file__).resolve().parent / "fixtures"
WANTED = {series_slug(s, d): (s, d) for s in ("btcusd", "ethusd") for d in (5, 15)}


def fixture(name: str) -> dict:
    return json.loads((FIX / name).read_text(encoding="utf-8"))


def shifted(event: dict, start_ts: float) -> dict:
    """Копия живого события Gamma с окном, сдвинутым на start_ts."""
    ev = copy.deepcopy(event)
    m = ev["markets"][0]
    dur = parse_iso(m["endDate"]) - parse_iso(m["eventStartTime"])
    m["eventStartTime"] = ev["startTime"] = iso(start_ts)
    m["endDate"] = ev["endDate"] = iso(start_ts + dur)
    ev["slug"] = m["slug"] = ev["slug"].rsplit("-", 1)[0] + f"-{int(start_ts)}"
    ev["id"] = f"{ev['id']}-{int(start_ts)}"
    m["clobTokenIds"] = json.dumps([f"{t}{int(start_ts)}" for t in json.loads(m["clobTokenIds"])])
    return ev


# ---------- разбор событий ----------

def test_series_slug():
    assert series_slug("btcusd", 5) == "btc-up-or-down-5m"
    assert series_slug("ethusd", 15) == "eth-up-or-down-15m"


def test_parse_live_btc_5m():
    ev = fixture("gamma_event_btc_5m_active.json")
    m = parse_event(ev, WANTED)
    assert m.symbol == "btcusd" and m.duration_min == 5
    assert m.end_ts - m.start_ts == 300
    assert m.slug == ev["slug"] and m.slug.endswith(str(int(m.start_ts)))
    up, down = json.loads(ev["markets"][0]["clobTokenIds"])
    assert (m.token_up, m.token_down) == (up, down)     # outcomes = ["Up", "Down"]
    assert m.tick_size == Decimal("0.01") and m.min_order_size == Decimal("5")
    assert m.fees == CRYPTO
    assert m.twap_window_sec == 60
    assert m.side_of(up) == "up" and m.side_of(down) == "down" and m.side_of("x") is None


def test_parse_live_eth_15m():
    m = parse_event(fixture("gamma_event_eth_15m_active.json"), WANTED)
    assert m.symbol == "ethusd" and m.duration_min == 15 and m.end_ts - m.start_ts == 900


def test_outcomes_mapped_by_label_not_index():
    ev = fixture("gamma_event_btc_5m_active.json")
    m0 = ev["markets"][0]
    up, down = json.loads(m0["clobTokenIds"])
    m0["outcomes"] = json.dumps(["Down", "Up"])
    m0["clobTokenIds"] = json.dumps([down, up])
    m = parse_event(ev, WANTED)
    assert (m.token_up, m.token_down) == (up, down)


def test_foreign_series_ignored():
    ev = fixture("gamma_event_btc_5m_active.json")
    ev["seriesSlug"] = "sol-up-or-down-5m"
    assert parse_event(ev, WANTED) is None


@pytest.mark.parametrize("mutate,match", [
    (lambda m: m.update(cryptoMarketConfig={"twapLookbackSeconds": 30}), "TWAP"),
    (lambda m: m.update(endDate="2026-10-03T10:30:00Z"), "длина окна"),
    (lambda m: m.update(outcomes='["Yes","No"]'), "Up/Down"),
    (lambda m: m.update(clobTokenIds='["a"]'), "два исхода"),
    (lambda m: m.update(feeSchedule={"rate": 0.07, "exponent": 2, "takerOnly": True, "rebateRate": 0.2}), "комиссия"),
    (lambda m: m.pop("orderMinSize"), "orderMinSize"),
])
def test_bad_markets_rejected(mutate, match):
    ev = fixture("gamma_event_btc_5m_active.json")
    mutate(ev["markets"][0])
    with pytest.raises(MarketParseError, match=match):
        parse_event(ev, WANTED)


# ---------- Gamma и реестр ----------

class FakeGamma(GammaClient):
    def __init__(self, events, pages=None):
        super().__init__("https://gamma.test", fetch=self._fake)
        self.events = events
        self.urls = []

    def _fake(self, url, timeout):
        self.urls.append(url)
        if "/events/slug/" in url:
            slug = url.rsplit("/", 1)[1]
            return next((e for e in self.events if e["slug"] == slug), None)
        return list(self.events)[:self.PAGE]


def test_registry_finds_live_markets_and_tokens():
    now = 1_791_100_000.0
    start = now - now % 300
    base = fixture("gamma_event_btc_5m_active.json")
    evs = [shifted(base, start), shifted(base, start + 300), shifted(base, start - 300)]
    gamma = FakeGamma(evs)
    seen, events = [], []
    reg = MarketRegistry(gamma, ["btcusd"], [5], on_market=seen.append, on_event=events.append, clock=lambda: now)
    new = asyncio.run(reg.refresh())
    assert len(new) == 3 and len(seen) == 3
    live = reg.live(now)
    assert len(live) == 1 and live[0].start_ts == start
    toks = reg.tokens_to_watch(now, before_start_sec=60, after_end_sec=30)
    assert toks == set(live[0].tokens)                         # следующее окно ещё дальше 60 с
    toks = reg.tokens_to_watch(start + 250, before_start_sec=60, after_end_sec=30)
    assert len(toks) == 4                                      # текущее + следующее
    assert reg.by_token(live[0].token_up).slug == live[0].slug
    assert not any(e["kind"] == "market_missing" for e in events)
    assert asyncio.run(reg.refresh()) == []                    # повторное чтение - ничего нового
    assert "tag_slug=up-or-down" in gamma.urls[0] and "closed=false" in gamma.urls[0]


def test_registry_reports_missing_and_rejected():
    now = 1_791_100_000.0
    bad = fixture("gamma_event_btc_5m_active.json")
    bad["markets"][0]["outcomes"] = '["Yes","No"]'
    events = []
    reg = MarketRegistry(FakeGamma([bad]), ["btcusd"], [5, 15], on_event=events.append, clock=lambda: now)
    asyncio.run(reg.refresh())
    kinds = [e["kind"] for e in events]
    assert "market_rejected" in kinds
    assert sorted(e["series"] for e in events if e["kind"] == "market_missing") == [
        "btc-up-or-down-15m", "btc-up-or-down-5m"]
    asyncio.run(reg.refresh())
    assert sum(e["kind"] == "market_missing" for e in events) == 2     # не повторяется каждые 30 с


def test_registry_run_survives_gamma_errors():
    events = []

    def boom(url, timeout):
        raise OSError("network down")

    reg = MarketRegistry(GammaClient(fetch=boom), ["btcusd"], [5], on_event=events.append)

    async def main():
        stop = asyncio.Event()
        task = asyncio.create_task(reg.run(stop, every_sec=0.01))
        await asyncio.sleep(0.05)
        stop.set()
        await task
    asyncio.run(main())
    assert reg.errors >= 2 and events[0]["kind"] == "gamma_error" and "network down" in events[0]["error"]


def test_list_splits_interval_when_page_is_full():
    calls = []

    def fetch(url, timeout):
        calls.append(url)
        # первый широкий запрос "упирается" в лимит, половинки - нет
        return [{"id": i} for i in range(100)] if len(calls) == 1 else [{"id": f"{len(calls)}-{i}"} for i in range(3)]
    out = asyncio.run(GammaClient(fetch=fetch).list_updown_events(1_791_000_000, 1_791_003_600, closed=True))
    assert len(calls) == 3 and len(out) == 6


# ---------- strike ----------

def test_candidates_exact_before_after():
    h = TwapHistory()
    for ts, v in [(99.0, "1"), (100.0, "2"), (101.0, "3")]:
        h.add("btcusd", ts, Decimal(v))
    c = h.candidates("btcusd", 100.0)
    assert c.exact == Decimal(2) and c.before == (99.0, Decimal(1)) and c.after == (101.0, Decimal(3))
    assert c.chosen() == Decimal(2)


def test_candidates_without_exact_print():
    h = TwapHistory()
    h.add("btcusd", 98.5, Decimal("1"))
    h.add("btcusd", 101.0, Decimal("3"))
    c = h.candidates("btcusd", 100.0)
    assert c.exact is None and c.before == (98.5, Decimal(1)) and c.after == (101.0, Decimal(3))
    assert c.chosen(max_gap_sec=2) == Decimal(1)
    assert c.chosen(max_gap_sec=1) is None         # последний принт слишком старый


def test_history_dedup_out_of_order_and_trim():
    h = TwapHistory(keep_sec=10)
    h.add("btcusd", 5.0, Decimal(5))
    h.add("btcusd", 3.0, Decimal(3))        # снапшот после переподключения приходит "в прошлое"
    h.add("btcusd", 5.0, Decimal(6))        # повтор той же секунды - перезапись
    assert h.candidates("btcusd", 5.0).exact == Decimal(6)
    h.add("btcusd", 20.0, Decimal(20))
    assert h.span("btcusd") == (20.0, 20.0)  # всё старше 10 с отрезано


def test_strike_tracker_fixes_strike_and_final():
    now = 1_791_100_000.0
    start = now - now % 300
    reg = MarketRegistry(FakeGamma([shifted(fixture("gamma_event_btc_5m_active.json"), start)]),
                         ["btcusd"], [5], clock=lambda: now)
    asyncio.run(reg.refresh())
    h = TwapHistory()
    for k in range(-5, 306):
        h.add("btcusd", start + k, Decimal(84000 + k))
    records = []
    tr = StrikeTracker(h, reg, on_record=records.append, settle_sec=3)
    tr.poll(start + 2)
    assert records == []                                  # ещё ждём settle
    tr.poll(start + 3)
    slug = next(iter(reg.markets))
    assert tr.strike(slug) == Decimal(84000)
    assert records[0]["t"] == "strike" and records[0]["exact"] == Decimal(84000)
    tr.poll(start + 303)
    assert records[-1]["t"] == "final" and records[-1]["chosen"] == Decimal(84300)
    tr.poll(start + 400)
    assert len(records) == 2                              # фиксируется один раз


def test_strike_unknown_when_no_prints():
    now = 1_791_100_000.0
    start = now - now % 300
    reg = MarketRegistry(FakeGamma([shifted(fixture("gamma_event_btc_5m_active.json"), start)]),
                         ["btcusd"], [5], clock=lambda: now)
    asyncio.run(reg.refresh())
    records = []
    tr = StrikeTracker(TwapHistory(), reg, on_record=records.append)
    tr.poll(start + 10)
    assert records[0]["chosen"] is None and tr.strike(records[0]["slug"]) is None


# ---------- сверка с Polymarket ----------

def test_check_against_closed_event_fixture():
    ev = fixture("gamma_event_btc_5m_closed.json")      # priceToBeat 84566.38667138515, finalPrice 84568.19447875889
    start = parse_iso(ev["markets"][0]["eventStartTime"])
    end = parse_iso(ev["markets"][0]["endDate"])
    strike = StrikeCandidates(start, Decimal("84566.386671385150000000"), (start - 1, Decimal("84566.1")), None)
    final = StrikeCandidates(end, Decimal("84568.194478758890000000"), None, None)
    r = check_against_gamma(ev, strike, final)
    assert r["strike_exact"] is True and r["strike_before"] is False and r["strike_chosen"] is True
    assert r["final_exact"] is True
    assert r["outcome"] == "up" and r["outcome_match"] is True
    assert abs(r["strike_diff"]) < 1e-6


def test_check_detects_wrong_outcome():
    ev = fixture("gamma_event_btc_5m_closed.json")
    strike = StrikeCandidates(0, Decimal("84570"), None, None)
    final = StrikeCandidates(0, Decimal("84568"), None, None)    # у нас вышло бы Down
    r = check_against_gamma(ev, strike, final)
    assert r["outcome"] == "up" and r["outcome_match"] is False and r["strike_chosen"] is False


def test_same_price_tolerance():
    assert same_price(Decimal("84566.38667138515"), 84566.38667138515) is True
    assert same_price(Decimal("84566.38"), 84566.38667138515) is False
    assert same_price(None, 1.0) is None and same_price(Decimal(1), None) is None
