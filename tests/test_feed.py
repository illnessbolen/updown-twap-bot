"""
Тесты PolyBoltFeed на локальном фейковом сервере (настоящий WebSocket на 127.0.0.1).
Сценарий задаётся на каждое соединение: так проверяются зависание, коды закрытия и переподключение.
"""
import asyncio
import json
import time
from decimal import Decimal
from http import HTTPStatus

import pytest
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from updown.config import ApiCreds
from updown.feed import SPOT, TWAP, FeedFatal, PolyBoltFeed

CREDS = ApiCreds("key-123", "SECRET-xyz-987", "PASS-abc-654")
SYMBOLS = ["btcusd", "ethusd"]


def now_ms(offset_ms: int = 0) -> int:
    return int(time.time() * 1000) + offset_ms


class FakePolyBolt:
    def __init__(self, scenarios, http_responses=None):
        self.scenarios = scenarios
        self.http_responses = list(http_responses or [])   # [(status, headers) | None] на попытку
        self.conns = 0
        self.attempts = 0
        self.frames: list[list[dict]] = []

    def process_request(self, connection, request):
        i = self.attempts
        self.attempts += 1
        if i < len(self.http_responses) and self.http_responses[i]:
            status, headers = self.http_responses[i]
            resp = connection.respond(status, "busy\n")
            for k, v in headers.items():
                resp.headers[k] = v
            return resp
        return None

    async def handler(self, ws):
        idx = self.conns
        self.conns += 1
        self.frames.append([])
        scenario = self.scenarios[min(idx, len(self.scenarios) - 1)]
        try:
            await scenario(self, ws, idx)
        except ConnectionClosed:
            pass

    async def recv_json(self, ws, idx):
        msg = json.loads(await ws.recv())
        self.frames[idx].append(msg)
        return msg


# ---------- строительные блоки сценариев ----------

async def handshake(srv, ws, idx):
    auth = await srv.recv_json(ws, idx)
    assert auth["op"] == "auth"
    await ws.send(json.dumps({"op": "authed", "rid": auth.get("rid")}))
    sub = await srv.recv_json(ws, idx)
    assert sub["op"] == "subscribe"
    return sub["subscriptions"]


def frame(sub, seqs, payload, snapshot=False, **extra):
    ch = sub["channel"]
    seqs[ch] = seqs.get(ch, 0) + 1
    payload = {"symbol": sub["filter"]["symbol"], **payload}
    if ch == TWAP:
        payload.setdefault("window_seconds", 60)
    msg = {"v": 1, "channel": ch, "seq": seqs[ch], "ts": now_ms(), "payload": payload, **extra}
    if snapshot:
        msg["snapshot"] = True
    return json.dumps(msg)


async def send_snapshots(ws, subs, seqs, empty=False):
    for s in subs:
        await ws.send(json.dumps({"op": "subscribed", "channel": s["channel"], "rid": "sub"}))
        data = [] if empty else [{"timestamp": now_ms(-1000), "value": 100.5, "full_accuracy_value": "100.5"}]
        await ws.send(frame(s, seqs, {"source": "chainlink", "data": data}, snapshot=True))


async def send_updates(ws, subs, seqs, value="101.25", ts_offset_ms=0):
    for s in subs:
        await ws.send(frame(s, seqs, {"value": float(value), "full_accuracy_value": value,
                                      "timestamp": now_ms(ts_offset_ms), "source": "chainlink"}))


def streaming(n=None, interval=0.02, ts_offset_ms=0, then=None, empty_snapshot=False):
    async def scenario(srv, ws, idx):
        subs = await handshake(srv, ws, idx)
        seqs = {}
        await send_snapshots(ws, subs, seqs, empty=empty_snapshot)
        k = 0
        while n is None or k < n:
            await send_updates(ws, subs, seqs, ts_offset_ms=ts_offset_ms)
            k += 1
            await asyncio.sleep(interval)
        if then:
            await then(ws)
    return scenario


async def hang(ws):
    await ws.wait_closed()   # соединение живо, но данных больше нет


def close_after_auth(code):
    async def scenario(srv, ws, idx):
        await srv.recv_json(ws, idx)
        await ws.close(code, "test")
    return scenario


def auth_error(code):
    async def scenario(srv, ws, idx):
        auth = await srv.recv_json(ws, idx)
        await ws.send(json.dumps({"op": "error", "code": code, "rid": auth.get("rid")}))
        await ws.wait_closed()
    return scenario


def close_after_data(code):
    async def scenario(srv, ws, idx):
        subs = await handshake(srv, ws, idx)
        seqs = {}
        await send_snapshots(ws, subs, seqs)
        await send_updates(ws, subs, seqs)
        await asyncio.sleep(0.05)
        await ws.close(code, "test")
    return scenario


# ---------- обвязка ----------

def make_feed(url, stale=0.4, **kw):
    ticks, events = [], []
    feed = PolyBoltFeed(
        CREDS, SYMBOLS, url=url, stale_after_sec=stale, backoff_sec=[0.01],
        drain_max_delay_sec=0.01, auth_timeout_sec=1.0,
        on_tick=ticks.append, on_event=events.append, connect_kwargs={"proxy": None}, **kw)
    return feed, ticks, events


def run_scenario(scenarios, until, *, http_responses=None, timeout=6.0, stale=0.4, sample=None):
    """Запускает фейковый сервер и feed; ждёт until(feed, srv, ticks, events) или окончания feed."""
    async def main():
        srv = FakePolyBolt(scenarios, http_responses)
        async with serve(srv.handler, "127.0.0.1", 0, process_request=srv.process_request) as server:
            port = server.sockets[0].getsockname()[1]
            feed, ticks, events = make_feed(f"ws://127.0.0.1:{port}", stale=stale)
            stop = asyncio.Event()
            task = asyncio.create_task(feed.run(stop))
            t0 = time.monotonic()
            try:
                while True:
                    if sample:
                        sample(feed)
                    if until(feed, srv, ticks, events) or task.done():
                        break
                    if time.monotonic() - t0 > timeout:
                        raise AssertionError(f"таймаут; события: {[e['kind'] for e in events]}")
                    await asyncio.sleep(0.01)
            finally:
                stop.set()
            error = None
            try:
                await asyncio.wait_for(task, 3)
            except FeedFatal as e:
                error = e
            return feed, srv, ticks, events, error
    return asyncio.run(main())


def kinds(events):
    return [e["kind"] for e in events]


def updates(ticks):
    return [t for t in ticks if not t.snapshot]


# ---------- тесты ----------

def test_auth_then_subscribe_then_ticks():
    fresh_seen = []
    feed, srv, ticks, events, err = run_scenario(
        [streaming()], lambda f, s, t, e: len(updates(t)) >= 8,
        sample=lambda f: fresh_seen.append(f.is_fresh("btcusd")))
    assert err is None
    auth, sub = srv.frames[0][0], srv.frames[0][1]
    assert auth == {"op": "auth", "rid": "auth", "auth": {
        "apiKey": "key-123", "secret": "SECRET-xyz-987", "passphrase": "PASS-abc-654"}}
    channels = {(s["channel"], s["filter"]["symbol"]) for s in sub["subscriptions"]}
    assert channels == {(TWAP, "btcusd"), (TWAP, "ethusd"), (SPOT, "btcusd"), (SPOT, "ethusd")}
    assert all(s["filter"]["window_seconds"] == 60 for s in sub["subscriptions"] if s["channel"] == TWAP)

    snaps = [t for t in ticks if t.snapshot]
    assert len(snaps) == 4 and all(t.value == Decimal("100.5") for t in snaps)
    ups = updates(ticks)
    assert all(t.value == Decimal("101.25") and isinstance(t.value, Decimal) for t in ups)
    assert all(t.source == "chainlink" for t in ticks)
    assert True in fresh_seen
    assert not feed.connected and not feed.is_fresh("btcusd")   # после остановки


def test_secrets_never_in_events():
    feed, srv, ticks, events, err = run_scenario([streaming()], lambda f, s, t, e: len(updates(t)) >= 4)
    dump = json.dumps(events, default=str) + repr(feed.creds)
    assert "SECRET-xyz-987" not in dump and "PASS-abc-654" not in dump


def test_spot_provider_pin_in_filter():
    async def main():
        srv = FakePolyBolt([streaming(n=1, then=hang)])
        async with serve(srv.handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            feed, ticks, _ = make_feed(f"ws://127.0.0.1:{port}", spot_provider="pyth")
            stop = asyncio.Event()
            task = asyncio.create_task(feed.run(stop))
            while len(srv.frames) < 1 or len(srv.frames[0]) < 2:
                await asyncio.sleep(0.01)
            stop.set()
            await asyncio.wait_for(task, 3)
            return srv
    srv = asyncio.run(main())
    spot = [s for s in srv.frames[0][1]["subscriptions"] if s["channel"] == SPOT]
    assert all(s["filter"]["provider"] == "pyth" for s in spot)


def test_silent_stall_triggers_reconnect_and_pauses_freshness():
    fresh_seen = []
    feed, srv, ticks, events, err = run_scenario(
        [streaming(n=3, then=hang), streaming()],
        lambda f, s, t, e: s.conns >= 2 and f.is_fresh("btcusd") and f.is_fresh("ethusd"),
        sample=lambda f: fresh_seen.append(f.is_fresh("btcusd")))
    assert err is None
    k = kinds(events)
    assert "stale" in k and "reconnect_wait" in k
    assert k.index("stale") < k.index("reconnect_wait")
    stale = next(e for e in events if e["kind"] == "stale")
    assert "нет данных" in stale["reason"]
    # было свежо -> стало несвежо (зависание) -> снова свежо после переподключения
    first_true = fresh_seen.index(True)
    false_after = fresh_seen.index(False, first_true)
    assert True in fresh_seen[false_after:]
    assert srv.frames[1][0]["op"] == "auth"            # после переподключения заново auth
    assert srv.frames[1][1]["op"] == "subscribe"       # и заново подписка


def test_close_4001_is_fatal_without_retry():
    feed, srv, ticks, events, err = run_scenario([close_after_auth(4001)], lambda *a: False)
    assert isinstance(err, FeedFatal) and "4001" in str(err)
    assert srv.conns == 1
    assert "fatal" in kinds(events) and "reconnect_wait" not in kinds(events)


def test_close_4008_is_fatal():
    feed, srv, ticks, events, err = run_scenario([close_after_data(4008)], lambda *a: False)
    assert isinstance(err, FeedFatal) and "4008" in str(err)
    assert srv.conns == 1


def test_close_4003_draining_reconnects():
    feed, srv, ticks, events, err = run_scenario(
        [close_after_data(4003), streaming()],
        lambda f, s, t, e: s.conns >= 2 and f.is_fresh("btcusd"))
    assert err is None
    waits = [e for e in events if e["kind"] == "reconnect_wait"]
    assert waits and "4003" in waits[0]["reason"]


def test_auth_invalid_is_fatal():
    feed, srv, ticks, events, err = run_scenario([auth_error("auth_invalid")], lambda *a: False)
    assert isinstance(err, FeedFatal) and "auth_invalid" in str(err)
    assert srv.conns == 1


def test_auth_unavailable_retries():
    feed, srv, ticks, events, err = run_scenario(
        [auth_error("auth_unavailable"), streaming()],
        lambda f, s, t, e: s.conns >= 2 and f.is_fresh("btcusd"))
    assert err is None
    assert "auth_unavailable" in next(e for e in events if e["kind"] == "reconnect_wait")["reason"]


def test_http_429_respects_retry_after():
    feed, srv, ticks, events, err = run_scenario(
        [streaming()], lambda f, s, t, e: f.is_fresh("btcusd"),
        http_responses=[(HTTPStatus.TOO_MANY_REQUESTS, {"Retry-After": "0.2"})])
    assert err is None
    wait = next(e for e in events if e["kind"] == "reconnect_wait")
    assert "429" in wait["reason"] and wait["delay_sec"] >= 0.2


def test_seq_gap_and_dropped_are_reported():
    async def scenario(srv, ws, idx):
        subs = await handshake(srv, ws, idx)
        seqs = {}
        await send_snapshots(ws, subs, seqs)
        twap_btc = subs[0]
        seqs[TWAP] += 1   # пропускаем один номер
        await ws.send(frame(twap_btc, seqs, {"full_accuracy_value": "101", "timestamp": now_ms()}, dropped=4))
        await hang(ws)
    feed, srv, ticks, events, err = run_scenario(
        [scenario, streaming()], lambda f, s, t, e: "seq_gap" in kinds(e) and "dropped" in kinds(e))
    gap = next(e for e in events if e["kind"] == "seq_gap")
    assert gap["channel"] == TWAP and gap["got"] == gap["expected"] + 1
    assert next(e for e in events if e["kind"] == "dropped")["count"] == 4


def test_old_prices_are_not_fresh_and_trigger_reconnect():
    fresh_seen = []
    feed, srv, ticks, events, err = run_scenario(
        [streaming(ts_offset_ms=-120_000)], lambda f, s, t, e: "stale" in kinds(e),
        sample=lambda f: fresh_seen.append(f.is_fresh("btcusd")))
    assert True not in fresh_seen
    stale = next(e for e in events if e["kind"] == "stale")
    assert "цене" in stale["reason"]


def test_empty_snapshot_is_not_data():
    feed, srv, ticks, events, err = run_scenario(
        [streaming(n=0, empty_snapshot=True, then=hang)], lambda f, s, t, e: "stale" in kinds(e))
    assert ticks == []
    assert not feed.is_fresh("btcusd")


def test_wrong_twap_window_is_rejected():
    async def scenario(srv, ws, idx):
        subs = await handshake(srv, ws, idx)
        seqs = {}
        await ws.send(frame(subs[0], seqs, {"full_accuracy_value": "99", "timestamp": now_ms(),
                                            "window_seconds": 30}))
        await hang(ws)
    feed, srv, ticks, events, err = run_scenario([scenario], lambda f, s, t, e: "bad_window" in kinds(e))
    assert ticks == []


def test_garbage_frame_is_reported_not_fatal():
    async def scenario(srv, ws, idx):
        subs = await handshake(srv, ws, idx)
        await ws.send("not json{")
        seqs = {}
        await send_snapshots(ws, subs, seqs)
        while True:
            await send_updates(ws, subs, seqs)
            await asyncio.sleep(0.02)
    feed, srv, ticks, events, err = run_scenario(
        [scenario], lambda f, s, t, e: "bad_frame" in kinds(e) and f.is_fresh("btcusd"))
    assert err is None


def test_stop_ends_cleanly():
    feed, srv, ticks, events, err = run_scenario([streaming()], lambda f, s, t, e: len(updates(t)) >= 4)
    assert err is None and not feed.connected
    assert "fatal" not in kinds(events)


@pytest.mark.parametrize("bad", ["0", "-5", "abc", None])
def test_bad_values_are_skipped(bad):
    async def scenario(srv, ws, idx):
        subs = await handshake(srv, ws, idx)
        seqs = {}
        point = {"timestamp": now_ms()}
        if bad is not None:
            point["full_accuracy_value"] = bad
        await ws.send(frame(subs[0], seqs, point))
        await hang(ws)
    feed, srv, ticks, events, err = run_scenario([scenario], lambda f, s, t, e: "bad_value" in kinds(e))
    assert ticks == []
