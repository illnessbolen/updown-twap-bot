"""
Тесты feed.py: детектор зависания, heartbeat, реконнект, коды закрытия, секреты.
Большинство - против локального фейкового сервера (tests/fake_polybolt.py).
"""
import asyncio
import json
import logging
import time
from pathlib import Path

import pytest
import pytest_asyncio

from config import Credentials
from feed import (CH_SPOT, CH_TWAP, Feed, FeedAuthError, FeedProtocolError, FeedSettings,
                  FeedSubscriptionError)
from recorder import JsonlRecorder, read_jsonl
from tests.fake_polybolt import FakePolyBolt, normal

KEY, SECRET, PASSPHRASE = "test-key-abc123", "test-secret-xyz789", "test-pass-qwe456"
SECRETS = (KEY, SECRET, PASSPHRASE)
CREDS = Credentials(KEY, SECRET, PASSPHRASE)
SERVER_CREDS = {"apiKey": KEY, "secret": SECRET, "passphrase": PASSPHRASE}


# ------------------------------------------------------------------ вспомогательное
@pytest_asyncio.fixture
async def srv():
    s = FakePolyBolt(SERVER_CREDS)
    await s.start()
    yield s
    await s.close()


_RECORDERS: list[JsonlRecorder] = []


@pytest.fixture(autouse=True)
def _close_recorders():
    """Тесты не обязаны закрывать рекордер сами: иначе ResourceWarning о незакрытом файле."""
    yield
    while _RECORDERS:
        _RECORDERS.pop().close()


def make_feed(url, tmp_path=None, *, creds=CREDS, rec=True, **kw):
    base = dict(url=url, symbols=("btcusd",), stale_after_sec=1.0, backoff_sec=(0.05,),
                heartbeat_sec=30.0, pong_timeout_sec=10.0, auth_timeout_sec=2.0,
                subscribe_timeout_sec=2.0, drain_delay_max_sec=0.1)
    base.update(kw)
    recorder = JsonlRecorder(tmp_path, gzip_closed=False) if (tmp_path and rec) else None
    if recorder is not None:
        _RECORDERS.append(recorder)
    return Feed(FeedSettings(**base), creds, recorder, rng=lambda: 0.0)


async def until(pred, timeout=6.0, step=0.02):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        await asyncio.sleep(step)
    return pred()


async def run_in_background(feed):
    return asyncio.create_task(feed.run())


async def shutdown(feed, task):
    await feed.stop()
    await asyncio.wait_for(task, 5)
    if feed._rec:
        feed._rec.close()


def journal(tmp_path):
    rows = []
    for p in sorted(Path(tmp_path).glob("*.jsonl*")):
        rows.extend(read_jsonl(p))
    return rows


def event_names(tmp_path):
    return [r["name"] for r in journal(tmp_path) if r["kind"] == "event"]


# ------------------------------------------------------------------ счастливый путь
async def test_happy_path_auth_then_subscribe_then_fresh(srv, tmp_path):
    async def slow_auth(conn):
        if await conn.handshake(authed_delay=0.2):
            await conn.stream()

    srv.scripts = [slow_auth]
    feed = make_feed(srv.url, tmp_path)
    ticks = []
    feed.on_tick(ticks.append)
    task = await run_in_background(feed)
    assert await feed.wait_fresh(4), feed.status()

    conn = srv.conns[0]
    assert conn.received[0] == {"op": "auth", "rid": "a1", "auth": SERVER_CREDS}
    assert conn.received[1]["op"] == "subscribe"
    # subscribe не уходит до authed
    assert conn.sub_received_at >= conn.authed_at
    assert conn.subs == [
        {"channel": CH_TWAP, "filter": {"symbol": "btcusd", "window_seconds": 60}},
        {"channel": CH_SPOT, "filter": {"symbol": "btcusd"}},
    ]
    assert feed.ready and feed.is_fresh() and feed.is_fresh("btcusd")
    assert feed.last_data_ts is not None and time.time() - feed.last_data_ts < 2
    twap, spot = feed.latest(CH_TWAP, "btcusd"), feed.latest(CH_SPOT, "btcusd")
    assert twap.kind == "twap" and spot.kind == "spot"
    assert twap.source == "chainlink" and twap.value > 80000 and twap.raw_value.startswith("84")
    assert any(t.snapshot for t in ticks) and any(not t.snapshot for t in ticks)
    # тики идут по возрастанию времени производителя
    for kind in ("twap", "spot"):
        ts = [t.ts_ms for t in ticks if t.kind == kind]
        assert ts == sorted(ts) and len(set(ts)) == len(ts)

    await shutdown(feed, task)
    names = event_names(tmp_path)
    assert names[:4] == ["connected", "subscribe_sent", "ready", "fresh"]
    assert names[-1] == "stopped"
    rx = [r for r in journal(tmp_path) if r["kind"] == "rx"]
    assert {r["msg"].get("channel") for r in rx if "channel" in r["msg"]} == {CH_TWAP, CH_SPOT}


async def test_secrets_are_never_written_or_logged(srv, tmp_path, caplog):
    caplog.set_level(logging.DEBUG)
    srv.scripts = [normal]
    feed = make_feed(srv.url, tmp_path)
    task = await run_in_background(feed)
    assert await feed.wait_fresh(4)
    await shutdown(feed, task)
    # исходящий auth-кадр нигде не записан
    rows = journal(tmp_path)
    assert not any(r.get("msg", {}).get("op") == "auth" for r in rows if r["kind"] == "rx")
    blob = "".join(p.read_text(encoding="utf-8") for p in Path(tmp_path).glob("*.jsonl*"))
    for s in SECRETS:
        assert s not in blob
        assert s not in caplog.text
    assert repr(CREDS).count("***") == 3


async def test_secrets_not_logged_on_auth_failure(srv, tmp_path, caplog):
    caplog.set_level(logging.DEBUG)
    bad = Credentials(KEY, "wrong-" + SECRET, PASSPHRASE)
    feed = make_feed(srv.url, tmp_path, creds=bad)
    with pytest.raises(FeedAuthError) as e:
        await asyncio.wait_for(feed.run(), 5)
    feed._rec.close()
    blob = "".join(p.read_text(encoding="utf-8") for p in Path(tmp_path).glob("*.jsonl*"))
    for s in SECRETS + ("wrong-" + SECRET,):
        assert s not in blob and s not in caplog.text and s not in str(e.value)


# ------------------------------------------------------------------ правило 5: зависание
async def test_silent_stall_with_working_heartbeat_triggers_reconnect(srv, tmp_path):
    """Сокет жив, pong приходит, а данных нет: это и есть «тихое зависание»."""
    async def stall(conn):
        if await conn.handshake():
            await conn.stream(count=4)
            await conn.idle()      # дальше тишина, но ping-pong работает

    srv.scripts = [stall, normal]
    feed = make_feed(srv.url, tmp_path, heartbeat_sec=0.5, pong_timeout_sec=0.2)
    task = await run_in_background(feed)
    assert await feed.wait_fresh(4)
    assert await until(lambda: srv.connections == 2 and feed.is_fresh(), timeout=8), feed.status()
    await shutdown(feed, task)

    assert feed.stats["stale_events"] >= 1
    assert feed.stats["pong_timeouts"] == 0          # heartbeat был в порядке
    names = event_names(tmp_path)
    assert names.index("fresh") < names.index("watchdog") < names.index("reconnect")
    assert names.count("fresh") >= 2                  # после реконнекта снова свежая
    wd = next(r for r in journal(tmp_path) if r["kind"] == "event" and r["name"] == "watchdog")
    assert "нет новых данных" in wd["reason"]


async def test_pong_timeout_triggers_reconnect_even_if_data_flows(srv, tmp_path):
    async def deaf(conn):
        if await conn.handshake():
            conn.mute_pong = True
            await conn.stream()

    srv.scripts = [deaf, normal]
    feed = make_feed(srv.url, tmp_path, heartbeat_sec=0.4, pong_timeout_sec=0.2)
    task = await run_in_background(feed)
    assert await until(lambda: srv.connections == 2, timeout=8), feed.status()
    await shutdown(feed, task)
    assert feed.stats["pong_timeouts"] >= 1
    assert feed.stats["stale_events"] == 0


async def test_heartbeat_pings_are_sent_and_answered(srv, tmp_path):
    srv.scripts = [normal]
    feed = make_feed(srv.url, tmp_path, heartbeat_sec=0.3, pong_timeout_sec=0.2)
    task = await run_in_background(feed)
    assert await feed.wait_fresh(4)
    assert await until(lambda: sum(1 for m in srv.conns[0].received if m.get("op") == "ping") >= 2)
    await asyncio.sleep(0.2)
    assert srv.connections == 1 and feed.stats["pong_timeouts"] == 0
    await shutdown(feed, task)


async def test_repeated_old_timestamps_are_not_data(srv, tmp_path):
    """Кадры идут, но время тика не растёт: так выглядит залипший источник."""
    async def frozen(conn):
        if await conn.handshake():
            await conn.tick_all()
            while True:
                await conn.tick_all(same_ts=True)
                await asyncio.sleep(0.05)

    srv.scripts = [frozen, normal]
    feed = make_feed(srv.url, tmp_path)
    task = await run_in_background(feed)
    assert await until(lambda: srv.connections == 2, timeout=8), feed.status()
    await shutdown(feed, task)
    assert feed.stats["old_ticks"] > 5 and feed.stats["stale_events"] >= 1


async def test_empty_snapshot_is_not_data(srv, tmp_path):
    async def empty(conn):
        if await conn.handshake(snapshot_points=0):
            await conn.idle()

    srv.scripts = [empty, normal]
    feed = make_feed(srv.url, tmp_path)
    task = await run_in_background(feed)
    assert await until(lambda: feed.ready, timeout=3)
    assert not feed.is_fresh()                         # подписки есть, цены нет
    assert await until(lambda: srv.connections == 2 and feed.is_fresh(), timeout=8)
    await shutdown(feed, task)
    assert "empty_snapshot" in event_names(tmp_path)
    assert feed.stats["stale_events"] >= 1


async def test_server_never_answers_auth_times_out_and_retries(srv, tmp_path):
    async def mute(conn):
        await conn.recv_json()          # auth получен, ответа нет
        await conn.ws.wait_closed()

    srv.scripts = [mute, normal]
    feed = make_feed(srv.url, tmp_path, auth_timeout_sec=0.5)
    task = await run_in_background(feed)
    assert await until(lambda: srv.connections == 2 and feed.is_fresh(), timeout=8), feed.status()
    await shutdown(feed, task)
    wd = next(r for r in journal(tmp_path) if r["kind"] == "event" and r["name"] == "watchdog")
    assert "auth" in wd["reason"]


# ------------------------------------------------------------------ обрывы и реконнект
async def test_abrupt_drop_without_close_frame_reconnects(srv, tmp_path):
    async def drop(conn):
        if await conn.handshake():
            await conn.tick_all()
            await asyncio.sleep(0.2)
            conn.ws.transport.abort()          # обрыв без close-кадра (1006)
            await asyncio.sleep(0.2)

    srv.scripts = [drop, normal]
    feed = make_feed(srv.url, tmp_path)
    task = await run_in_background(feed)
    assert await until(lambda: srv.connections == 2 and feed.is_fresh(), timeout=8), feed.status()
    await shutdown(feed, task)
    closed = [r for r in journal(tmp_path) if r["kind"] == "event" and r["name"] == "closed"]
    assert closed[0]["code"] == 1006
    assert feed.stats["reconnects"] >= 1


async def test_close_4003_reconnects_with_short_delay(srv, tmp_path):
    async def drain(conn):
        if await conn.handshake():
            await conn.tick_all()
            await conn.ws.close(4003, "draining")

    srv.scripts = [drain, normal]
    feed = make_feed(srv.url, tmp_path)
    task = await run_in_background(feed)
    assert await until(lambda: srv.connections == 2 and feed.is_fresh(), timeout=8)
    await shutdown(feed, task)
    rec = next(r for r in journal(tmp_path) if r["kind"] == "event" and r["name"] == "reconnect")
    assert "4003" in rec["reason"] and rec["delay_sec"] <= 0.11


async def test_close_4002_reconnects_with_backoff(srv, tmp_path):
    async def slow_consumer(conn):
        if await conn.handshake():
            await conn.ws.close(4002, "slow consumer")

    srv.scripts = [slow_consumer, normal]
    feed = make_feed(srv.url, tmp_path)
    task = await run_in_background(feed)
    assert await until(lambda: srv.connections == 2 and feed.is_fresh(), timeout=8)
    await shutdown(feed, task)


async def test_backoff_grows_on_flapping_and_resets_after_data(srv, tmp_path):
    async def flap(conn):
        await conn.recv_json()
        await conn.ws.close(1011, "boom")

    srv.scripts = [flap, flap, flap, normal]
    feed = make_feed(srv.url, tmp_path, backoff_sec=(0.1, 0.2, 0.4))
    task = await run_in_background(feed)
    assert await until(lambda: srv.connections == 4 and feed.is_fresh(), timeout=10)
    await shutdown(feed, task)
    delays = [r["delay_sec"] for r in journal(tmp_path)
              if r["kind"] == "event" and r["name"] == "reconnect"]
    # rng=0 -> половина шага: 0.05, 0.1, 0.2
    assert delays[:3] == pytest.approx([0.05, 0.1, 0.2])


async def test_http_429_respects_retry_after(srv, tmp_path):
    srv.reject_next = [(429, {"Retry-After": "1"})]
    feed = make_feed(srv.url, tmp_path)
    t0 = time.monotonic()
    task = await run_in_background(feed)
    assert await feed.wait_fresh(8)
    assert time.monotonic() - t0 >= 0.95
    await shutdown(feed, task)
    assert srv.http_attempts == 2


async def test_unreachable_server_keeps_retrying_and_stays_not_fresh(tmp_path):
    feed = make_feed("ws://127.0.0.1:9/ws", tmp_path, connect_timeout_sec=0.5)
    task = await run_in_background(feed)
    assert await until(lambda: feed.stats["reconnects"] >= 3, timeout=6)
    assert not feed.is_fresh() and not feed.ready
    await shutdown(feed, task)


# ------------------------------------------------------------------ фатальные ошибки
async def test_auth_invalid_is_fatal_and_not_retried(srv, tmp_path):
    srv.scripts = [lambda c: c.handshake(auth_error="auth_invalid")]
    feed = make_feed(srv.url, tmp_path)
    with pytest.raises(FeedAuthError):
        await asyncio.wait_for(feed.run(), 5)
    assert srv.connections == 1
    feed._rec.close()
    assert "fatal" in event_names(tmp_path)


async def test_close_4001_is_fatal_and_not_retried(srv, tmp_path):
    async def reject(conn):
        await conn.recv_json()
        await conn.ws.close(4001, "auth failed")

    srv.scripts = [reject]
    feed = make_feed(srv.url, tmp_path)
    with pytest.raises(FeedAuthError):
        await asyncio.wait_for(feed.run(), 5)
    assert srv.connections == 1


async def test_close_4008_is_fatal(srv, tmp_path):
    async def violate(conn):
        if await conn.handshake():
            await conn.ws.close(4008, "policy violation")

    srv.scripts = [violate]
    feed = make_feed(srv.url, tmp_path)
    with pytest.raises(FeedProtocolError):
        await asyncio.wait_for(feed.run(), 5)
    assert srv.connections == 1


async def test_rejected_subscription_is_fatal(srv, tmp_path):
    srv.scripts = [lambda c: c.handshake(sub_error="bad_filter")]
    feed = make_feed(srv.url, tmp_path)
    with pytest.raises(FeedSubscriptionError, match="bad_filter"):
        await asyncio.wait_for(feed.run(), 5)
    assert srv.connections == 1


async def test_auth_unavailable_is_retried(srv, tmp_path):
    async def unavailable(conn):
        await conn.recv_json()
        await conn.send({"op": "error", "code": "auth_unavailable", "rid": "a1"})
        await conn.ws.wait_closed()

    srv.scripts = [unavailable, normal]
    feed = make_feed(srv.url, tmp_path)
    task = await run_in_background(feed)
    assert await until(lambda: srv.connections == 2 and feed.is_fresh(), timeout=8)
    await shutdown(feed, task)


# ------------------------------------------------------------------ детали потока
async def test_ready_via_data_when_ack_count_differs(srv, tmp_path):
    async def no_acks(conn):
        if await conn.handshake(ack=False):
            await conn.stream()

    srv.scripts = [no_acks]
    feed = make_feed(srv.url, tmp_path)
    task = await run_in_background(feed)
    assert await feed.wait_fresh(4)
    await shutdown(feed, task)
    ready = next(r for r in journal(tmp_path) if r["kind"] == "event" and r["name"] == "ready")
    assert ready["via"] == "data"


async def test_seq_gap_and_dropped_are_counted_and_recorded(srv, tmp_path):
    async def gappy(conn):
        if await conn.handshake():
            await conn.tick_all()
            conn.skip_seq(CH_TWAP, 2)
            await conn.tick(CH_TWAP, "btcusd", dropped=5)
            await conn.stream()

    srv.scripts = [gappy]
    feed = make_feed(srv.url, tmp_path)
    task = await run_in_background(feed)
    assert await until(lambda: feed.stats["seq_gaps"] == 1 and feed.stats["dropped_frames"] == 5)
    await shutdown(feed, task)
    names = event_names(tmp_path)
    assert "seq_gap" in names and "dropped" in names
    gap = next(r for r in journal(tmp_path) if r["kind"] == "event" and r["name"] == "seq_gap")
    assert gap["channel"] == CH_TWAP and gap["got"] - gap["expected"] == 2


async def test_source_change_is_reported(srv, tmp_path):
    async def switch(conn):
        if await conn.handshake():
            await conn.tick_all()
            await conn.tick(CH_SPOT, "btcusd", source="pyth")
            await conn.stream()

    srv.scripts = [switch]
    feed = make_feed(srv.url, tmp_path)
    task = await run_in_background(feed)
    assert await feed.wait_fresh(4)
    await shutdown(feed, task)
    ev = next(r for r in journal(tmp_path) if r["kind"] == "event" and r["name"] == "source_changed")
    assert (ev["old"], ev["new"]) == ("chainlink", "pyth")


async def test_snapshot_after_reconnect_does_not_duplicate_ticks(srv, tmp_path):
    async def short(conn):
        if await conn.handshake(snapshot_points=5):
            await conn.stream(count=3)
            await conn.ws.close(1001, "going away")

    srv.scripts = [short]
    feed = make_feed(srv.url, tmp_path)
    ticks = []
    feed.on_tick(ticks.append)
    task = await run_in_background(feed)
    assert await until(lambda: srv.connections >= 2 and feed.is_fresh(), timeout=8)
    await shutdown(feed, task)
    for kind in ("twap", "spot"):
        ts = [t.ts_ms for t in ticks if t.kind == kind]
        assert len(ts) == len(set(ts)) and ts == sorted(ts)


async def test_callback_error_does_not_kill_feed(srv, tmp_path):
    feed = make_feed(srv.url, tmp_path)

    def boom(tick):
        raise RuntimeError("subscriber bug")

    feed.on_tick(boom)
    task = await run_in_background(feed)
    assert await feed.wait_fresh(4)
    assert feed.stats["callback_errors"] > 0 and not task.done()
    await shutdown(feed, task)


async def test_two_symbols_all_streams_required(srv, tmp_path):
    feed = make_feed(srv.url, tmp_path, symbols=("btcusd", "ethusd"))
    task = await run_in_background(feed)
    assert await feed.wait_fresh(4)
    assert len(srv.conns[0].subs) == 4
    assert feed.is_fresh("btcusd") and feed.is_fresh("ethusd")
    await shutdown(feed, task)


async def test_spot_provider_pin_is_sent(srv, tmp_path):
    feed = make_feed(srv.url, tmp_path, spot_provider="pyth")
    task = await run_in_background(feed)
    assert await feed.wait_fresh(4)
    spot = [s for s in srv.conns[0].subs if s["channel"] == CH_SPOT][0]
    assert spot["filter"] == {"symbol": "btcusd", "provider": "pyth"}
    await shutdown(feed, task)


async def test_spot_can_be_disabled(srv, tmp_path):
    feed = make_feed(srv.url, tmp_path, use_spot=False)
    task = await run_in_background(feed)
    assert await feed.wait_fresh(4)
    assert [s["channel"] for s in srv.conns[0].subs] == [CH_TWAP]
    await shutdown(feed, task)


async def test_stop_is_prompt_and_clean(srv, tmp_path):
    feed = make_feed(srv.url, tmp_path)
    task = await run_in_background(feed)
    assert await feed.wait_fresh(4)
    t0 = time.monotonic()
    await feed.stop()
    await asyncio.wait_for(task, 3)
    assert time.monotonic() - t0 < 2.5 and task.exception() is None
    assert not feed.is_fresh()


async def test_stop_during_backoff_sleep(tmp_path):
    feed = make_feed("ws://127.0.0.1:9/ws", tmp_path, backoff_sec=(30,), connect_timeout_sec=0.3)
    task = await run_in_background(feed)
    assert await until(lambda: feed.stats["reconnects"] >= 1)
    t0 = time.monotonic()
    await feed.stop()
    await asyncio.wait_for(task, 3)
    assert time.monotonic() - t0 < 2


# ------------------------------------------------------------------ is_fresh без сети
class FakeClock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def wall(self):
        return self.t

    def mono(self):
        return self.t


def offline_feed(**kw):
    clk = FakeClock()
    settings = FeedSettings(url="ws://unused", symbols=("btcusd",), stale_after_sec=45.0, **kw)
    f = Feed(settings, CREDS, None, clock=clk.wall, mono=clk.mono)
    f._phase = "live"
    return f, clk


def frame(ch, ts_ms, *, snapshot=False, seq=1, value=84000.0, symbol="btcusd"):
    if snapshot:
        payload = {"symbol": symbol, "source": "chainlink",
                   "data": [{"timestamp": ts_ms, "value": value,
                             "full_accuracy_value": f"{value:.8f}"}]}
    else:
        payload = {"symbol": symbol, "value": value, "full_accuracy_value": f"{value:.8f}",
                   "timestamp": ts_ms, "source": "chainlink"}
    env = {"v": 1, "channel": ch, "seq": seq, "ts": ts_ms, "payload": payload}
    if snapshot:
        env["snapshot"] = True
    return json.dumps(env)


async def test_is_fresh_expires_by_age_and_requires_all_streams():
    f, clk = offline_feed()
    now_ms = int(clk.t * 1000)
    assert not f.is_fresh()
    await f._on_frame(frame(CH_TWAP, now_ms))
    assert not f.is_fresh()                      # спота ещё нет
    await f._on_frame(frame(CH_SPOT, now_ms))
    assert f.is_fresh() and f.last_data_ts == clk.t
    clk.t += 44
    assert f.is_fresh()
    clk.t += 2                                    # 46 с без новых данных
    assert not f.is_fresh()
    await f._on_frame(frame(CH_TWAP, int(clk.t * 1000), seq=2))
    assert not f.is_fresh()                      # спот всё ещё старый
    await f._on_frame(frame(CH_SPOT, int(clk.t * 1000), seq=2))
    assert f.is_fresh()


async def test_is_fresh_false_when_producer_time_is_old_even_if_data_arrives():
    f, clk = offline_feed()
    old_ms = int((clk.t - 100) * 1000)
    await f._on_frame(frame(CH_TWAP, old_ms))
    await f._on_frame(frame(CH_SPOT, old_ms))
    assert f.stats["ticks"] == 2 and not f.is_fresh()   # данные идут, но устаревшие


async def test_is_fresh_false_when_not_live():
    f, clk = offline_feed()
    now_ms = int(clk.t * 1000)
    await f._on_frame(frame(CH_TWAP, now_ms))
    await f._on_frame(frame(CH_SPOT, now_ms))
    assert f.is_fresh()
    f._phase = "closed"
    assert not f.is_fresh()


async def test_duplicate_and_older_ticks_do_not_refresh():
    f, clk = offline_feed()
    now_ms = int(clk.t * 1000)
    await f._on_frame(frame(CH_TWAP, now_ms))
    await f._on_frame(frame(CH_SPOT, now_ms))
    clk.t += 30
    for seq in (2, 3):
        await f._on_frame(frame(CH_TWAP, now_ms, seq=seq))       # то же время
    await f._on_frame(frame(CH_TWAP, now_ms - 5000, seq=4))      # назад во времени
    assert f.stats["old_ticks"] == 3 and f.stats["ticks"] == 2
    clk.t += 20
    assert not f.is_fresh()


async def test_bad_values_are_rejected():
    f, clk = offline_feed()
    now_ms = int(clk.t * 1000)
    for bad in (0.0, -5.0, float("nan"), float("inf")):
        await f._on_frame(frame(CH_TWAP, now_ms, value=bad))
    await f._on_frame("not json at all")
    await f._on_frame('{"channel":"price.crypto","payload":{"symbol":"btcusd","value":"x","timestamp":1}}')
    assert f.stats["ticks"] == 0 and f.stats["bad_values"] >= 4 and f.stats["bad_json"] == 1


async def test_unexpected_symbol_and_unknown_channel_ignored():
    f, clk = offline_feed()
    now_ms = int(clk.t * 1000)
    await f._on_frame(frame(CH_TWAP, now_ms, symbol="solusd"))
    await f._on_frame(json.dumps({"v": 1, "channel": "price.equity", "seq": 1, "ts": now_ms,
                                  "payload": {"symbol": "aapl", "value": 1, "timestamp": now_ms}}))
    assert f.stats["ticks"] == 0
    assert f.stats["unexpected_symbol"] == 1 and f.stats["unknown_frames"] == 1


async def test_snapshot_ticks_seed_state_but_are_flagged():
    f, clk = offline_feed()
    now_ms = int(clk.t * 1000)
    got = []
    f.on_tick(got.append)
    await f._on_frame(frame(CH_TWAP, now_ms, snapshot=True))
    assert got[0].snapshot is True and f.latest(CH_TWAP, "btcusd").value == 84000.0


async def test_status_reports_counters():
    f, clk = offline_feed()
    await f._on_frame(frame(CH_TWAP, int(clk.t * 1000)))
    st = f.status()
    assert st["ready"] is True and st["fresh"] is False and st["ticks"] == 1
    assert st["last_data_age_sec"] == 0.0
