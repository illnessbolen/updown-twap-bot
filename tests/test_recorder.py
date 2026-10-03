import asyncio
import gzip
import json
import os
import time
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

from websockets.asyncio.server import serve

from updown.config import ApiCreds, load_config
from updown.feed import TWAP, PriceTick
from updown.main import feed_stats, run_record, strike_check
from updown.markets import GammaClient
from updown.recorder import JsonlRecorder, iter_records, price_record, stream_files, utc_day

from test_book import FakeClob, clob_streaming
from test_feed import FakePolyBolt, streaming
from test_markets import fixture, shifted

ROOT = Path(__file__).resolve().parent.parent


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_write_and_flush(tmp_path):
    rec = JsonlRecorder(tmp_path)
    ts = 1791017700.5   # 2026-10-03 UTC
    tick = PriceTick(TWAP, "btcusd", ts - 1, Decimal("84608.904251027760000001"), ts, "chainlink", False, 7)
    rec.write("prices", price_record(tick))
    rec.write("feed_events", {"kind": "connected", "recv_ts": ts, "note": "проверка"})
    assert rec.flush() == 2
    rows = read_jsonl(tmp_path / "2026-10-03" / "prices.jsonl")
    assert rows == [{"t": "price", "ch": TWAP, "sym": "btcusd", "ts": ts - 1,
                     "v": "84608.904251027760000001", "src": "chainlink", "snap": False, "seq": 7,
                     "recv_ts": ts}]
    # Decimal пишется строкой без потери точности, кириллица читается
    assert Decimal(rows[0]["v"]) == tick.value
    assert read_jsonl(tmp_path / "2026-10-03" / "feed_events.jsonl")[0]["note"] == "проверка"


def test_append_and_day_rotation(tmp_path):
    rec = JsonlRecorder(tmp_path)
    day1 = 1791071999.0   # 2026-10-03 23:59:59 UTC
    rec.write("prices", {"t": "x", "recv_ts": day1})
    rec.flush()
    rec.write("prices", {"t": "y", "recv_ts": day1})
    rec.write("prices", {"t": "z", "recv_ts": day1 + 2})
    rec.flush()
    assert [r["t"] for r in read_jsonl(tmp_path / "2026-10-03" / "prices.jsonl")] == ["x", "y"]
    assert [r["t"] for r in read_jsonl(tmp_path / "2026-10-04" / "prices.jsonl")] == ["z"]
    assert rec.written == 3


def test_flush_on_stop(tmp_path):
    rec = JsonlRecorder(tmp_path)

    async def main():
        stop = asyncio.Event()
        task = asyncio.create_task(rec.run(stop, flush_every_sec=10))
        rec.write("prices", {"t": "a", "recv_ts": time.time()})
        await asyncio.sleep(0.01)
        stop.set()
        await task
    asyncio.run(main())
    assert rec.written == 1


def test_gzip_stream_flushes_are_readable_while_open(tmp_path):
    rec = JsonlRecorder(tmp_path, compress=True, run_id="r1")
    ts = 1791017700.0
    day = tmp_path / "2026-10-03"
    for i in range(3):
        rec.write("book", {"i": i, "recv_ts": ts})
        rec.flush()
        # файл не закрыт (бот работает), но всё сброшенное уже читается
        assert [r["i"] for r in iter_records(day, "book")] == list(range(i + 1))
    rec.close()
    assert [r["i"] for r in iter_records(day, "book")] == [0, 1, 2]
    assert gzip.decompress((day / "book.r1.jsonl.gz").read_bytes()).count(b"\n") == 3


def test_crash_truncated_tail_keeps_earlier_lines(tmp_path):
    rec = JsonlRecorder(tmp_path, compress=True, run_id="r1")
    ts = 1791017700.0
    rec.write("prices", {"i": 1, "recv_ts": ts})
    rec.flush()
    rec.write("prices", {"i": 2, "pad": os.urandom(400).hex(), "recv_ts": ts})   # несжимаемое
    rec.flush()
    path = tmp_path / "2026-10-03" / "prices.r1.jsonl.gz"
    data = path.read_bytes()
    path.write_bytes(data[:-40])                      # авария посреди записи второго сброса
    assert [r["i"] for r in iter_records(path.parent, "prices")] == [1]


def test_parts_of_several_runs_and_plain_file(tmp_path):
    ts = 1791017700.0
    day = tmp_path / "2026-10-03"
    plain = JsonlRecorder(tmp_path, compress=False)
    plain.write("prices", {"i": "plain", "recv_ts": ts})
    plain.flush()
    for run in ("20261003T100000", "20261003T110000"):
        r = JsonlRecorder(tmp_path, compress=True, run_id=run)
        r.write("prices", {"i": run, "recv_ts": ts})
        r.close()
    assert [r["i"] for r in iter_records(day, "prices")] == ["plain", "20261003T100000", "20261003T110000"]
    assert set(stream_files(day)) == {"prices"}


def test_closed_then_reopened_part_appends_new_member(tmp_path):
    rec = JsonlRecorder(tmp_path, compress=True, run_id="r1")
    rec.IDLE_CLOSE_SEC = 0.0                          # закрывать часть после каждого сброса
    ts = 1791017700.0
    for i in range(3):
        rec.write("trades", {"i": i, "recv_ts": ts})
        rec.flush()
    assert [r["i"] for r in iter_records(tmp_path / "2026-10-03", "trades")] == [0, 1, 2]


def live_gamma(now: float) -> GammaClient:
    """Фейковый Gamma: текущий и следующий 5m-рынок BTC на основе живого ответа."""
    start = now - now % 300
    events = [shifted(fixture("gamma_event_btc_5m_active.json"), start),
              shifted(fixture("gamma_event_btc_5m_active.json"), start + 300)]
    return GammaClient("https://gamma.test", fetch=lambda url, timeout: events)


def test_record_end_to_end(tmp_path):
    """record: фейковые PolyBolt + CLOB + Gamma -> JSONL.gz на диске -> feed-stats."""
    cfg = load_config(ROOT / "config.example.toml")
    creds = ApiCreds("k", "SECRET-e2e", "PASS-e2e")

    async def main():
        pb = FakePolyBolt([streaming(n=5, then=lambda ws: ws.close(4002, "slow consumer")), streaming()])
        clob = FakeClob([clob_streaming()])
        async with serve(pb.handler, "127.0.0.1", 0) as s1, serve(clob.handler, "127.0.0.1", 0) as s2:
            c = replace(cfg,
                        general=replace(cfg.general, log_dir=str(tmp_path), symbols=["btcusd", "ethusd"], windows=[5]),
                        feed=replace(cfg.feed, url=f"ws://127.0.0.1:{s1.sockets[0].getsockname()[1]}",
                                     stale_after_sec=0.5, reconnect_backoff_sec=[0.01]),
                        book=replace(cfg.book, url=f"ws://127.0.0.1:{s2.sockets[0].getsockname()[1]}"),
                        record=replace(cfg.record, book_every_sec=0.1))
            stop = asyncio.Event()
            task = asyncio.create_task(run_record(c, creds, stop=stop, connect_kwargs={"proxy": None},
                                                  gamma=live_gamma(time.time()), status_every_sec=0.3))
            t0 = time.monotonic()
            while (pb.conns < 2 or clob.conns < 1) and time.monotonic() - t0 < 10:
                await asyncio.sleep(0.05)
            await asyncio.sleep(1.2)
            stop.set()
            return await asyncio.wait_for(task, 5)

    assert asyncio.run(main()) == 0
    day = tmp_path / utc_day(time.time())
    prices = list(iter_records(day, "prices"))
    assert {(r["ch"], r["sym"]) for r in prices} == {
        ("price.crypto.twap", "btcusd"), ("price.crypto.twap", "ethusd"),
        ("price.crypto", "btcusd"), ("price.crypto", "ethusd")}
    assert any(r["snap"] for r in prices) and any(not r["snap"] for r in prices)
    events = list(iter_records(day, "feed_events"))
    kinds = [e["kind"] for e in events]
    assert kinds.count("connected") >= 3 and "reconnect_wait" in kinds     # 2 PolyBolt + 1 CLOB

    markets = list(iter_records(day, "markets"))
    assert len(markets) == 2 and all(m["sym"] == "btcusd" and m["dur"] == 5 for m in markets)
    strikes = list(iter_records(day, "strikes"))
    assert strikes and strikes[0]["t"] == "strike"
    book = list(iter_records(day, "book"))
    live = markets[0] if markets[0]["start_ts"] <= time.time() else markets[1]
    assert {r["tok"] for r in book} >= {live["token_up"], live["token_down"]}
    assert all(r["slug"] and r["side"] in ("up", "down") for r in book)
    assert list(iter_records(day, "trades"))

    assert not any(n.endswith(".jsonl") for n in (p.name for p in day.iterdir()))   # всё сжато
    text = "".join(gzip.decompress(p.read_bytes()).decode() for p in day.iterdir())
    assert "SECRET-e2e" not in text and "PASS-e2e" not in text

    summary = feed_stats(day)
    assert "btcusd TWAP" in summary and "рынков найдено: 2" in summary and "стаканы: снимков" in summary
    assert "Размер файлов" in summary


def test_strike_check_on_recorded_prices(tmp_path):
    """strike-check: записанный TWAP + закрытое событие Gamma -> совпадение strike и итога."""
    ev = fixture("gamma_event_btc_5m_closed.json")
    from updown.markets import parse_iso
    start = parse_iso(ev["markets"][0]["eventStartTime"])
    end = parse_iso(ev["markets"][0]["endDate"])
    ptb = Decimal(str(ev["eventMetadata"]["priceToBeat"]))
    fin = Decimal(str(ev["eventMetadata"]["finalPrice"]))
    rec = JsonlRecorder(tmp_path, compress=True)
    for k in range(-10, 311):
        ts = start + k
        v = ptb if ts == start else fin if ts == end else Decimal("84500") + k
        rec.write("prices", {"t": "price", "ch": TWAP, "sym": "btcusd", "ts": ts, "v": v, "snap": False, "recv_ts": ts + 1.5})
    rec.flush()
    cfg = load_config(ROOT / "config.example.toml")
    cfg = replace(cfg, general=replace(cfg.general, log_dir=str(tmp_path)))
    gamma = GammaClient("https://gamma.test", fetch=lambda url, timeout: [ev])
    out = asyncio.run(strike_check(cfg, [utc_day(start)], gamma=gamma, now=end + 3600))
    assert "проверено рынков 1" in out
    assert "strike = принт ровно на начале окна:      1/1" in out
    assert "итог = принт ровно на конце окна:         1/1" in out
    assert "исход Up/Down совпал с нашим расчётом:     1/1" in out
