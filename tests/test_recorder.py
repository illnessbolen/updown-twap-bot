import asyncio
import json
import time
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

from websockets.asyncio.server import serve

from updown.config import ApiCreds, load_config
from updown.feed import TWAP, PriceTick
from updown.main import feed_stats, run_record
from updown.recorder import JsonlRecorder, price_record, utc_day

from test_feed import FakePolyBolt, streaming

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


def test_record_end_to_end_with_fake_polybolt(tmp_path):
    """record: фейковый PolyBolt -> JSONL на диске -> сводка feed-stats."""
    cfg = load_config(ROOT / "config.example.toml")
    creds = ApiCreds("k", "SECRET-e2e", "PASS-e2e")

    async def main():
        srv = FakePolyBolt([streaming(n=5, then=lambda ws: ws.close(4002, "slow consumer")), streaming()])
        async with serve(srv.handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            c = replace(cfg,
                        general=replace(cfg.general, log_dir=str(tmp_path)),
                        feed=replace(cfg.feed, url=f"ws://127.0.0.1:{port}", stale_after_sec=0.5,
                                     reconnect_backoff_sec=[0.01]))
            stop = asyncio.Event()
            task = asyncio.create_task(run_record(c, creds, stop=stop, connect_kwargs={"proxy": None},
                                                  status_every_sec=0.2))
            t0 = time.monotonic()
            while srv.conns < 2 and time.monotonic() - t0 < 10:
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.5)
            stop.set()
            return await asyncio.wait_for(task, 5)

    assert asyncio.run(main()) == 0
    day = tmp_path / utc_day(time.time())
    prices = read_jsonl(day / "prices.jsonl")
    events = read_jsonl(day / "feed_events.jsonl")
    assert {(r["ch"], r["sym"]) for r in prices} == {
        ("price.crypto.twap", "btcusd"), ("price.crypto.twap", "ethusd"),
        ("price.crypto", "btcusd"), ("price.crypto", "ethusd")}
    assert any(r["snap"] for r in prices) and any(not r["snap"] for r in prices)
    kinds = [e["kind"] for e in events]
    assert kinds.count("connected") >= 2 and "reconnect_wait" in kinds
    raw = (day / "feed_events.jsonl").read_text(encoding="utf-8") + (day / "prices.jsonl").read_text(encoding="utf-8")
    assert "SECRET-e2e" not in raw and "PASS-e2e" not in raw

    summary = feed_stats(day)
    assert "btcusd TWAP" in summary and "ethusd спот" in summary
    assert "reconnect_wait=" in summary
