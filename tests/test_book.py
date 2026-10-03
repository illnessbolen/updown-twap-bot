"""
OrderBook на синтетике и на реальном фрагменте потока CLOB (tests/fixtures, 3 с записи 2026-10-03),
ClobBookFeed на локальном фейковом сервере.
"""
import asyncio
import gzip
import json
import time
from decimal import Decimal as D
from pathlib import Path

import pytest
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from updown.book import ClobBookFeed, OrderBook

FIX = Path(__file__).resolve().parent / "fixtures"


def lv(*pairs):
    return [{"price": p, "size": s} for p, s in pairs]


# ---------- OrderBook ----------

def test_best_prices_do_not_depend_on_level_order():
    b = OrderBook("t")
    # как в живых данных: bids по возрастанию, asks по убыванию (лучшие в конце)
    b.apply_snapshot(lv(("0.01", "100"), ("0.47", "10"), ("0.48", "5")),
                     lv(("0.99", "100"), ("0.52", "7"), ("0.50", "3")), recv_ts=1.0)
    assert b.best_bid() == (D("0.48"), D("5"))
    assert b.best_ask() == (D("0.50"), D("3"))
    assert b.spread() == D("0.02") and b.mid() == D("0.49")
    assert [p for p, _ in b.levels("bid")] == [D("0.48"), D("0.47"), D("0.01")]
    assert [p for p, _ in b.levels("ask", 2)] == [D("0.50"), D("0.52")]


def test_price_change_buy_is_bid_sell_is_ask_and_zero_removes():
    b = OrderBook("t")
    b.apply_snapshot(lv(("0.48", "5")), lv(("0.50", "3")), recv_ts=1.0)
    assert b.apply_change("BUY", "0.49", "4", recv_ts=2.0, best_bid="0.49", best_ask="0.50")
    assert b.best_bid() == (D("0.49"), D("4"))
    assert b.apply_change("SELL", "0.50", "0", recv_ts=3.0, best_bid="0.49", best_ask=None)
    assert b.best_ask() is None
    assert b.consistent and b.version == 3


def test_mismatch_trims_and_next_matching_change_restores():
    b = OrderBook("t")
    b.apply_snapshot(lv(("0.08", "10"), ("0.09", "5")), lv(("0.10", "3")), recv_ts=1.0)
    # уровень 0.09 съеден сделкой без отдельного price_change; сервер говорит best_bid = 0.08
    ok = b.apply_change("BUY", "0.07", "1", recv_ts=2.0, best_bid="0.08", best_ask="0.10")
    assert ok is False and not b.consistent and b.mismatches == 1
    assert b.best_bid() == (D("0.08"), D("10"))          # 0.09 убран
    assert b.apply_change("BUY", "0.08", "38.63", recv_ts=2.001, best_bid="0.08", best_ask="0.10")
    assert b.consistent


def test_snapshot_restores_consistency():
    b = OrderBook("t")
    b.apply_snapshot(lv(("0.08", "10")), lv(("0.10", "3")), recv_ts=1.0)
    b.apply_change("SELL", "0.11", "1", recv_ts=2.0, best_bid="0.08", best_ask="0.09")   # у нас нет 0.09
    assert not b.consistent
    b.apply_snapshot(lv(("0.08", "10")), lv(("0.09", "2"), ("0.10", "3")), recv_ts=2.002)
    assert b.consistent


def test_empty_side_sentinels_from_server():
    # живые данные: пустую сторону сервер пишет как best_bid "0" и best_ask "1"
    b = OrderBook("t")
    b.apply_snapshot(lv(("0.98", "100")), lv(("0.99", "5")), recv_ts=1.0)
    assert b.apply_change("SELL", "0.99", "0", recv_ts=2.0, best_bid="0.98", best_ask="1")
    assert b.best_ask() is None and b.consistent
    b2 = OrderBook("t2")
    b2.apply_snapshot(lv(("0.01", "5")), lv(("0.02", "100")), recv_ts=1.0)
    assert b2.apply_change("BUY", "0.01", "0", recv_ts=2.0, best_bid="0", best_ask="0.02")
    assert b2.best_bid() is None and b2.consistent


def test_bad_price_change_raises():
    b = OrderBook("t")
    b.apply_snapshot([], [], recv_ts=1.0)
    for args in (("HOLD", "0.5", "1"), ("BUY", "abc", "1"), ("BUY", "0.5", "-1")):
        with pytest.raises(ValueError):
            b.apply_change(*args, recv_ts=2.0)


def test_depth_and_walk():
    b = OrderBook("t")
    b.apply_snapshot(lv(("0.48", "10"), ("0.47", "20")), lv(("0.50", "10"), ("0.51", "20"), ("0.60", "100")), recv_ts=1)
    assert b.depth_usd("ask", 2) == D("0.50") * 10 + D("0.51") * 20
    filled, notional = b.walk("ask", D(25))
    assert filled == 25 and notional == D("0.50") * 10 + D("0.51") * 15
    filled, notional = b.walk("ask", D(50), limit_price=D("0.51"))   # дороже 0.51 не покупаем
    assert filled == 30
    filled, notional = b.walk("bid", D(15))
    assert filled == 15 and notional == D("0.48") * 10 + D("0.47") * 5
    filled, _ = b.walk("bid", D(1000))
    assert filled == 30                                             # больше, чем есть в стакане, не исполнить


def test_crossed_and_record():
    b = OrderBook("tok")
    b.apply_snapshot(lv(("0.55", "1")), lv(("0.50", "1")), recv_ts=1.0, server_ts=0.9)
    assert b.is_crossed()
    r = b.record(5, recv_ts=1.5)
    assert r["tok"] == "tok" and r["bb"] == D("0.55") and r["ba"] == D("0.50") and r["server_ts"] == 0.9
    assert r["bids"] == [[D("0.55"), D("1")]]


def test_replay_real_capture_matches_server_bbo():
    """Реальный поток CLOB: после каждого price_change наш лучший bid/ask совпадает с серверным."""
    books: dict[str, OrderBook] = {}
    checked = mismatched = 0
    with gzip.open(FIX / "clob_market_ws_capture.jsonl.gz", "rt", encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            if r["raw"] == "PONG":
                continue
            msg = json.loads(r["raw"])
            for ev in (msg if isinstance(msg, list) else [msg]):
                if ev.get("event_type") == "book":
                    books.setdefault(ev["asset_id"], OrderBook(ev["asset_id"])).apply_snapshot(
                        ev["bids"], ev["asks"], recv_ts=r["recv_ts"])
                elif ev.get("event_type") == "price_change":
                    for pc in ev["price_changes"]:
                        b = books.get(pc["asset_id"])
                        if not b:
                            continue
                        ok = b.apply_change(pc["side"], pc["price"], pc["size"], recv_ts=r["recv_ts"],
                                            best_bid=pc.get("best_bid"), best_ask=pc.get("best_ask"))
                        checked += 1
                        mismatched += not ok
    assert len(books) == 2 and checked > 1000
    assert mismatched / checked < 0.01
    assert all(b.consistent for b in books.values())      # к концу записи всё сошлось


# ---------- фейковый сервер CLOB ----------

def book_event(tok, bid="0.48", ask="0.50"):
    return {"event_type": "book", "asset_id": tok, "market": "0xm", "timestamp": str(int(time.time() * 1000)),
            "bids": lv(("0.01", "100"), (bid, "10")), "asks": lv(("0.99", "100"), (ask, "10")), "tick_size": "0.01"}


class FakeClob:
    def __init__(self, scenarios):
        self.scenarios = scenarios
        self.conns = 0
        self.frames: list[list] = []
        self.pings = 0

    async def handler(self, ws):
        idx = self.conns
        self.conns += 1
        self.frames.append([])
        try:
            await self.scenarios[min(idx, len(self.scenarios) - 1)](self, ws, idx)
        except ConnectionClosed:
            pass

    async def recv(self, ws, idx):
        raw = await ws.recv()
        if raw == "PING":
            self.pings += 1
            await ws.send("PONG")
            return None
        msg = json.loads(raw)
        self.frames[idx].append(msg)
        return msg


def clob_streaming(send_books=True, interval=0.02):
    async def scenario(srv, ws, idx):
        sub = await srv.recv(ws, idx)
        tokens = set(sub["assets_ids"])
        if send_books:
            await ws.send(json.dumps([book_event(t) for t in sorted(tokens)]))

        async def reader():
            while True:
                msg = await srv.recv(ws, idx)
                if msg and msg.get("operation") == "subscribe":
                    tokens.update(msg["assets_ids"])
                    await ws.send(json.dumps([book_event(t) for t in msg["assets_ids"]]))
                elif msg and msg.get("operation") == "unsubscribe":
                    tokens.difference_update(msg["assets_ids"])
        task = asyncio.create_task(reader())
        try:
            k = 0
            while True:
                k += 1
                for t in sorted(tokens):
                    await ws.send(json.dumps({"event_type": "price_change", "market": "0xm",
                                              "timestamp": str(int(time.time() * 1000)),
                                              "price_changes": [{"asset_id": t, "price": "0.47", "size": str(k),
                                                                 "side": "BUY", "best_bid": "0.48", "best_ask": "0.50"}]}))
                    await ws.send(json.dumps({"event_type": "last_trade_price", "asset_id": t, "market": "0xm",
                                              "price": "0.50", "size": "3", "side": "BUY", "fee_rate_bps": "0",
                                              "timestamp": str(int(time.time() * 1000))}))
                await asyncio.sleep(interval)
        finally:
            task.cancel()
    return scenario


def clob_silent():
    async def scenario(srv, ws, idx):
        await srv.recv(ws, idx)
        while True:
            await srv.recv(ws, idx)      # отвечаем на PING, но данных не шлём
    return scenario


def run_clob(scenarios, until, tokens=("A", "B"), timeout=6.0, change_tokens=None, **kw):
    async def main():
        srv = FakeClob(scenarios)
        async with serve(srv.handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            events, trades = [], []
            feed = ClobBookFeed(f"ws://127.0.0.1:{port}", backoff_sec=[0.01], on_event=events.append,
                                on_trade=trades.append, connect_kwargs={"proxy": None}, **kw)
            feed.set_tokens(tokens)
            stop = asyncio.Event()
            task = asyncio.create_task(feed.run(stop))
            t0 = time.monotonic()
            changed = False
            try:
                while not until(feed, srv, events, trades):
                    if change_tokens and not changed and feed.book_fresh(tokens[0]):
                        feed.set_tokens(change_tokens)
                        changed = True
                    if time.monotonic() - t0 > timeout:
                        raise AssertionError(f"таймаут; события {[e['kind'] for e in events]}")
                    await asyncio.sleep(0.01)
            finally:
                stop.set()
            await asyncio.wait_for(task, 3)
            return feed, srv, events, trades
    return asyncio.run(main())


def test_subscribe_snapshot_changes_trades_and_ping():
    feed, srv, events, trades = run_clob(
        [clob_streaming()], lambda f, s, e, t: s.pings >= 2 and len(t) >= 4 and f.book_fresh("A"),
        ping_every_sec=0.05, stale_after_sec=1.0)
    sub = srv.frames[0][0]
    assert sub == {"assets_ids": ["A", "B"], "type": "market", "custom_feature_enabled": True}
    b = feed.book("A")
    assert b.best_bid()[0] == D("0.48") and b.best_ask()[0] == D("0.50")
    assert D("0.47") in b.bids                      # price_change BUY применён к bids
    assert trades[0]["t"] == "trade" and trades[0]["price"] == D("0.50")
    assert not feed.connected and not feed.book_fresh("A")   # после остановки


def test_dynamic_subscribe_and_unsubscribe():
    feed, srv, events, trades = run_clob(
        [clob_streaming()],
        lambda f, s, e, t: f.book_fresh("C") and "B" not in f.books,
        tokens=("A", "B"), change_tokens=("A", "C"), ping_every_sec=0.5, stale_after_sec=2.0)
    ops = [m for m in srv.frames[0] if "operation" in m]
    assert {"assets_ids": ["C"], "operation": "subscribe"} in ops
    assert {"assets_ids": ["B"], "operation": "unsubscribe"} in ops
    assert srv.conns == 1                            # без переподключения


def test_silence_triggers_reconnect():
    feed, srv, events, trades = run_clob(
        [clob_silent(), clob_streaming()], lambda f, s, e, t: s.conns >= 2 and f.book_fresh("A"),
        ping_every_sec=0.05, stale_after_sec=0.3)
    kinds = [e["kind"] for e in events]
    assert "stale" in kinds and "reconnect_wait" in kinds


def test_missing_snapshot_triggers_reconnect():
    feed, srv, events, trades = run_clob(
        [clob_streaming(send_books=False), clob_streaming()],
        lambda f, s, e, t: s.conns >= 2 and f.book_fresh("A"),
        ping_every_sec=0.05, stale_after_sec=1.0, snapshot_timeout_sec=0.2)
    wait = next(e for e in events if e["kind"] == "reconnect_wait")
    assert "снимка" in wait["reason"]


def test_no_tokens_no_connection():
    async def main():
        srv = FakeClob([clob_streaming()])
        async with serve(srv.handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            feed = ClobBookFeed(f"ws://127.0.0.1:{port}", connect_kwargs={"proxy": None})
            stop = asyncio.Event()
            task = asyncio.create_task(feed.run(stop))
            await asyncio.sleep(0.2)
            conns_before = srv.conns
            feed.set_tokens(["A"])
            t0 = time.monotonic()
            while not feed.book_fresh("A") and time.monotonic() - t0 < 3:
                await asyncio.sleep(0.01)
            stop.set()
            await asyncio.wait_for(task, 3)
            return conns_before, feed
    conns_before, feed = asyncio.run(main())
    assert conns_before == 0
