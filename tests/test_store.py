import sqlite3

import pytest

from updown.store import Store


@pytest.fixture
def store():
    return Store(":memory:")


def open_pos(store, market="m1", token="tok-up"):
    return store.open_position(market_id=market, token_id=token, side="up", symbol="btcusd",
                               twap_window=60, strike=84608.9, resolve_ts=1791018000.0,
                               signal_edge=0.05, dry_run=1)


def test_one_open_position_per_market(store):
    open_pos(store)
    with pytest.raises(sqlite3.IntegrityError):
        open_pos(store)
    open_pos(store, market="m2", token="tok2")


def test_duplicate_trade_is_ignored(store):
    pid = open_pos(store)
    store.new_order(pid, "c1", "entry", "buy", 0.5, 10)
    assert store.record_fill("c1", "t1", 10, 0.5, fee=0.175) is True
    assert store.record_fill("c1", "t1", 10, 0.5, fee=0.175) is False   # повтор уведомления
    s = store.open_states()[0]
    assert s["qty_bought"] == 10 and s["fees"] == pytest.approx(0.175)


def test_partial_fills(store):
    pid = open_pos(store)
    store.new_order(pid, "c1", "entry", "buy", 0.5, 10)
    store.record_fill("c1", "t1", 4, 0.50)
    assert store.db.execute("SELECT status FROM orders WHERE client_id='c1'").fetchone()[0] == "partial"
    store.record_fill("c1", "t2", 6, 0.51)
    assert store.db.execute("SELECT status FROM orders WHERE client_id='c1'").fetchone()[0] == "filled"
    s = store.open_states()[0]
    assert s["qty_open"] == 10
    assert s["avg_entry"] == pytest.approx((4 * 0.50 + 6 * 0.51) / 10)


def test_close_only_when_flat(store):
    pid = open_pos(store)
    assert store.close_if_flat(pid) is False            # ничего не куплено
    store.new_order(pid, "c1", "entry", "buy", 0.5, 10)
    store.record_fill("c1", "t1", 10, 0.5)
    store.new_order(pid, "c2", "take_profit", "sell", 0.6, 10)
    store.record_fill("c2", "t2", 7, 0.6)
    assert store.close_if_flat(pid) is False            # остаток 3
    store.record_fill("c2", "t3", 3, 0.6)
    assert store.close_if_flat(pid) is True
    assert store.open_states() == []
    open_pos(store)                                     # после закрытия можно снова


def test_unknown_order_fill_raises(store):
    with pytest.raises(KeyError):
        store.record_fill("nope", "t1", 1, 0.5)


def test_reconcile_reports_all_discrepancy_kinds(store):
    p1 = open_pos(store, "m1", "tokA")
    store.new_order(p1, "a1", "entry", "buy", 0.5, 10)
    store.mark_sent("a1", "ex-a1")
    store.record_fill("a1", "f1", 10, 0.5)
    p2 = open_pos(store, "m2", "tokB")
    store.new_order(p2, "b1", "entry", "buy", 0.5, 5)
    store.record_fill("b1", "f2", 5, 0.5)
    p3 = open_pos(store, "m3", "tokC")
    store.new_order(p3, "c1", "entry", "buy", 0.4, 8)
    store.mark_sent("c1", "ex-c1")                        # активен в БД, на бирже нет

    found = store.reconcile(
        exchange_positions={"tokA": 10, "tokB": 3, "tokZ": 7},
        exchange_open_order_ids=set())
    kinds = sorted(d.kind for d in found)
    assert kinds == ["ghost_order", "qty_mismatch", "untracked_position"]
    # позиция, которой на бирже нет совсем
    found = store.reconcile(exchange_positions={"tokB": 5}, exchange_open_order_ids={"ex-c1"})
    assert [d.kind for d in found] == ["ghost_position"]


def test_reconcile_clean(store):
    p1 = open_pos(store)
    store.new_order(p1, "a1", "entry", "buy", 0.5, 10)
    store.mark_sent("a1", "ex-a1")
    store.record_fill("a1", "f1", 10, 0.5)
    assert store.reconcile({"tok-up": 10}, set()) == []
