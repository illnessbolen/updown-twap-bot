"""
Хранилище позиций с учётом частичных исполнений.

Идея: позиция не хранит "исполнено/нет", а выводится из журнала
ордеров и сделок (fills). Остаток = куплено - продано.
Так частичные исполнения, отмены и повторные уведомления биржи
не ломают состояние, а после рестарта его можно сверить с биржей.
"""
from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS positions (
    id            INTEGER PRIMARY KEY,
    market_id     TEXT NOT NULL,
    token_id      TEXT NOT NULL,              -- токен Up или Down
    side          TEXT NOT NULL CHECK (side IN ('up','down')),
    symbol        TEXT NOT NULL,              -- например 'btcusd' (символ PolyBolt)
    twap_window   INTEGER NOT NULL,           -- окно TWAP, сек (сейчас всегда 60)
    strike        REAL NOT NULL,              -- цена в начале интервала
    resolve_ts    REAL NOT NULL,              -- unix time расчёта
    signal_edge   REAL,                       -- edge в момент входа (для анализа)
    status        TEXT NOT NULL DEFAULT 'open'
                  CHECK (status IN ('open','closing','closed')),
    dry_run       INTEGER NOT NULL DEFAULT 1,
    opened_at     REAL NOT NULL,
    closed_at     REAL
);
-- не больше одной незакрытой позиции на рынок
CREATE UNIQUE INDEX IF NOT EXISTS ux_one_open_per_market
    ON positions(market_id) WHERE status != 'closed';

CREATE TABLE IF NOT EXISTS orders (
    id            INTEGER PRIMARY KEY,
    position_id   INTEGER NOT NULL REFERENCES positions(id),
    client_id     TEXT NOT NULL UNIQUE,       -- наш id, создаётся ДО отправки
    exchange_id   TEXT,                       -- id биржи, появляется после отправки
    kind          TEXT NOT NULL CHECK (kind IN ('entry','take_profit','stop','time_exit')),
    side          TEXT NOT NULL CHECK (side IN ('buy','sell')),
    price         REAL NOT NULL,
    qty           REAL NOT NULL,
    status        TEXT NOT NULL DEFAULT 'new'
                  CHECK (status IN ('new','sent','partial','filled','canceled','rejected')),
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS fills (
    id            INTEGER PRIMARY KEY,
    order_id      INTEGER NOT NULL REFERENCES orders(id),
    trade_id      TEXT NOT NULL UNIQUE,       -- id сделки на бирже: защита от дублей
    qty           REAL NOT NULL,
    price         REAL NOT NULL,
    fee           REAL NOT NULL DEFAULT 0,
    ts            REAL NOT NULL
);

-- Состояние позиции, вычисленное из сделок
CREATE VIEW IF NOT EXISTS position_state AS
SELECT
    p.id AS position_id,
    p.market_id, p.token_id, p.side, p.status, p.resolve_ts,
    COALESCE(SUM(CASE WHEN o.side='buy'  THEN f.qty END), 0)            AS qty_bought,
    COALESCE(SUM(CASE WHEN o.side='sell' THEN f.qty END), 0)            AS qty_sold,
    COALESCE(SUM(CASE WHEN o.side='buy'  THEN f.qty END), 0)
      - COALESCE(SUM(CASE WHEN o.side='sell' THEN f.qty END), 0)        AS qty_open,
    CASE WHEN SUM(CASE WHEN o.side='buy' THEN f.qty END) > 0
         THEN SUM(CASE WHEN o.side='buy' THEN f.qty*f.price END)
              / SUM(CASE WHEN o.side='buy' THEN f.qty END) END          AS avg_entry,
    COALESCE(SUM(f.fee), 0)                                             AS fees
FROM positions p
LEFT JOIN orders o ON o.position_id = p.id
LEFT JOIN fills  f ON f.order_id = o.id
GROUP BY p.id;
"""


@dataclass
class Discrepancy:
    kind: str     # 'ghost_position', 'untracked_position', 'qty_mismatch', 'ghost_order'
    detail: str


class Store:
    def __init__(self, path: str = "bot.db"):
        self.db = sqlite3.connect(path, isolation_level=None)  # autocommit; транзакции явно
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)

    # ---------- запись ----------
    def open_position(self, **kw) -> int:
        cols = ("market_id token_id side symbol twap_window strike resolve_ts "
                "signal_edge dry_run").split()
        vals = [kw.get(c) for c in cols]
        cur = self.db.execute(
            f"INSERT INTO positions ({','.join(cols)}, opened_at) "
            f"VALUES ({','.join('?' * len(cols))}, ?)",
            (*vals, time.time()),
        )  # упадёт на UNIQUE-индексе, если на рынке уже есть позиция
        return cur.lastrowid

    def new_order(self, position_id: int, client_id: str, kind: str,
                  side: str, price: float, qty: float) -> int:
        """Ордер записывается ДО отправки: после краша мы знаем, что мог уйти на биржу."""
        now = time.time()
        cur = self.db.execute(
            "INSERT INTO orders (position_id, client_id, kind, side, price, qty, "
            "created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
            (position_id, client_id, kind, side, price, qty, now, now),
        )
        return cur.lastrowid

    def mark_sent(self, client_id: str, exchange_id: str) -> None:
        self.db.execute(
            "UPDATE orders SET exchange_id=?, status='sent', updated_at=? WHERE client_id=?",
            (exchange_id, time.time(), client_id),
        )

    def record_fill(self, order_client_id: str, trade_id: str,
                    qty: float, price: float, fee: float = 0.0) -> bool:
        """Идемпотентно: повторное уведомление о той же сделке игнорируется."""
        o = self.db.execute(
            "SELECT id, qty FROM orders WHERE client_id=?", (order_client_id,)
        ).fetchone()
        if o is None:
            raise KeyError(f"unknown order {order_client_id}")
        try:
            self.db.execute("BEGIN")
            self.db.execute(
                "INSERT INTO fills (order_id, trade_id, qty, price, fee, ts) "
                "VALUES (?,?,?,?,?,?)",
                (o["id"], trade_id, qty, price, fee, time.time()),
            )
            filled = self.db.execute(
                "SELECT SUM(qty) s FROM fills WHERE order_id=?", (o["id"],)
            ).fetchone()["s"]
            status = "filled" if filled >= o["qty"] - 1e-9 else "partial"
            self.db.execute(
                "UPDATE orders SET status=?, updated_at=? WHERE id=?",
                (status, time.time(), o["id"]),
            )
            self.db.execute("COMMIT")
            return True
        except sqlite3.IntegrityError:
            self.db.execute("ROLLBACK")
            return False  # дубликат trade_id

    def close_if_flat(self, position_id: int) -> bool:
        """Позиция закрыта, только если остаток 0 и купленное > 0."""
        s = self.db.execute(
            "SELECT qty_bought, qty_open FROM position_state WHERE position_id=?",
            (position_id,),
        ).fetchone()
        if s and s["qty_bought"] > 0 and abs(s["qty_open"]) < 1e-9:
            self.db.execute(
                "UPDATE positions SET status='closed', closed_at=? WHERE id=?",
                (time.time(), position_id),
            )
            return True
        return False

    # ---------- чтение ----------
    def open_states(self) -> list[sqlite3.Row]:
        return self.db.execute(
            "SELECT * FROM position_state WHERE status != 'closed'"
        ).fetchall()

    def working_orders(self) -> list[sqlite3.Row]:
        return self.db.execute(
            "SELECT * FROM orders WHERE status IN ('new','sent','partial')"
        ).fetchall()

    # ---------- сверка с биржей при старте ----------
    def reconcile(self, exchange_positions: dict[str, float],
                  exchange_open_order_ids: set[str],
                  tol: float = 1e-6) -> list[Discrepancy]:
        """
        exchange_positions: {token_id: qty} по данным биржи
        exchange_open_order_ids: id активных ордеров на бирже
        Возвращает расхождения. Автоматически ничего не чинит:
        решение (досинхронизировать, отменить, закрыть) принимает вызывающий код.
        """
        out: list[Discrepancy] = []
        local = {r["token_id"]: r for r in self.open_states()}

        for token, row in local.items():
            ex_qty = exchange_positions.get(token, 0.0)
            if abs(ex_qty - row["qty_open"]) > tol:
                kind = "ghost_position" if ex_qty == 0 else "qty_mismatch"
                out.append(Discrepancy(
                    kind, f"{token}: db={row['qty_open']}, exchange={ex_qty}"))
        for token, qty in exchange_positions.items():
            if qty > tol and token not in local:
                out.append(Discrepancy("untracked_position", f"{token}: exchange={qty}"))

        for o in self.working_orders():
            if o["exchange_id"] and o["exchange_id"] not in exchange_open_order_ids:
                out.append(Discrepancy(
                    "ghost_order",
                    f"{o['client_id']} ({o['exchange_id']}) в БД активен, на бирже нет"))
        return out
