"""
Запись данных в JSONL для replay и калибровки.

Файлы: <log_dir>/<ГГГГ-ММ-ДД по UTC>/<поток>.jsonl, одна JSON-запись на строку.
Потоки M1: prices (тики TWAP и спота), feed_events (подключения, зависания,
переподключения, ошибки протокола). Дальше добавятся book, features, decisions.

Запись буферизуется и сбрасывается на диск раз в flush_every_sec. Ошибка записи
не глотается: она останавливает бота (нет молчаливых сбоев).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from .feed import PriceTick

log = logging.getLogger(__name__)


def _default(o: Any) -> Any:
    if isinstance(o, Decimal):
        return str(o)   # точное значение, без потерь float
    raise TypeError(f"не сериализуется в JSON: {type(o).__name__}")


def utc_day(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def price_record(t: PriceTick) -> dict:
    return {"t": "price", "ch": t.channel, "sym": t.symbol, "ts": t.ts, "v": t.value,
            "src": t.source, "snap": t.snapshot, "seq": t.seq, "recv_ts": t.recv_ts}


class JsonlRecorder:
    def __init__(self, root: str | Path, clock: Callable[[], float] = time.time):
        self.root = Path(root)
        self.clock = clock
        self._buf: dict[Path, list[str]] = defaultdict(list)
        self.written = 0

    def write(self, stream: str, record: dict) -> None:
        ts = record.get("recv_ts") or self.clock()
        path = self.root / utc_day(ts) / f"{stream}.jsonl"
        self._buf[path].append(
            json.dumps(record, default=_default, ensure_ascii=False, separators=(",", ":")))

    def flush(self) -> int:
        n = 0
        for path, lines in list(self._buf.items()):
            if not lines:
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8", newline="\n") as f:
                f.write("\n".join(lines) + "\n")
            n += len(lines)
            del self._buf[path]
        self.written += n
        return n

    async def run(self, stop: asyncio.Event, flush_every_sec: float = 1.0) -> None:
        try:
            while not stop.is_set():
                try:
                    await asyncio.wait_for(stop.wait(), timeout=flush_every_sec)
                except asyncio.TimeoutError:
                    pass
                self.flush()
        finally:
            self.flush()
