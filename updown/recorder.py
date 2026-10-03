"""
Запись данных в JSONL для replay и калибровки.

Файлы: <log_dir>/<ГГГГ-ММ-ДД по UTC>/<поток>.jsonl[.gz], одна JSON-запись на строку.
Потоки: prices (тики TWAP и спота), feed_events (события фидов), markets (найденные рынки),
strikes (strike и итог по нашей записи TWAP), book (прореженные снимки стакана),
bbo (лучшие цены от сервера), trades (сделки). Дальше добавятся features и decisions.

При compress=True каждый запуск пишет свои файлы-части <поток>.<время старта>.jsonl.gz.
Внутри один gzip-поток со сбросом (Z_SYNC_FLUSH) раз в секунду: сжатие идёт с общим
контекстом (в ~2.3 раза компактнее отдельных блоков на живых данных), а всё сброшенное
читается даже из незакрытого файла. При аварии теряется только недописанный хвост одной части.
Ошибка записи не глотается: она останавливает бота (нет молчаливых сбоев).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import zlib
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterator

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


class _GzStream:
    """Один gzip-поток в файл; каждый write заканчивается Z_SYNC_FLUSH, поэтому сброшенное сразу читаемо."""

    def __init__(self, path: Path):
        self.f = path.open("ab")
        self.z = zlib.compressobj(6, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
        self.last_write = time.monotonic()

    def write(self, data: bytes) -> None:
        self.f.write(self.z.compress(data) + self.z.flush(zlib.Z_SYNC_FLUSH))
        self.f.flush()
        self.last_write = time.monotonic()

    def close(self) -> None:
        self.f.write(self.z.flush(zlib.Z_FINISH))
        self.f.close()


class JsonlRecorder:
    IDLE_CLOSE_SEC = 300.0     # закрывать части, в которые давно не писали (например, вчерашний день)

    def __init__(self, root: str | Path, *, compress: bool = False, clock: Callable[[], float] = time.time,
                 run_id: str | None = None):
        self.root = Path(root)
        self.compress = compress
        self.clock = clock
        self.run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        self._buf: dict[Path, list[str]] = defaultdict(list)
        self._open: dict[Path, _GzStream] = {}
        self.written = 0
        self.written_by_stream: dict[str, int] = defaultdict(int)

    def write(self, stream: str, record: dict) -> None:
        ts = record.get("recv_ts") or self.clock()
        name = f"{stream}.{self.run_id}.jsonl.gz" if self.compress else f"{stream}.jsonl"
        path = self.root / utc_day(ts) / name
        self._buf[path].append(
            json.dumps(record, default=_default, ensure_ascii=False, separators=(",", ":")))
        self.written_by_stream[stream] += 1

    def flush(self) -> int:
        n = 0
        for path, lines in list(self._buf.items()):
            if not lines:
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            data = ("\n".join(lines) + "\n").encode("utf-8")
            if self.compress:
                stream = self._open.get(path)
                if stream is None:
                    stream = self._open[path] = _GzStream(path)
                stream.write(data)
            else:
                with path.open("ab") as f:
                    f.write(data)
            n += len(lines)
            del self._buf[path]
        now = time.monotonic()
        for path in [p for p, st in self._open.items() if now - st.last_write > self.IDLE_CLOSE_SEC]:
            self._open.pop(path).close()
        self.written += n
        return n

    def close(self) -> None:
        self.flush()
        for stream in self._open.values():
            stream.close()
        self._open.clear()

    async def run(self, stop: asyncio.Event, flush_every_sec: float = 1.0) -> None:
        try:
            while not stop.is_set():
                try:
                    await asyncio.wait_for(stop.wait(), timeout=flush_every_sec)
                except asyncio.TimeoutError:
                    pass
                self.flush()
        finally:
            self.close()


def _read_gzip_lines(path: Path) -> Iterator[str]:
    """
    Читает gzip из одного или нескольких блоков. Незакрытый поток (бот ещё пишет или упал)
    и повреждённый хвост не мешают: возвращаются все целые строки до них.
    """
    data = path.read_bytes()
    out = bytearray()
    pos, chunk = 0, 1 << 16
    while pos < len(data):
        d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        i = pos
        try:
            while i < len(data) and not d.eof:
                out += d.decompress(data[i:i + chunk])
                i += chunk
        except zlib.error:
            log.warning("%s: повреждённые данные после %d байт, дальше не читаем", path, i)
            break
        if not d.eof:
            break     # поток не закрыт: берём то, что уже сброшено
        pos = min(i, len(data)) - len(d.unused_data)
    text = out.decode("utf-8", errors="replace")
    lines = text.split("\n")
    if not text.endswith("\n"):
        lines = lines[:-1]    # недописанная последняя строка
    for line in lines:
        if line:
            yield line


def _stream_paths(day_dir: Path, stream: str) -> list[Path]:
    plain = [p for p in (day_dir / f"{stream}.jsonl", day_dir / f"{stream}.jsonl.gz") if p.exists()]
    parts = sorted(day_dir.glob(f"{stream}.*.jsonl.gz"))
    return plain + [p for p in parts if p not in plain]


def iter_records(day_dir: str | Path, stream: str) -> Iterator[dict]:
    """Все записи потока за день из всех файлов-частей (и старого несжатого .jsonl)."""
    for path in _stream_paths(Path(day_dir), stream):
        if path.suffix == ".gz":
            for line in _read_gzip_lines(path):
                yield json.loads(line)
        else:
            with path.open(encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        yield json.loads(line)


def stream_files(day_dir: str | Path) -> dict[str, int]:
    """Размеры файлов дня по потокам, байты."""
    out: dict[str, int] = defaultdict(int)
    for p in Path(day_dir).glob("*.jsonl*"):
        out[p.name.split(".")[0]] += p.stat().st_size
    return dict(out)
