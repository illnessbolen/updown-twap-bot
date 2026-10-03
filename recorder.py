"""
Запись данных в JSONL: файл на каждый час UTC, закрытые часы сжимаются в gzip.

Строка: {"t": <unix-время записи, с>, "kind": "...", ...поля}. Время "t" - локальное
время приёма, оно нужно для replay и калибровки задержек. Сырые кадры потока пишутся
как {"kind": "rx", "msg": <кадр>}; служебные события - {"kind": "event", "name": ...}.

Ошибки записи (диск полон, нет прав) не глотаются: исключение уходит вызывающему,
чтобы запись не молчала.
"""
from __future__ import annotations

import gzip
import json
import logging
import os
import shutil
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, TextIO

log = logging.getLogger("recorder")


def hour_key(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y%m%dT%H")


def gzip_file(path: Path) -> Path:
    """Сжимает path в path.gz (или .1.gz, .2.gz..., если такой уже есть) и удаляет исходный."""
    target = path.with_name(path.name + ".gz")
    n = 0
    while target.exists():
        n += 1
        target = path.with_name(f"{path.name}.{n}.gz")
    tmp = target.with_name(target.name + ".tmp")
    with path.open("rb") as src, gzip.open(tmp, "wb", compresslevel=6) as dst:
        shutil.copyfileobj(src, dst)
    os.replace(tmp, target)
    path.unlink()
    return target


class JsonlRecorder:
    def __init__(self, directory: str | Path, prefix: str = "feed",
                 gzip_closed: bool = True, clock: Callable[[], float] = time.time):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.prefix = prefix
        self.gzip_closed = gzip_closed
        self._clock = clock
        self._fh: TextIO | None = None
        self._key: str | None = None
        self._threads: list[threading.Thread] = []
        self.lines_written = 0
        self.files_compressed = 0
        # после аварийного останова могли остаться несжатые часы
        self._compress_leftovers(current=hour_key(self._clock()))

    # ------------------------------------------------------------ запись
    def path_for(self, key: str) -> Path:
        return self.dir / f"{self.prefix}-{key}.jsonl"

    def write(self, kind: str, **fields) -> None:
        now = self._clock()
        key = hour_key(now)
        if key != self._key:
            self._rotate(key)
        rec = {"t": round(now, 3), "kind": kind, **fields}
        assert self._fh is not None
        self._fh.write(json.dumps(rec, separators=(",", ":"), ensure_ascii=False) + "\n")
        self._fh.flush()
        self.lines_written += 1

    def _rotate(self, new_key: str) -> None:
        old_path = None
        if self._fh is not None:
            old_path = self.path_for(self._key)  # type: ignore[arg-type]
            self._fh.close()
            self._fh = None
        self._key = new_key
        self._fh = self.path_for(new_key).open("a", encoding="utf-8")
        if old_path is not None and self.gzip_closed:
            self._compress_async(old_path)

    # ------------------------------------------------------------ сжатие
    def _compress_async(self, path: Path) -> None:
        def work() -> None:
            try:
                if path.exists():
                    gzip_file(path)
                    self.files_compressed += 1
            except Exception:  # noqa: BLE001 - не теряем причину, но и не роняем запись
                log.exception("не удалось сжать %s", path)

        t = threading.Thread(target=work, name=f"gzip-{path.name}", daemon=False)
        t.start()
        self._threads = [x for x in self._threads if x.is_alive()] + [t]

    def _compress_leftovers(self, current: str) -> None:
        if not self.gzip_closed:
            return
        for p in sorted(self.dir.glob(f"{self.prefix}-*.jsonl")):
            if p == self.path_for(current):
                continue
            try:
                gzip_file(p)
                self.files_compressed += 1
            except Exception:  # noqa: BLE001
                log.exception("не удалось сжать %s", p)

    # ------------------------------------------------------------ закрытие
    def close(self) -> None:
        """Закрывает файл. Текущий час остаётся несжатым и будет сжат при следующем старте."""
        if self._fh is not None:
            self._fh.close()
            self._fh = None
        for t in self._threads:
            t.join(timeout=30)
        self._threads = []


def read_jsonl(path: str | Path):
    """Читает .jsonl или .jsonl.gz, отдаёт словари. Для тестов, replay и отчётов."""
    p = Path(path)
    opener = gzip.open if p.suffix == ".gz" else open
    with opener(p, "rt", encoding="utf-8") as fh:  # type: ignore[operator]
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)
