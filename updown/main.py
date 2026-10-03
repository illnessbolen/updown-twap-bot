"""
Запуск и остановка.

Команды (из папки проекта):
  py -m updown check        проверить конфиг, ключи, режим и часы
  py -m updown record       записывать цены PolyBolt в JSONL (без торговли)
  py -m updown feed-stats   сводка по записанным ценам и событиям feed

Режим M1: торговли нет. Флаг --live только показывает, что сказало бы правило
допуска; команда record ордеров не отправляет никогда.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import statistics
import sys
import time
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

from .config import ConfigError, find_env_file, live_gate, load_api_creds, load_config, load_env, mask
from .feed import SPOT, TWAP, FeedFatal, PolyBoltFeed
from .recorder import JsonlRecorder, price_record, utc_day

log = logging.getLogger("updown")

CLOB_URL = "https://clob.polymarket.com"


def setup_logging(log_dir: str | None) -> None:
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    root.addHandler(console)
    if log_dir:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(Path(log_dir) / "bot.log", encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)
    logging.getLogger("websockets").setLevel(logging.WARNING)


def clock_skew_sec(timeout: float = 5.0) -> float | None:
    """Локальные часы минус часы сервера CLOB (GET /time). None, если проверить не удалось."""
    req = urllib.request.Request(CLOB_URL + "/time", headers={"User-Agent": "updown-twap-bot/0.1"})
    try:
        t0 = time.time()
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            server = float(resp.read().decode().strip())
        t1 = time.time()
    except (OSError, ValueError):
        return None
    return (t0 + t1) / 2 - server


# ---------- check ----------

def cmd_check(args) -> int:
    cfg = load_config(args.config)
    print(f"Конфиг: {args.config} - OK, профиль {cfg.general.profile}, "
          f"символы {cfg.general.symbols}, окна {cfg.general.windows} мин, TWAP {cfg.feed.twap_window_sec} с")
    env_path = find_env_file(args.env_file)
    env = load_env(env_path)
    print(f".env: {env_path if env_path else 'не найден (берём только переменные окружения)'}")
    try:
        creds = load_api_creds(env)
        print(f"Ключи PolyBolt: есть (apiKey {mask(creds.api_key, 8)})")
    except ConfigError as e:
        print(f"Ключи PolyBolt: НЕТ - {e}")
    gate = live_gate(cfg.general.dry_run, env, args.live)
    if gate.allowed:
        print("Режим: LIVE разрешён правилом допуска (в M1 ордеров всё равно нет)")
    else:
        print("Режим: DRY_RUN, реальных ордеров нет. Причины: " + "; ".join(gate.reasons))
    skew = clock_skew_sec()
    if skew is None:
        print("Часы: не удалось сверить с сервером Polymarket")
    else:
        warn = "  <- синхронизируйте время Windows!" if abs(skew) > 2 else ""
        print(f"Часы: расхождение с сервером {skew:+.2f} с{warn}")
    return 0


# ---------- record ----------

async def run_record(cfg, creds, *, stop: asyncio.Event | None = None,
                     connect_kwargs: dict | None = None, status_every_sec: float = 30.0) -> int:
    stop = stop or asyncio.Event()
    rec = JsonlRecorder(cfg.general.log_dir)
    feed = PolyBoltFeed(
        creds, cfg.general.symbols,
        url=cfg.feed.url,
        spot=cfg.feed.use_spot_for_sigma,
        spot_provider=cfg.feed.spot_provider,
        stale_after_sec=cfg.feed.stale_after_sec,
        backoff_sec=cfg.feed.reconnect_backoff_sec,
        on_tick=lambda t: rec.write("prices", price_record(t)),
        on_event=lambda e: rec.write("feed_events", {"t": "feed", **e}),
        connect_kwargs=connect_kwargs,
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError, ValueError):
            pass   # Windows: Ctrl+C придёт как KeyboardInterrupt, см. main()

    async def status() -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=status_every_sec)
            except asyncio.TimeoutError:
                pass
            if stop.is_set():
                return
            now = time.time()
            parts = []
            for (ch, sym), st in feed.streams.items():
                name = f"{sym} {'TWAP' if ch == TWAP else 'спот'}"
                if st.last_value is None:
                    parts.append(f"{name}: нет данных")
                else:
                    parts.append(f"{name}: {st.last_value:.2f} ({now - st.last_price_ts:.0f} с назад)")
            fresh = {s: ("да" if feed.is_fresh(s) else "НЕТ") for s in feed.symbols}
            log.info("статус | %s | свежие: %s | переподключений: %d | записано строк: %d",
                     " | ".join(parts), fresh, feed.reconnects, rec.written)

    tasks = [asyncio.create_task(feed.run(stop), name="feed"),
             asyncio.create_task(rec.run(stop), name="recorder"),
             asyncio.create_task(status(), name="status")]
    log.info("запись началась: %s, данные в %s/<дата>/. Остановка: Ctrl+C",
             ", ".join(f"{s['channel']} {s['filter']['symbol']}" for s in feed.subscriptions),
             cfg.general.log_dir)
    code = 0
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        for t in done:
            exc = t.exception()
            if isinstance(exc, FeedFatal):
                log.error("остановка: %s", exc)
                code = 2
            elif exc is not None:
                log.error("остановка из-за ошибки в задаче %s: %r", t.get_name(), exc)
                code = 3
    finally:
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        rec.flush()
        log.info("запись остановлена, всего строк: %d", rec.written)
    return code


def cmd_record(args) -> int:
    cfg = load_config(args.config)
    setup_logging(cfg.general.log_dir)
    env = load_env(find_env_file(args.env_file))
    creds = load_api_creds(env)
    gate = live_gate(cfg.general.dry_run, env, args.live)
    log.info("режим: %s; команда record ордеров не отправляет",
             "LIVE разрешён правилом допуска" if gate.allowed else "DRY_RUN")
    skew = clock_skew_sec()
    if skew is not None and abs(skew) > 2:
        log.warning("часы расходятся с сервером Polymarket на %+.1f с - синхронизируйте время", skew)
    try:
        return asyncio.run(run_record(cfg, creds))
    except KeyboardInterrupt:
        return 0


# ---------- feed-stats ----------

def feed_stats(day_dir: Path) -> str:
    prices = day_dir / "prices.jsonl"
    events = day_dir / "feed_events.jsonl"
    if not prices.exists():
        return f"нет файла {prices}"
    by_stream: dict[tuple[str, str], list[dict]] = defaultdict(list)
    with prices.open(encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            if not r.get("snap"):
                by_stream[(r["ch"], r["sym"])].append(r)
    out = [f"Данные за {day_dir.name} (UTC):"]
    for (ch, sym), rows in sorted(by_stream.items()):
        recv = [r["recv_ts"] for r in rows]
        gaps = [b - a for a, b in zip(recv, recv[1:])]
        lags = [r["recv_ts"] - r["ts"] for r in rows]
        srcs = Counter(r.get("src") for r in rows)
        span_h = (recv[-1] - recv[0]) / 3600 if len(recv) > 1 else 0
        out.append(
            f"  {sym} {'TWAP' if ch == TWAP else 'спот' if ch == SPOT else ch}: {len(rows)} обновлений "
            f"за {span_h:.2f} ч, макс. пауза {max(gaps, default=0):.1f} с, "
            f"задержка медиана {statistics.median(lags):.2f} с, источник {dict(srcs)}")
    if events.exists():
        kinds: Counter[str] = Counter()
        with events.open(encoding="utf-8") as f:
            for line in f:
                kinds[json.loads(line).get("kind")] += 1
        out.append("События feed: " + ", ".join(f"{k}={v}" for k, v in sorted(kinds.items())))
    return "\n".join(out)


def cmd_feed_stats(args) -> int:
    cfg = load_config(args.config)
    day = args.day or utc_day(time.time())
    print(feed_stats(Path(cfg.general.log_dir) / day))
    return 0


# ---------- вход ----------

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="py -m updown", description="updown-twap-bot")
    p.add_argument("--config", default="config.toml", help="путь к config.toml")
    p.add_argument("--env-file", default=None, help="путь к .env (по умолчанию ./.env или ~/.updown-bot/.env)")
    p.add_argument("--live", action="store_true", help="запросить live (нужны все условия допуска)")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check", help="проверить конфиг, ключи, режим и часы")
    sub.add_parser("record", help="записывать цены PolyBolt в JSONL (без торговли)")
    fs = sub.add_parser("feed-stats", help="сводка по записанным данным")
    fs.add_argument("--day", help="дата ГГГГ-ММ-ДД (UTC), по умолчанию сегодня")
    args = p.parse_args(argv)
    handlers = {"check": cmd_check, "record": cmd_record, "feed-stats": cmd_feed_stats}
    try:
        return handlers[args.cmd](args)
    except ConfigError as e:
        print(f"Ошибка конфигурации: {e}", file=sys.stderr)
        return 1
