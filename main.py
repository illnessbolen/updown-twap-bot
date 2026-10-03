"""
Запуск бота. На вехе M1 доступны три команды, ни одна не отправляет ордера:

  python main.py check-config   проверить config.toml и наличие ключей в окружении
  python main.py smoke          короткий прогон на живом PolyBolt: ждёт свежую цену и
                                сохраняет сырые кадры, чтобы сверить протокол с документацией
  python main.py record         писать поток TWAP и спота в JSONL (часовые файлы, gzip)

Режим всегда бумажный: брокер на M1 не создаётся. Условия включения live (DRY_RUN=0,
--live, файл LIVE_OK) описаны в config.live_allowed и будут подключены с ClobBroker (M5).
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import time
from pathlib import Path

from config import (ENV_API_KEY, ENV_API_PASSPHRASE, ENV_API_SECRET, Config, ConfigError,
                    Credentials, MissingCredentials, load_config)
from feed import CH_SPOT, CH_TWAP, Feed, FeedFatalError, FeedSettings
from recorder import JsonlRecorder

log = logging.getLogger("main")

EXIT_OK, EXIT_FAIL, EXIT_USAGE = 0, 1, 2


def build_feed(cfg: Config, creds: Credentials, recorder) -> Feed:
    return Feed(FeedSettings.from_config(cfg.feed, cfg.general.symbols), creds, recorder)


async def _status_loop(feed: Feed, every: float) -> None:
    while True:
        await asyncio.sleep(every)
        log.info("статус feed: %s", feed.status())


async def run_record(cfg: Config, creds: Credentials, stop: asyncio.Event | None = None) -> int:
    """Пишет поток в cfg.general.log_dir до сигнала остановки. Возвращает код выхода."""
    stop = stop or asyncio.Event()
    recorder = (JsonlRecorder(cfg.general.log_dir, gzip_closed=cfg.recording.gzip)
                if cfg.recording.enabled else None)
    if recorder is None:
        log.warning("recording.enabled = false: поток НЕ записывается")
    feed = build_feed(cfg, creds, recorder)
    feed_task = asyncio.create_task(feed.run(), name="feed")
    status_task = asyncio.create_task(_status_loop(feed, cfg.recording.status_every_sec))
    stop_task = asyncio.create_task(stop.wait())
    code = EXIT_OK
    try:
        done, _ = await asyncio.wait({feed_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        if feed_task in done:
            exc = feed_task.exception()
            if exc is not None:
                if isinstance(exc, FeedFatalError):
                    log.error("запись остановлена: %s", exc)
                    code = EXIT_FAIL
                else:
                    raise exc
    finally:
        await feed.stop()
        for t in (status_task, stop_task):
            t.cancel()
        await asyncio.gather(status_task, stop_task, return_exceptions=True)
        if not feed_task.done():
            await asyncio.wait_for(feed_task, 10)
        if recorder is not None:
            recorder.close()
    return code


async def run_smoke(cfg: Config, creds: Credentials, seconds: float, out_dir: str | Path) -> int:
    """Ждёт свежую цену до `seconds` секунд, печатает итог. 0 - цена получена."""
    recorder = JsonlRecorder(out_dir, prefix="smoke", gzip_closed=False)
    feed = build_feed(cfg, creds, recorder)
    ticks: dict[str, int] = {}
    feed.on_tick(lambda t: ticks.__setitem__(f"{t.channel}/{t.symbol}",
                                              ticks.get(f"{t.channel}/{t.symbol}", 0) + 1))
    task = asyncio.create_task(feed.run())
    code, error = EXIT_FAIL, None
    try:
        waiter = asyncio.create_task(feed.wait_fresh(seconds))
        done, _ = await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
        if task in done and task.exception() is not None:
            error = task.exception()
        elif waiter in done and waiter.result():
            await asyncio.sleep(min(5.0, seconds))   # немного живых данных после готовности
            code = EXIT_OK
        waiter.cancel()
    finally:
        await feed.stop()
        await asyncio.gather(task, return_exceptions=True)
        recorder.close()

    print("--- smoke ---")
    print(f"свежая цена получена : {'да' if code == EXIT_OK else 'НЕТ'}")
    if error is not None:
        print(f"фатальная ошибка     : {type(error).__name__}: {error}")
    for ch, sym in feed.required:
        t = feed.latest(ch, sym)
        shown = "нет данных" if t is None else f"{t.value} (source={t.source}, ts={t.ts_ms})"
        print(f"{ch:18s} {sym:8s} тиков={ticks.get(f'{ch}/{sym}', 0):4d}  последний: {shown}")
    print(f"счётчики             : {dict(sorted(feed.stats.items()))}")
    print(f"сырые кадры          : {Path(out_dir)}  (приложите файл smoke-*.jsonl к отчёту)")
    return code


# ------------------------------------------------------------------ CLI
def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="main.py", description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    for name, helptext in (("check-config", "проверить конфиг и ключи"),
                           ("smoke", "прогон на живом PolyBolt"),
                           ("record", "писать поток в JSONL")):
        sp = sub.add_parser(name, help=helptext)
        sp.add_argument("--config", default="config.toml", help="путь к config.toml")
        sp.add_argument("-v", "--verbose", action="store_true", help="подробный лог")
    smoke = sub.choices["smoke"]
    smoke.add_argument("--seconds", type=float, default=60.0, help="сколько ждать свежую цену")
    smoke.add_argument("--out", default="data/smoke", help="куда сохранить сырые кадры")
    return p


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s", datefmt="%H:%M:%S")


def _load(args) -> Config | None:
    try:
        return load_config(args.config)
    except ConfigError as e:
        print(f"ошибка конфигурации: {e}", file=sys.stderr)
        return None


def cmd_check_config(args) -> int:
    cfg = _load(args)
    if cfg is None:
        return EXIT_USAGE
    import os
    have = {n: bool(os.environ.get(n)) for n in (ENV_API_KEY, ENV_API_SECRET, ENV_API_PASSPHRASE)}
    print(f"конфиг               : {args.config} - ок")
    print("режим                : PAPER (бумажная торговля, ордера не отправляются)")
    print(f"символы / окна       : {', '.join(cfg.general.symbols)} / {list(cfg.general.windows)} мин, "
          f"TWAP {cfg.feed.twap_window_sec} с")
    print(f"профиль              : {cfg.general.profile}, min_edge={cfg.profile.min_edge}")
    print(f"порог устаревания    : {cfg.feed.stale_after_sec:g} с")
    print(f"каталог данных       : {cfg.general.log_dir}")
    for name, ok in have.items():
        print(f"{name:26s}: {'задана' if ok else 'НЕ ЗАДАНА'}")
    return EXIT_OK if all(have.values()) else EXIT_USAGE


def _config_and_creds(args):
    cfg = _load(args)
    if cfg is None:
        return None, None
    try:
        return cfg, Credentials.from_env()
    except MissingCredentials as e:
        print(f"ошибка: {e}", file=sys.stderr)
        return None, None


def cmd_record(args) -> int:
    cfg, creds = _config_and_creds(args)
    if cfg is None:
        return EXIT_USAGE
    print("режим: PAPER (бумажная торговля), запись потока; ордера не отправляются")

    async def main() -> int:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        return await run_record(cfg, creds, stop)

    return asyncio.run(main())


def cmd_smoke(args) -> int:
    cfg, creds = _config_and_creds(args)
    if cfg is None:
        return EXIT_USAGE
    return asyncio.run(run_smoke(cfg, creds, args.seconds, args.out))


def cli(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    _setup_logging(args.verbose)
    handler = {"check-config": cmd_check_config, "record": cmd_record, "smoke": cmd_smoke}[args.cmd]
    return handler(args)


if __name__ == "__main__":
    sys.exit(cli())
