"""
Запуск и остановка.

Команды (из папки проекта):
  py -m updown check          проверить конфиг, ключи, режим и часы
  py -m updown markets        показать текущие рынки Up/Down из Gamma
  py -m updown record         записывать цены, рынки, strike и стаканы в JSONL (без торговли)
  py -m updown feed-stats     сводка по записанным данным за день
  py -m updown strike-check   сверить наш strike и итог с данными Polymarket

Торговли пока нет. Флаг --live только показывает, что сказало бы правило допуска;
команда record ордеров не отправляет никогда.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import statistics
import sys
import time
import urllib.request
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path

from .book import ClobBookFeed
from .config import Config, ConfigError, find_env_file, live_gate, load_api_creds, load_config, load_env, mask
from .feed import SPOT, TWAP, FeedFatal, PolyBoltFeed
from .markets import (
    GammaClient, MarketParseError, MarketRegistry, StrikeTracker, TwapHistory, check_against_gamma, iso,
    parse_event, series_slug,
)
from .recorder import JsonlRecorder, iter_records, price_record, stream_files, utc_day

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


def _fmt(x: Decimal | None, nd: int = 2) -> str:
    return "-" if x is None else f"{x:.{nd}f}"


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
        print("Режим: LIVE разрешён правилом допуска (ордеров всё равно пока нет)")
    else:
        print("Режим: DRY_RUN, реальных ордеров нет. Причины: " + "; ".join(gate.reasons))
    skew = clock_skew_sec()
    if skew is None:
        print("Часы: не удалось сверить с сервером Polymarket")
    else:
        warn = "  <- синхронизируйте время Windows!" if abs(skew) > 2 else ""
        print(f"Часы: расхождение с сервером {skew:+.2f} с{warn}")
    return 0


# ---------- markets ----------

def cmd_markets(args) -> int:
    cfg = load_config(args.config)

    async def go() -> list:
        reg = MarketRegistry(GammaClient(cfg.markets.gamma_url), cfg.general.symbols, cfg.general.windows,
                             lookahead_sec=cfg.markets.lookahead_min * 60)
        await reg.refresh()
        return sorted(reg.markets.values(), key=lambda m: (m.start_ts, m.symbol, m.duration_min))

    markets = asyncio.run(go())
    now = time.time()
    if not markets:
        print("Рынков не найдено")
        return 1
    for m in markets:
        state = "идёт" if m.is_live(now) else "скоро" if m.start_ts > now else "закончился"
        print(f"{m.slug:28s} {state:10s} {iso(m.start_ts)} - {iso(m.end_ts)}  "
              f"тик {m.tick_size} мин {m.min_order_size}  комиссия {m.fees.rate}  "
              f"Up {m.token_up[:8]}… Down {m.token_down[:8]}…")
    return 0


# ---------- record ----------

async def run_record(cfg: Config, creds, *, stop: asyncio.Event | None = None,
                     connect_kwargs: dict | None = None, gamma: GammaClient | None = None,
                     status_every_sec: float = 30.0) -> int:
    stop = stop or asyncio.Event()
    rec = JsonlRecorder(cfg.general.log_dir, compress=cfg.record.compress)
    history = TwapHistory()

    def on_tick(t):
        rec.write("prices", price_record(t))
        if t.channel == TWAP:
            history.add(t.symbol, t.ts, t.value)

    feed = PolyBoltFeed(
        creds, cfg.general.symbols,
        url=cfg.feed.url,
        spot=cfg.feed.use_spot_for_sigma,
        spot_provider=cfg.feed.spot_provider,
        stale_after_sec=cfg.feed.stale_after_sec,
        backoff_sec=cfg.feed.reconnect_backoff_sec,
        on_tick=on_tick,
        on_event=lambda e: rec.write("feed_events", {"t": "feed", "src": "prices", **e}),
        connect_kwargs=connect_kwargs,
    )
    registry = MarketRegistry(
        gamma or GammaClient(cfg.markets.gamma_url), cfg.general.symbols, cfg.general.windows,
        lookahead_sec=cfg.markets.lookahead_min * 60,
        on_market=lambda m: rec.write("markets", {**m.record(), "recv_ts": time.time()}),
        on_event=lambda e: rec.write("feed_events", {"t": "feed", "src": "markets", **e}),
    )
    strikes = StrikeTracker(history, registry, on_record=lambda r: rec.write("strikes", r),
                            settle_sec=cfg.markets.strike_settle_sec, max_gap_sec=cfg.markets.strike_max_gap_sec)
    books = ClobBookFeed(
        cfg.book.url, ping_every_sec=cfg.book.ping_sec, stale_after_sec=cfg.book.stale_after_sec,
        backoff_sec=cfg.feed.reconnect_backoff_sec,
        on_event=lambda e: rec.write("feed_events", {"t": "feed", **e}),
        on_trade=lambda r: rec.write("trades", r),
        on_bbo=lambda r: rec.write("bbo", r),
        connect_kwargs=connect_kwargs,
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError, ValueError):
            pass   # Windows: Ctrl+C придёт как KeyboardInterrupt, см. cmd_record()

    async def every_second() -> None:
        """Подписки стакана по расписанию рынков, фиксация strike, прореженная запись стакана."""
        sampled: dict[str, tuple[int, float]] = {}
        while not stop.is_set():
            now = time.time()
            books.set_tokens(registry.tokens_to_watch(now, cfg.book.subscribe_before_start_sec,
                                                      cfg.book.keep_after_end_sec))
            strikes.poll(now)
            for tok, b in list(books.books.items()):
                if not b.initialized:
                    continue
                version, last_t = sampled.get(tok, (-1, 0.0))
                if b.version != version and now - last_t >= cfg.record.book_every_sec:
                    m = registry.by_token(tok)
                    r = b.record(cfg.record.book_levels, now)
                    if m:
                        r.update(slug=m.slug, side=m.side_of(tok))
                    rec.write("book", r)
                    sampled[tok] = (b.version, now)
            for tok in [t for t in sampled if t not in books.books]:
                del sampled[tok]
            try:
                await asyncio.wait_for(stop.wait(), timeout=min(1.0, cfg.record.book_every_sec))
            except asyncio.TimeoutError:
                pass

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
                parts.append(f"{name}: нет данных" if st.last_value is None
                             else f"{name}: {st.last_value:.2f} ({now - st.last_price_ts:.0f} с)")
            log.info("цены | %s | свежие: %s | переподключений: %d",
                     " | ".join(parts), {s: ("да" if feed.is_fresh(s) else "НЕТ") for s in feed.symbols},
                     feed.reconnects)
            for m in registry.live(now):
                b = books.book(m.token_up)
                bb = b.best_bid() if b else None
                ba = b.best_ask() if b else None
                log.info("рынок %s | осталось %3.0f с | strike %s | Up bid/ask %s/%s | стакан %s",
                         m.slug, m.secs_left(now), _fmt(strikes.strike(m.slug)),
                         _fmt(bb[0] if bb else None, 3), _fmt(ba[0] if ba else None, 3),
                         "свежий" if books.book_fresh(m.token_up, now) else "НЕ свежий")
            st = books.stats
            log.info("стаканы | токенов %d | book %d, price_change %d, сделок %d, расхождений %d | "
                     "переподключений %d | записано строк %d",
                     len(books.subscribed), st["book"], st["price_change"], st["last_trade_price"],
                     st["bbo_mismatch"], books.reconnects, rec.written)

    tasks = [asyncio.create_task(feed.run(stop), name="prices"),
             asyncio.create_task(registry.run(stop, cfg.markets.refresh_sec), name="markets"),
             asyncio.create_task(books.run(stop), name="books"),
             asyncio.create_task(every_second(), name="scheduler"),
             asyncio.create_task(rec.run(stop), name="recorder"),
             asyncio.create_task(status(), name="status")]
    log.info("запись началась: цены %s, рынки %s мин, данные в %s/<дата>/. Остановка: Ctrl+C",
             ", ".join(cfg.general.symbols), cfg.general.windows, cfg.general.log_dir)
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
        rec.close()     # дописать поздние события и закрыть gzip-потоки
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
    if skew is not None:
        rec_note = f"часы: расхождение с сервером Polymarket {skew:+.2f} с"
        (log.warning if abs(skew) > 2 else log.info)(rec_note + (" - синхронизируйте время" if abs(skew) > 2 else ""))
    try:
        return asyncio.run(run_record(cfg, creds))
    except KeyboardInterrupt:
        return 0


# ---------- feed-stats ----------

def feed_stats(day_dir: Path) -> str:
    if not day_dir.exists():
        return f"нет данных за {day_dir.name} ({day_dir})"
    out = [f"Данные за {day_dir.name} (UTC):"]
    by_stream: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in iter_records(day_dir, "prices"):
        if not r.get("snap"):
            by_stream[(r["ch"], r["sym"])].append(r)
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

    markets = list(iter_records(day_dir, "markets"))
    strikes = [r for r in iter_records(day_dir, "strikes") if r.get("t") == "strike"]
    if markets or strikes:
        no_strike = sum(1 for r in strikes if r.get("chosen") is None)
        exact = sum(1 for r in strikes if r.get("exact") is not None)
        out.append(f"  рынков найдено: {len(markets)}; strike зафиксирован: {len(strikes)} "
                   f"(принт ровно на границе: {exact}, без strike: {no_strike})")
    book_n = Counter(r.get("tok") for r in iter_records(day_dir, "book"))
    trades = sum(1 for _ in iter_records(day_dir, "trades"))
    if book_n or trades:
        out.append(f"  стаканы: снимков {sum(book_n.values())} по {len(book_n)} токенам, сделок {trades}")

    kinds: Counter[str] = Counter()
    for r in iter_records(day_dir, "feed_events"):
        kinds[f"{r.get('src', 'prices')}:{r.get('kind')}"] += 1
    if kinds:
        out.append("События: " + ", ".join(f"{k}={v}" for k, v in sorted(kinds.items())))
    sizes = stream_files(day_dir)
    if sizes:
        out.append("Размер файлов: " + ", ".join(f"{k} {v / 2 ** 20:.1f} МБ" for k, v in sorted(sizes.items()))
                   + f"; всего {sum(sizes.values()) / 2 ** 20:.1f} МБ")
    return "\n".join(out)


def cmd_feed_stats(args) -> int:
    cfg = load_config(args.config)
    day = args.day or utc_day(time.time())
    print(feed_stats(Path(cfg.general.log_dir) / day))
    return 0


# ---------- strike-check ----------

async def strike_check(cfg: Config, days: list[str], gamma: GammaClient | None = None,
                       now: float | None = None) -> str:
    """Наш strike/итог (по записанному TWAP) против eventMetadata закрытых рынков Polymarket."""
    root = Path(cfg.general.log_dir)
    history = TwapHistory(keep_sec=10 ** 9)
    for day in days:
        for r in iter_records(root / day, "prices"):
            if r.get("ch") == TWAP:
                history.add(r["sym"], float(r["ts"]), Decimal(str(r["v"])))
    spans = {s: history.span(s) for s in cfg.general.symbols}
    spans = {s: sp for s, sp in spans.items() if sp}
    if not spans:
        return f"нет записанных TWAP за {', '.join(days)}"
    now = time.time() if now is None else now
    lo = min(sp[0] for sp in spans.values())
    hi = min(max(sp[1] for sp in spans.values()), now - 180)   # Polymarket публикует итог ~через минуту
    if hi <= lo:
        return "записи слишком свежие: подождите пару минут после конца окна"
    gamma = gamma or GammaClient(cfg.markets.gamma_url)
    events = await gamma.list_updown_events(lo, hi, closed=True)
    wanted = {series_slug(s, d): (s, d) for s in cfg.general.symbols for d in cfg.general.windows}

    tally: Counter[str] = Counter()
    diffs: list[float] = []
    bad: list[str] = []
    for ev in events:
        try:
            m = parse_event(ev, wanted)
        except MarketParseError:
            continue
        if m is None or m.symbol not in spans:
            continue
        sp = spans[m.symbol]
        if m.start_ts < sp[0] or m.end_ts > sp[1]:
            continue
        s_c, f_c = history.candidates(m.symbol, m.start_ts), history.candidates(m.symbol, m.end_ts)
        if s_c.exact is None and s_c.before is None:
            tally["нет нашей записи"] += 1
            continue
        r = check_against_gamma(ev, s_c, f_c, cfg.markets.strike_max_gap_sec)
        if r["price_to_beat"] is None:
            tally["Polymarket ещё не опубликовал"] += 1
            continue
        tally["проверено"] += 1
        for key in ("strike_exact", "strike_before", "strike_after", "strike_chosen", "final_exact",
                    "final_chosen", "outcome_match"):
            if r[key] is True:
                tally[key] += 1
            elif r[key] is False:
                tally[key + "_нет"] += 1
        if r["strike_diff"] is not None:
            diffs.append(abs(r["strike_diff"]))
        if r["strike_chosen"] is False and len(bad) < 5:
            bad.append(f"{m.slug}: Polymarket {r['price_to_beat']}, у нас {s_c.chosen(cfg.markets.strike_max_gap_sec)}")

    n = tally["проверено"]
    lines = [f"Сверка strike за {', '.join(days)}: проверено рынков {n}"
             + (f", нет нашей записи {tally['нет нашей записи']}" if tally["нет нашей записи"] else "")
             + (f", Polymarket ещё не опубликовал {tally['Polymarket ещё не опубликовал']}"
                if tally["Polymarket ещё не опубликовал"] else "")]
    if n:
        def share(k: str) -> str:
            yes, no = tally[k], tally[k + "_нет"]
            return f"{yes}/{yes + no}"
        lines += [
            f"  strike = принт ровно на начале окна:      {share('strike_exact')}",
            f"  strike = последний принт до начала:       {share('strike_before')}",
            f"  strike = первый принт после начала:       {share('strike_after')}",
            f"  наше правило (конфиг) совпало:            {share('strike_chosen')}",
            f"  итог = принт ровно на конце окна:         {share('final_exact')}",
            f"  исход Up/Down совпал с нашим расчётом:     {share('outcome_match')}",
            f"  макс. расхождение strike: {max(diffs, default=0):.10f}",
        ]
        lines += [f"  расхождение: {b}" for b in bad]
    return "\n".join(lines)


def cmd_strike_check(args) -> int:
    cfg = load_config(args.config)
    days = args.day or [utc_day(time.time())]
    print(asyncio.run(strike_check(cfg, days)))
    return 0


# ---------- вход ----------

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="py -m updown", description="updown-twap-bot")
    p.add_argument("--config", default="config.toml", help="путь к config.toml")
    p.add_argument("--env-file", default=None, help="путь к .env (по умолчанию ./.env или ~/.updown-bot/.env)")
    p.add_argument("--live", action="store_true", help="запросить live (нужны все условия допуска)")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check", help="проверить конфиг, ключи, режим и часы")
    sub.add_parser("markets", help="показать текущие рынки Up/Down")
    sub.add_parser("record", help="записывать цены, рынки, strike и стаканы (без торговли)")
    fs = sub.add_parser("feed-stats", help="сводка по записанным данным")
    fs.add_argument("--day", help="дата ГГГГ-ММ-ДД (UTC), по умолчанию сегодня")
    sc = sub.add_parser("strike-check", help="сверить strike и итог с Polymarket")
    sc.add_argument("--day", action="append", help="дата ГГГГ-ММ-ДД (UTC); можно несколько раз")
    args = p.parse_args(argv)
    handlers = {"check": cmd_check, "markets": cmd_markets, "record": cmd_record,
                "feed-stats": cmd_feed_stats, "strike-check": cmd_strike_check}
    try:
        return handlers[args.cmd](args)
    except ConfigError as e:
        print(f"Ошибка конфигурации: {e}", file=sys.stderr)
        return 1
