"""
Загрузка config.toml, переменных окружения и .env; правило допуска к live.

Секреты только из окружения (или .env, который не коммитится). В repr и логах
ключи маскируются.
"""
from __future__ import annotations

import os
import re
import tomllib
import typing
from dataclasses import MISSING, dataclass, field, fields
from pathlib import Path
from typing import Any, Mapping

SUPPORTED_WINDOWS = {5, 15}
SYMBOL_RE = re.compile(r"^[a-z0-9]+usd$")
PROFILE_NAMES = ("conservative", "moderate", "aggressive")


class ConfigError(ValueError):
    pass


# ---------- секции config.toml ----------

@dataclass(frozen=True)
class General:
    dry_run: bool
    profile: str
    symbols: list[str]
    windows: list[int]
    log_dir: str
    db_path: str

    @property
    def data_dir(self) -> Path:
        """Папка данных; '~' раскрывается в домашнюю папку пользователя."""
        return Path(self.log_dir).expanduser()

    def validate(self) -> None:
        if not self.symbols:
            raise ConfigError("general.symbols пуст")
        for s in self.symbols:
            if not SYMBOL_RE.match(s):
                raise ConfigError(
                    f"general.symbols: '{s}' - нужен формат PolyBolt, например 'btcusd' (не 'btc/usd')")
        bad = set(self.windows) - SUPPORTED_WINDOWS
        if not self.windows or bad:
            raise ConfigError(f"general.windows: допустимы только {sorted(SUPPORTED_WINDOWS)}")


@dataclass(frozen=True)
class Feed:
    url: str
    twap_window_sec: int
    stale_after_sec: float
    reconnect_backoff_sec: list[float]
    use_spot_for_sigma: bool
    spot_provider: str

    def validate(self) -> None:
        if not self.url.startswith(("wss://", "ws://")):
            raise ConfigError("feed.url должен начинаться с wss://")
        if self.twap_window_sec != 60:
            raise ConfigError("feed.twap_window_sec: PolyBolt отдаёт только окно 60 с")
        if self.stale_after_sec <= 0:
            raise ConfigError("feed.stale_after_sec должен быть > 0")
        if not self.reconnect_backoff_sec or any(x <= 0 for x in self.reconnect_backoff_sec):
            raise ConfigError("feed.reconnect_backoff_sec: нужен непустой список положительных чисел")
        if self.spot_provider not in ("", "chainlink", "pyth"):
            raise ConfigError("feed.spot_provider: '', 'chainlink' или 'pyth'")


@dataclass(frozen=True)
class Markets:
    gamma_url: str
    refresh_sec: float
    lookahead_min: float
    strike_settle_sec: float
    strike_max_gap_sec: float

    def validate(self) -> None:
        if not self.gamma_url.startswith("https://"):
            raise ConfigError("markets.gamma_url должен начинаться с https://")
        if self.refresh_sec < 5:
            raise ConfigError("markets.refresh_sec должен быть >= 5 (лимиты Gamma)")
        if self.lookahead_min <= 0:
            raise ConfigError("markets.lookahead_min должен быть > 0")
        if not 0 <= self.strike_settle_sec <= 30 or not 0 <= self.strike_max_gap_sec <= 10:
            raise ConfigError("markets.strike_settle_sec в [0, 30], strike_max_gap_sec в [0, 10]")


@dataclass(frozen=True)
class Book:
    url: str
    ping_sec: float
    stale_after_sec: float
    subscribe_before_start_sec: float
    keep_after_end_sec: float

    def validate(self) -> None:
        if not self.url.startswith(("wss://", "ws://")):
            raise ConfigError("book.url должен начинаться с wss://")
        if not 1 <= self.ping_sec <= 10:
            raise ConfigError("book.ping_sec в [1, 10]: сервер ждёт PING каждые 10 с")
        if self.stale_after_sec <= self.ping_sec:
            raise ConfigError("book.stale_after_sec должен быть больше ping_sec")
        if self.subscribe_before_start_sec < 0 or self.keep_after_end_sec < 0:
            raise ConfigError("book.subscribe_before_start_sec и keep_after_end_sec должны быть >= 0")


@dataclass(frozen=True)
class Record:
    compress: bool
    book_levels: int
    book_every_sec: float

    def validate(self) -> None:
        if not 1 <= self.book_levels <= 100:
            raise ConfigError("record.book_levels в [1, 100]")
        if self.book_every_sec <= 0:
            raise ConfigError("record.book_every_sec должен быть > 0")


@dataclass(frozen=True)
class Signal:
    min_prints: int
    cost: float
    min_secs_left_windows: float

    def validate(self) -> None:
        if self.min_prints < 2:
            raise ConfigError("signal.min_prints должен быть >= 2")
        if not 0 <= self.cost < 1:
            raise ConfigError("signal.cost должен быть в [0, 1)")
        if self.min_secs_left_windows < 0:
            raise ConfigError("signal.min_secs_left_windows должен быть >= 0")


@dataclass(frozen=True)
class Execution:
    prefer_maker: bool
    order_ttl_sec: float
    stop_slip_floor: float

    def validate(self) -> None:
        if self.order_ttl_sec <= 0:
            raise ConfigError("execution.order_ttl_sec должен быть > 0")
        if not 0 <= self.stop_slip_floor < 1:
            raise ConfigError("execution.stop_slip_floor должен быть в [0, 1)")


@dataclass(frozen=True)
class Paper:
    latency_ms: float
    fill_only_on_book: bool

    def validate(self) -> None:
        if self.latency_ms < 0:
            raise ConfigError("paper.latency_ms должен быть >= 0")
        if not self.fill_only_on_book:
            raise ConfigError("paper.fill_only_on_book = false запрещён: PaperBroker обязан быть консервативным")


@dataclass(frozen=True)
class Profile:
    min_edge: float
    position_usd: float
    max_concurrent: int
    daily_loss_limit_usd: float
    stop_pct: float
    min_book_depth_usd: float
    max_spread: float

    def validate(self, name: str) -> None:
        p = f"profiles.{name}"
        if not 0 < self.min_edge < 1:
            raise ConfigError(f"{p}.min_edge должен быть в (0, 1)")
        if self.position_usd <= 0 or self.daily_loss_limit_usd <= 0:
            raise ConfigError(f"{p}: position_usd и daily_loss_limit_usd должны быть > 0")
        if self.max_concurrent < 1:
            raise ConfigError(f"{p}.max_concurrent должен быть >= 1")
        if not 0 < self.stop_pct < 1 or not 0 < self.max_spread < 1:
            raise ConfigError(f"{p}: stop_pct и max_spread должны быть в (0, 1)")
        if self.min_book_depth_usd < 0:
            raise ConfigError(f"{p}.min_book_depth_usd должен быть >= 0")


@dataclass(frozen=True)
class Monitor:
    kill_switch_file: str
    report_every_min: float


@dataclass(frozen=True)
class Config:
    general: General
    feed: Feed
    markets: Markets
    book: Book
    record: Record
    signal: Signal
    execution: Execution
    paper: Paper
    profiles: dict[str, Profile]
    monitor: Monitor

    @property
    def profile(self) -> Profile:
        return self.profiles[self.general.profile]


# ---------- разбор ----------

def _coerce(value: Any, tp: Any, where: str) -> Any:
    origin = typing.get_origin(tp)
    if origin is list:
        (item_tp,) = typing.get_args(tp)
        if not isinstance(value, list):
            raise ConfigError(f"{where}: ожидается список")
        return [_coerce(v, item_tp, f"{where}[{i}]") for i, v in enumerate(value)]
    if tp is bool:
        if not isinstance(value, bool):
            raise ConfigError(f"{where}: ожидается true/false")
        return value
    if tp is int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ConfigError(f"{where}: ожидается целое число")
        return value
    if tp is float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ConfigError(f"{where}: ожидается число")
        return float(value)
    if tp is str:
        if not isinstance(value, str):
            raise ConfigError(f"{where}: ожидается строка")
        return value
    raise TypeError(f"неподдерживаемый тип поля {where}: {tp}")


def _build(cls: type, data: Any, where: str) -> Any:
    if not isinstance(data, dict):
        raise ConfigError(f"[{where}] должна быть таблицей")
    hints = typing.get_type_hints(cls)
    names = {f.name for f in fields(cls)}
    unknown = set(data) - names
    if unknown:
        raise ConfigError(f"[{where}] неизвестные ключи: {sorted(unknown)}")
    kwargs = {}
    for f in fields(cls):
        if f.name in data:
            kwargs[f.name] = _coerce(data[f.name], hints[f.name], f"{where}.{f.name}")
        elif f.default is MISSING and f.default_factory is MISSING:
            raise ConfigError(f"[{where}] нет ключа '{f.name}'")
    return cls(**kwargs)


def parse_config(data: Mapping[str, Any]) -> Config:
    sections = {"general", "feed", "markets", "book", "record", "signal", "execution", "paper",
                "profiles", "monitor"}
    unknown = set(data) - sections
    if unknown:
        raise ConfigError(f"неизвестные секции: {sorted(unknown)}")
    missing = sections - set(data)
    if missing:
        raise ConfigError(f"нет секций: {sorted(missing)}. Если вы обновили бота, скопируйте заново "
                          f"config.example.toml в config.toml и перенесите свои изменения")

    raw_profiles = data["profiles"]
    if not isinstance(raw_profiles, dict) or not raw_profiles:
        raise ConfigError("[profiles] пуст")
    profiles = {}
    for name, p in raw_profiles.items():
        if name not in PROFILE_NAMES:
            raise ConfigError(f"неизвестный профиль '{name}', допустимы {PROFILE_NAMES}")
        profiles[name] = _build(Profile, p, f"profiles.{name}")
        profiles[name].validate(name)

    cfg = Config(
        general=_build(General, data["general"], "general"),
        feed=_build(Feed, data["feed"], "feed"),
        markets=_build(Markets, data["markets"], "markets"),
        book=_build(Book, data["book"], "book"),
        record=_build(Record, data["record"], "record"),
        signal=_build(Signal, data["signal"], "signal"),
        execution=_build(Execution, data["execution"], "execution"),
        paper=_build(Paper, data["paper"], "paper"),
        profiles=profiles,
        monitor=_build(Monitor, data["monitor"], "monitor"),
    )
    for section in (cfg.general, cfg.feed, cfg.markets, cfg.book, cfg.record, cfg.signal,
                    cfg.execution, cfg.paper):
        section.validate()
    if cfg.general.profile not in profiles:
        raise ConfigError(f"general.profile = '{cfg.general.profile}', но такого профиля нет")
    return cfg


def load_config(path: str | Path = "config.toml") -> Config:
    path = Path(path)
    if not path.exists():
        raise ConfigError(
            f"нет файла {path}. Скопируйте config.example.toml в {path.name} "
            f"(Windows: copy config.example.toml config.toml)")
    with path.open("rb") as f:
        try:
            data = tomllib.load(f)
        except tomllib.TOMLDecodeError as e:
            raise ConfigError(f"{path}: ошибка синтаксиса TOML: {e}") from e
    return parse_config(data)


# ---------- окружение и .env ----------

def parse_env_file(text: str) -> dict[str, str]:
    """Простой разбор .env: KEY=VALUE, комментарии #, необязательные кавычки и 'export '."""
    out: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key.startswith("export "):
            key = key[len("export "):].strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        if key:
            out[key] = value
    return out


def find_env_file(explicit: str | Path | None = None, cwd: Path | None = None,
                  home: Path | None = None) -> Path | None:
    """Порядок: явный путь, ./.env, ~/.updown-bot/.env (там .env переживает обновление папки проекта)."""
    if explicit:
        p = Path(explicit).expanduser()
        if not p.exists():
            raise ConfigError(f"env-файл {p} не найден")
        return p
    for p in ((cwd or Path.cwd()) / ".env", (home or Path.home()) / ".updown-bot" / ".env"):
        if p.exists():
            return p
    return None


def load_env(env_file: Path | None, environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """Переменные окружения процесса важнее значений из .env."""
    file_vars = parse_env_file(env_file.read_text(encoding="utf-8")) if env_file else {}
    return {**file_vars, **dict(os.environ if environ is None else environ)}


def mask(value: str, keep: int = 4) -> str:
    if not value:
        return "<пусто>"
    return value[:keep] + "…" if len(value) > keep else "…"


@dataclass(frozen=True)
class ApiCreds:
    """CLOB API-ключ для PolyBolt. Секрет и passphrase не попадают в repr."""
    api_key: str
    secret: str = field(repr=False)
    passphrase: str = field(repr=False)

    def __repr__(self) -> str:
        return f"ApiCreds(api_key={mask(self.api_key)})"

    __str__ = __repr__


def load_api_creds(env: Mapping[str, str]) -> ApiCreds:
    names = ("POLY_API_KEY", "POLY_API_SECRET", "POLY_API_PASSPHRASE")
    missing = [n for n in names if not env.get(n)]
    if missing:
        raise ConfigError(
            f"нет {', '.join(missing)}. Получите ключ скриптом scripts/get_clob_api_key.py "
            f"и положите .env в папку проекта или в ~/.updown-bot/.env")
    return ApiCreds(env["POLY_API_KEY"], env["POLY_API_SECRET"], env["POLY_API_PASSPHRASE"])


# ---------- допуск к live ----------

@dataclass(frozen=True)
class LiveGate:
    allowed: bool
    reasons: tuple[str, ...]   # почему live запрещён (пусто, если разрешён)


def live_gate(cfg_dry_run: bool, env: Mapping[str, str], cli_live: bool,
              live_ok_path: str | Path = "LIVE_OK") -> LiveGate:
    """
    Реальные ордера возможны только если одновременно:
    dry_run = false в config.toml, DRY_RUN=0 в окружении, --live в командной строке
    и существует файл LIVE_OK (создаёт человек вручную). Иначе - только PaperBroker.
    """
    reasons = []
    if cfg_dry_run:
        reasons.append("config.toml: dry_run = true")
    if env.get("DRY_RUN", "1").strip() != "0":
        reasons.append("DRY_RUN не равен 0")
    if not cli_live:
        reasons.append("нет флага --live")
    if not Path(live_ok_path).exists():
        reasons.append(f"нет файла {live_ok_path}")
    return LiveGate(allowed=not reasons, reasons=tuple(reasons))
