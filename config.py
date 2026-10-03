"""
Загрузка и проверка config.toml, секреты из окружения, условия включения live.

Конфиг строгий: неизвестный ключ или неверное значение - ошибка при старте, а не
молчаливое значение по умолчанию. Секреты в конфиг не попадают никогда: ключи
Polymarket читаются только из переменных окружения.
"""
from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Mapping

ENV_API_KEY = "POLYMARKET_API_KEY"
ENV_API_SECRET = "POLYMARKET_API_SECRET"
ENV_API_PASSPHRASE = "POLYMARKET_API_PASSPHRASE"

_SYMBOL_RE = re.compile(r"^[a-z0-9]+usd$")
_PROFILE_NAMES = ("conservative", "moderate", "aggressive")


class ConfigError(ValueError):
    pass


class MissingCredentials(RuntimeError):
    """Нет API-ключей в окружении. В сообщении только имена переменных."""


# ---------------------------------------------------------------- секреты
@dataclass(frozen=True)
class Credentials:
    """CLOB API credentials для PolyBolt. repr не раскрывает значения."""
    api_key: str = field(repr=False)
    api_secret: str = field(repr=False)
    api_passphrase: str = field(repr=False)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Credentials":
        env = os.environ if env is None else env
        names = (ENV_API_KEY, ENV_API_SECRET, ENV_API_PASSPHRASE)
        missing = [n for n in names if not env.get(n)]
        if missing:
            raise MissingCredentials(
                "не заданы переменные окружения: " + ", ".join(missing))
        return cls(env[ENV_API_KEY], env[ENV_API_SECRET], env[ENV_API_PASSPHRASE])

    def __repr__(self) -> str:
        return "Credentials(api_key=***, api_secret=***, api_passphrase=***)"

    __str__ = __repr__


def live_allowed(cfg: "Config", env: Mapping[str, str], live_flag: bool,
                 live_ok_path: str | Path = "LIVE_OK") -> bool:
    """
    Реальные ордера возможны, только если выполнено ВСЁ сразу:
      1. DRY_RUN=0 в окружении (любое другое значение или отсутствие - бумага),
      2. флаг --live в командной строке,
      3. существует файл LIVE_OK (его создаёт человек вручную),
      4. в config.toml dry_run = false (дополнительная страховка).
    Иначе бот работает только через PaperBroker.
    """
    return (env.get("DRY_RUN") == "0"
            and bool(live_flag)
            and Path(live_ok_path).is_file()
            and not cfg.general.dry_run)


# ---------------------------------------------------------------- секции
def _pos(name: str, value: float) -> None:
    if not value > 0:
        raise ConfigError(f"{name} должно быть > 0, получено {value!r}")


@dataclass(frozen=True)
class General:
    dry_run: bool = True
    profile: str = "conservative"
    symbols: tuple[str, ...] = ("btcusd", "ethusd")
    windows: tuple[int, ...] = (5, 15)
    log_dir: str = "data"
    db_path: str = "bot.db"

    def __post_init__(self) -> None:
        if self.profile not in _PROFILE_NAMES:
            raise ConfigError(f"general.profile: ожидается одно из {_PROFILE_NAMES}")
        if not self.symbols:
            raise ConfigError("general.symbols пуст")
        for s in self.symbols:
            if not _SYMBOL_RE.match(s):
                hint = " (формат PolyBolt: btcusd, а не btc/usd или btcusdt)"
                raise ConfigError(f"general.symbols: неверный символ {s!r}{hint}")
        if len(set(self.symbols)) != len(self.symbols):
            raise ConfigError("general.symbols содержит дубликаты")
        if not self.windows or any(w not in (5, 15) for w in self.windows):
            raise ConfigError("general.windows: допустимы только 5 и 15 (минуты)")


@dataclass(frozen=True)
class Feed:
    url: str = "wss://ws-live-v2.polymarket.com/ws"
    twap_window_sec: int = 60
    stale_after_sec: float = 45.0
    reconnect_backoff_sec: tuple[float, ...] = (1, 2, 5, 10, 30)
    use_spot_for_sigma: bool = True
    spot_provider: str = ""            # "" = по умолчанию (Chainlink); или "chainlink"/"pyth"
    heartbeat_sec: float = 15.0        # прикладной {"op":"ping"}
    pong_timeout_sec: float = 10.0
    auth_timeout_sec: float = 10.0
    subscribe_timeout_sec: float = 15.0
    connect_timeout_sec: float = 15.0

    def __post_init__(self) -> None:
        if not self.url.startswith(("wss://", "ws://")):
            raise ConfigError("feed.url должен начинаться с wss://")
        if self.twap_window_sec != 60:
            raise ConfigError(
                "feed.twap_window_sec: PolyBolt отдаёт только 60 с (API_NOTES.md, раздел 0)")
        for n in ("stale_after_sec", "heartbeat_sec", "pong_timeout_sec",
                  "auth_timeout_sec", "subscribe_timeout_sec", "connect_timeout_sec"):
            _pos(f"feed.{n}", getattr(self, n))
        if not self.reconnect_backoff_sec or any(x <= 0 for x in self.reconnect_backoff_sec):
            raise ConfigError("feed.reconnect_backoff_sec: нужен непустой список чисел > 0")
        if self.spot_provider not in ("", "chainlink", "pyth"):
            raise ConfigError("feed.spot_provider: '', 'chainlink' или 'pyth'")
        if self.pong_timeout_sec >= self.heartbeat_sec:
            raise ConfigError("feed.pong_timeout_sec должен быть меньше heartbeat_sec")


@dataclass(frozen=True)
class Signal:
    min_prints: int = 20
    # cost для twap_signal.evaluate = fee_per_share(ask) + slippage
    slippage: float = 0.005
    min_secs_left_windows: float = 2

    def __post_init__(self) -> None:
        if self.min_prints < 2:
            raise ConfigError("signal.min_prints должно быть >= 2")
        if not 0 <= self.slippage < 0.5:
            raise ConfigError("signal.slippage вне [0, 0.5)")
        _pos("signal.min_secs_left_windows", self.min_secs_left_windows)


@dataclass(frozen=True)
class Execution:
    prefer_maker: bool = True
    order_ttl_sec: float = 10
    stop_slip_floor: float = 0.03

    def __post_init__(self) -> None:
        _pos("execution.order_ttl_sec", self.order_ttl_sec)
        if not 0 <= self.stop_slip_floor < 1:
            raise ConfigError("execution.stop_slip_floor вне [0, 1)")


@dataclass(frozen=True)
class Paper:
    latency_ms: float = 300
    fill_only_on_book: bool = True

    def __post_init__(self) -> None:
        if self.latency_ms < 0:
            raise ConfigError("paper.latency_ms < 0")
        # Taker delay на крипто-рынках 150 мс (API_NOTES.md, раздел 5): меньшая
        # задержка делала бы бумажные исполнения нереалистично быстрыми.
        if self.latency_ms < 150:
            raise ConfigError("paper.latency_ms должно быть >= 150 (taker delay 150 мс)")
        if not self.fill_only_on_book:
            raise ConfigError("paper.fill_only_on_book = false запрещено: "
                              "исполнение только по доступному объёму стакана")


@dataclass(frozen=True)
class Profile:
    min_edge: float
    position_usd: float
    max_concurrent: int
    daily_loss_limit_usd: float
    stop_pct: float
    min_book_depth_usd: float
    max_spread: float

    def __post_init__(self) -> None:
        if not 0 < self.min_edge < 1:
            raise ConfigError("profile.min_edge вне (0, 1)")
        for n in ("position_usd", "daily_loss_limit_usd", "min_book_depth_usd"):
            _pos(f"profile.{n}", getattr(self, n))
        if self.max_concurrent < 1:
            raise ConfigError("profile.max_concurrent должно быть >= 1")
        if not 0 < self.stop_pct < 1:
            raise ConfigError("profile.stop_pct вне (0, 1)")
        if not 0 < self.max_spread < 1:
            raise ConfigError("profile.max_spread вне (0, 1)")


@dataclass(frozen=True)
class Monitor:
    kill_switch_file: str = "KILL"
    report_every_min: float = 60

    def __post_init__(self) -> None:
        _pos("monitor.report_every_min", self.report_every_min)


@dataclass(frozen=True)
class Recording:
    enabled: bool = True
    gzip: bool = True                  # сжимать закрытые часовые файлы
    status_every_sec: float = 60       # строка статуса в лог

    def __post_init__(self) -> None:
        _pos("recording.status_every_sec", self.status_every_sec)


@dataclass(frozen=True)
class Config:
    general: General
    feed: Feed
    signal: Signal
    execution: Execution
    paper: Paper
    profiles: dict[str, Profile]
    monitor: Monitor
    recording: Recording

    @property
    def profile(self) -> Profile:
        return self.profiles[self.general.profile]


# ---------------------------------------------------------------- загрузка
def _build(cls, data: Mapping, where: str):
    if not isinstance(data, Mapping):
        raise ConfigError(f"[{where}] должна быть таблицей")
    allowed = {f.name for f in fields(cls)}
    unknown = sorted(set(data) - allowed)
    if unknown:
        raise ConfigError(f"[{where}]: неизвестные ключи {unknown}; допустимы {sorted(allowed)}")
    kw = {}
    for f in fields(cls):
        if f.name not in data:
            continue
        v = data[f.name]
        if isinstance(v, list):
            v = tuple(v)
        kw[f.name] = v
    try:
        return cls(**kw)
    except TypeError as e:  # не хватает обязательных ключей профиля
        raise ConfigError(f"[{where}]: {e}") from None


def parse_config(data: Mapping) -> Config:
    known = {"general", "feed", "signal", "execution", "paper", "profiles", "monitor",
             "recording"}
    unknown = sorted(set(data) - known)
    if unknown:
        raise ConfigError(f"неизвестные секции {unknown}; допустимы {sorted(known)}")

    profiles_raw = data.get("profiles", {})
    profiles = {name: _build(Profile, profiles_raw[name], f"profiles.{name}")
                for name in profiles_raw}
    bad = sorted(set(profiles) - set(_PROFILE_NAMES))
    if bad:
        raise ConfigError(f"profiles: неизвестные профили {bad}; допустимы {_PROFILE_NAMES}")

    cfg = Config(
        general=_build(General, data.get("general", {}), "general"),
        feed=_build(Feed, data.get("feed", {}), "feed"),
        signal=_build(Signal, data.get("signal", {}), "signal"),
        execution=_build(Execution, data.get("execution", {}), "execution"),
        paper=_build(Paper, data.get("paper", {}), "paper"),
        profiles=profiles,
        monitor=_build(Monitor, data.get("monitor", {}), "monitor"),
        recording=_build(Recording, data.get("recording", {}), "recording"),
    )
    if cfg.general.profile not in cfg.profiles:
        raise ConfigError(f"general.profile = {cfg.general.profile!r}, "
                          f"но секции [profiles.{cfg.general.profile}] нет")
    return cfg


def load_config(path: str | Path = "config.toml") -> Config:
    p = Path(path)
    if not p.is_file():
        raise ConfigError(f"нет файла {p}; скопируйте config.example.toml в config.toml")
    try:
        with p.open("rb") as fh:
            data = tomllib.load(fh)
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f"{p}: ошибка TOML: {e}") from None
    return parse_config(data)
