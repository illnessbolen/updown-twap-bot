import copy
import tomllib
from pathlib import Path

import pytest

import config as cfgmod
from config import ConfigError, Credentials, MissingCredentials, live_allowed, load_config, parse_config

ROOT = Path(__file__).resolve().parent.parent
EXAMPLE = ROOT / "config.example.toml"


@pytest.fixture
def raw():
    with EXAMPLE.open("rb") as fh:
        return tomllib.load(fh)


def test_example_config_loads():
    cfg = load_config(EXAMPLE)
    assert cfg.general.symbols == ("btcusd", "ethusd")
    assert cfg.general.windows == (5, 15)
    assert cfg.general.dry_run is True
    assert cfg.feed.twap_window_sec == 60
    assert cfg.feed.stale_after_sec == 45
    assert cfg.profile.min_edge == 0.05
    assert set(cfg.profiles) == {"conservative", "moderate", "aggressive"}


def test_missing_file_has_helpful_message(tmp_path):
    with pytest.raises(ConfigError, match="config.example.toml"):
        load_config(tmp_path / "config.toml")


def test_defaults_are_safe_when_sections_empty():
    cfg = parse_config({"profiles": {"conservative": {
        "min_edge": 0.05, "position_usd": 5, "max_concurrent": 1,
        "daily_loss_limit_usd": 10, "stop_pct": 0.03,
        "min_book_depth_usd": 300, "max_spread": 0.01}}})
    assert cfg.general.dry_run is True
    assert cfg.feed.stale_after_sec == 45


def test_unknown_key_is_rejected(raw):
    raw["feed"]["stale_after"] = 10          # опечатка
    with pytest.raises(ConfigError, match="неизвестные ключи"):
        parse_config(raw)


def test_unknown_section_is_rejected(raw):
    raw["secrets"] = {"api_key": "x"}
    with pytest.raises(ConfigError, match="неизвестные секции"):
        parse_config(raw)


@pytest.mark.parametrize("bad", ["btc/usd", "btcusdt", "BTCUSD", "btc"])
def test_old_symbol_formats_are_rejected(raw, bad):
    raw["general"]["symbols"] = [bad]
    with pytest.raises(ConfigError, match="символ"):
        parse_config(raw)


def test_30s_twap_window_is_rejected(raw):
    raw["feed"]["twap_window_sec"] = 30
    with pytest.raises(ConfigError, match="60"):
        parse_config(raw)


def test_windows_validation(raw):
    raw["general"]["windows"] = [5, 60]
    with pytest.raises(ConfigError, match="windows"):
        parse_config(raw)


def test_paper_latency_below_taker_delay_rejected(raw):
    raw["paper"]["latency_ms"] = 50
    with pytest.raises(ConfigError, match="150"):
        parse_config(raw)


def test_paper_cannot_fill_off_book(raw):
    raw["paper"]["fill_only_on_book"] = False
    with pytest.raises(ConfigError, match="стакан"):
        parse_config(raw)


def test_profile_must_exist(raw):
    raw["general"]["profile"] = "moderate"
    del raw["profiles"]["moderate"]
    with pytest.raises(ConfigError, match="moderate"):
        parse_config(raw)


def test_unknown_profile_name_rejected(raw):
    raw["profiles"]["yolo"] = copy.deepcopy(raw["profiles"]["aggressive"])
    with pytest.raises(ConfigError, match="yolo"):
        parse_config(raw)


def test_profile_missing_field(raw):
    del raw["profiles"]["conservative"]["min_edge"]
    with pytest.raises(ConfigError, match="min_edge"):
        parse_config(raw)


def test_profile_value_range(raw):
    raw["profiles"]["conservative"]["min_edge"] = 1.5
    with pytest.raises(ConfigError, match="min_edge"):
        parse_config(raw)


def test_feed_pong_timeout_must_be_below_heartbeat(raw):
    raw["feed"]["pong_timeout_sec"] = 20
    with pytest.raises(ConfigError, match="pong_timeout"):
        parse_config(raw)


def test_feed_backoff_validation(raw):
    raw["feed"]["reconnect_backoff_sec"] = []
    with pytest.raises(ConfigError, match="backoff"):
        parse_config(raw)


# ------------------------------------------------------------------ секреты
def test_credentials_from_env_and_repr_hides_values():
    env = {cfgmod.ENV_API_KEY: "KEY-123", cfgmod.ENV_API_SECRET: "SECRET-456",
           cfgmod.ENV_API_PASSPHRASE: "PASS-789"}
    c = Credentials.from_env(env)
    assert (c.api_key, c.api_secret, c.api_passphrase) == ("KEY-123", "SECRET-456", "PASS-789")
    for text in (repr(c), str(c), f"{c}", f"{c!r}"):
        assert "KEY-123" not in text and "SECRET-456" not in text and "PASS-789" not in text


def test_missing_credentials_message_has_names_only():
    with pytest.raises(MissingCredentials) as e:
        Credentials.from_env({cfgmod.ENV_API_KEY: "KEY-123"})
    msg = str(e.value)
    assert cfgmod.ENV_API_SECRET in msg and cfgmod.ENV_API_PASSPHRASE in msg
    assert "KEY-123" not in msg


def test_empty_credential_counts_as_missing():
    env = {cfgmod.ENV_API_KEY: "a", cfgmod.ENV_API_SECRET: "", cfgmod.ENV_API_PASSPHRASE: "c"}
    with pytest.raises(MissingCredentials):
        Credentials.from_env(env)


def test_example_config_contains_no_secret_values():
    text = EXAMPLE.read_text(encoding="utf-8").lower()
    for word in ("private_key", "0x"):
        assert word not in text


# ------------------------------------------------------------------ live gate
def _cfg(dry_run: bool):
    with EXAMPLE.open("rb") as fh:
        data = tomllib.load(fh)
    data["general"]["dry_run"] = dry_run
    return parse_config(data)


def test_live_blocked_by_default(tmp_path):
    ok = tmp_path / "LIVE_OK"
    assert live_allowed(_cfg(True), {}, False, ok) is False


def test_live_requires_every_condition(tmp_path):
    ok = tmp_path / "LIVE_OK"
    cfg_live = _cfg(False)
    env_live = {"DRY_RUN": "0"}

    # нет файла LIVE_OK
    assert live_allowed(cfg_live, env_live, True, ok) is False
    ok.write_text("")
    # всё есть
    assert live_allowed(cfg_live, env_live, True, ok) is True
    # нет флага --live
    assert live_allowed(cfg_live, env_live, False, ok) is False
    # DRY_RUN не "0" (в т.ч. не задан, "1", "false", "")
    for v in (None, "1", "false", "", "00"):
        env = {} if v is None else {"DRY_RUN": v}
        assert live_allowed(cfg_live, env, True, ok) is False
    # config.dry_run = true
    assert live_allowed(_cfg(True), env_live, True, ok) is False


def test_live_ok_must_be_a_file_not_a_directory(tmp_path):
    d = tmp_path / "LIVE_OK"
    d.mkdir()
    assert live_allowed(_cfg(False), {"DRY_RUN": "0"}, True, d) is False
