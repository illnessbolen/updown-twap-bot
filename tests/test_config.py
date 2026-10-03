import copy
import itertools
import tomllib
from pathlib import Path

import pytest

from updown.config import (
    ApiCreds, ConfigError, find_env_file, live_gate, load_api_creds, load_config, load_env,
    parse_config, parse_env_file,
)

ROOT = Path(__file__).resolve().parent.parent
EXAMPLE = ROOT / "config.example.toml"


def example_data() -> dict:
    with EXAMPLE.open("rb") as f:
        return tomllib.load(f)


def test_example_config_is_valid():
    cfg = load_config(EXAMPLE)
    assert cfg.general.symbols == ["btcusd", "ethusd"]
    assert cfg.general.dry_run is True
    assert cfg.feed.twap_window_sec == 60
    assert cfg.feed.stale_after_sec == 45.0
    assert cfg.profile.min_edge == 0.05      # conservative
    assert set(cfg.profiles) == {"conservative", "moderate", "aggressive"}


def test_data_dir_expands_home():
    cfg = load_config(EXAMPLE)
    assert cfg.general.data_dir == Path.home() / ".updown-bot" / "data"


def test_missing_config_file_explains_what_to_do(tmp_path):
    with pytest.raises(ConfigError, match="config.example.toml"):
        load_config(tmp_path / "config.toml")


def test_profile_selection():
    data = example_data()
    data["general"]["profile"] = "aggressive"
    assert parse_config(data).profile.position_usd == 20


@pytest.mark.parametrize("mutate,match", [
    (lambda d: d["general"].update(profile="yolo"), "профиля нет"),
    (lambda d: d["general"].update(symbols=["btc/usd"]), "btcusd"),
    (lambda d: d["general"].update(windows=[1, 5]), "windows"),
    (lambda d: d["general"].update(typo_key=1), "неизвестные ключи"),
    (lambda d: d["feed"].update(twap_window_sec=30), "60"),
    (lambda d: d["feed"].update(stale_after_sec=0), "stale_after_sec"),
    (lambda d: d["feed"].update(stale_after_sec="45"), "число"),
    (lambda d: d["feed"].update(spot_provider="binance"), "spot_provider"),
    (lambda d: d["general"].update(dry_run=1), "true/false"),
    (lambda d: d["paper"].update(fill_only_on_book=False), "консервативным"),
    (lambda d: d["profiles"]["moderate"].update(min_edge=1.5), "min_edge"),
    (lambda d: d["profiles"].update(degen={}), "неизвестный профиль"),
    (lambda d: d.pop("monitor"), "нет секций"),
    (lambda d: d["signal"].pop("cost"), "нет ключа"),
])
def test_invalid_configs_are_rejected(mutate, match):
    data = copy.deepcopy(example_data())
    mutate(data)
    with pytest.raises(ConfigError, match=match):
        parse_config(data)


def test_parse_env_file():
    text = "\n".join([
        "# комментарий",
        "POLY_API_KEY=abc",
        'POLY_API_SECRET="s3cr=et"',
        "export POLY_API_PASSPHRASE='pp'",
        "",
        "BROKEN LINE",
    ])
    assert parse_env_file(text) == {
        "POLY_API_KEY": "abc", "POLY_API_SECRET": "s3cr=et", "POLY_API_PASSPHRASE": "pp"}


def test_process_env_wins_over_env_file(tmp_path):
    f = tmp_path / ".env"
    f.write_text("DRY_RUN=0\nPOLY_API_KEY=from_file\n", encoding="utf-8")
    env = load_env(f, environ={"DRY_RUN": "1"})
    assert env["DRY_RUN"] == "1" and env["POLY_API_KEY"] == "from_file"


def test_find_env_file_order(tmp_path):
    cwd, home = tmp_path / "proj", tmp_path / "home"
    (home / ".updown-bot").mkdir(parents=True)
    cwd.mkdir()
    assert find_env_file(cwd=cwd, home=home) is None
    (home / ".updown-bot" / ".env").write_text("A=1")
    assert find_env_file(cwd=cwd, home=home) == home / ".updown-bot" / ".env"
    (cwd / ".env").write_text("A=2")
    assert find_env_file(cwd=cwd, home=home) == cwd / ".env"
    with pytest.raises(ConfigError):
        find_env_file(explicit=tmp_path / "nope.env")


def test_creds_are_masked():
    c = ApiCreds("7b1e2d60-aaaa", "TOP-SECRET-VALUE", "PASS-PHRASE-VALUE")
    for text in (repr(c), str(c), f"{c}", repr([c])):
        assert "TOP-SECRET-VALUE" not in text and "PASS-PHRASE-VALUE" not in text
        assert "7b1e2d60-aaaa" not in text


def test_missing_creds_explain_how_to_get_them():
    with pytest.raises(ConfigError, match="get_clob_api_key"):
        load_api_creds({"POLY_API_KEY": "k"})


def test_creds_loaded():
    c = load_api_creds({"POLY_API_KEY": "k", "POLY_API_SECRET": "s", "POLY_API_PASSPHRASE": "p"})
    assert (c.api_key, c.secret, c.passphrase) == ("k", "s", "p")


@pytest.mark.parametrize("cfg_dry,dry_env,cli,live_ok",
                         list(itertools.product([True, False], [None, "1", "0", "false"],
                                                [False, True], [False, True])))
def test_live_gate_requires_all_conditions(tmp_path, cfg_dry, dry_env, cli, live_ok):
    live_ok_path = tmp_path / "LIVE_OK"
    if live_ok:
        live_ok_path.write_text("")
    env = {} if dry_env is None else {"DRY_RUN": dry_env}
    gate = live_gate(cfg_dry, env, cli, live_ok_path)
    expected = (not cfg_dry) and dry_env == "0" and cli and live_ok
    assert gate.allowed is expected
    assert bool(gate.reasons) is not expected


def test_live_gate_default_is_dry_run(tmp_path):
    gate = live_gate(False, {}, True, tmp_path / "LIVE_OK")
    assert not gate.allowed
    assert any("DRY_RUN" in r for r in gate.reasons)
