import asyncio
import tomllib
from pathlib import Path

import pytest

import config as cfgmod
import main
from config import Credentials, parse_config
from recorder import read_jsonl
from tests.fake_polybolt import FakePolyBolt
from tests.test_feed import CREDS, SERVER_CREDS, SECRETS, until

ROOT = Path(__file__).resolve().parent.parent
EXAMPLE = ROOT / "config.example.toml"


def cfg_for(url, log_dir, **feed):
    with EXAMPLE.open("rb") as fh:
        raw = tomllib.load(fh)
    raw["feed"].update(url=url, stale_after_sec=1.0, reconnect_backoff_sec=[0.05],
                       heartbeat_sec=30, auth_timeout_sec=2, subscribe_timeout_sec=2, **feed)
    raw["general"]["log_dir"] = str(log_dir)
    raw["general"]["symbols"] = ["btcusd"]
    return parse_config(raw)


@pytest.fixture
async def srv():
    s = FakePolyBolt(SERVER_CREDS)
    await s.start()
    yield s
    await s.close()


def clear_env(monkeypatch):
    for n in (cfgmod.ENV_API_KEY, cfgmod.ENV_API_SECRET, cfgmod.ENV_API_PASSPHRASE):
        monkeypatch.delenv(n, raising=False)


def set_env(monkeypatch):
    monkeypatch.setenv(cfgmod.ENV_API_KEY, CREDS.api_key)
    monkeypatch.setenv(cfgmod.ENV_API_SECRET, CREDS.api_secret)
    monkeypatch.setenv(cfgmod.ENV_API_PASSPHRASE, CREDS.api_passphrase)


# ------------------------------------------------------------------ CLI
def test_check_config_reports_paper_mode_and_missing_keys(monkeypatch, capsys):
    clear_env(monkeypatch)
    code = main.cli(["check-config", "--config", str(EXAMPLE)])
    out = capsys.readouterr().out
    assert code == main.EXIT_USAGE
    assert "PAPER" in out and "НЕ ЗАДАНА" in out
    assert "btcusd, ethusd" in out and "60 с" in out


def test_check_config_ok_with_keys_never_prints_values(monkeypatch, capsys):
    set_env(monkeypatch)
    code = main.cli(["check-config", "--config", str(EXAMPLE)])
    out = capsys.readouterr().out
    assert code == main.EXIT_OK
    for s in SECRETS:
        assert s not in out


def test_record_without_keys_fails_with_names_only(monkeypatch, capsys):
    clear_env(monkeypatch)
    code = main.cli(["record", "--config", str(EXAMPLE)])
    err = capsys.readouterr().err
    assert code == main.EXIT_USAGE
    assert cfgmod.ENV_API_KEY in err and cfgmod.ENV_API_SECRET in err


def test_missing_config_file_gives_usage_error(tmp_path, monkeypatch, capsys):
    set_env(monkeypatch)
    code = main.cli(["record", "--config", str(tmp_path / "nope.toml")])
    assert code == main.EXIT_USAGE and "config.example.toml" in capsys.readouterr().err


def test_no_live_flag_exists_in_m1():
    with pytest.raises(SystemExit):
        main.cli(["record", "--live"])


# ------------------------------------------------------------------ record / smoke
async def test_record_writes_stream_and_stops_on_event(srv, tmp_path):
    cfg = cfg_for(srv.url, tmp_path / "data")
    stop = asyncio.Event()
    task = asyncio.create_task(main.run_record(cfg, CREDS, stop))
    assert await until(lambda: any((tmp_path / "data").glob("feed-*.jsonl"))
                       and srv.connections == 1, timeout=5)
    await asyncio.sleep(0.6)
    stop.set()
    assert await asyncio.wait_for(task, 10) == main.EXIT_OK
    files = list((tmp_path / "data").glob("feed-*.jsonl*"))
    rows = [r for f in files for r in read_jsonl(f)]
    channels = {r["msg"].get("channel") for r in rows if r["kind"] == "rx" and "channel" in r["msg"]}
    assert channels == {"price.crypto.twap", "price.crypto"}
    assert any(r["kind"] == "event" and r["name"] == "stopped" for r in rows)
    blob = "".join(f.read_text(encoding="utf-8") for f in files if f.suffix == ".jsonl")
    assert not any(s in blob for s in SECRETS)


async def test_record_returns_failure_on_fatal_feed_error(srv, tmp_path):
    cfg = cfg_for(srv.url, tmp_path / "data")
    bad = Credentials("k", "s", "p")        # сервер вернёт auth_invalid
    code = await asyncio.wait_for(main.run_record(cfg, bad), 10)
    assert code == main.EXIT_FAIL
    rows = [r for f in (tmp_path / "data").glob("feed-*.jsonl*") for r in read_jsonl(f)]
    assert any(r["kind"] == "event" and r["name"] == "fatal" for r in rows)


async def test_smoke_succeeds_and_saves_raw_frames(srv, tmp_path, capsys):
    cfg = cfg_for(srv.url, tmp_path / "data")
    code = await main.run_smoke(cfg, CREDS, 5.0, tmp_path / "smoke")
    out = capsys.readouterr().out
    assert code == main.EXIT_OK
    assert "свежая цена получена : да" in out and "price.crypto.twap" in out
    files = list((tmp_path / "smoke").glob("smoke-*.jsonl"))
    assert files and any(r["kind"] == "rx" for r in read_jsonl(files[0]))
    assert not any(s in out for s in SECRETS)


async def test_smoke_fails_when_no_data(srv, tmp_path, capsys):
    async def silent(conn):
        if await conn.handshake(snapshot_points=0):
            await conn.idle()

    srv.scripts = [silent] * 5
    cfg = cfg_for(srv.url, tmp_path / "data")
    code = await main.run_smoke(cfg, CREDS, 1.5, tmp_path / "smoke")
    out = capsys.readouterr().out
    assert code == main.EXIT_FAIL and "НЕТ" in out
