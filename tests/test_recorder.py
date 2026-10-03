import gzip
import json
from datetime import datetime, timezone

import pytest

from recorder import JsonlRecorder, hour_key, read_jsonl


def ts(h, m=0, s=0, day=3):
    return datetime(2026, 10, day, h, m, s, tzinfo=timezone.utc).timestamp()


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def test_writes_one_json_object_per_line(tmp_path):
    clk = Clock(ts(8, 5))
    rec = JsonlRecorder(tmp_path, clock=clk)
    rec.write("rx", msg={"channel": "price.crypto", "seq": 1})
    rec.write("event", name="connected")
    rec.close()
    files = list(tmp_path.glob("*.jsonl"))
    assert [f.name for f in files] == ["feed-20261003T08.jsonl"]
    rows = list(read_jsonl(files[0]))
    assert rows[0] == {"t": ts(8, 5), "kind": "rx", "msg": {"channel": "price.crypto", "seq": 1}}
    assert rows[1]["kind"] == "event" and rows[1]["name"] == "connected"
    assert rec.lines_written == 2


def test_rotates_each_utc_hour_and_gzips_closed_hour(tmp_path):
    clk = Clock(ts(8, 59, 59))
    rec = JsonlRecorder(tmp_path, clock=clk)
    rec.write("rx", n=1)
    clk.t = ts(9, 0, 1)
    rec.write("rx", n=2)
    rec.close()  # дожидается фонового gzip
    names = sorted(p.name for p in tmp_path.iterdir())
    assert names == ["feed-20261003T08.jsonl.gz", "feed-20261003T09.jsonl"]
    assert [r["n"] for r in read_jsonl(tmp_path / "feed-20261003T08.jsonl.gz")] == [1]
    assert [r["n"] for r in read_jsonl(tmp_path / "feed-20261003T09.jsonl")] == [2]
    assert rec.files_compressed == 1


def test_gzip_can_be_disabled(tmp_path):
    clk = Clock(ts(8))
    rec = JsonlRecorder(tmp_path, gzip_closed=False, clock=clk)
    rec.write("rx", n=1)
    clk.t = ts(10)
    rec.write("rx", n=2)
    rec.close()
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "feed-20261003T08.jsonl", "feed-20261003T10.jsonl"]


def test_leftover_files_are_compressed_on_start(tmp_path):
    # аварийный останов: часы 06 и 07 остались несжатыми, 08 - текущий
    for h in (6, 7, 8):
        (tmp_path / f"feed-20261003T{h:02d}.jsonl").write_text(json.dumps({"h": h}) + "\n")
    rec = JsonlRecorder(tmp_path, clock=Clock(ts(8, 30)))
    names = sorted(p.name for p in tmp_path.iterdir())
    assert names == ["feed-20261003T06.jsonl.gz", "feed-20261003T07.jsonl.gz",
                     "feed-20261003T08.jsonl"]
    rec.write("rx", n=1)          # текущий час дописывается, а не затирается
    rec.close()
    rows = list(read_jsonl(tmp_path / "feed-20261003T08.jsonl"))
    assert rows[0] == {"h": 8} and rows[1]["n"] == 1


def test_restart_in_same_hour_appends(tmp_path):
    clk = Clock(ts(8, 1))
    r1 = JsonlRecorder(tmp_path, clock=clk)
    r1.write("rx", n=1)
    r1.close()
    r2 = JsonlRecorder(tmp_path, clock=clk)
    r2.write("rx", n=2)
    r2.close()
    assert [r["n"] for r in read_jsonl(tmp_path / "feed-20261003T08.jsonl")] == [1, 2]


def test_gzip_name_collision_gets_suffix(tmp_path):
    (tmp_path / "feed-20261003T06.jsonl.gz").write_bytes(gzip.compress(b'{"old":1}\n'))
    (tmp_path / "feed-20261003T06.jsonl").write_text('{"new":1}\n')
    JsonlRecorder(tmp_path, clock=Clock(ts(8)))
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "feed-20261003T06.jsonl.1.gz", "feed-20261003T06.jsonl.gz"]
    assert list(read_jsonl(tmp_path / "feed-20261003T06.jsonl.gz")) == [{"old": 1}]
    assert list(read_jsonl(tmp_path / "feed-20261003T06.jsonl.1.gz")) == [{"new": 1}]


def test_unicode_and_compact_format(tmp_path):
    rec = JsonlRecorder(tmp_path, clock=Clock(ts(8)))
    rec.write("event", name="проверка")
    rec.close()
    line = (tmp_path / "feed-20261003T08.jsonl").read_text(encoding="utf-8").strip()
    assert "проверка" in line and ", " not in line and ": " not in line


def test_write_error_is_not_swallowed(tmp_path):
    rec = JsonlRecorder(tmp_path, clock=Clock(ts(8)))
    rec.write("rx", n=1)
    rec._fh.close()  # имитируем сломанный файл
    with pytest.raises(ValueError):
        rec.write("rx", n=2)


def test_hour_key_is_utc():
    assert hour_key(ts(23, 59, 59)) == "20261003T23"
    assert hour_key(ts(0, 0, 0, day=4)) == "20261004T00"
