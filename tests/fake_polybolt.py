"""
Локальный фейковый PolyBolt для тестов feed.py.

Реализует то, что описано в docs/API_NOTES.md, раздел 2.1: auth -> authed, subscribe ->
subscribed + снапшот, конверт {"v","channel","seq","ts","snapshot"?,"dropped"?,"payload"},
прикладной ping -> pong, коды закрытия. Это проверка клиента против ДОКУМЕНТАЦИИ, а не
против настоящего сервера (см. раздел 9 API_NOTES: PolyBolt живьём не проверялся).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import Counter
from http import HTTPStatus

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

CH_TWAP = "price.crypto.twap"
CH_SPOT = "price.crypto"


class Conn:
    def __init__(self, srv: "FakePolyBolt", ws, idx: int):
        self.srv, self.ws, self.idx = srv, ws, idx
        self.received: list[dict] = []
        self.subs: list[dict] = []
        self.seq: Counter[str] = Counter()
        self.last_ts: dict[tuple[str, str], int] = {}
        self.authed_at = self.sub_received_at = 0.0
        self.mute_pong = False
        self._pump: asyncio.Task | None = None

    # ------------------------------------------------------------ приём
    async def recv_json(self) -> dict:
        msg = json.loads(await self.ws.recv())
        self.received.append(msg)
        return msg

    async def _pump_loop(self) -> None:
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                self.received.append(msg)
                if msg.get("op") == "ping" and not self.mute_pong:
                    await self.ws.send(json.dumps({"op": "pong", "rid": msg.get("rid")}))
        except ConnectionClosed:
            pass

    def start_pump(self) -> None:
        self._pump = asyncio.create_task(self._pump_loop())

    # ------------------------------------------------------------ отправка
    async def send(self, obj: dict) -> None:
        await self.ws.send(json.dumps(obj))

    def skip_seq(self, channel: str, n: int) -> None:
        self.seq[channel] += n

    async def send_env(self, channel: str, payload: dict, *, snapshot: bool = False,
                       dropped: int | None = None, ts: int | None = None) -> None:
        self.seq[channel] += 1
        env = {"v": 1, "channel": channel, "seq": self.seq[channel],
               "ts": ts if ts is not None else int(time.time() * 1000), "payload": payload}
        if snapshot:
            env["snapshot"] = True
        if dropped:
            env["dropped"] = dropped
        await self.send(env)

    def _next_ts(self, key, ts: int | None = None) -> int:
        now = ts if ts is not None else int(time.time() * 1000)
        t = max(now, self.last_ts.get(key, 0) + 1)
        self.last_ts[key] = t
        return t

    async def handshake(self, *, authed_delay: float = 0.0, ack: bool = True,
                        snapshot_points: int = 3, auth_error: str | None = None,
                        sub_error: str | None = None, send_authed: bool = True) -> bool:
        """auth -> (authed) -> subscribe -> (subscribed + снапшот). False, если сценарий оборван."""
        auth = await self.recv_json()
        assert auth["op"] == "auth", auth
        if auth_error or auth["auth"] != self.srv.creds:
            await self.send({"op": "error", "code": auth_error or "auth_invalid", "rid": auth.get("rid")})
            return False
        if not send_authed:
            await self.ws.wait_closed()
            return False
        if authed_delay:
            await asyncio.sleep(authed_delay)
        self.authed_at = time.monotonic()
        await self.send({"op": "authed", "rid": auth.get("rid")})

        sub = await self.recv_json()
        self.sub_received_at = time.monotonic()
        assert sub["op"] == "subscribe", sub
        self.subs = sub["subscriptions"]
        if sub_error:
            await self.send({"op": "error", "code": sub_error,
                             "channel": self.subs[0]["channel"], "rid": sub.get("rid")})
            return False
        for s in self.subs:
            if ack:
                await self.send({"op": "subscribed", "channel": s["channel"], "rid": sub.get("rid")})
            await self._send_snapshot(s, snapshot_points)
        self.start_pump()
        return True

    async def _send_snapshot(self, sub: dict, n: int) -> None:
        ch, sym = sub["channel"], sub["filter"]["symbol"]
        now = int(time.time() * 1000)
        data = []
        for i in range(n):
            ts = self._next_ts((ch, sym), now - (n - i) * 1000)
            v = self.srv.value(ch, sym)
            data.append({"timestamp": ts, "value": v, "full_accuracy_value": f"{v:.8f}"})
        await self.send_env(ch, {"symbol": sym, "source": "chainlink", "data": data}, snapshot=True)

    async def tick_all(self, *, same_ts: bool = False, **env_kw) -> None:
        for s in self.subs:
            await self.tick(s["channel"], s["filter"]["symbol"], same_ts=same_ts, **env_kw)

    async def tick(self, ch: str, sym: str, *, same_ts: bool = False, source: str = "chainlink",
                   **env_kw) -> None:
        key = (ch, sym)
        if same_ts:
            ts = self.last_ts[key]
        else:
            ts = self._next_ts(key)
        v = self.srv.value(ch, sym)
        await self.send_env(ch, {"symbol": sym, "value": v, "full_accuracy_value": f"{v:.8f}",
                                 "timestamp": ts, "source": source}, **env_kw)

    async def stream(self, interval: float = 0.05, count: int | None = None, **kw) -> None:
        n = 0
        while count is None or n < count:
            await self.tick_all(**kw)
            n += 1
            await asyncio.sleep(interval)

    async def idle(self) -> None:
        """Ничего не шлёт, но соединение держит и на ping отвечает (пока не mute_pong)."""
        await self.ws.wait_closed()


class FakePolyBolt:
    def __init__(self, creds: dict):
        self.creds = creds
        self.scripts: list = []              # по одному сценарию на соединение, потом normal
        self.conns: list[Conn] = []
        self.reject_next: list[tuple[int, dict]] = []   # HTTP-отказы на рукопожатии
        self.http_attempts = 0
        self._ticks = 0
        self._server = None
        self.url = ""

    @property
    def connections(self) -> int:
        return len(self.conns)

    def value(self, ch: str, sym: str) -> float:
        self._ticks += 1
        base = 84000.0 if sym == "btcusd" else 2400.0
        return base + (0.0 if ch == CH_TWAP else 5.0) + self._ticks * 0.01

    async def start(self) -> str:
        quiet = logging.getLogger("fake_polybolt")
        quiet.setLevel(logging.WARNING)   # серверная сторона websockets тоже печатает кадры на DEBUG
        self._server = await serve(self._handler, "127.0.0.1", 0, process_request=self._process,
                                   logger=quiet)
        port = self._server.sockets[0].getsockname()[1]
        self.url = f"ws://127.0.0.1:{port}/ws"
        return self.url

    async def close(self) -> None:
        self._server.close()
        await self._server.wait_closed()

    def _process(self, connection, request):
        self.http_attempts += 1
        if self.reject_next:
            status, headers = self.reject_next.pop(0)
            resp = connection.respond(HTTPStatus(status), "rejected\n")
            for k, v in headers.items():
                resp.headers[k] = v
            return resp
        return None

    async def _handler(self, ws) -> None:
        conn = Conn(self, ws, len(self.conns))
        self.conns.append(conn)
        script = self.scripts.pop(0) if self.scripts else normal
        try:
            await script(conn)
        except ConnectionClosed:
            pass
        finally:
            if conn._pump:
                conn._pump.cancel()


async def normal(conn: Conn) -> None:
    if await conn.handshake():
        await conn.stream()
