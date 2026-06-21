"""Pytest config + fixtures for the neurogossip-client test suite.

The client package must be testable on its own (it does not depend on
``neurogossip-server``). These fixtures spin up a *tiny* in-process WebSocket server
that speaks just enough of the Neurogossip wire protocol to exercise the client's
real connection lifecycle: registration, the listen loop, the heartbeat loop,
presence/agent_list/message handling, reconnection, and graceful disconnect.
"""

import asyncio
import json
import os
import socket
import sys
from contextlib import closing

import pytest
import websockets
from websockets.asyncio.server import serve

_SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from neurogossip_client import NeurogossipClient  # noqa: E402


def _free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class StubServer:
    """Minimal Neurogossip-protocol server for client connection-lifecycle tests.

    Tracks registered agents and supports the frames the client sends/expects:
    register → registered, list_agents → agent_list, message → echo back as a
    delivered message, received/conversation_end/block/unblock/pong → acknowledged.
    """

    def __init__(self):
        self.port = _free_port()
        self.received = []          # frames the server has seen, newest last
        self.connections = 0        # number of accepted connections (for reconnect tests)
        self._server = None
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    async def start(self):
        self._server = await serve(self._handler, "127.0.0.1", self.port)
        self.url = f"ws://127.0.0.1:{self.port}"

    async def stop(self):
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        for t in self._tasks:
            t.cancel()

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, v):
        self._url = v

    async def _handler(self, ws):
        self.connections += 1
        # First frame must be register.
        raw = await ws.recv()
        frame = json.loads(raw)
        self.received.append(frame)
        agent_id = frame.get("agent_id", "x")
        await ws.send(json.dumps({
            "type": "registered", "agent_id": agent_id,
            "session_id": f"sess-{agent_id}-{self.connections}",
            "heartbeat_interval_s": 0.2,
        }))
        # Optionally broadcast a presence change so the client populates online_agents.
        await ws.send(json.dumps({"type": "presence", "changes": [
            {"agent_id": "other", "status": "online",
             "metadata": {"display_name": "Other", "version": "1.0"}},
        ]}))

        async def pinger():
            while True:
                try:
                    await ws.send(json.dumps({"type": "ping", "server_time": "t"}))
                except Exception:
                    return
                await asyncio.sleep(0.1)

        ping_task = asyncio.create_task(pinger())
        try:
            async for raw in ws:
                frame = json.loads(raw)
                self.received.append(frame)
                t = frame.get("type")
                if t == "list_agents":
                    await ws.send(json.dumps({"type": "agent_list", "agents": [
                        {"agent_id": "other", "status": "online",
                         "metadata": {"display_name": "Other", "version": "1.0"}},
                    ]}))
                elif t == "message":
                    # Echo back as a message delivered to the sender.
                    await ws.send(json.dumps({
                        "type": "message", "from": frame.get("to"),
                        "body": f"echo:{frame.get('body')}",
                        "msg_id": f"srv-{frame.get('msg_id')}",
                        "reply_to": frame.get("msg_id"),
                        "conversation_id": "conv-stub", "seq": 1, "depth": 1,
                        "ts": "t",
                    }))
                # received / conversation_end / block / unblock / pong → no response needed
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            ping_task.cancel()


@pytest.fixture
async def stub():
    s = StubServer()
    await s.start()
    try:
        yield s
    finally:
        await s.stop()