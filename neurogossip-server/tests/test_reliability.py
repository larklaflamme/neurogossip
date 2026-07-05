"""Reliability soak — 4 agents exchanging messages through the in-process server.

Runs the same all-to-all + reply-conversation pattern as ``examples/reliable_demo.py``
against a real in-process ``NeurogossipServer`` for a configurable duration (default
90 s, override with ``NEUROGOSSIP_RELIABILITY_DURATION``). Asserts that every sent
message was delivered (receipt) and no agent was unexpectedly disconnected during the
run — i.e. the redesigned server sustains reliable agent-to-agent messaging under load.
"""
import asyncio
import logging
import os
import socket
import time
from contextlib import closing

import pytest

from neurogossip_client import NeurogossipClient
from neurogossip_server.server import NeurogossipServer

AGENTS = ["axioma", "skye", "thea", "theoria"]


def _duration() -> float:
    return float(os.getenv("NEUROGOSSIP_RELIABILITY_DURATION", "90"))


def _free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
async def reliable_server():
    """A server tuned for a sustained all-to-all stress run: fast timeouts + raised
    rate/circuit-breaker limits so the reliability run isn't throttled by the
    default flood protections (which are correct for untrusted load, not for this
    cooperative soak)."""
    srv = NeurogossipServer(
        host="127.0.0.1", port=_free_port(),
        heartbeat_interval=0.5, max_missed=3,
        delivery_timeout=2.0, presence_debounce=0.0,
        rate_limit_agent=6000, rate_limit_pair=6000,
        circuit_breaker_max=100000, circuit_breaker_cooldown=1.0,
        ws_ping_interval=20.0, ws_ping_timeout=45.0,
        log_level="CRITICAL",
    )
    srv.grace_period_s = 0.1
    await srv.serve()
    try:
        yield srv
    finally:
        await srv.stop()


class _Agent:
    def __init__(self, server, agent_id: str, stats: dict, log: logging.Logger):
        self.id = agent_id
        self.stats = stats
        self.client = NeurogossipClient(
            server_url=f"ws://127.0.0.1:{server.port}",
            agent_id=agent_id,
            metadata={"display_name": agent_id.title(), "version": "1.0"},
            auto_receipt=True,
            open_timeout_s=10.0,
            logger=log,
        )
        self.client.on_message(self._on_message)
        self.client.on_ack(self._on_ack)
        self.client.on_error(self._on_error)
        self.peers = [a for a in AGENTS if a != agent_id]
        self._n = 0

    async def start(self):
        await self.client.connect()

    async def stop(self):
        try:
            await self.client.disconnect()
        except Exception:
            pass

    async def send_all(self):
        for peer in self.peers:
            self._n += 1
            try:
                mid = await self.client.send(peer, f"msg-{self._n}", ttl=60)
                self.stats["sent"][mid] = (self.id, peer)
            except Exception:
                self.stats["send_errors"] += 1

    async def _on_message(self, frame):
        if frame.get("reply_to") is not None:
            return  # don't reply to replies (bounded conversations)
        peer = frame.get("from")
        mid = frame.get("msg_id")
        if peer and mid:
            try:
                rid = await self.client.send(peer, "reply", reply_to=mid, ttl=60)
                self.stats["sent"][rid] = (self.id, peer)  # track replies for loss check
                self.stats["replies"] += 1
            except Exception:
                self.stats["send_errors"] += 1

    async def _on_ack(self, ack):
        mid = ack.get("msg_id")
        if mid is None:
            return
        self.stats["acks"][mid] = ack.get("status")
        if ack.get("status") == "delivered":
            self.stats["delivered"] += 1
        elif ack.get("status") in ("failed", "offline", "unconfirmed", "ttl_expired"):
            self.stats["lost_detail"].append((mid, ack.get("status")))

    async def _on_error(self, frame):
        self.stats["server_errors"] += 1


@pytest.mark.asyncio
async def test_four_agents_reliable_exchange(reliable_server, caplog):
    """4 agents exchange messages for the configured duration with zero losses."""
    duration = _duration()
    log = logging.getLogger("reliability")
    stats: dict = {
        "sent": {}, "acks": {}, "delivered": 0, "replies": 0,
        "send_errors": 0, "server_errors": 0, "lost_detail": [],
    }
    agents = [_Agent(reliable_server, aid, stats, log) for aid in AGENTS]
    await asyncio.gather(*(a.start() for a in agents))

    # Watch for unexpected "Connection lost" reconnects during the run.
    reconnects = []

    def _filter(record):
        if "Connection lost, reconnecting" in record.getMessage():
            reconnects.append(record.getMessage())
        return False
    caplog.set_level(logging.WARNING, logger="client.thea")
    handler = logging.Handler()
    handler.filter = _filter
    for aid in AGENTS:
        logging.getLogger(f"client.{aid}").addHandler(handler)

    async def ticker():
        end = time.monotonic() + duration
        while time.monotonic() < end:
            await asyncio.gather(*(a.send_all() for a in agents))
            await asyncio.sleep(1.0)

    try:
        await asyncio.wait_for(ticker(), timeout=duration + 10)
    finally:
        await asyncio.sleep(3.0)  # drain in-flight ACKs
        for a in agents:
            assert a.client.is_connected, f"{a.id} disconnected during the run"
        await asyncio.gather(*(a.stop() for a in agents), return_exceptions=True)
        for aid in AGENTS:
            logging.getLogger(f"client.{aid}").removeHandler(handler)

    # Reconcile: every sent message should have ended "delivered".
    lost = [(mid, frm, to, stats["acks"].get(mid))
            for mid, (frm, to) in stats["sent"].items()
            if stats["acks"].get(mid) != "delivered"]

    print(f"\n[reliability] duration={duration}s sent={len(stats['sent'])} "
          f"delivered={stats['delivered']} replies={stats['replies']} "
          f"send_errors={stats['send_errors']} server_errors={stats['server_errors']} "
          f"reconnects={len(reconnects)} lost={len(lost)}")

    assert stats["server_errors"] == 0, f"server errors: {stats['server_errors']}"
    assert len(reconnects) == 0, f"unexpected reconnects: {reconnects[:3]}"
    assert lost == [], f"{len(lost)} messages not delivered: {lost[:5]}"