"""Shared fixtures: a shared fakeredis server so multiple NeuroGossipClient instances
see the same Redis state (pub/sub + streams + keys) without a Docker Redis."""
from __future__ import annotations

import pytest
import fakeredis
from fakeredis import FakeAsyncRedis

from neurogossip_client import AgentIdentity, NeuroGossipClient


@pytest.fixture
def fake_server() -> fakeredis.FakeServer:
    return fakeredis.FakeServer()


def _make(agent_id: str, fake_server: fakeredis.FakeServer, **kw) -> NeuroGossipClient:
    """Build a client backed by a shared fakeredis server (no real Redis, no REDIS_URL)."""
    defaults = dict(heartbeat_interval_s=999.0, presence_ttl_s=999.0)  # off by default
    defaults.update(kw)  # test overrides win
    return NeuroGossipClient(
        agent=AgentIdentity(agent_id=agent_id, agent_name=agent_id.upper()),
        namespace="test",
        redis_client=FakeAsyncRedis(server=fake_server, decode_responses=True),
        **defaults,
    )


@pytest.fixture
def make_client(fake_server):
    """Factory: make_client("skye") -> connected-ready NeuroGossipClient (call 'async with')."""
    def _factory(agent_id: str, **kw) -> NeuroGossipClient:
        return _make(agent_id, fake_server, **kw)
    return _factory