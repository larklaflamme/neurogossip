"""Shared pytest fixtures for the neurogossip-server test suite.

The fixtures spin up a real in-process ``NeurogossipServer`` on an ephemeral port with
fast heartbeat/delivery-timeout/presence-debounce settings, and connect real
``neurogossip_client.NeurogossipClient`` instances to it. Exercising the end-to-end ACK
path over real connections avoids the deadlock a MockWS-based test hits (``route()``
blocks waiting for a receipt that can only arrive on the *target's* separate connection
coroutine).

Run with both packages installed editable::

    pip install -e ../neurogossip-client
    pip install -e ".[test]"
    pytest
"""

import logging
import socket
from contextlib import closing

import pytest

from neurogossip_server.server import NeurogossipServer
from tests._helpers import make_client  # noqa: F401  (re-exported for test modules)

# Silence server/client logging during tests.
for name in ("neurogossip", "heartbeat", "router", "presence"):
    logging.getLogger(name).setLevel(logging.CRITICAL)


def _free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
async def server():
    """A running in-process NeurogossipServer with fast timeouts for tests."""
    port = _free_port()
    srv = NeurogossipServer(
        host="127.0.0.1",
        port=port,
        heartbeat_interval=0.5,
        max_missed=3,
        delivery_timeout=1.0,
        presence_debounce=0.0,  # immediate flush so tests can assert presence directly
        max_conversation_depth=20,
        circuit_breaker_window=60.0,
        circuit_breaker_max=30,
        circuit_breaker_cooldown=120.0,
        rate_limit_agent=600,
        rate_limit_pair=600,
        log_level="CRITICAL",
    )
    srv.grace_period_s = 0.1  # fast graceful shutdown in tests
    await srv.serve()
    try:
        yield srv
    finally:
        await srv.stop()


@pytest.fixture
def server_url(server):
    return f"ws://127.0.0.1:{server.port}"


@pytest.fixture
async def two_clients(server):
    """Two connected clients (a, b) for end-to-end tests."""
    a = await make_client(server, "a")
    b = await make_client(server, "b")
    try:
        yield a, b
    finally:
        await a.disconnect()
        await b.disconnect()