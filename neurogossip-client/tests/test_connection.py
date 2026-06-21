"""Connection-lifecycle tests for NeurogossipClient against a tiny in-process stub server.

These exercise the real WebSocket paths that the FakeWS unit tests bypass:
connect/_register, the listen loop, the proactive heartbeat loop, directory query,
send + receive echo, explicit receipt, reconnection after the server drops the
connection, and graceful disconnect.
"""

import asyncio
import json

import pytest
import websockets

from neurogossip_client import NeurogossipClient
from tests._helpers import make_client


async def test_connect_registers_and_list_agents(stub):
    c = make_client(stub)
    await c.connect()
    try:
        assert c.is_connected
        assert c.session_id.startswith("sess-axioma-")
        agents = await c.list_agents()
        assert any(a["agent_id"] == "other" for a in agents)
        # presence broadcast populated online_agents
        await asyncio.sleep(0.1)
        assert "other" in c.online_agents
    finally:
        await c.disconnect()
    assert c.is_connected is False


async def test_send_and_receive_echo(stub):
    c = make_client(stub)
    received = []

    async def on_msg(frame):
        received.append(frame)
    c.on_message(on_msg)
    await c.connect()
    try:
        msg_id = await c.send("other", "hello")
        # Wait for the echo to arrive.
        for _ in range(40):
            if received:
                break
            await asyncio.sleep(0.05)
        assert received, "client should receive the echoed message"
        assert received[0]["body"] == "echo:hello"
        assert received[0]["reply_to"] == msg_id

        # The server received our message + we can send an explicit receipt.
        await c.send_receipt(received[0]["msg_id"])
        # wait_for_message also returns the queued frame.
        again = await c.wait_for_message(timeout=0.05)
        assert again is received[0]
    finally:
        await c.disconnect()


async def test_heartbeat_loop_runs(stub):
    c = make_client(stub)
    await c.connect()
    try:
        # The client's proactive heartbeat loop sends pongs every heartbeat_interval_s
        # (0.2s from the stub). Let it run a few cycles.
        await asyncio.sleep(0.5)
        assert c.is_connected
        pongs = [f for f in stub.received if f.get("type") == "pong"]
        assert pongs, "client should be sending proactive pongs"
        # The server sent pings; the client should have answered them too.
        # (pong responses are also pongs — covered above.)
    finally:
        await c.disconnect()


async def test_reconnect_after_server_drops_connection(stub):
    c = make_client(stub)
    await c.connect()
    first_session = c.session_id
    try:
        assert c.is_connected
        # Drop the underlying connection from the client side to simulate a dead link.
        # The _watch_connection loop polls every 1s, then reconnects.
        await c.websocket.close()
        # Wait for the watcher to notice and reconnect.
        for _ in range(40):
            if c.is_connected and c.session_id != first_session:
                break
            await asyncio.sleep(0.1)
        assert c.is_connected, "client should auto-reconnect"
        assert c.session_id != first_session, "reconnect should yield a new session"
        # And it should still be functional.
        agents = await c.list_agents()
        assert any(a["agent_id"] == "other" for a in agents)
        assert stub.connections >= 2, "stub should have accepted a second connection"
    finally:
        await c.disconnect()


async def test_disconnect_cancels_tasks(stub):
    c = make_client(stub)
    await c.connect()
    listen = c._listen_task
    watch = c._reconnect_task
    hb = c._heartbeat_task
    await c.disconnect()
    # Give cancellation a moment to settle.
    await asyncio.sleep(0.05)
    assert listen.cancelled() or listen.done()
    assert watch.cancelled() or watch.done()
    assert hb.cancelled() or hb.done()
    assert c._running is False


async def test_connect_failure_raises(stub):
    # Point at a port nothing listens on.
    c = NeurogossipClient(
        server_url="ws://127.0.0.1:1",  # port 1 — nothing there
        agent_id="axioma",
        metadata={"display_name": "A", "version": "1.0"},
    )
    with pytest.raises(Exception):
        await c.connect()


async def test_send_receipt_is_delivered(stub):
    c = make_client(stub)
    await c.connect()
    try:
        await c.send_receipt("some-msg-id")
        # Let the listen loop flush it.
        await asyncio.sleep(0.1)
        assert any(f.get("type") == "received" and f.get("msg_id") == "some-msg-id"
                   for f in stub.received)
    finally:
        await c.disconnect()


async def test_reconnect_failure_backoff_then_success(stub):
    """A failed reconnect attempt backs off (configurable) then succeeds on retry."""
    c = make_client(stub, reconnect_backoff_s=0.02)
    await c.connect()
    first_session = c.session_id
    try:
        # Make the first reconnect attempt fail, then restore real behavior.
        original = c._connect_socket
        calls = {"n": 0}

        async def flaky_connect():
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("simulated network blip")
            return await original()

        c._connect_socket = flaky_connect
        # Drop the connection so the watcher triggers a reconnect.
        await c.websocket.close()
        for _ in range(60):
            if c.is_connected and c.session_id != first_session:
                break
            await asyncio.sleep(0.05)
        assert c.is_connected, "client should reconnect after a failed attempt + backoff"
        assert calls["n"] >= 2, "the first reconnect attempt should have failed"
        assert c.session_id != first_session
    finally:
        await c.disconnect()