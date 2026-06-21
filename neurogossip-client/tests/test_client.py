"""Unit tests for client.py — API surface, frame routing, and send methods.

These bypass the real network by injecting a FakeWS into the client and driving
``_handle_frame`` / send methods directly. End-to-end behavior over a real server is
covered by ``test_integration.py``.
"""

import asyncio
import json
import pytest

from neurogossip_client import NeurogossipClient


class FakeWS:
    """Stand-in for a connected WebSocket."""

    def __init__(self):
        self.sent = []
        self.closed = False

    async def send(self, msg):
        self.sent.append(json.loads(msg))

    async def close(self):
        self.closed = True

    def recv(self):
        raise RuntimeError("not used in unit tests")


def make_client(agent_id="axioma"):
    c = NeurogossipClient(
        server_url="ws://example.invalid:8765",
        agent_id=agent_id,
        metadata={"display_name": agent_id.title(), "version": "1.0",
                  "capabilities": ["test"]},
    )
    c.websocket = FakeWS()
    c._running = True
    c._connected = True
    return c


# ---------------------------------------------------------------------------
# Construction + properties
# --------------------------------------------------------------------

def test_construction_defaults():
    c = NeurogossipClient("ws://x", "axioma")
    assert c.agent_id == "axioma"
    assert c.is_connected is False
    assert c.online_agents == []
    assert c.session_id is None
    assert c.metadata == {}


def test_construction_with_metadata():
    c = NeurogossipClient("ws://x", "axioma",
                          metadata={"display_name": "A", "version": "1.0"},
                          auth_token="sekret")
    assert c.metadata["display_name"] == "A"
    assert c.auth_token == "sekret"


async def _cb(frame, sink):
    sink.append(frame)


def test_callback_registration():
    c = make_client()

    async def cb(f):
        return f
    c.on_message(cb)
    c.on_presence(cb)
    c.on_ack(cb)
    c.on_conversation_ended(cb)
    c.on_error(cb)
    assert c._on_message is cb
    assert c._on_presence is cb
    assert c._on_ack is cb
    assert c._on_conversation_ended is cb
    assert c._on_error is cb


def test_send_receipt_is_public():
    c = make_client()
    assert callable(c.send_receipt)
    # The design requires send_receipt to be a PUBLIC method (v4.0 review N5).
    assert not c.send_receipt.__name__.startswith("_")


# ---------------------------------------------------------------------------
# Send methods build the right frames
# --------------------------------------------------------------------

async def test_send_builds_frame():
    c = make_client()
    ws = c.websocket
    msg_id = await c.send("thea", "hello", reply_to="m0", conversation_id="c1", ttl=30)
    assert msg_id  # generated uuid
    assert ws.sent[-1] == {
        "type": "message", "to": "thea", "body": "hello", "msg_id": msg_id,
        "reply_to": "m0", "conversation_id": "c1", "ttl": 30, "reopen": False,
    }


async def test_send_reopen_flag():
    c = make_client()
    await c.send("thea", "again", conversation_id="c1", reopen=True)
    assert c.websocket.sent[-1]["reopen"] is True


async def test_send_receipt_builds_frame():
    c = make_client()
    await c.send_receipt("m123")
    assert c.websocket.sent[-1] == {"type": "received", "msg_id": "m123"}


async def test_end_conversation_builds_frame():
    c = make_client()
    await c.end_conversation("conv1", reason="done")
    assert c.websocket.sent[-1] == {
        "type": "conversation_end", "conversation_id": "conv1", "reason": "done",
    }


async def test_end_conversation_default_reason():
    c = make_client()
    await c.end_conversation("conv1")
    assert c.websocket.sent[-1]["reason"] == "ended"


async def test_block_unblock_build_frames():
    c = make_client()
    await c.block("skye")
    assert c.websocket.sent[-1] == {"type": "block", "agent_id": "skye"}
    await c.unblock("skye")
    assert c.websocket.sent[-1] == {"type": "unblock", "agent_id": "skye"}


async def test_send_raises_when_not_connected():
    c = NeurogossipClient("ws://x", "a")
    with pytest.raises(RuntimeError):
        await c.send("b", "hi")


# ---------------------------------------------------------------------------
# list_agents future handling (C2 fix)
# --------------------------------------------------------------------

async def test_list_agents_resolves():
    c = make_client()
    ws = c.websocket
    fut = asyncio.create_task(c.list_agents())
    await asyncio.sleep(0)  # let the request frame be sent
    assert ws.sent[-1] == {"type": "list_agents"}
    # Server replies:
    await c._handle_frame({"type": "agent_list", "agents": [{"agent_id": "b"}]})
    assert await fut == [{"agent_id": "b"}]


async def test_list_agents_concurrent_calls_do_not_clobber():
    c = make_client()
    f1 = asyncio.create_task(c.list_agents())
    f2 = asyncio.create_task(c.list_agents())
    await asyncio.sleep(0)
    # Two requests queued; two responses resolve them in order.
    await c._handle_frame({"type": "agent_list", "agents": [{"agent_id": "x"}]})
    await c._handle_frame({"type": "agent_list", "agents": [{"agent_id": "y"}]})
    assert (await f1) == [{"agent_id": "x"}]
    assert (await f2) == [{"agent_id": "y"}]


# ---------------------------------------------------------------------------
# Frame routing (_handle_frame)
# --------------------------------------------------------------------

async def test_handle_message_invokes_callback_and_queues():
    c = make_client()
    seen = []

    async def cb(f):
        seen.append(f)
    c.on_message(cb)
    msg = {"type": "message", "from": "thea", "body": "hi", "msg_id": "m1",
           "reply_to": None, "conversation_id": "c", "seq": 1, "depth": 1, "ts": "t"}
    await c._handle_frame(msg)
    assert seen == [msg]
    # Also available via wait_for_message
    got = await c.wait_for_message(timeout=0.1)
    assert got is msg


async def test_handle_presence_updates_online_agents():
    c = make_client()
    events = []

    async def cb(e):
        events.append(e)
    c.on_presence(cb)
    await c._handle_frame({"type": "presence", "changes": [
        {"agent_id": "thea", "status": "online", "metadata": {"display_name": "Thea"}},
        {"agent_id": "skye", "status": "offline"},
    ]})
    assert "thea" in c.online_agents
    assert "skye" not in c.online_agents
    assert events  # callback fired


async def test_handle_ack_invokes_callback():
    c = make_client()
    acks = []

    async def cb(a):
        acks.append(a)
    c.on_ack(cb)
    await c._handle_frame({"type": "message_ack", "msg_id": "m1", "status": "delivered",
                           "to": "thea", "ts": "t"})
    assert acks[0]["status"] == "delivered"


async def test_handle_conversation_ended():
    c = make_client()
    evts = []

    async def cb(e):
        evts.append(e)
    c.on_conversation_ended(cb)
    await c._handle_frame({"type": "conversation_ended", "conversation_id": "c",
                           "reason": "resolved", "by": "thea", "ts": "t"})
    assert evts[0]["conversation_id"] == "c"


async def test_handle_error_invokes_callback():
    c = make_client()
    errs = []

    async def cb(e):
        errs.append(e)
    c.on_error(cb)
    await c._handle_frame({"type": "error", "code": "RATE_LIMITED", "message": "slow down",
                           "msg_id": "m1"})
    assert errs[0]["code"] == "RATE_LIMITED"


async def test_handle_ping_responds_pong():
    c = make_client()
    await c._handle_frame({"type": "ping", "server_time": "t"})
    assert c.websocket.sent[-1]["type"] == "pong"
    assert "agent_time" in c.websocket.sent[-1]


async def test_handle_shutdown_sets_disconnected():
    c = make_client()
    await c._handle_frame({"type": "shutdown", "reason": "server_going_down"})
    assert c.is_connected is False


async def test_handle_registered_updates_session():
    c = make_client()
    await c._handle_frame({"type": "registered", "agent_id": "axioma",
                           "session_id": "sess_new", "heartbeat_interval_s": 7})
    assert c.session_id == "sess_new"
    assert c.heartbeat_interval_s == 7


async def test_handle_unknown_frame_does_not_raise():
    c = make_client()
    await c._handle_frame({"type": "mystery"})
    # Should be a no-op (logged at debug).


# ---------------------------------------------------------------------------
# wait_for_message timeout
# --------------------------------------------------------------------

async def test_wait_for_message_timeout_returns_none():
    c = make_client()
    assert await c.wait_for_message(timeout=0.05) is None


# ---------------------------------------------------------------------------
# disconnect
# --------------------------------------------------------------------

async def test_disconnect_closes_websocket():
    c = make_client()
    ws = c.websocket
    # Start a listen loop so we can verify it gets cancelled.
    c._listen_task = asyncio.create_task(c._listen_loop())
    c._reconnect_task = asyncio.create_task(c._watch_connection())
    c._heartbeat_task = asyncio.create_task(c._heartbeat_loop())
    await c.disconnect()
    assert ws.closed is True
    assert c.is_connected is False
    assert c._running is False


# ---------------------------------------------------------------------------
# _register
# --------------------------------------------------------------------

async def test_register_success():
    c = NeurogossipClient("ws://x", "axioma",
                          metadata={"display_name": "A", "version": "1.0"})
    c.websocket = FakeWS()
    # _register sends a frame then reads one frame.
    async def fake_recv():
        return json.dumps({"type": "registered", "agent_id": "axioma",
                           "session_id": "s1", "heartbeat_interval_s": 15})
    c.websocket.recv = fake_recv
    await c._register()
    assert c.session_id == "s1"
    assert c.websocket.sent[0]["type"] == "register"
    assert c.websocket.sent[0]["agent_id"] == "axioma"


async def test_register_with_auth_token():
    c = NeurogossipClient("ws://x", "axioma",
                          metadata={"display_name": "A", "version": "1.0"},
                          auth_token="sekret")
    c.websocket = FakeWS()
    async def fake_recv():
        return json.dumps({"type": "registered", "agent_id": "axioma",
                           "session_id": "s1", "heartbeat_interval_s": 15})
    c.websocket.recv = fake_recv
    await c._register()
    assert c.websocket.sent[0]["auth_token"] == "sekret"


async def test_register_error_raises():
    c = NeurogossipClient("ws://x", "axioma",
                          metadata={"display_name": "A", "version": "1.0"})
    c.websocket = FakeWS()
    async def fake_recv():
        return json.dumps({"type": "error", "code": "BAD_REGISTRATION",
                           "message": "nope"})
    c.websocket.recv = fake_recv
    with pytest.raises(RuntimeError, match="Registration failed"):
        await c._register()


async def test_register_unexpected_response_raises():
    c = NeurogossipClient("ws://x", "axioma",
                          metadata={"display_name": "A", "version": "1.0"})
    c.websocket = FakeWS()
    async def fake_recv():
        return json.dumps({"type": "presence", "changes": []})
    c.websocket.recv = fake_recv
    with pytest.raises(RuntimeError, match="Unexpected response"):
        await c._register()