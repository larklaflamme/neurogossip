"""End-to-end tests: two real NeurogossipClients through a real in-process server.

These exercise the full wire protocol over real WebSocket connections — registration,
presence, end-to-end ACK (delivered/unconfirmed/queued/offline), per-pair ordering,
block list, conversation end/reopen, dedup, reconnect queued delivery, and graceful
shutdown. The e2e ACK works here (unlike a MockWS unit test) because the server runs the
sender's ``route()`` on the sender's connection coroutine while the target's ``received``
frame arrives on the target's *separate* connection coroutine — no deadlock.
"""

import asyncio
import json
import pytest
import websockets

from neurogossip_client import NeurogossipClient
from tests._helpers import make_client, Listener


# ---------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------

async def make_ack_client(server, agent_id, record=None):
    """A client that auto-sends a receipt for every incoming message.

    If ``record`` (a list) is provided, incoming messages are appended to it in addition
    to being receipted — so tests that need to inspect received messages don't have to
    overwrite the receipt handler.
    """
    c = await make_client(server, agent_id)

    async def on_message(frame):
        if record is not None:
            record.append(frame)
        await c.send_receipt(frame["msg_id"])
    c.on_message(on_message)
    return c


# ---------------------------------------------------------------------------
# Registration + directory + presence
# --------------------------------------------------------------------

async def test_two_clients_register_and_list(server):
    a = await make_client(server, "axioma")
    b = await make_client(server, "thea")
    try:
        agents = await a.list_agents()
        ids = {ag["agent_id"] for ag in agents}
        assert {"axioma", "thea"} <= ids
        # Presence broadcast (debounce=0 in fixture → immediate) populates online_agents.
        # Give the listen loop a moment to process the presence frame.
        await asyncio.sleep(0.1)
        assert "thea" in a.online_agents or "axioma" in b.online_agents
    finally:
        await a.disconnect()
        await b.disconnect()


async def test_duplicate_registration_rejected(server):
    a = await make_client(server, "axioma")
    try:
        dup = NeurogossipClient(
            server_url=f"ws://127.0.0.1:{server.port}", agent_id="axioma",
            metadata={"display_name": "Impostor", "version": "1.0"},
        )
        with pytest.raises(RuntimeError, match="Registration failed"):
            await dup.connect()
    finally:
        await a.disconnect()


async def test_bad_agent_id_rejected(server):
    c = NeurogossipClient(
        server_url=f"ws://127.0.0.1:{server.port}", agent_id="UPPER_CASE",
        metadata={"display_name": "Bad", "version": "1.0"},
    )
    with pytest.raises(RuntimeError, match="Registration failed"):
        await c.connect()


async def test_bad_metadata_rejected(server):
    c = NeurogossipClient(
        server_url=f"ws://127.0.0.1:{server.port}", agent_id="goodid",
        metadata={"display_name": "Missing version"},
    )
    with pytest.raises(RuntimeError, match="Registration failed"):
        await c.connect()


# ---------------------------------------------------------------------------
# End-to-end ACK
# --------------------------------------------------------------------

async def test_e2e_delivered(server):
    a = await make_client(server, "axioma")
    b = await make_ack_client(server, "thea")
    acks = Listener()
    a.on_ack(acks)
    try:
        msg_id = await a.send("thea", "What is the integral of e^(-x^2)?")
        ack = await acks.wait_for(lambda f: f.get("msg_id") == msg_id
                                  and f.get("status") == "delivered", timeout=3.0)
        assert ack is not None, "sender should receive a delivered ACK"
    finally:
        await a.disconnect()
        await b.disconnect()


async def test_e2e_unconfirmed_when_no_receipt(server):
    a = await make_client(server, "axioma")
    b = await make_client(server, "thea", auto_receipt=False)  # no receipt → unconfirmed
    acks = Listener()
    a.on_ack(acks)
    try:
        msg_id = await a.send("thea", "anyone there?")
        # delivery_timeout is 1.0s in the fixture → unconfirmed after ~1s.
        ack = await acks.wait_for(lambda f: f.get("msg_id") == msg_id
                                  and f.get("status") == "unconfirmed", timeout=3.0)
        assert ack is not None, "sender should receive unconfirmed after delivery timeout"
    finally:
        await a.disconnect()
        await b.disconnect()


async def test_target_not_found_error(server):
    a = await make_client(server, "axioma")
    errs = Listener()
    a.on_error(errs)
    try:
        await a.send("ghost", "hello?")
        err = await errs.wait_for(lambda f: f.get("code") == "AGENT_NOT_FOUND", timeout=3.0)
        assert err is not None
    finally:
        await a.disconnect()


# ---------------------------------------------------------------------------
# Per-pair ordering
# --------------------------------------------------------------------

async def test_per_pair_ordering_over_wire(server):
    a = await make_client(server, "axioma")
    received = []
    b = await make_ack_client(server, "thea", record=received)
    try:
        for i in range(3):
            await a.send("thea", f"m{i}")
        # Wait until all three arrive (in order).
        for _ in range(40):
            if len(received) >= 3:
                break
            await asyncio.sleep(0.05)
        seqs = [f["seq"] for f in received]
        assert seqs == [1, 2, 3]
    finally:
        await a.disconnect()
        await b.disconnect()


# ---------------------------------------------------------------------------
# Offline queue + reconnect queued delivery (B1 fix)
# --------------------------------------------------------------------

async def test_offline_queue_then_reconnect_delivered(server):
    a = await make_client(server, "axioma")
    acks = Listener()
    a.on_ack(acks)
    # b connects briefly, then disconnects so its session is offline but retained.
    b = await make_client(server, "thea")
    await b.disconnect()
    await asyncio.sleep(0.2)  # let the server process the disconnect

    # a sends while thea is offline with a TTL → queued ACK.
    msg_id = await a.send("thea", "queued hello", ttl=60)
    queued = await acks.wait_for(lambda f: f.get("msg_id") == msg_id
                                 and f.get("status") == "queued", timeout=3.0)
    assert queued is not None, "sender should receive queued ACK"

    # thea reconnects (same agent_id) and receives the queued message + auto-receipts.
    received = []

    async def on_msg(frame):
        received.append(frame)
    b2 = await make_ack_client(server, "thea")
    b2.on_message(on_msg)

    # The server delivers the queued message and sends a "delivered" ACK to a (v5.0).
    delivered = await acks.wait_for(lambda f: f.get("msg_id") == msg_id
                                    and f.get("status") == "delivered", timeout=3.0)
    assert delivered is not None, "sender should receive delivered ACK after reconnect"
    assert len(received) == 1
    assert received[0]["body"] == "queued hello"

    await a.disconnect()
    await b2.disconnect()


# ---------------------------------------------------------------------------
# Block list
# --------------------------------------------------------------------

async def test_block_over_wire(server):
    a = await make_client(server, "axioma")
    b = await make_client(server, "thea")
    errs = Listener()
    a.on_error(errs)
    try:
        await b.block("axioma")
        await asyncio.sleep(0.1)
        await a.send("thea", "still there?")
        err = await errs.wait_for(lambda f: f.get("code") == "BLOCKED", timeout=3.0)
        assert err is not None
    finally:
        await a.disconnect()
        await b.disconnect()


# ---------------------------------------------------------------------------
# Conversation end + reopen
# --------------------------------------------------------------------

async def test_conversation_end_and_reopen_over_wire(server):
    a = await make_client(server, "axioma")
    a_errs = Listener()
    a.on_error(a_errs)
    b_msgs = []
    b = await make_ack_client(server, "thea", record=b_msgs)
    try:
        await a.send("thea", "first")
        # Wait for b to receive it so we can read the conversation_id.
        for _ in range(40):
            if b_msgs:
                break
            await asyncio.sleep(0.05)
        assert b_msgs, "b should receive the first message"
        conv_id = b_msgs[0]["conversation_id"]

        # End the conversation.
        await a.end_conversation(conv_id, reason="resolved")
        await asyncio.sleep(0.2)

        # Send again without reopen → CONVERSATION_ENDED.
        await a.send("thea", "after end", conversation_id=conv_id)
        err = await a_errs.wait_for(lambda f: f.get("code") == "CONVERSATION_ENDED",
                                    timeout=3.0)
        assert err is not None

        # Reopen with reopen=True → delivered.
        n_before = len(b_msgs)
        await a.send("thea", "reopened", conversation_id=conv_id, reopen=True)
        for _ in range(40):
            if len(b_msgs) > n_before:
                break
            await asyncio.sleep(0.05)
        assert len(b_msgs) > n_before, "reopened message should be delivered"
        assert b_msgs[-1]["body"] == "reopened"
    finally:
        await a.disconnect()
        await b.disconnect()


# ---------------------------------------------------------------------------
# Deduplication
# --------------------------------------------------------------------

async def test_dedup_over_wire(server):
    a = await make_client(server, "axioma")
    b = await make_ack_client(server, "thea")
    received = []

    async def on_msg(frame):
        received.append(frame)
    b.on_message(on_msg)
    try:
        # send() generates a new msg_id each call, so drive dedup by reusing one id
        # via the raw frame path: send two messages with the same msg_id.
        msg_id = "dedup-fixed-001"
        frame = {"type": "message", "to": "thea", "body": "first",
                 "msg_id": msg_id, "reply_to": None, "conversation_id": None, "ttl": 0}
        await a._send_frame(frame)
        await asyncio.sleep(0.2)
        await a._send_frame(frame)  # duplicate
        await asyncio.sleep(0.3)
        assert len(received) == 1, "duplicate msg_id should not be re-delivered"
    finally:
        await a.disconnect()
        await b.disconnect()


# ---------------------------------------------------------------------------
# Graceful shutdown
# --------------------------------------------------------------------

async def test_graceful_shutdown_notifies_clients(server):
    a = await make_client(server, "axioma")
    b = await make_client(server, "thea")
    a_seen = Listener()
    a.on_error(a_seen)  # not used; we watch via a custom handler on shutdown
    shutdowns = Listener()

    async def on_any(frame):
        # We can't register a generic handler, so hook via on_presence is wrong;
        # instead inspect the listen loop indirectly: register on_message won't catch
        # shutdown. Use a patched _handle_frame wrapper.
        pass
    # Monkey-patch a._handle_frame to capture shutdown frames.
    orig = a._handle_frame

    async def patched(frame):
        if frame.get("type") == "shutdown":
            await shutdowns(frame)
        return await orig(frame)
    a._handle_frame = patched

    try:
        # Trigger graceful shutdown (runs as a task; grace period is 0.1s in fixture).
        shutdown_task = asyncio.create_task(server.shutdown())
        sh = await shutdowns.wait_for(lambda f: f.get("type") == "shutdown", timeout=3.0)
        assert sh is not None, "clients should receive a shutdown notification"
        await asyncio.wait_for(shutdown_task, timeout=5.0)
    finally:
        # The server is already stopped by shutdown(); just close clients.
        try:
            await a.disconnect()
        except Exception:
            pass
        try:
            await b.disconnect()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Heartbeat: client auto-responds to ping → stays online
# --------------------------------------------------------------------

async def test_heartbeat_keeps_client_online(server):
    # The fixture server has heartbeat_interval=0.5, max_missed=3. The client auto-pongs
    # on ping. After several intervals the agent must still be registered + online.
    a = await make_client(server, "axioma")
    try:
        await asyncio.sleep(1.5)  # ~3 heartbeat intervals
        assert "axioma" in server.registry.agents
        assert server.registry.agents["axioma"].is_online is True
    finally:
        await a.disconnect()


async def test_heartbeat_declares_silent_client_offline(server):
    # A raw websocket that registers but never responds to pings should be declared
    # offline after max_missed pings.
    ws = await websockets.connect(f"ws://127.0.0.1:{server.port}")
    await ws.send(json.dumps({"type": "register", "agent_id": "silent",
                              "metadata": {"display_name": "Silent", "version": "1.0"}}))
    raw = await ws.recv()
    assert json.loads(raw)["type"] == "registered"
    try:
        # Do not respond to pings. Wait for the server to declare offline.
        for _ in range(40):
            if not server.registry.agents["silent"].is_online:
                break
            await asyncio.sleep(0.1)
        assert server.registry.agents["silent"].is_online is False
    finally:
        await ws.close()


# ---------------------------------------------------------------------------
# Connection-handler edge cases (invalid JSON, unknown type, pre-register,
# list_agents, block/unblock over the wire, disconnect cleanup)
# --------------------------------------------------------------------

async def test_invalid_json_returns_error(server):
    ws = await websockets.connect(f"ws://127.0.0.1:{server.port}")
    try:
        await ws.send("not json at all")
        raw = await ws.recv()
        frame = json.loads(raw)
        assert frame["type"] == "error"
        assert frame["code"] == "BAD_REQUEST"
    finally:
        await ws.close()


async def test_message_before_register_rejected(server):
    ws = await websockets.connect(f"ws://127.0.0.1:{server.port}")
    try:
        await ws.send(json.dumps({"type": "message", "to": "thea", "body": "hi"}))
        raw = await ws.recv()
        frame = json.loads(raw)
        assert frame["type"] == "error"
        assert frame["code"] == "BAD_REGISTRATION"
    finally:
        await ws.close()


async def test_unknown_message_type_returns_error(server):
    ws = await websockets.connect(f"ws://127.0.0.1:{server.port}")
    try:
        await ws.send(json.dumps({"type": "register", "agent_id": "x",
                                  "metadata": {"display_name": "X", "version": "1.0"}}))
        await ws.recv()  # registered
        await ws.send(json.dumps({"type": "frobnicate"}))
        raw = await ws.recv()
        # Skip any ping/presence frames; look for the BAD_REQUEST error.
        for _ in range(5):
            frame = json.loads(raw)
            if frame.get("type") == "error" and frame.get("code") == "BAD_REQUEST":
                break
            raw = await ws.recv()
        assert frame["type"] == "error"
        assert frame["code"] == "BAD_REQUEST"
    finally:
        await ws.close()


async def test_list_agents_over_wire(server):
    # Register one agent via the library, then query via a raw connection.
    a = await make_client(server, "axioma")
    ws = await websockets.connect(f"ws://127.0.0.1:{server.port}")
    try:
        await ws.send(json.dumps({"type": "register", "agent_id": "thea",
                                   "metadata": {"display_name": "Thea", "version": "1.0"}}))
        await ws.recv()  # registered
        # Drain any presence frame, then request the list.
        await ws.send(json.dumps({"type": "list_agents"}))
        agent_list = None
        for _ in range(10):
            frame = json.loads(await ws.recv())
            if frame.get("type") == "agent_list":
                agent_list = frame
                break
        assert agent_list is not None
        ids = {ag["agent_id"] for ag in agent_list["agents"]}
        assert {"axioma", "thea"} <= ids
    finally:
        await ws.close()
        await a.disconnect()


async def test_block_and_unblock_over_wire(server):
    a = await make_client(server, "axioma")
    b = await make_client(server, "thea")
    a_errs = Listener()
    a.on_error(a_errs)
    try:
        await b.block("axioma")
        await asyncio.sleep(0.1)
        await a.send("thea", "hi")
        err = await a_errs.wait_for(lambda f: f.get("code") == "BLOCKED", timeout=3.0)
        assert err is not None

        await b.unblock("axioma")
        await asyncio.sleep(0.1)
        a_errs2 = Listener()
        a.on_error(a_errs2)
        acks = Listener()
        a.on_ack(acks)
        msg_id = await a.send("thea", "hi again")
        # No BLOCKED error this time; should get a pending/delivered ack.
        delivered = await acks.wait_for(lambda f: f.get("msg_id") == msg_id
                                        and f.get("status") in ("pending", "delivered"),
                                        timeout=3.0)
        assert delivered is not None
        blocked = a_errs2.wait_for(lambda f: f.get("code") == "BLOCKED", timeout=0.2)
        assert await blocked is None
    finally:
        await a.disconnect()
        await b.disconnect()


async def test_disconnect_removes_from_online(server):
    a = await make_client(server, "axioma")
    await a.disconnect()
    await asyncio.sleep(0.2)
    # The agent record may be retained (offline) but is no longer online.
    sess = server.registry.agents.get("axioma")
    assert sess is None or sess.is_online is False