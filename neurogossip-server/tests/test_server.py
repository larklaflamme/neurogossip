"""Unit tests for server.py — Registry, HeartbeatEngine, MessageRouter,
PresenceBroadcaster, CliRenderer, metadata/agent_id validation, pruning.

The end-to-end ACK path is exercised with a MockWS that auto-acks received messages
via a concurrent task, mirroring how the real server receives the ``received`` frame on
the target's *separate* connection coroutine (which is what makes e2e ACK deadlock-free).
"""

import asyncio
import json
import os
import time
import uuid
from collections import deque

import pytest

from neurogossip_server.server import (
    AGENT_ID_RE,
    AgentSession,
    CircuitBreakerRecord,
    ConversationRecord,
    HeartbeatEngine,
    MessageRecord,
    MessageRouter,
    NeurogossipServer,
    PresenceBroadcaster,
    Registry,
    _SuppressConnectionClosedFilter,
    _validate_metadata,
)


# ---------------------------------------------------------------------------
# Mock WebSocket
# ---------------------------------------------------------------------------

class MockWS:
    """Minimal async WebSocket stand-in for unit tests.

    ``on_message`` is an optional async callback invoked (as a task) whenever a frame of
    type ``message`` is "sent" to this ws — used to auto-ack receipts and unblock the
    router's end-to-end ACK wait.
    """

    def __init__(self, on_message=None, *, close_code=None, live=True):
        self.sent = []
        self.closed = False
        self._on_message = on_message
        # websockets parity: ``close_code`` is None while the socket is open; a live
        # session pongs immediately. Tests can force a dead socket via close_code /
        # live=False to exercise stale-session eviction.
        self.close_code = close_code
        self._live = live

    async def send(self, msg):
        frame = json.loads(msg)
        self.sent.append(frame)
        if self._on_message and frame.get("type") == "message":
            asyncio.create_task(self._on_message(frame))

    async def ping(self, data=None):
        # Mimic a live socket: pong arrives immediately. A dead socket would raise.
        if not self._live:
            raise OSError("connection closed")
        return 0.0

    async def close(self):
        self.closed = True
        self.close_code = 1000


def make_session(registry, agent_id, ws=None, **kw):
    ws = ws or MockWS()
    sess = AgentSession(
        agent_id=agent_id, websocket=ws,
        metadata={"display_name": agent_id.upper(), "version": "1.0"},
        session_id=f"sess_{agent_id}", connected_at=time.monotonic(),
        last_pong_time=time.monotonic(), **kw,
    )
    registry.agents[agent_id] = sess
    return sess, ws


def auto_ack_cb(router, agent_id):
    async def cb(frame):
        await router.on_received(agent_id, {"msg_id": frame["msg_id"]})
    return cb


async def _drain(n=12):
    """Yield control so the per-connection writer tasks + auto-ack callbacks drain.

    Outbound frames now go through a bounded per-connection queue drained by a writer
    task, so ``ws.sent`` isn't populated synchronously after ``route()``/``flush()``.
    A few loop ticks let the writer → MockWS → auto-ack → watcher chain complete.
    """
    for _ in range(n):
        await asyncio.sleep(0)


# ---------------------------------------------------------------------------
# Metadata + agent_id validation
# ---------------------------------------------------------------------------

def test_metadata_valid():
    assert _validate_metadata({"display_name": "Axioma", "version": "1.0"}) is None
    assert _validate_metadata(
        {"display_name": "A", "version": "1.0", "capabilities": ["x", "y"]}) is None


def test_metadata_missing_required():
    assert _validate_metadata({"display_name": "A"}) is not None   # missing version
    assert _validate_metadata({"version": "1.0"}) is not None      # missing display_name


def test_metadata_wrong_types():
    assert _validate_metadata({"display_name": 1, "version": "1.0"}) is not None
    assert _validate_metadata(
        {"display_name": "A", "version": "1.0", "capabilities": "nope"}) is not None
    assert _validate_metadata(
        {"display_name": "A", "version": "1.0", "capabilities": [1, 2]}) is not None


def test_metadata_not_dict():
    assert _validate_metadata("nope") is not None
    assert _validate_metadata(None) is not None
    assert _validate_metadata([]) is not None


def test_metadata_max_length():
    assert _validate_metadata({"display_name": "x" * 65, "version": "1.0"}) is not None
    assert _validate_metadata({"display_name": "x", "version": "y" * 33}) is not None


def test_agent_id_regex():
    assert AGENT_ID_RE.match("axioma")
    assert AGENT_ID_RE.match("agent-1")
    assert AGENT_ID_RE.match("a-b-c-123")
    assert not AGENT_ID_RE.match("Axioma")        # uppercase
    assert not AGENT_ID_RE.match("agent_1")      # underscore
    assert not AGENT_ID_RE.match("agent.1")      # dot
    assert not AGENT_ID_RE.match(" a ")          # spaces
    assert not AGENT_ID_RE.match("")             # empty
    assert not AGENT_ID_RE.match("x" * 65)       # too long


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

def test_registry_empty():
    r = Registry()
    assert r.agents == {}
    assert r.messages == {}
    assert r.msg_id_cache == {}
    assert r.delivery_events == {}


# ---------------------------------------------------------------------------
# Heartbeat
# --------------------------------------------------------------------

async def test_heartbeat_on_pong_resets():
    registry = Registry()
    hb = HeartbeatEngine(registry, interval_s=0.1, max_missed=3)
    sess, _ = make_session(registry, "a")
    sess.missed_pings = 2
    old = sess.last_pong_time
    await hb.on_pong("a", {})
    assert sess.missed_pings == 0
    assert sess.last_pong_time >= old


async def test_heartbeat_on_pong_unknown_agent():
    registry = Registry()
    hb = HeartbeatEngine(registry)
    # Should be a no-op, not raise.
    await hb.on_pong("ghost", {})


async def test_heartbeat_declare_offline():
    registry = Registry()
    hb = HeartbeatEngine(registry, interval_s=0.1, max_missed=3,
                         presence=PresenceBroadcaster(registry, debounce_s=0.0))
    sess, _ = make_session(registry, "a")
    sess.missed_pings = 3
    await hb._declare_offline("a")
    assert sess.is_online is False


async def test_heartbeat_loop_declares_offline_when_send_fails():
    registry = Registry()
    hb = HeartbeatEngine(registry, interval_s=0.05, max_missed=2,
                         presence=PresenceBroadcaster(registry, debounce_s=0.0))

    class FailingWS:
        async def send(self, msg):
            raise OSError("dead")
    sess = AgentSession(
        agent_id="a", websocket=FailingWS(),
        metadata={"display_name": "A", "version": "1.0"},
        session_id="s1", connected_at=time.monotonic(),
        last_pong_time=time.monotonic(),
    )
    registry.agents["a"] = sess
    task = asyncio.create_task(hb.start())
    # Wait long enough for the heartbeat to enqueue a ping and the writer to fail.
    await asyncio.sleep(0.25)
    await hb.stop()
    await task
    assert sess.is_online is False
    # The new model: the writer task fails on the first send → writer_failed latch,
    # which marks the session offline immediately (no need for missed_pings to climb).
    assert sess.writer_failed is True


async def test_heartbeat_loop_declares_offline_when_no_pong():
    registry = Registry()
    hb = HeartbeatEngine(registry, interval_s=0.05, max_missed=2,
                         presence=PresenceBroadcaster(registry, debounce_s=0.0))
    ws = MockWS()
    sess = AgentSession(
        agent_id="a", websocket=ws,
        metadata={"display_name": "A", "version": "1.0"},
        session_id="s1", connected_at=time.monotonic(),
        # last pong long ago so elapsed // interval quickly exceeds max_missed
        last_pong_time=time.monotonic() - 10.0,
    )
    registry.agents["a"] = sess
    task = asyncio.create_task(hb.start())
    await asyncio.sleep(0.25)
    await hb.stop()
    await task
    assert sess.is_online is False


# ---------------------------------------------------------------------------
# Message routing — end-to-end ACK
# --------------------------------------------------------------------

async def test_route_delivered_e2e():
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=1.0)
    _, ws1 = make_session(registry, "a")
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    await router.route("a", {"to": "b", "body": "hi", "msg_id": "m1"})
    await _drain()

    # ws2 received the message
    delivered = [m for m in ws2.sent if m.get("type") == "message"]
    assert len(delivered) == 1
    assert delivered[0]["body"] == "hi"
    assert delivered[0]["from"] == "a"
    assert delivered[0]["seq"] == 1

    # ws1 received pending then delivered
    acks = [m for m in ws1.sent if m.get("type") == "message_ack"]
    statuses = [a["status"] for a in acks]
    assert "pending" in statuses
    assert "delivered" in statuses
    assert registry.messages["m1"].status == "delivered"


async def test_route_unconfirmed_when_no_receipt():
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.2)
    _, ws1 = make_session(registry, "a")
    _, ws2 = make_session(registry, "b")  # no auto-ack

    await router.route("a", {"to": "b", "body": "hi", "msg_id": "m1"})
    await asyncio.sleep(0.3)  # past delivery_timeout (0.2) so the watcher timer fires

    acks = [m["status"] for m in ws1.sent if m.get("type") == "message_ack"]
    assert "pending" in acks
    assert "unconfirmed" in acks
    assert registry.messages["m1"].status == "unconfirmed"


async def test_route_missing_fields():
    registry = Registry()
    router = MessageRouter(registry)
    _, ws1 = make_session(registry, "a")
    _, ws2 = make_session(registry, "b")

    await router.route("a", {"body": "no target"})
    await _drain()
    errs = [m for m in ws1.sent if m.get("type") == "error"]
    assert errs and errs[0]["code"] == "BAD_REQUEST"

    await router.route("a", {"to": "b"})
    await _drain()
    errs = [m for m in ws1.sent if m.get("type") == "error" and m["code"] == "BAD_REQUEST"]
    assert len(errs) >= 2


async def test_route_agent_not_found():
    registry = Registry()
    router = MessageRouter(registry)
    _, ws1 = make_session(registry, "a")
    await router.route("a", {"to": "ghost", "body": "x", "msg_id": "m"})
    await _drain()
    errs = [m for m in ws1.sent if m.get("type") == "error"]
    assert errs[0]["code"] == "AGENT_NOT_FOUND"


async def test_route_message_too_large():
    registry = Registry()
    router = MessageRouter(registry, max_message_bytes=64)
    _, ws1 = make_session(registry, "a")
    _, ws2 = make_session(registry, "b")
    big = "x" * 200
    await router.route("a", {"to": "b", "body": big, "msg_id": "m"})
    await _drain()
    errs = [m for m in ws1.sent if m.get("type") == "error"]
    assert errs[0]["code"] == "MESSAGE_TOO_LARGE"
    # Target got nothing.
    assert not any(m.get("type") == "message" for m in ws2.sent)


# ---------------------------------------------------------------------------
# Per-pair sequence ordering
# --------------------------------------------------------------------

async def test_per_pair_sequence_ordering():
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.1)
    _, ws1 = make_session(registry, "a")
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    for i in range(3):
        await router.route("a", {"to": "b", "body": f"m{i}", "msg_id": f"id{i}"})
        await _drain()

    msgs = [m for m in ws2.sent if m.get("type") == "message"]
    assert [m["seq"] for m in msgs] == [1, 2, 3]
    assert registry.pair_seqs[("a", "b")] == 3


async def test_per_pair_sequence_independent():
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.1)
    _, _ = make_session(registry, "a")
    _, _ = make_session(registry, "b")
    _, _ = make_session(registry, "c")
    await router.route("a", {"to": "b", "body": "1", "msg_id": "x1"})
    await _drain()
    await router.route("a", {"to": "c", "body": "1", "msg_id": "x2"})
    await _drain()
    assert registry.pair_seqs[("a", "b")] == 1
    assert registry.pair_seqs[("a", "c")] == 1


# ---------------------------------------------------------------------------
# Offline queue + queued delivery ACK
# --------------------------------------------------------------------

async def test_route_offline_queued():
    registry = Registry()
    router = MessageRouter(registry)
    _, ws1 = make_session(registry, "a")
    sess_b, _ = make_session(registry, "b", ws=MockWS(close_code=1000))  # socket closed
    sess_b.is_online = False

    await router.route("a", {"to": "b", "body": "queued", "msg_id": "q1", "ttl": 60})
    await _drain()
    assert len(sess_b.pending_messages) == 1
    assert sess_b.pending_messages[0]["body"] == "queued"
    acks = [m for m in ws1.sent if m.get("type") == "message_ack"]
    # Server sends "pending" immediately, then "queued" for the offline+ttl path.
    statuses = [a["status"] for a in acks]
    assert statuses[-1] == "queued"
    assert acks[-1]["ttl"] == 60
    assert registry.messages["q1"].status == "queued"


async def test_route_offline_no_ttl_returns_offline():
    registry = Registry()
    router = MessageRouter(registry)
    _, ws1 = make_session(registry, "a")
    sess_b, _ = make_session(registry, "b", ws=MockWS(close_code=1000))  # socket closed
    sess_b.is_online = False

    await router.route("a", {"to": "b", "body": "x", "msg_id": "q1"})
    await _drain()
    acks = [m for m in ws1.sent if m.get("type") == "message_ack"]
    statuses = [a["status"] for a in acks]
    assert statuses[-1] == "offline"
    assert registry.messages["q1"].status == "offline"
    assert sess_b.pending_messages == []


async def test_route_send_failure_with_ttl_queues_for_reconnect():
    """A send that raises (target socket just died) with TTL > 0 queues the message for
    reconnect redelivery instead of dropping it — the race where route() sees the target
    as online but the connection is already dead (design §2.5)."""
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.1)

    class DeadWS:
        async def send(self, msg):
            raise OSError("connection reset")
    _, ws1 = make_session(registry, "a")
    sess_b, _ = make_session(registry, "b", ws=DeadWS())
    sess_b.on_send_failure = router._handle_writer_failure
    # b is marked online but its socket is dead — the exact race window.
    assert sess_b.is_online is True

    await router.route("a", {"to": "b", "body": "in-flight", "msg_id": "race1", "ttl": 60})
    await asyncio.sleep(0.1)  # let the writer task fail + failure handler run

    # Target marked offline so subsequent messages take the queue path immediately.
    assert sess_b.is_online is False
    # Message queued for reconnect redelivery, not dropped/failed.
    assert registry.messages["race1"].status == "queued"
    assert any(m.get("body") == "in-flight" for m in sess_b.pending_messages)
    acks = [m for m in ws1.sent if m.get("type") == "message_ack"]
    statuses = [a["status"] for a in acks]
    assert statuses[-1] == "queued"
    assert acks[-1]["ttl"] == 60


async def test_route_send_failure_no_ttl_marks_failed():
    """A send that raises with no TTL cannot be redelivered, so the message is failed."""
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.1)

    class DeadWS:
        async def send(self, msg):
            raise OSError("connection reset")
    _, ws1 = make_session(registry, "a")
    sess_b, _ = make_session(registry, "b", ws=DeadWS())
    sess_b.on_send_failure = router._handle_writer_failure

    await router.route("a", {"to": "b", "body": "in-flight", "msg_id": "race2"})
    await asyncio.sleep(0.1)  # let the writer task fail + failure handler run

    assert sess_b.is_online is False
    assert registry.messages["race2"].status == "failed"
    assert sess_b.pending_messages == []
    acks = [m for m in ws1.sent if m.get("type") == "message_ack"]
    assert acks[-1]["status"] == "failed"


async def test_route_send_failure_then_reconnect_redelivers():
    """Queued-on-send-failure message is delivered when the target reconnects."""
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.1)

    class DeadWS:
        async def send(self, msg):
            raise OSError("connection reset")
    _, ws1 = make_session(registry, "a")
    sess_b, _ = make_session(registry, "b", ws=DeadWS())
    sess_b.on_send_failure = router._handle_writer_failure

    await router.route("a", {"to": "b", "body": "in-flight", "msg_id": "race3", "ttl": 60})
    await asyncio.sleep(0.1)  # let the writer task fail + failure handler run
    assert registry.messages["race3"].status == "queued"

    # Target comes back with a live websocket and auto-acks.
    live_ws = MockWS(on_message=auto_ack_cb(router, "b"))
    sess_b.websocket = live_ws
    sess_b.is_online = True
    sess_b.writer_failed = False  # fresh socket on reconnect
    await router.on_reconnect("b")
    await _drain()

    delivered = [m for m in live_ws.sent if m.get("type") == "message"]
    assert len(delivered) == 1
    assert delivered[0]["body"] == "in-flight"
    assert registry.messages["race3"].status == "delivered"
    acks = [m for m in ws1.sent if m.get("type") == "message_ack"]
    assert acks[-1]["status"] == "delivered"
    assert sess_b.pending_messages == []


async def test_on_reconnect_delivers_queued_and_acks_sender():
    registry = Registry()
    router = MessageRouter(registry)
    _, ws1 = make_session(registry, "a")
    sess_b, ws2 = make_session(registry, "b", ws=MockWS(close_code=1000))  # socket closed
    sess_b.is_online = False

    await router.route("a", {"to": "b", "body": "queued", "msg_id": "q1", "ttl": 60})
    await _drain()
    sess_b.is_online = True
    sess_b.websocket.close_code = None  # reconnect → open socket
    await router.on_reconnect("b")
    await _drain()

    delivered = [m for m in ws2.sent if m.get("type") == "message"]
    assert len(delivered) == 1
    assert delivered[0]["body"] == "queued"
    # sender got "delivered" ACK (v5.0 feature)
    acks = [m for m in ws1.sent if m.get("type") == "message_ack"]
    assert acks[-1]["status"] == "delivered"
    # record status updated (v5.0 fix)
    assert registry.messages["q1"].status == "delivered"
    assert sess_b.pending_messages == []


async def test_on_reconnect_ttl_expired():
    registry = Registry()
    router = MessageRouter(registry)
    _, ws1 = make_session(registry, "a")
    sess_b, _ = make_session(registry, "b", ws=MockWS(close_code=1000))  # socket closed
    sess_b.is_online = False

    await router.route("a", {"to": "b", "body": "queued", "msg_id": "q1", "ttl": 60})
    await _drain()
    # Make the record artificially old so TTL has elapsed.
    registry.messages["q1"].created_at = time.monotonic() - 120.0
    sess_b.is_online = True
    sess_b.websocket.close_code = None  # reconnect → open socket
    await router.on_reconnect("b")
    await _drain()

    assert registry.messages["q1"].status == "ttl_expired"
    acks = [m for m in ws1.sent if m.get("type") == "message_ack"]
    assert acks[-1]["status"] == "ttl_expired"
    assert sess_b.pending_messages == []


async def test_on_reconnect_unknown_agent():
    registry = Registry()
    router = MessageRouter(registry)
    # No session — should be a no-op.
    await router.on_reconnect("ghost")
    await _drain()


# ---------------------------------------------------------------------------
# Deduplication
# --------------------------------------------------------------------

async def test_dedup_same_msg_id_not_redelivered():
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.1)
    _, ws1 = make_session(registry, "a")
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    await router.route("a", {"to": "b", "body": "first", "msg_id": "dup1"})
    await _drain()
    await router.route("a", {"to": "b", "body": "second", "msg_id": "dup1"})
    await _drain()
    msgs = [m for m in ws2.sent if m.get("type") == "message"]
    assert len(msgs) == 1
    assert "dup1" in registry.msg_id_cache


# ---------------------------------------------------------------------------
# Rate limiting
# --------------------------------------------------------------------

async def test_rate_limit_per_pair():
    registry = Registry()
    router = MessageRouter(registry, rate_limit_per_agent=100, rate_limit_per_pair=3,
                           delivery_timeout=0.05)
    _, ws1 = make_session(registry, "a")
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    for i in range(3):
        await router.route("a", {"to": "b", "body": f"m{i}", "msg_id": f"r{i}"})
        await _drain()
    await router.route("a", {"to": "b", "body": "over", "msg_id": "rover"})
    await _drain()
    errs = [m for m in ws1.sent if m.get("type") == "error" and m["code"] == "RATE_LIMITED"]
    assert errs


async def test_rate_limit_per_agent_token_bucket():
    registry = Registry()
    router = MessageRouter(registry, rate_limit_per_agent=2, rate_limit_per_pair=100,
                           delivery_timeout=0.05)
    _, ws1 = make_session(registry, "a")
    _, _ = make_session(registry, "b")
    _, _ = make_session(registry, "c")
    # Drain budget exactly to 0 via two sends.
    await router.route("a", {"to": "b", "body": "1", "msg_id": "a1"})
    await _drain()
    await router.route("a", {"to": "c", "body": "1", "msg_id": "a2"})
    await _drain()
    # Third should be rate-limited (budget empty, no refill instant).
    await router.route("a", {"to": "b", "body": "2", "msg_id": "a3"})
    await _drain()
    errs = [m for m in ws1.sent if m.get("type") == "error" and m["code"] == "RATE_LIMITED"]
    assert errs


# ---------------------------------------------------------------------------
# Block list
# --------------------------------------------------------------------

async def test_block_unblock():
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.1)
    _, ws1 = make_session(registry, "a")
    sess_b, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    await router.on_block("b", {"agent_id": "a"})
    await _drain()
    assert "a" in sess_b.blocked_agents
    await router.route("a", {"to": "b", "body": "x", "msg_id": "bk1"})
    await _drain()
    errs = [m for m in ws1.sent if m.get("type") == "error" and m["code"] == "BLOCKED"]
    assert errs
    assert not any(m.get("type") == "message" for m in ws2.sent)

    await router.on_unblock("b", {"agent_id": "a"})
    await _drain()
    assert "a" not in sess_b.blocked_agents
    await router.route("a", {"to": "b", "body": "y", "msg_id": "bk2"})
    await _drain()
    assert any(m.get("type") == "message" for m in ws2.sent)


async def test_block_missing_agent_id():
    registry = Registry()
    router = MessageRouter(registry)
    _, ws1 = make_session(registry, "a")
    await router.on_block("a", {})
    await _drain()
    errs = [m for m in ws1.sent if m.get("type") == "error"]
    assert errs[0]["code"] == "BAD_REQUEST"


# ---------------------------------------------------------------------------
# Reply authorization
# --------------------------------------------------------------------

async def test_forged_reply_rejected():
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.1)
    _, ws1 = make_session(registry, "a", ws=MockWS(on_message=auto_ack_cb(router, "a")))
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))
    _, ws3 = make_session(registry, "c")

    # a -> b
    await router.route("a", {"to": "b", "body": "hello", "msg_id": "orig"})
    await _drain()
    # c tries to forge a reply to "orig" (c was not the recipient)
    await router.route("c", {"to": "a", "body": "forged", "msg_id": "forged",
                             "reply_to": "orig"})
    await _drain()
    errs = [m for m in ws3.sent if m.get("type") == "error" and m["code"] == "FORGED_REPLY"]
    assert errs


async def test_received_from_wrong_agent_rejected():
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.5)
    _, ws1 = make_session(registry, "a", ws=MockWS(on_message=auto_ack_cb(router, "a")))
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))
    _, ws3 = make_session(registry, "c")

    await router.route("a", {"to": "b", "body": "hi", "msg_id": "orig"})
    await _drain()
    # c claims receipt for a message sent to b
    await router.on_received("c", {"msg_id": "orig"})
    await _drain()
    errs = [m for m in ws3.sent if m.get("type") == "error" and m["code"] == "FORGED_REPLY"]
    assert errs


async def test_received_unknown_msg_id_noop():
    registry = Registry()
    router = MessageRouter(registry)
    _, ws1 = make_session(registry, "a")
    # Should not raise.
    await router.on_received("a", {"msg_id": "ghost"})
    await _drain()
    await router.on_received("a", {})
    await _drain()


# ---------------------------------------------------------------------------
# Conversation management
# --------------------------------------------------------------------

async def test_conversation_end_and_reopen():
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.1)
    _, ws1 = make_session(registry, "a", ws=MockWS(on_message=auto_ack_cb(router, "a")))
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    await router.route("a", {"to": "b", "body": "first", "msg_id": "cm1"})
    await _drain()
    conv_id = registry.messages["cm1"].conversation_id

    await router.on_conversation_end("a", {"conversation_id": conv_id, "reason": "done"})
    await _drain()
    conv = registry.conversations[conv_id]
    assert conv.is_ended is True
    assert conv.ended_by == "a"
    # b should have been notified
    assert any(m.get("type") == "conversation_ended" for m in ws2.sent)

    # Message after end is rejected.
    await router.route("a", {"to": "b", "body": "after", "msg_id": "cm2",
                             "conversation_id": conv_id})
    await _drain()
    errs = [m for m in ws1.sent if m.get("type") == "error" and m["code"] == "CONVERSATION_ENDED"]
    assert errs

    # Reopen with reopen=True.
    await router.route("a", {"to": "b", "body": "reopen", "msg_id": "cm3",
                             "conversation_id": conv_id, "reopen": True})
    await _drain()
    assert registry.conversations[conv_id].is_ended is False


async def test_conversation_end_errors():
    registry = Registry()
    router = MessageRouter(registry)
    _, ws1 = make_session(registry, "a")
    _, ws2 = make_session(registry, "b")
    _, ws3 = make_session(registry, "c")

    # Missing conversation_id
    await router.on_conversation_end("a", {})
    await _drain()
    assert any(m.get("code") == "BAD_REQUEST" for m in ws1.sent if m.get("type") == "error")

    # Unknown conversation
    await router.on_conversation_end("a", {"conversation_id": "ghost"})
    await _drain()
    assert any(m.get("code") == "CONVERSATION_NOT_FOUND"
               for m in ws1.sent if m.get("type") == "error")

    # Not a participant: build a conversation between a and b only.
    await router.route("a", {"to": "b", "body": "x", "msg_id": "p1"})
    await _drain()
    conv_id = registry.messages["p1"].conversation_id
    await router.on_conversation_end("c", {"conversation_id": conv_id})
    await _drain()
    assert any(m.get("code") == "NOT_CONVERSATION_PARTICIPANT"
               for m in ws3.sent if m.get("type") == "error")


# ---------------------------------------------------------------------------
# Depth tracking
# --------------------------------------------------------------------

async def test_depth_exceeded():
    registry = Registry()
    router = MessageRouter(registry, max_conversation_depth=3, delivery_timeout=0.1)
    _, ws1 = make_session(registry, "a", ws=MockWS(on_message=auto_ack_cb(router, "a")))
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    conv_id = str(uuid.uuid4())
    prev = None
    for i in range(3):
        mid = f"d{i}"
        sender = "a" if i % 2 == 0 else "b"
        target = "b" if i % 2 == 0 else "a"
        await router.route(sender, {"to": target, "body": f"d{i}", "msg_id": mid,
                                    "reply_to": prev, "conversation_id": conv_id})
        await _drain()
        prev = mid
    # 4th reply: b replies to d2 (which was a→b), depth 4 > 3 → DEPTH_EXCEEDED.
    await router.route("b", {"to": "a", "body": "too-deep", "msg_id": "d3",
                             "reply_to": prev, "conversation_id": conv_id})
    await _drain()
    errs = [m for m in ws2.sent if m.get("type") == "error" and m["code"] == "DEPTH_EXCEEDED"]
    assert errs


async def test_reply_to_missing_original_starts_new_conversation():
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.1)
    _, ws1 = make_session(registry, "a")
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    await router.route("a", {"to": "b", "body": "x", "msg_id": "m1",
                             "reply_to": "nonexistent"})
    await _drain()
    rec = registry.messages["m1"]
    assert rec.depth == 1
    assert rec.conversation_id is not None


# ---------------------------------------------------------------------------
# Circuit breaker + loop detection
# --------------------------------------------------------------------

async def test_circuit_breaker_opens_on_volume():
    registry = Registry()
    router = MessageRouter(registry, circuit_breaker_window=60, circuit_breaker_max=3,
                           delivery_timeout=0.05)
    _, ws1 = make_session(registry, "a")
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    for i in range(3):
        await router.route("a", {"to": "b", "body": f"m{i}", "msg_id": f"cb{i}"})
        await _drain()
    # 4th triggers the breaker (3 already recorded; the 4th updates then opens).
    await router.route("a", {"to": "b", "body": "over", "msg_id": "cb4"})
    await _drain()
    assert registry.circuit_breakers[("a", "b")].is_open is True


async def test_circuit_breaker_blocks_then_resets():
    registry = Registry()
    router = MessageRouter(registry, circuit_breaker_window=60, circuit_breaker_max=2,
                           circuit_breaker_cooldown=0.05, delivery_timeout=0.05)
    _, ws1 = make_session(registry, "a")
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    for i in range(2):
        await router.route("a", {"to": "b", "body": f"m{i}", "msg_id": f"cb{i}"})
        await _drain()
    # Force-open with a near-zero cooldown so it auto-resets.
    router._open_circuit_breaker("a", "b")
    assert router._is_circuit_open("a", "b") is True

    await router.route("a", {"to": "b", "body": "blocked", "msg_id": "cbblk"})
    await _drain()
    errs = [m for m in ws1.sent if m.get("type") == "error" and m["code"] == "CIRCUIT_BREAKER"]
    assert errs

    # After cooldown elapses, breaker resets and routing works again.
    await asyncio.sleep(0.1)
    assert router._is_circuit_open("a", "b") is False
    await router.route("a", {"to": "b", "body": "after", "msg_id": "cbafter"})
    await _drain()
    assert any(m.get("type") == "message" and m["msg_id"] == "cbafter" for m in ws2.sent)


async def test_loop_detection_opens_breaker():
    """6 alternating messages that cycle the SAME content (a true echo loop) open the
    breaker in both directions. Distinct content alone is not a loop (see below)."""
    registry = Registry()
    router = MessageRouter(registry, max_conversation_depth=20,
                           delivery_timeout=0.05)
    _, ws1 = make_session(registry, "a", ws=MockWS(on_message=auto_ack_cb(router, "a")))
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    conv_id = str(uuid.uuid4())
    prev = None
    for i in range(6):
        sender = "a" if i % 2 == 0 else "b"
        target = "b" if i % 2 == 0 else "a"
        mid = f"lp{i}"
        # Echo loop: the same two bodies bounce back and forth.
        body = "echo" if i % 2 == 0 else "echo-back"
        await router.route(sender, {"to": target, "body": body, "msg_id": mid,
                                    "reply_to": prev, "conversation_id": conv_id})
        await _drain()
        prev = mid
    # The 6th message completes the alternating + repeating pattern → breaker opened.
    assert registry.circuit_breakers[("a", "b")].is_open is True
    assert registry.circuit_breakers[("b", "a")].is_open is True


async def test_loop_detection_distinct_content_does_not_trip():
    """A normal multi-turn request/reply conversation (alternating senders, distinct
    bodies) must NOT trip the loop detector — replies keep getting delivered."""
    registry = Registry()
    router = MessageRouter(registry, max_conversation_depth=20,
                           delivery_timeout=0.05)
    _, ws1 = make_session(registry, "a", ws=MockWS(on_message=auto_ack_cb(router, "a")))
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    conv_id = str(uuid.uuid4())
    prev = None
    delivered = []
    for i in range(10):
        sender = "a" if i % 2 == 0 else "b"
        target = "b" if i % 2 == 0 else "a"
        mid = f"dc{i}"
        await router.route(sender, {"to": target, "body": f"unique-body-{i}", "msg_id": mid,
                                    "reply_to": prev, "conversation_id": conv_id})
        await _drain()
        prev = mid
        target_ws = ws2 if target == "b" else ws1
        if any(m.get("type") == "message" and m.get("msg_id") == mid for m in target_ws.sent):
            delivered.append(mid)
    # All 10 turns delivered — no breaker tripped by distinct content.
    assert len(delivered) == 10
    assert ("a", "b") not in registry.circuit_breakers or \
        registry.circuit_breakers[("a", "b")].is_open is False


def test_detect_loop_no_conversation():
    registry = Registry()
    router = MessageRouter(registry)
    assert router._detect_loop("ghost") is False


def test_detect_loop_too_few_messages():
    registry = Registry()
    router = MessageRouter(registry)
    conv = ConversationRecord(conversation_id="c", participants={"a", "b"}, depth=1,
                              created_at=time.monotonic())
    registry.conversations["c"] = conv
    for i in range(5):
        conv.recent_messages.append(MessageRecord(
            msg_id=f"m{i}", sender_id="a" if i % 2 == 0 else "b", recipient_id="b",
            body="x", reply_to=None, conversation_id="c", seq=i, depth=1,
            status="pending", created_at=time.monotonic(), ttl=0))
    assert router._detect_loop("c") is False


def test_detect_loop_three_participants():
    registry = Registry()
    router = MessageRouter(registry)
    conv = ConversationRecord(conversation_id="c", participants={"a", "b", "c"},
                              depth=1, created_at=time.monotonic())
    registry.conversations["c"] = conv
    senders = ["a", "b", "c", "a", "b", "c"]
    for i, s in enumerate(senders):
        conv.recent_messages.append(MessageRecord(
            msg_id=f"m{i}", sender_id=s, recipient_id="x", body="x", reply_to=None,
            conversation_id="c", seq=i, depth=1, status="pending",
            created_at=time.monotonic(), ttl=0))
    assert router._detect_loop("c") is False  # 3 participants → not a 2-agent loop


# ---------------------------------------------------------------------------
# Pruning
# --------------------------------------------------------------------

async def test_prune_once_removes_expired():
    registry = Registry()
    router = MessageRouter(registry, msg_record_ttl=0.05, msg_id_cache_ttl=0.05)
    _, ws1 = make_session(registry, "a")
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    await router.route("a", {"to": "b", "body": "x", "msg_id": "p1"})
    await _drain()
    assert "p1" in registry.messages
    assert "p1" in registry.msg_id_cache

    # Age the records past TTL.
    registry.messages["p1"].created_at = time.monotonic() - 10.0
    registry.msg_id_cache["p1"] = time.monotonic() - 10.0
    router._prune_once()

    assert "p1" not in registry.messages
    assert "p1" not in registry.msg_id_cache


async def test_prune_once_keeps_fresh():
    registry = Registry()
    router = MessageRouter(registry, msg_record_ttl=300, msg_id_cache_ttl=300)
    _, ws1 = make_session(registry, "a")
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))
    await router.route("a", {"to": "b", "body": "x", "msg_id": "p1"})
    await _drain()
    router._prune_once()
    assert "p1" in registry.messages
    assert "p1" in registry.msg_id_cache


async def test_prune_once_clears_set_events():
    registry = Registry()
    router = MessageRouter(registry)
    registry.delivery_events["done"] = asyncio.Event()
    registry.delivery_events["done"].set()
    registry.delivery_events["pending"] = asyncio.Event()
    router._prune_once()
    assert "done" not in registry.delivery_events
    assert "pending" in registry.delivery_events


# ---------------------------------------------------------------------------
# Presence broadcaster
# --------------------------------------------------------------------

async def test_presence_debounce_batches_changes():
    registry = Registry()
    pres = PresenceBroadcaster(registry, debounce_s=0.05)
    _, ws1 = make_session(registry, "a")
    _, ws2 = make_session(registry, "b")
    _, ws3 = make_session(registry, "c")

    # Three rapid updates collapse into a single batched frame.
    await pres.update("a", "online", {"display_name": "A", "version": "1.0"})
    await pres.update("b", "online", {"display_name": "B", "version": "1.0"})
    await pres.update("c", "offline")

    await asyncio.sleep(0.1)  # let the debounce timer fire
    for ws in (ws1, ws2, ws3):
        pres_frames = [m for m in ws.sent if m.get("type") == "presence"]
        assert len(pres_frames) == 1, "presence should be batched into one frame"
        changes = pres_frames[0]["changes"]
        ids = {c["agent_id"] for c in changes}
        assert ids == {"a", "b", "c"}


async def test_presence_immediate_when_debounce_zero():
    registry = Registry()
    pres = PresenceBroadcaster(registry, debounce_s=0.0)
    _, ws1 = make_session(registry, "a")
    await pres.update("b", "online", {"display_name": "B", "version": "1.0"})
    await _drain()
    pres_frames = [m for m in ws1.sent if m.get("type") == "presence"]
    assert len(pres_frames) == 1


async def test_presence_flush_manual():
    registry = Registry()
    pres = PresenceBroadcaster(registry, debounce_s=10.0)  # long; rely on manual flush
    _, ws1 = make_session(registry, "a")
    await pres.update("b", "online", {"display_name": "B", "version": "1.0"})
    # Nothing sent yet (timer hasn't fired).
    assert not any(m.get("type") == "presence" for m in ws1.sent)
    await pres.flush()
    await _drain()
    assert any(m.get("type") == "presence" for m in ws1.sent)


async def test_presence_latest_status_wins():
    registry = Registry()
    pres = PresenceBroadcaster(registry, debounce_s=0.0)
    _, ws1 = make_session(registry, "a")
    await pres.update("b", "online", {"display_name": "B", "version": "1.0"})
    await pres.update("b", "offline")
    await _drain()
    pres_frames = [m for m in ws1.sent if m.get("type") == "presence"]
    # Two immediate flushes; the second reflects the latest status.
    last = pres_frames[-1]["changes"][0]
    assert last["status"] == "offline"


# ---------------------------------------------------------------------------
# CLI renderer (pure logic; escape-sequence stdin reads intentionally untested)
# --------------------------------------------------------------------

def test_cli_add_message_and_render(capsys):
    from neurogossip_server.server import CliRenderer
    registry = Registry()
    cli = CliRenderer(registry)
    cli.add_message("server", "hello")
    assert cli.messages[0][2] == "hello"
    assert cli.scroll_offset == 0
    cli.render()  # should not raise
    out = capsys.readouterr().out
    assert "NEUROGOSSIP" in out


async def test_cli_process_send_valid():
    from neurogossip_server.server import CliRenderer
    registry = Registry()
    cli = CliRenderer(registry)
    make_session(registry, "thea")
    cli._process_input("@thea: hello there")
    assert any("thea" in text for _, _, text in cli.messages)


def test_cli_process_send_unknown_agent():
    from neurogossip_server.server import CliRenderer
    registry = Registry()
    cli = CliRenderer(registry)
    cli._process_input("@ghost: hi")
    assert any("not found" in text.lower() for _, _, text in cli.messages)


def test_cli_process_send_missing_colon():
    from neurogossip_server.server import CliRenderer
    registry = Registry()
    cli = CliRenderer(registry)
    cli._process_input("@thea no colon")
    assert any("format" in text.lower() for _, _, text in cli.messages)


async def test_cli_process_send_at_in_body_not_handle():
    from neurogossip_server.server import CliRenderer
    registry = Registry()
    cli = CliRenderer(registry)
    make_session(registry, "thea")
    cli._process_input("@thea: @skye told me")
    # The body should contain the literal @skye, not be re-parsed as a handle.
    send_msgs = [text for ts, mt, text in cli.messages if mt == "send"]
    assert any("@skye" in m for m in send_msgs)


async def test_cli_process_send_first_handle_wins():
    from neurogossip_server.server import CliRenderer
    registry = Registry()
    cli = CliRenderer(registry)
    make_session(registry, "thea")
    cli._process_input("@thea:@skye: hello")
    send_msgs = [text for ts, mt, text in cli.messages if mt == "send"]
    # Body is everything after the first ": " → "@skye: hello"
    assert any("@skye: hello" in m for m in send_msgs)


def test_cli_commands_list_status_help():
    from neurogossip_server.server import CliRenderer
    registry = Registry()
    cli = CliRenderer(registry)
    make_session(registry, "a")
    make_session(registry, "b")
    cli._process_input("/list")
    texts = [text for _, _, text in cli.messages]
    assert any("a" in t for t in texts)
    assert any("b" in t for t in texts)
    cli._process_input("/status a")
    assert any("online" in text for _, _, text in cli.messages)
    cli._process_input("/help")
    assert any("Commands" in text for _, _, text in cli.messages)
    cli._process_input("/status ghost")
    assert any("not found" in text.lower() for _, _, text in cli.messages)


def test_cli_unknown_command_and_input():
    from neurogossip_server.server import CliRenderer
    registry = Registry()
    cli = CliRenderer(registry)
    cli._process_input("/nope")
    assert any("unknown" in text.lower() for _, _, text in cli.messages)
    cli._process_input("plain text")
    assert any("unknown" in text.lower() for _, _, text in cli.messages)


def test_cli_search_enters_mode():
    from neurogossip_server.server import CliRenderer
    registry = Registry()
    cli = CliRenderer(registry)
    cli._process_input("/search hello")
    assert cli.is_searching is True
    assert cli.search_term == "hello"


def test_cli_search_then_backspace_and_escape():
    from neurogossip_server.server import CliRenderer
    registry = Registry()
    cli = CliRenderer(registry)
    cli._process_input("/search abc")
    assert cli.is_searching is True
    cli._process_char("\x7f")  # backspace in search
    assert cli.search_term == "ab"
    cli._process_char("\x1b")  # escape cancels search
    assert cli.is_searching is False
    assert cli.search_term == ""


def test_cli_process_char_normal_input_and_enter():
    from neurogossip_server.server import CliRenderer
    registry = Registry()
    cli = CliRenderer(registry)
    cli._process_char("x")
    assert cli.input_buffer == "x"
    cli._process_char("\x7f")
    assert cli.input_buffer == ""
    make_session(registry, "thea")
    cli._process_char("@")
    cli._process_char("t")
    cli._process_char("\n")
    # "@t" has no colon → format error
    assert any("format" in text.lower() for _, _, text in cli.messages)


def test_cli_export_log(tmp_path, monkeypatch):
    from neurogossip_server.server import CliRenderer
    monkeypatch.chdir(tmp_path)
    registry = Registry()
    cli = CliRenderer(registry)
    cli.add_message("server", "line one")
    cli.add_message("recv", "line two")
    cli._process_input("/export")
    exports = list(tmp_path.glob("neurogossip_log_*.txt"))
    assert len(exports) == 1
    content = exports[0].read_text()
    assert "line one" in content
    assert "line two" in content


# ---------------------------------------------------------------------------
# NeurogossipServer construction
# --------------------------------------------------------------------

def test_server_constructs_with_defaults():
    srv = NeurogossipServer(host="127.0.0.1", port=0)
    assert srv.registry is not None
    assert srv.heartbeat is not None
    assert srv.router is not None
    assert srv.presence is not None


# ---------------------------------------------------------------------------
# Logging filter — benign websockets close/handshake noise
# --------------------------------------------------------------------

def test_log_filter_downgrades_connection_closed_error():
    """The opening-handshake-failed traceback (ConnectionClosed) is downgraded to DEBUG
    and its exc_info is stripped + folded into the message so no traceback renders."""
    import logging
    from websockets.exceptions import ConnectionClosedError

    filt = _SuppressConnectionClosedFilter()

    benign = logging.LogRecord(
        "websockets.server", logging.ERROR, __file__, 1,
        "opening handshake failed", None, None)
    benign.exc_info = (ConnectionClosedError, ConnectionClosedError(None, None), None)
    assert filt.filter(benign) is True
    assert benign.levelno == logging.DEBUG
    assert benign.levelname == "DEBUG"
    # exc_info stripped → handler can't render the multi-line traceback.
    assert benign.exc_info is None
    assert benign.exc_text is None
    # Exception summary folded into a single line.
    assert "opening handshake failed" in benign.getMessage()


def test_log_filter_keeps_genuine_internal_error():
    """A non-ConnectionClosed exception (e.g. an internal bug) stays at ERROR with its
    traceback intact."""
    import logging

    filt = _SuppressConnectionClosedFilter()
    real = logging.LogRecord(
        "websockets.server", logging.ERROR, __file__, 1,
        "unexpected internal error", None, None)
    real.exc_info = (RuntimeError, RuntimeError("boom"), None)
    assert filt.filter(real) is True
    assert real.levelno == logging.ERROR
    assert real.exc_info is not None  # traceback preserved


def test_log_filter_downgrades_invalid_handshake_message():
    """A handshake failure from a bad/short HTTP request (InvalidMessage, not a
    ConnectionClosed subclass) is still downgraded + exc_info stripped via the message match."""
    import logging
    from websockets.exceptions import InvalidMessage

    filt = _SuppressConnectionClosedFilter()
    rec = logging.LogRecord(
        "websockets.server", logging.ERROR, __file__, 1,
        "opening handshake failed", None, None)
    rec.exc_info = (InvalidMessage, InvalidMessage("did not receive a valid HTTP request"),
                    None)
    assert filt.filter(rec) is True
    assert rec.levelno == logging.DEBUG
    assert rec.exc_info is None
    assert "did not receive a valid HTTP request" in rec.getMessage()


def test_log_filter_leaves_non_error_records_alone():
    import logging
    filt = _SuppressConnectionClosedFilter()
    info = logging.LogRecord("websockets.server", logging.INFO, __file__, 1,
                             "server listening", None, None)
    assert filt.filter(info) is True
    assert info.levelno == logging.INFO


def test_server_install_attaches_filter_idempotently():
    """Constructing a server installs the filter once on websockets loggers."""
    import logging
    NeurogossipServer(host="127.0.0.1", port=0)  # installs via __init__
    ws_logger = logging.getLogger("websockets.server")
    assert any(isinstance(f, _SuppressConnectionClosedFilter) for f in ws_logger.filters)
    # Second construction must not double-add the filter.
    NeurogossipServer(host="127.0.0.1", port=0)
    count = sum(1 for f in ws_logger.filters
                if isinstance(f, _SuppressConnectionClosedFilter))
    assert count == 1


# ---------------------------------------------------------------------------
# Additional CLI coverage: broadcast, block/unblock notices, empty handle/body
# --------------------------------------------------------------------

async def test_cli_broadcast_sends_to_online_agents():
    from neurogossip_server.server import CliRenderer
    registry = Registry()
    cli = CliRenderer(registry)
    _, ws_a = make_session(registry, "a")
    _, ws_b = make_session(registry, "b")
    cli._process_input("/broadcast hello everyone")
    # Let the fire-and-forget sends complete.
    await asyncio.sleep(0.05)
    for ws in (ws_a, ws_b):
        assert any(m.get("type") == "message" and m.get("body") == "hello everyone"
                   for m in ws.sent)


def test_cli_block_unblock_notices():
    from neurogossip_server.server import CliRenderer
    registry = Registry()
    cli = CliRenderer(registry)
    cli._process_input("/block skye")
    assert any("agent connection" in text.lower() for _, _, text in cli.messages)
    cli._process_input("/unblock skye")
    assert any("agent connection" in text.lower() for _, _, text in cli.messages)


def test_cli_process_send_empty_handle_or_body():
    from neurogossip_server.server import CliRenderer
    registry = Registry()
    cli = CliRenderer(registry)
    make_session(registry, "thea")
    cli._process_input("@thea:")  # empty body
    assert any("format" in text.lower() for _, _, text in cli.messages)
    cli._process_input("@: hi")  # empty handle
    assert any("format" in text.lower() for _, _, text in cli.messages)


async def test_cli_process_send_offline_agent():
    from neurogossip_server.server import CliRenderer
    registry = Registry()
    cli = CliRenderer(registry)
    sess, _ = make_session(registry, "thea")
    sess.is_online = False
    cli._process_input("@thea: hi")
    assert any("offline" in text.lower() for _, _, text in cli.messages)


# ---------------------------------------------------------------------------
# Registration auth + connection-handler edge cases
# --------------------------------------------------------------------

async def test_handle_register_requires_auth():
    srv = NeurogossipServer(host="127.0.0.1", port=0, require_auth=True,
                           secret="s3cret", presence_debounce=0.0, log_level="CRITICAL")
    ws = MockWS()
    # No token → rejected.
    aid = await srv._handle_register(ws, {"agent_id": "axioma",
                                          "metadata": {"display_name": "A", "version": "1.0"}})
    assert aid is None
    assert any(m.get("code") == "BAD_REGISTRATION" for m in ws.sent)
    ws2 = MockWS()
    # Wrong token → rejected.
    await srv._handle_register(ws2, {"agent_id": "axioma", "auth_token": "wrong",
                                      "metadata": {"display_name": "A", "version": "1.0"}})
    assert any(m.get("code") == "BAD_REGISTRATION" for m in ws2.sent)
    # Correct token → registered.
    ws3 = MockWS()
    aid = await srv._handle_register(ws3, {"agent_id": "axioma", "auth_token": "s3cret",
                                            "metadata": {"display_name": "A", "version": "1.0"}})
    assert aid == "axioma"
    assert any(m.get("type") == "registered" for m in ws3.sent)
    # Cleanup the lingering presence flush task if any.
    if srv.presence._flush_task is not None and not srv.presence._flush_task.done():
        srv.presence._flush_task.cancel()


async def test_handle_register_carries_over_pending_on_reregister():
    """B1 fix: re-registering an offline agent preserves its queued messages."""
    srv = NeurogossipServer(host="127.0.0.1", port=0, presence_debounce=0.0,
                            log_level="CRITICAL")
    # First registration.
    ws1 = MockWS()
    await srv._handle_register(ws1, {"agent_id": "axioma",
                                     "metadata": {"display_name": "A", "version": "1.0"}})
    # Queue a message against thea (offline) — actually queue against axioma itself by
    # marking it offline and appending a pending message.
    sess = srv.registry.agents["axioma"]
    sess.is_online = False
    sess.pending_messages.append({"type": "message", "from": "x", "body": "queued",
                                  "msg_id": "pq1"})
    # Re-register with a new websocket.
    ws2 = MockWS()
    aid = await srv._handle_register(ws2, {"agent_id": "axioma",
                                            "metadata": {"display_name": "A", "version": "1.0"}})
    assert aid == "axioma"
    new_sess = srv.registry.agents["axioma"]
    # Pending messages carried over AND delivered on reconnect.
    assert new_sess.websocket is ws2
    assert any(m.get("body") == "queued" for m in ws2.sent)
    # Old session_id cleaned up; new one present.
    assert new_sess.session_id in srv.registry.sessions
    if srv.presence._flush_task is not None and not srv.presence._flush_task.done():
        srv.presence._flush_task.cancel()


async def test_handle_register_already_online_rejected():
    srv = NeurogossipServer(host="127.0.0.1", port=0, presence_debounce=0.0,
                            log_level="CRITICAL")
    ws1 = MockWS()
    await srv._handle_register(ws1, {"agent_id": "axioma",
                                     "metadata": {"display_name": "A", "version": "1.0"}})
    ws2 = MockWS()
    aid = await srv._handle_register(ws2, {"agent_id": "axioma",
                                            "metadata": {"display_name": "A", "version": "1.0"}})
    assert aid is None
    assert any(m.get("code") == "ALREADY_REGISTERED" for m in ws2.sent)
    if srv.presence._flush_task is not None and not srv.presence._flush_task.done():
        srv.presence._flush_task.cancel()


# ---------------------------------------------------------------------------
# main() / argparse
# --------------------------------------------------------------------

def test_main_help_exits(monkeypatch):
    import neurogossip_server.server as srv_mod
    monkeypatch.setattr("sys.argv", ["server.py", "--help"])
    with pytest.raises(SystemExit):
        srv_mod.main()


def test_main_builds_and_runs(monkeypatch, tmp_path):
    import logging
    import neurogossip_server.server as srv_mod

    async def fake_start(self):
        return None
    monkeypatch.setattr(srv_mod.NeurogossipServer, "start", fake_start)
    # Don't create a real rotating log file during this build/run smoke test.
    from types import SimpleNamespace
    monkeypatch.setattr(srv_mod, "_setup_logging",
                        lambda **kw: SimpleNamespace(maxBytes=0, backupCount=0))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", [
        "server.py", "--port", "9999", "--host", "127.0.0.1",
        "--presence-debounce", "0.5", "--require-auth", "--secret", "k",
        "--rate-limit-agent", "10", "--rate-limit-pair", "5",
        "--max-conversation-depth", "7", "--circuit-breaker-max", "9",
        "--circuit-breaker-cooldown", "30", "--delivery-timeout", "5",
        "--max-message-bytes", "4096", "--log-level", "WARNING",
        "--log", str(tmp_path / "srv.log"),
    ])
    srv_mod.main()  # builds the server and runs fake_start; should not raise


def test_main_module_exposes_entry_point():
    import neurogossip_server.__main__ as m
    assert callable(m.main)


async def test_start_handles_cancellation_gracefully():
    """Cancelling start() (as asyncio.run does on SIGINT) triggers a graceful shutdown."""
    import socket
    from contextlib import closing
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    srv = NeurogossipServer(host="127.0.0.1", port=port, log_level="CRITICAL")
    srv.grace_period_s = 0.05
    task = asyncio.create_task(srv.start())
    await asyncio.sleep(0.2)  # let it bind + install the SIGTERM handler
    task.cancel()  # simulates asyncio.run's SIGINT cancellation
    await task  # start() catches CancelledError and runs shutdown → completes normally
    assert srv._running is False
    assert srv._bg_tasks == []


def test_module_run_via_dash_m(tmp_path):
    """``python -m neurogossip_server`` boots a server on a free port then exits 0 on SIGINT."""
    import socket
    import subprocess
    import sys
    import time
    import signal as _signal
    from contextlib import closing
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    proc = subprocess.Popen(
        [sys.executable, "-m", "neurogossip_server", "--port", str(port),
         "--host", "127.0.0.1", "--log-level", "CRITICAL",
         "--log", str(tmp_path / "dashm.log")],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        cwd=str(tmp_path),
    )
    try:
        time.sleep(1.0)
        proc.send_signal(_signal.SIGINT)
    finally:
        try:
            out, _ = proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, _ = proc.communicate()
    assert proc.returncode == 0, f"unexpected exit {proc.returncode}: {out}"

# ---------------------------------------------------------------------------
# /quit CLI command + graceful shutdown wiring
# --------------------------------------------------------------------

def test_cli_quit_invokes_quit_callback():
    """The /quit command calls the registered quit callback (server.request_shutdown)."""
    from neurogossip_server.server import CliRenderer
    registry = Registry()
    called = []
    cli = CliRenderer(registry, quit_callback=lambda: called.append(True))
    cli._process_input("/quit")
    assert called == [True]
    assert any("Shutting down" in t for _, _, t in cli.messages)


def test_cli_quit_without_callback_reports_error():
    from neurogossip_server.server import CliRenderer
    cli = CliRenderer(Registry())
    cli._process_input("/quit")
    assert any("shutdown handler" in t.lower() for _, _, t in cli.messages)


def test_cli_help_lists_quit():
    from neurogossip_server.server import CliRenderer
    cli = CliRenderer(Registry())
    cli._process_input("/help")
    assert any("/quit" in t for _, _, t in cli.messages)


def test_server_cli_quit_callback_wired_to_request_shutdown():
    """Constructing a CLI-mode server wires /quit to server.request_shutdown."""
    srv = NeurogossipServer(host="127.0.0.1", port=0, cli_mode=True, log_level="CRITICAL")
    assert srv.cli is not None
    assert srv.cli.quit_callback == srv.request_shutdown


async def test_request_shutdown_ends_serve_forever_and_runs_shutdown():
    """request_shutdown() closes the listener so serve_forever() returns; start()
    then runs the full shutdown() to completion (no half-finished drain)."""
    import socket
    from contextlib import closing
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    srv = NeurogossipServer(host="127.0.0.1", port=port, log_level="CRITICAL")
    srv.grace_period_s = 0.05

    async def trigger_after_listen():
        # Wait until the server is listening, then request shutdown (like /quit).
        for _ in range(40):
            if srv.server is not None and srv.server.is_serving():
                break
            await asyncio.sleep(0.05)
        srv.request_shutdown()

    trig = asyncio.create_task(trigger_after_listen())
    await srv.start()        # returns only after the full shutdown completes
    await trig
    assert srv._shutting_down is True
    assert srv._running is False
    assert srv._bg_tasks == []


async def test_shutdown_is_idempotent():
    """A second shutdown() call is a no-op so the sequence runs exactly once."""
    import socket
    from contextlib import closing
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    srv = NeurogossipServer(host="127.0.0.1", port=port, log_level="CRITICAL")
    srv.grace_period_s = 0.0
    await srv.serve()
    await srv.shutdown()
    assert srv._shutting_down is True
    # Second call must not raise or re-run steps (e.g. wait_closed on a torn-down server).
    await srv.shutdown()


async def test_read_stdin_swallows_keyboard_interrupt():
    """A KeyboardInterrupt raised mid-read (second Ctrl-C during shutdown) must not
    surface as an unretrieved task exception from read_stdin."""
    from neurogossip_server.server import CliRenderer

    cli = CliRenderer(Registry())

    async def raising_read(*args, **kwargs):
        raise KeyboardInterrupt()
    import os as _os
    orig = _os.read
    _os.read = raising_read
    try:
        await cli.read_stdin()  # must not raise
    finally:
        _os.read = orig


# ---------------------------------------------------------------------------
# CLI message logging + router on_message_routed hook
# --------------------------------------------------------------------

async def test_router_invokes_on_message_routed_hook():
    """The hook fires once per accepted message with (sender, target, body, reply_to)."""
    registry = Registry()
    logged = []
    router = MessageRouter(registry, delivery_timeout=0.1,
                           on_message_routed=lambda s, t, b, r=None: logged.append((s, t, b, r)))
    _, _ = make_session(registry, "a")
    _, _ = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    await router.route("a", {"to": "b", "body": "hello", "msg_id": "m1"})
    await _drain()
    assert logged == [("a", "b", "hello", None)]


async def test_router_hook_passes_reply_to():
    """A reply (reply_to set) is forwarded to the hook with the original msg_id."""
    registry = Registry()
    logged = []
    router = MessageRouter(registry, delivery_timeout=0.1,
                           on_message_routed=lambda s, t, b, r=None: logged.append((s, t, b, r)))
    _, _ = make_session(registry, "a", ws=MockWS(on_message=auto_ack_cb(router, "a")))
    _, _ = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    await router.route("a", {"to": "b", "body": "q", "msg_id": "q1"})
    await _drain()
    await router.route("b", {"to": "a", "body": "a", "msg_id": "a1", "reply_to": "q1"})
    await _drain()
    assert logged[0] == ("a", "b", "q", None)
    assert logged[1] == ("b", "a", "a", "q1")


async def test_router_hook_not_called_for_rejected_or_duplicate():
    """Rejected messages (AGENT_NOT_FOUND) and duplicates don't reach the hook."""
    registry = Registry()
    logged = []
    router = MessageRouter(registry, delivery_timeout=0.1,
                           on_message_routed=lambda s, t, b, r=None: logged.append((s, t, b, r)))
    _, _ = make_session(registry, "a")
    _, _ = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    # Rejected: target not found.
    await router.route("a", {"to": "ghost", "body": "x", "msg_id": "g1"})
    await _drain()
    # Accepted once.
    await router.route("a", {"to": "b", "body": "hi", "msg_id": "d1"})
    await _drain()
    # Duplicate msg_id → dedup, not re-logged.
    await router.route("a", {"to": "b", "body": "again", "msg_id": "d1"})
    await _drain()
    assert logged == [("a", "b", "hi", None)]


def test_cli_log_routed_message_stores_header_and_body():
    from neurogossip_server.server import CliRenderer
    cli = CliRenderer(Registry())
    cli.log_routed_message("axioma", "thea", "# Title\nbody text")
    assert len(cli.messages) == 1
    header, msg_type, body = cli.messages[0]
    assert msg_type == "msg"
    assert header.startswith("[")
    assert "axioma -> thea" in header
    # Header format: [HH:MM:SS] <from> -> <to> | REQUEST
    import re
    assert re.match(r"^\[\d{2}:\d{2}:\d{2}\] axioma -> thea \| REQUEST$", header)
    assert body == "# Title\nbody text"


def test_cli_log_routed_message_non_string_body_jsonified():
    from neurogossip_server.server import CliRenderer
    cli = CliRenderer(Registry())
    cli.log_routed_message("a", "b", {"key": "value"})
    _, _, body = cli.messages[0]
    import json
    assert json.loads(body) == {"key": "value"}


def test_cli_log_routed_reply_tagged_reply():
    """A reply is tagged | REPLY (vs | REQUEST) in the header."""
    from neurogossip_server.server import CliRenderer
    cli = CliRenderer(Registry())
    cli.log_routed_message("thea", "axioma", "sqrt(pi)", reply_to="m1")
    header, msg_type, body = cli.messages[0]
    assert msg_type == "msg"
    assert "thea -> axioma" in header
    assert "| REPLY" in header
    assert body == "sqrt(pi)"


async def test_server_on_message_routed_tags_request_and_reply():
    """The server hook forwards messages to the CLI with REQUEST / REPLY tags."""
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.1)
    _, _ = make_session(registry, "axioma", ws=MockWS(on_message=auto_ack_cb(router, "axioma")))
    _, _ = make_session(registry, "thea", ws=MockWS(on_message=auto_ack_cb(router, "thea")))
    srv = NeurogossipServer(host="127.0.0.1", port=0, cli_mode=True, log_level="CRITICAL")
    srv.registry = registry        # share the populated registry
    srv.router = router
    srv.router.on_message_routed = srv._on_message_routed

    # axioma -> thea (request), then thea -> axioma (reply to that request).
    await router.route("axioma", {"to": "thea", "body": "question", "msg_id": "q1"})
    await _drain()
    await router.route("thea", {"to": "axioma", "body": "answer", "msg_id": "a1",
                                "reply_to": "q1"})
    await _drain()
    msg_entries = [(h, b) for h, mt, b in srv.cli.messages if mt == "msg"]
    assert len(msg_entries) == 2
    req_header, _ = msg_entries[0]
    reply_header, _ = msg_entries[1]
    assert "axioma -> thea" in req_header and "| REQUEST" in req_header
    assert "thea -> axioma" in reply_header and "| REPLY" in reply_header


def test_cli_render_markdown_entry_is_multiline():
    """A 'msg' entry renders to a header line + markdown body + blank + '---'."""
    from neurogossip_server.server import CliRenderer
    cli = CliRenderer(Registry())
    cli.log_routed_message("a", "b", "# Heading\n\n- item one\n- item two")
    lines = cli._render_entry_lines(*cli.messages[0])
    # First line is the styled header (REQUEST tag colored).
    assert lines[0].startswith("\033[")
    assert "a -> b" in lines[0]
    assert "REQUEST" in lines[0]
    # Body expands to several markdown lines, then a blank line and a '---' separator.
    assert len(lines) > 2
    assert lines[-2] == ""
    assert "---" in lines[-1]
    # Body content is present somewhere in the rendered lines.
    flat = "\n".join(lines)
    assert "Heading" in flat
    assert "item one" in flat


def test_cli_render_reply_tag_colored_differently():
    """REQUEST and REPLY tags use different colors."""
    from neurogossip_server.server import CliRenderer
    cli = CliRenderer(Registry())
    cli.log_routed_message("a", "b", "q")
    cli.log_routed_message("b", "a", "r", reply_to="x")
    req_lines = cli._render_entry_lines(*cli.messages[0])
    rep_lines = cli._render_entry_lines(*cli.messages[1])
    assert "\033[36m" in req_lines[0]   # cyan REQUEST
    assert "\033[35m" in rep_lines[0]   # magenta REPLY


def test_cli_render_does_not_raise_with_markdown(capsys):
    from neurogossip_server.server import CliRenderer
    cli = CliRenderer(Registry())
    cli.log_routed_message("a", "b", "# Title\n\nSome **bold** text and `code`.")
    cli.render()  # must not raise
    out = capsys.readouterr().out
    assert "a -> b" in out
    assert "Title" in out
    assert "---" in out


def test_server_wires_router_hook_only_in_cli_mode():
    srv_cli = NeurogossipServer(host="127.0.0.1", port=0, cli_mode=True, log_level="CRITICAL")
    assert srv_cli.router.on_message_routed is not None
    srv_headless = NeurogossipServer(host="127.0.0.1", port=0, cli_mode=False, log_level="CRITICAL")
    assert srv_headless.router.on_message_routed is None


def test_server_on_message_routed_forwards_to_cli():
    srv = NeurogossipServer(host="127.0.0.1", port=0, cli_mode=True, log_level="CRITICAL")
    srv._on_message_routed("axioma", "thea", "hi there")
    assert any(mt == "msg" and "axioma -> thea" in h for h, mt, b in srv.cli.messages)


# ---------------------------------------------------------------------------
# Stale-session eviction on reconnect (half-open socket)
# --------------------------------------------------------------------

async def test_reconnect_evicts_stale_online_session():
    """A reconnecting agent whose old session is half-open (dead socket) is evicted
    and replaced instead of waiting 45s for the heartbeat timeout."""
    srv = NeurogossipServer(host="127.0.0.1", port=0, presence_debounce=0.0,
                            log_level="CRITICAL", liveness_timeout=0.5)
    # First registration with a "half-open" socket: the server marks it online, but
    # the socket is dead (won't pong).
    dead_ws = MockWS(live=False)
    await srv._handle_register(dead_ws, {"agent_id": "axioma",
                                         "metadata": {"display_name": "A", "version": "1.0"}})
    assert srv.registry.agents["axioma"].is_online is True
    # Queue an offline message against the stale session to verify carry-over.
    srv.registry.agents["axioma"].pending_messages.append(
        {"type": "message", "from": "x", "body": "queued", "msg_id": "stale-q"})

    # Reconnect with a live socket — must succeed (not ALREADY_REGISTERED).
    live_ws = MockWS(live=True)
    aid = await srv._handle_register(live_ws, {"agent_id": "axioma",
                                               "metadata": {"display_name": "A", "version": "1.0"}})
    assert aid == "axioma"
    assert any(m.get("type") == "registered" for m in live_ws.sent)
    new_sess = srv.registry.agents["axioma"]
    assert new_sess.websocket is live_ws
    assert new_sess.is_online is True
    # Queued message was carried over and delivered to the new live socket on reconnect.
    assert any(m.get("body") == "queued" for m in live_ws.sent)
    assert new_sess.pending_messages == []
    if srv.presence._flush_task is not None and not srv.presence._flush_task.done():
        srv.presence._flush_task.cancel()


async def test_live_session_not_evicted_on_reconnect():
    """A genuinely-live online session is NOT evicted — anti-hijack preserved (design §2.1)."""
    srv = NeurogossipServer(host="127.0.0.1", port=0, presence_debounce=0.0,
                            log_level="CRITICAL", liveness_timeout=0.5)
    live_ws = MockWS(live=True)
    await srv._handle_register(live_ws, {"agent_id": "axioma",
                                          "metadata": {"display_name": "A", "version": "1.0"}})
    second_ws = MockWS(live=True)
    aid = await srv._handle_register(second_ws, {"agent_id": "axioma",
                                                  "metadata": {"display_name": "A", "version": "1.0"}})
    assert aid is None  # rejected
    assert any(m.get("code") == "ALREADY_REGISTERED" for m in second_ws.sent)
    # Original session untouched.
    assert srv.registry.agents["axioma"].websocket is live_ws
    assert srv.registry.agents["axioma"].is_online is True
    if srv.presence._flush_task is not None and not srv.presence._flush_task.done():
        srv.presence._flush_task.cancel()


async def test_disconnect_from_old_socket_does_not_clobber_reconnected_session():
    """After eviction replaces a session, the old connection's teardown must not mark
    the new (live) session offline."""
    srv = NeurogossipServer(host="127.0.0.1", port=0, presence_debounce=0.0,
                            log_level="CRITICAL", liveness_timeout=0.5)
    dead_ws = MockWS(live=False)
    await srv._handle_register(dead_ws, {"agent_id": "axioma",
                                         "metadata": {"display_name": "A", "version": "1.0"}})
    live_ws = MockWS(live=True)
    await srv._handle_register(live_ws, {"agent_id": "axioma",
                                          "metadata": {"display_name": "A", "version": "1.0"}})
    assert srv.registry.agents["axioma"].is_online is True

    # The dead socket's connection handler finally runs disconnect with the OLD ws.
    await srv._handle_disconnect("axioma", dead_ws)
    # New session must remain online — old teardown ignored.
    assert srv.registry.agents["axioma"].is_online is True
    assert srv.registry.agents["axioma"].websocket is live_ws
    if srv.presence._flush_task is not None and not srv.presence._flush_task.done():
        srv.presence._flush_task.cancel()


def test_is_session_live_dead_when_close_code_set():
    """A socket the library already knows is closed is dead without a ping probe."""
    srv = NeurogossipServer(host="127.0.0.1", port=0, log_level="CRITICAL")
    ws = MockWS(close_code=1001)  # already closed
    sess = AgentSession(agent_id="a", websocket=ws,
                        metadata={"display_name": "A", "version": "1.0"},
                        session_id="s", connected_at=time.monotonic(),
                        last_pong_time=time.monotonic())

    async def check():
        return await srv._is_session_live(sess)
    assert asyncio.run(check()) is False


# ---------------------------------------------------------------------------
# .env loading + rotating debug logging
# --------------------------------------------------------------------

def test_load_env_parses_file(tmp_path):
    from neurogossip_server.server import _load_env
    envfile = tmp_path / ".env"
    envfile.write_text(
        "# a comment\n"
        "NEUROGOSSIP_SERVER_BIND=127.0.0.1\n"
        "export NEUROGOSSIP_SERVER_PORT=8966\n"
        "NEUROGOSSIP_SERVER_LOG='logs/srv.log'  # inline comment\n"
        'NEUROGOSSIP_SERVER_SECRET="s3cret"\n'
        "BLANK=\n"
        "NOVAL\n"
    )
    env = _load_env(str(envfile))
    assert env["NEUROGOSSIP_SERVER_BIND"] == "127.0.0.1"
    assert env["NEUROGOSSIP_SERVER_PORT"] == "8966"
    assert env["NEUROGOSSIP_SERVER_LOG"] == "logs/srv.log"
    assert env["NEUROGOSSIP_SERVER_SECRET"] == "s3cret"
    assert env["BLANK"] == ""
    assert "NOVAL" not in env


def test_load_env_missing_file_is_empty(tmp_path):
    from neurogossip_server.server import _load_env
    assert _load_env(str(tmp_path / "nope.env")) == {}


def test_parse_bytes():
    from neurogossip_server.server import _parse_bytes
    assert _parse_bytes("250MB", 0) == 250 * 1024 * 1024
    assert _parse_bytes("250M", 0) == 250 * 1024 * 1024
    assert _parse_bytes("1GB", 0) == 1024 ** 3
    assert _parse_bytes("1024", 0) == 1024
    assert _parse_bytes("512KB", 0) == 512 * 1024
    assert _parse_bytes("", 123) == 123
    assert _parse_bytes("garbage", 999) == 999


def test_parse_bool():
    from neurogossip_server.server import _parse_bool
    for v in ("true", "TRUE", "1", "yes", "on", "Y"):
        assert _parse_bool(v) is True
    for v in ("false", "0", "no", "off", "n", ""):
        assert _parse_bool(v) is False
    assert _parse_bool(None, True) is True


def test_setup_logging_rotating_handler_config(tmp_path):
    import logging
    from neurogossip_server.server import _setup_logging
    log_path = tmp_path / "srv.log"
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    try:
        fh = _setup_logging(str(log_path), "debug", max_bytes=300, backup_count=3,
                            cli_mode=True)
        assert isinstance(fh, logging.handlers.RotatingFileHandler)
        assert fh.maxBytes == 300
        assert fh.backupCount == 3
        assert fh.level == logging.DEBUG
        assert root.level == logging.DEBUG
        assert log_path.exists()
    finally:
        for h in list(root.handlers):
            root.removeHandler(h)
            try: h.close()
            except Exception: pass
        root.handlers = saved_handlers
        root.setLevel(saved_level)


def test_rotating_log_archives_when_size_reached(tmp_path):
    """When the log reaches maxBytes it archives (.log.1) and starts a new file."""
    import logging
    from logging.handlers import RotatingFileHandler
    log_path = tmp_path / "neurogossip-server.log"
    fh = RotatingFileHandler(str(log_path), maxBytes=300, backupCount=5,
                             encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(message)s"))
    log = logging.getLogger("neurogossip.testrotate")
    log.handlers = [fh]
    log.setLevel(logging.DEBUG)
    log.propagate = False
    try:
        for i in range(40):
            log.info("line-%04d-padding-padding-padding", i)
        fh.flush()
        names = sorted(p.name for p in tmp_path.iterdir())
        assert "neurogossip-server.log" in names
        assert "neurogossip-server.log.1" in names  # at least one archive
    finally:
        fh.close()
        log.handlers = []


async def test_route_logs_message_at_debug(caplog):
    """The router logs every routed message (from/to/body) at DEBUG level."""
    import logging
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.1)
    _, _ = make_session(registry, "a")
    _, _ = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))
    rl = logging.getLogger("router")
    saved_level = rl.level
    rl.setLevel(logging.DEBUG)
    try:
        with caplog.at_level(logging.DEBUG, logger="router"):
            await router.route("a", {"to": "b", "body": "hello world", "msg_id": "lm1"})
            await _drain()
        joined = "\n".join(r.getMessage() for r in caplog.records)
        assert "message from=a to=b" in joined
        assert "hello world" in joined
    finally:
        rl.setLevel(saved_level)


def test_main_loads_env_defaults(tmp_path, monkeypatch):
    """main() reads .env and uses it for argparse defaults (e.g. port)."""
    import logging
    from types import SimpleNamespace
    import neurogossip_server.server as srv_mod
    envfile = tmp_path / ".env"
    envfile.write_text("NEUROGOSSIP_SERVER_PORT=7777\nNEUROGOSSIP_SERVER_BIND=127.0.0.1\n")
    async def fake_start(self):
        return None
    monkeypatch.setattr(srv_mod.NeurogossipServer, "start", fake_start)
    monkeypatch.setattr(srv_mod, "_setup_logging",
                        lambda **kw: SimpleNamespace(maxBytes=0, backupCount=0))
    monkeypatch.chdir(tmp_path)
    captured = {}
    real_init = srv_mod.NeurogossipServer.__init__
    def spy_init(self, *a, **kw):
        captured.update(kw)
        # don't actually construct (avoids needing a real event loop)
        self.__dict__.update({"host": kw.get("host"), "port": kw.get("port")})
    monkeypatch.setattr(srv_mod.NeurogossipServer, "__init__", spy_init)
    monkeypatch.setattr("sys.argv", ["server.py"])
    srv_mod.main()
    assert captured.get("port") == 7777
    assert captured.get("host") == "127.0.0.1"


# ---------------------------------------------------------------------------
# False-offline delivery + ERROR-level drop logging
# --------------------------------------------------------------------

async def test_route_delivers_to_offline_flagged_open_socket():
    """A target the heartbeat flagged offline (is_online=False) but whose socket is
    still open (close_code None — e.g. a busy agent not app-ponging) still receives
    the message instead of being dropped. (The axioma->skye root cause.)"""
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.3)
    _, ws1 = make_session(registry, "a")
    # skye-like: flagged offline, but socket alive and pongs.
    sess_b, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))
    sess_b.is_online = False  # missed app pongs → heartbeat flagged offline
    # close_code stays None → socket open
    await router.route("a", {"to": "b", "body": "reply", "msg_id": "fo1"})
    await _drain()
    delivered = [m for m in ws2.sent if m.get("type") == "message"]
    assert len(delivered) == 1 and delivered[0]["body"] == "reply"
    acks = [m for m in ws1.sent if m.get("type") == "message_ack"]
    assert acks[-1]["status"] == "delivered"


async def test_route_offline_open_socket_no_receipt_is_unconfirmed():
    """Open socket but no receipt → unconfirmed (sent to the socket), not dropped."""
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.2)
    _, ws1 = make_session(registry, "a")
    sess_b, _ = make_session(registry, "b")  # close_code None, no auto-ack
    sess_b.is_online = False
    await router.route("a", {"to": "b", "body": "hi", "msg_id": "uc1"})
    await asyncio.sleep(0.3)  # past delivery_timeout (0.2) so the watcher timer fires
    assert registry.messages["uc1"].status == "unconfirmed"
    acks = [m for m in ws1.sent if m.get("type") == "message_ack"]
    assert acks[-1]["status"] == "unconfirmed"


async def test_route_offline_closed_socket_no_ttl_drop_logged_as_error(caplog):
    """A genuinely-closed target socket with no ttl drops the message and logs ERROR."""
    import logging
    registry = Registry()
    router = MessageRouter(registry)
    _, ws1 = make_session(registry, "a")
    sess_b, _ = make_session(registry, "b", ws=MockWS(close_code=1000))  # closed
    sess_b.is_online = False
    with caplog.at_level(logging.ERROR, logger="router"):
        await router.route("a", {"to": "b", "body": "lost", "msg_id": "e1"})
        await _drain()
    assert registry.messages["e1"].status == "offline"
    assert any(r.levelno == logging.ERROR and "DROPPED" in r.getMessage() and "e1" in r.getMessage()
               for r in caplog.records)


async def test_delivery_failure_no_ttl_logs_error(caplog):
    """A send failure with no ttl marks the message failed and logs ERROR."""
    import logging
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.1)

    class DeadWS:
        async def send(self, msg):
            raise OSError("connection reset")
        @property
        def close_code(self):
            return None
    _, ws1 = make_session(registry, "a")
    sess_b, _ = make_session(registry, "b", ws=DeadWS())  # open per close_code, but send fails
    sess_b.on_send_failure = router._handle_writer_failure
    with caplog.at_level(logging.ERROR, logger="router"):
        await router.route("a", {"to": "b", "body": "x", "msg_id": "e2"})
        await asyncio.sleep(0.1)  # let the writer task fail + failure handler run
    assert registry.messages["e2"].status == "failed"
    assert any(r.levelno == logging.ERROR and "DROPPED" in r.getMessage() and "e2" in r.getMessage()
               for r in caplog.records)


# ---------------------------------------------------------------------------
# Performance: third-party logger quieting, markdown cache, coalesced render
# --------------------------------------------------------------------

def test_setup_logging_quiets_chatty_third_party_loggers(tmp_path):
    """markdown_it/rich/websockets/asyncio are pinned to WARNING so their DEBUG
    noise doesn't flood the rotating log and stall the event loop."""
    import logging
    from neurogossip_server.server import _setup_logging
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    names = ("markdown_it", "markdown_it.tree", "rich", "websockets", "asyncio")
    saved_levels = {n: logging.getLogger(n).level for n in names}
    try:
        _setup_logging(str(tmp_path / "s.log"), "debug", 1000, 2, cli_mode=True)
        for n in names:
            assert logging.getLogger(n).level == logging.WARNING, n
    finally:
        for h in list(root.handlers):
            root.removeHandler(h)
            try: h.close()
            except Exception: pass
        root.handlers = saved_handlers
        root.setLevel(saved_level)
        for n, lv in saved_levels.items():
            logging.getLogger(n).setLevel(lv)


def test_cli_render_markdown_caches_body():
    """Rendered markdown lines are cached so re-rendering the window doesn't
    re-parse (and re-flood markdown_it logs) on every paint."""
    from neurogossip_server.server import CliRenderer
    cli = CliRenderer(Registry())
    body = "# Title\n\nsome **bold** body"
    lines1 = cli._render_markdown(body)
    assert body in cli._md_cache
    lines2 = cli._render_markdown(body)
    assert lines2 == lines1  # served from cache, identical


async def test_cli_schedule_render_coalesces_burst():
    """A burst of _schedule_render calls produces a single render, deferred (not
    synchronous), so per-message rendering can't pin the event loop."""
    from neurogossip_server.server import CliRenderer
    cli = CliRenderer(Registry())
    calls = []
    cli.render = lambda: calls.append(1)
    for _ in range(5):
        cli._schedule_render()
    assert calls == []  # deferred, not rendered synchronously
    await asyncio.sleep(0.12)  # past the 50ms coalesce window
    assert calls == [1]  # exactly one coalesced render


def test_server_keepalive_defaults_are_lenient_for_busy_agents():
    """Default keepalive must be generous enough that an agent blocking its event
    loop for a long turn (>10s) isn't reaped — a 10s timeout closed all agents at
    once (mass drops + reconnect storms)."""
    srv = NeurogossipServer(host="127.0.0.1", port=0, log_level="CRITICAL")
    assert srv.ws_ping_interval == 20.0
    assert srv.ws_ping_timeout == 45.0
    assert srv.ws_ping_timeout >= srv.heartbeat.interval_s * srv.heartbeat.max_missed  # >= app-heartbeat window


async def test_cli_schedule_render_does_not_block_event_loop_on_slow_stdout():
    """A stalled terminal makes ``render()`` (stdout write) block. _schedule_render
    must return immediately — the blocking write happens in the render thread, not
    on the asyncio event loop (which would freeze keepalives/handshakes)."""
    from neurogossip_server.server import CliRenderer
    import time as _t
    cli = CliRenderer(Registry())
    def slow_render():
        _t.sleep(0.5)  # simulate a blocked stdout write (stalled SSH/terminal)
    cli.render = slow_render
    loop = asyncio.get_event_loop()
    t0 = loop.time()
    for _ in range(3):
        cli._schedule_render()  # must NOT block
    elapsed = loop.time() - t0
    assert elapsed < 0.1, f"_schedule_render blocked the loop for {elapsed:.3f}s"
    await asyncio.sleep(0.8)  # let the render thread finish its slow paint
