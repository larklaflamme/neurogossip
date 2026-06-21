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

    def __init__(self, on_message=None):
        self.sent = []
        self.closed = False
        self._on_message = on_message

    async def send(self, msg):
        frame = json.loads(msg)
        self.sent.append(frame)
        if self._on_message and frame.get("type") == "message":
            asyncio.create_task(self._on_message(frame))

    async def close(self):
        self.closed = True


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
    # Wait long enough for >= max_missed failed pings.
    await asyncio.sleep(0.25)
    await hb.stop()
    await task
    assert sess.is_online is False
    assert sess.missed_pings >= 2


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
    errs = [m for m in ws1.sent if m.get("type") == "error"]
    assert errs and errs[0]["code"] == "BAD_REQUEST"

    await router.route("a", {"to": "b"})
    errs = [m for m in ws1.sent if m.get("type") == "error" and m["code"] == "BAD_REQUEST"]
    assert len(errs) >= 2


async def test_route_agent_not_found():
    registry = Registry()
    router = MessageRouter(registry)
    _, ws1 = make_session(registry, "a")
    await router.route("a", {"to": "ghost", "body": "x", "msg_id": "m"})
    errs = [m for m in ws1.sent if m.get("type") == "error"]
    assert errs[0]["code"] == "AGENT_NOT_FOUND"


async def test_route_message_too_large():
    registry = Registry()
    router = MessageRouter(registry, max_message_bytes=64)
    _, ws1 = make_session(registry, "a")
    _, ws2 = make_session(registry, "b")
    big = "x" * 200
    await router.route("a", {"to": "b", "body": big, "msg_id": "m"})
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
    await router.route("a", {"to": "c", "body": "1", "msg_id": "x2"})
    assert registry.pair_seqs[("a", "b")] == 1
    assert registry.pair_seqs[("a", "c")] == 1


# ---------------------------------------------------------------------------
# Offline queue + queued delivery ACK
# --------------------------------------------------------------------

async def test_route_offline_queued():
    registry = Registry()
    router = MessageRouter(registry)
    _, ws1 = make_session(registry, "a")
    sess_b, _ = make_session(registry, "b")
    sess_b.is_online = False

    await router.route("a", {"to": "b", "body": "queued", "msg_id": "q1", "ttl": 60})
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
    sess_b, _ = make_session(registry, "b")
    sess_b.is_online = False

    await router.route("a", {"to": "b", "body": "x", "msg_id": "q1"})
    acks = [m for m in ws1.sent if m.get("type") == "message_ack"]
    statuses = [a["status"] for a in acks]
    assert statuses[-1] == "offline"
    assert registry.messages["q1"].status == "offline"
    assert sess_b.pending_messages == []


async def test_on_reconnect_delivers_queued_and_acks_sender():
    registry = Registry()
    router = MessageRouter(registry)
    _, ws1 = make_session(registry, "a")
    sess_b, ws2 = make_session(registry, "b")
    sess_b.is_online = False

    await router.route("a", {"to": "b", "body": "queued", "msg_id": "q1", "ttl": 60})
    sess_b.is_online = True
    await router.on_reconnect("b")

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
    sess_b, _ = make_session(registry, "b")
    sess_b.is_online = False

    await router.route("a", {"to": "b", "body": "queued", "msg_id": "q1", "ttl": 60})
    # Make the record artificially old so TTL has elapsed.
    registry.messages["q1"].created_at = time.monotonic() - 120.0
    sess_b.is_online = True
    await router.on_reconnect("b")

    assert registry.messages["q1"].status == "ttl_expired"
    acks = [m for m in ws1.sent if m.get("type") == "message_ack"]
    assert acks[-1]["status"] == "ttl_expired"
    assert sess_b.pending_messages == []


async def test_on_reconnect_unknown_agent():
    registry = Registry()
    router = MessageRouter(registry)
    # No session — should be a no-op.
    await router.on_reconnect("ghost")


# ---------------------------------------------------------------------------
# Deduplication
# --------------------------------------------------------------------

async def test_dedup_same_msg_id_not_redelivered():
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.1)
    _, ws1 = make_session(registry, "a")
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    await router.route("a", {"to": "b", "body": "first", "msg_id": "dup1"})
    await router.route("a", {"to": "b", "body": "second", "msg_id": "dup1"})
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
    await router.route("a", {"to": "b", "body": "over", "msg_id": "rover"})
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
    await router.route("a", {"to": "c", "body": "1", "msg_id": "a2"})
    # Third should be rate-limited (budget empty, no refill instant).
    await router.route("a", {"to": "b", "body": "2", "msg_id": "a3"})
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
    assert "a" in sess_b.blocked_agents
    await router.route("a", {"to": "b", "body": "x", "msg_id": "bk1"})
    errs = [m for m in ws1.sent if m.get("type") == "error" and m["code"] == "BLOCKED"]
    assert errs
    assert not any(m.get("type") == "message" for m in ws2.sent)

    await router.on_unblock("b", {"agent_id": "a"})
    assert "a" not in sess_b.blocked_agents
    await router.route("a", {"to": "b", "body": "y", "msg_id": "bk2"})
    assert any(m.get("type") == "message" for m in ws2.sent)


async def test_block_missing_agent_id():
    registry = Registry()
    router = MessageRouter(registry)
    _, ws1 = make_session(registry, "a")
    await router.on_block("a", {})
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
    # c tries to forge a reply to "orig" (c was not the recipient)
    await router.route("c", {"to": "a", "body": "forged", "msg_id": "forged",
                             "reply_to": "orig"})
    errs = [m for m in ws3.sent if m.get("type") == "error" and m["code"] == "FORGED_REPLY"]
    assert errs


async def test_received_from_wrong_agent_rejected():
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.5)
    _, ws1 = make_session(registry, "a", ws=MockWS(on_message=auto_ack_cb(router, "a")))
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))
    _, ws3 = make_session(registry, "c")

    await router.route("a", {"to": "b", "body": "hi", "msg_id": "orig"})
    # c claims receipt for a message sent to b
    await router.on_received("c", {"msg_id": "orig"})
    errs = [m for m in ws3.sent if m.get("type") == "error" and m["code"] == "FORGED_REPLY"]
    assert errs


async def test_received_unknown_msg_id_noop():
    registry = Registry()
    router = MessageRouter(registry)
    _, ws1 = make_session(registry, "a")
    # Should not raise.
    await router.on_received("a", {"msg_id": "ghost"})
    await router.on_received("a", {})


# ---------------------------------------------------------------------------
# Conversation management
# --------------------------------------------------------------------

async def test_conversation_end_and_reopen():
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.1)
    _, ws1 = make_session(registry, "a", ws=MockWS(on_message=auto_ack_cb(router, "a")))
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    await router.route("a", {"to": "b", "body": "first", "msg_id": "cm1"})
    conv_id = registry.messages["cm1"].conversation_id

    await router.on_conversation_end("a", {"conversation_id": conv_id, "reason": "done"})
    conv = registry.conversations[conv_id]
    assert conv.is_ended is True
    assert conv.ended_by == "a"
    # b should have been notified
    assert any(m.get("type") == "conversation_ended" for m in ws2.sent)

    # Message after end is rejected.
    await router.route("a", {"to": "b", "body": "after", "msg_id": "cm2",
                             "conversation_id": conv_id})
    errs = [m for m in ws1.sent if m.get("type") == "error" and m["code"] == "CONVERSATION_ENDED"]
    assert errs

    # Reopen with reopen=True.
    await router.route("a", {"to": "b", "body": "reopen", "msg_id": "cm3",
                             "conversation_id": conv_id, "reopen": True})
    assert registry.conversations[conv_id].is_ended is False


async def test_conversation_end_errors():
    registry = Registry()
    router = MessageRouter(registry)
    _, ws1 = make_session(registry, "a")
    _, ws2 = make_session(registry, "b")
    _, ws3 = make_session(registry, "c")

    # Missing conversation_id
    await router.on_conversation_end("a", {})
    assert any(m.get("code") == "BAD_REQUEST" for m in ws1.sent if m.get("type") == "error")

    # Unknown conversation
    await router.on_conversation_end("a", {"conversation_id": "ghost"})
    assert any(m.get("code") == "CONVERSATION_NOT_FOUND"
               for m in ws1.sent if m.get("type") == "error")

    # Not a participant: build a conversation between a and b only.
    await router.route("a", {"to": "b", "body": "x", "msg_id": "p1"})
    conv_id = registry.messages["p1"].conversation_id
    await router.on_conversation_end("c", {"conversation_id": conv_id})
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
        prev = mid
    # 4th reply: b replies to d2 (which was a→b), depth 4 > 3 → DEPTH_EXCEEDED.
    await router.route("b", {"to": "a", "body": "too-deep", "msg_id": "d3",
                             "reply_to": prev, "conversation_id": conv_id})
    errs = [m for m in ws2.sent if m.get("type") == "error" and m["code"] == "DEPTH_EXCEEDED"]
    assert errs


async def test_reply_to_missing_original_starts_new_conversation():
    registry = Registry()
    router = MessageRouter(registry, delivery_timeout=0.1)
    _, ws1 = make_session(registry, "a")
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    await router.route("a", {"to": "b", "body": "x", "msg_id": "m1",
                             "reply_to": "nonexistent"})
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
    # 4th triggers the breaker (3 already recorded; the 4th updates then opens).
    await router.route("a", {"to": "b", "body": "over", "msg_id": "cb4"})
    assert registry.circuit_breakers[("a", "b")].is_open is True


async def test_circuit_breaker_blocks_then_resets():
    registry = Registry()
    router = MessageRouter(registry, circuit_breaker_window=60, circuit_breaker_max=2,
                           circuit_breaker_cooldown=0.05, delivery_timeout=0.05)
    _, ws1 = make_session(registry, "a")
    _, ws2 = make_session(registry, "b", ws=MockWS(on_message=auto_ack_cb(router, "b")))

    for i in range(2):
        await router.route("a", {"to": "b", "body": f"m{i}", "msg_id": f"cb{i}"})
    # Force-open with a near-zero cooldown so it auto-resets.
    router._open_circuit_breaker("a", "b")
    assert router._is_circuit_open("a", "b") is True

    await router.route("a", {"to": "b", "body": "blocked", "msg_id": "cbblk"})
    errs = [m for m in ws1.sent if m.get("type") == "error" and m["code"] == "CIRCUIT_BREAKER"]
    assert errs

    # After cooldown elapses, breaker resets and routing works again.
    await asyncio.sleep(0.1)
    assert router._is_circuit_open("a", "b") is False
    await router.route("a", {"to": "b", "body": "after", "msg_id": "cbafter"})
    assert any(m.get("type") == "message" and m["msg_id"] == "cbafter" for m in ws2.sent)


async def test_loop_detection_opens_breaker():
    """6 alternating messages A→B→A→B→A→B with depth limit high enough to not trip first."""
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
        await router.route(sender, {"to": target, "body": f"lp{i}", "msg_id": mid,
                                    "reply_to": prev, "conversation_id": conv_id})
        prev = mid
    # The 6th message completes the alternating pattern → breaker opened for both dirs.
    assert registry.circuit_breakers[("a", "b")].is_open is True
    assert registry.circuit_breakers[("b", "a")].is_open is True


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
    assert any(m.get("type") == "presence" for m in ws1.sent)


async def test_presence_latest_status_wins():
    registry = Registry()
    pres = PresenceBroadcaster(registry, debounce_s=0.0)
    _, ws1 = make_session(registry, "a")
    await pres.update("b", "online", {"display_name": "B", "version": "1.0"})
    await pres.update("b", "offline")
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


def test_main_builds_and_runs(monkeypatch):
    import neurogossip_server.server as srv_mod

    async def fake_start(self):
        return None
    monkeypatch.setattr(srv_mod.NeurogossipServer, "start", fake_start)
    monkeypatch.setattr("sys.argv", [
        "server.py", "--port", "9999", "--host", "127.0.0.1",
        "--presence-debounce", "0.5", "--require-auth", "--secret", "k",
        "--rate-limit-agent", "10", "--rate-limit-pair", "5",
        "--max-conversation-depth", "7", "--circuit-breaker-max", "9",
        "--circuit-breaker-cooldown", "30", "--delivery-timeout", "5",
        "--max-message-bytes", "4096", "--log-level", "WARNING",
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
         "--host", "127.0.0.1", "--log-level", "CRITICAL"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
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