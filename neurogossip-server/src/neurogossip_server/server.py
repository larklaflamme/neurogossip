#!/usr/bin/env python3
"""Neurogossip v5.0 FINAL — WebSocket Agent Registry & Direct Messaging Server.

Implements all 12 critical features from DESIGN_v5_FINAL.md:
  1. Registration (no force flag)
  2. Timestamp-based heartbeat (no race condition)
  3. Message routing with end-to-end ACK
  4. Per-pair sequence number ordering
  5. Offline message queue with TTL check + queued delivery ACK
  6. Rate limiting (per-agent token bucket + per-pair sliding window)
  7. Loop prevention (depth tracking, circuit breaker, loop detector, end signal)
  8. Conversation management
  9. Graceful shutdown
  10. CLI mode (ANSI escape codes, no threading)
  11. Message deduplication (TTL-based msg_id cache)
  12. Message record pruning

Usage:
    python server.py --port 8765
    python server.py --port 8765 --cli
"""

import argparse
import asyncio
import json
import logging
import os
import re
import signal
import sys
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import websockets
from websockets.asyncio.server import ServerConnection, serve

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class AgentSession:
    agent_id: str
    websocket: ServerConnection
    metadata: dict
    session_id: str
    connected_at: float
    last_pong_time: float
    missed_pings: int = 0
    is_online: bool = True
    pending_messages: list[dict] = field(default_factory=list)
    blocked_agents: set[str] = field(default_factory=set)
    rate_budget: float = 60.0
    _rate_last_refill: float = 0.0
    _pair_counters: dict[tuple[str, str], deque] = field(default_factory=dict)


@dataclass
class MessageRecord:
    msg_id: str
    sender_id: str
    recipient_id: str
    body: str
    reply_to: Optional[str]
    conversation_id: str
    seq: int
    depth: int
    status: str
    created_at: float
    ttl: int


@dataclass
class ConversationRecord:
    conversation_id: str
    participants: set[str]
    depth: int
    created_at: float
    is_ended: bool = False
    ended_by: Optional[str] = None
    ended_at: Optional[float] = None
    recent_messages: deque = field(default_factory=lambda: deque(maxlen=10))


@dataclass
class CircuitBreakerRecord:
    pair_key: tuple[str, str]
    timestamps: deque
    is_open: bool = False
    cooldown_until: Optional[float] = None


class Registry:
    """In-memory server state — all data structures from the design."""

    def __init__(self):
        self.agents: dict[str, AgentSession] = {}
        self.sessions: dict[str, str] = {}
        self.messages: dict[str, MessageRecord] = {}
        self.conversations: dict[str, ConversationRecord] = {}
        self.pair_seqs: dict[tuple[str, str], int] = {}
        self.circuit_breakers: dict[tuple[str, str], CircuitBreakerRecord] = {}
        self.msg_id_cache: dict[str, float] = {}
        self.delivery_events: dict[str, asyncio.Event] = {}


# ---------------------------------------------------------------------------
# Metadata schema
# ---------------------------------------------------------------------------

METADATA_SCHEMA = {
    "display_name": {"type": str, "required": True, "max_length": 64},
    "version": {"type": str, "required": True, "max_length": 32},
    "capabilities": {"type": list, "required": False, "item_type": str},
}

# Design §2.1: agent_id is lowercase, alphanumeric + hyphens, max 64 chars.
AGENT_ID_RE = re.compile(r"^[a-z0-9-]{1,64}$")


def _validate_metadata(metadata: dict) -> Optional[str]:
    """Validate metadata against schema. Returns error string or None."""
    if not isinstance(metadata, dict):
        return "metadata must be a dict"
    for field_name, rules in METADATA_SCHEMA.items():
        value = metadata.get(field_name)
        if rules["required"] and value is None:
            return f"metadata.{field_name} is required"
        if value is not None:
            if not isinstance(value, rules["type"]):
                return f"metadata.{field_name} must be {rules['type'].__name__}"
            max_len = rules.get("max_length")
            if max_len and isinstance(value, str) and len(value) > max_len:
                return f"metadata.{field_name} exceeds max length {max_len}"
            item_type = rules.get("item_type")
            if item_type and isinstance(value, list):
                for i, item in enumerate(value):
                    if not isinstance(item, item_type):
                        return f"metadata.{field_name}[{i}] must be {item_type.__name__}"
    return None


# ---------------------------------------------------------------------------
# Presence broadcaster (debounced batch updates — design §2.3 / M3)
# ---------------------------------------------------------------------------

class PresenceBroadcaster:
    """Debounced presence broadcast.

    Collects presence changes and flushes a single batched ``presence`` frame after a
    debounce window (default 1 second). This collapses burst connect/disconnect into one
    update per window, turning an O(N²) burst into O(N) (design §2.3).
    """

    def __init__(self, registry: Registry, debounce_s: float = 1.0,
                 logger: logging.Logger = None):
        self.registry = registry
        self.debounce_s = debounce_s
        self.logger = logger or logging.getLogger("presence")
        # agent_id -> change dict (latest status wins, so bursts collapse)
        self._pending: dict[str, dict] = {}
        self._flush_task: Optional[asyncio.Task] = None

    async def update(self, agent_id: str, status: str, metadata: dict = None):
        """Record a presence change and schedule a batched flush."""
        change = {"agent_id": agent_id, "status": status}
        if status == "online" and metadata is not None:
            change["metadata"] = metadata
        self._pending[agent_id] = change
        if self.debounce_s <= 0:
            await self.flush()
            return
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = asyncio.create_task(self._flush_after())

    async def _flush_after(self):
        try:
            await asyncio.sleep(self.debounce_s)
            await self.flush()
        except asyncio.CancelledError:
            pass

    async def flush(self):
        """Flush all pending changes as one batched presence frame."""
        current = asyncio.current_task()
        if (self._flush_task is not None and self._flush_task is not current
                and not self._flush_task.done()):
            self._flush_task.cancel()
        self._flush_task = None
        if not self._pending:
            return
        changes = list(self._pending.values())
        self._pending = {}
        msg = json.dumps({"type": "presence", "changes": changes})
        tasks = []
        for session in list(self.registry.agents.values()):
            if session.is_online:
                tasks.append(session.websocket.send(msg))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


# ---------------------------------------------------------------------------
# Heartbeat engine
# ---------------------------------------------------------------------------

class HeartbeatEngine:
    """Timestamp-based heartbeat with no race condition."""

    def __init__(self, registry: Registry, interval_s: float = 15.0,
                 max_missed: int = 3, logger: logging.Logger = None,
                 presence: "PresenceBroadcaster" = None):
        self.registry = registry
        self.interval_s = interval_s
        self.max_missed = max_missed
        self.logger = logger or logging.getLogger("heartbeat")
        self.presence = presence
        self._running = False

    async def start(self):
        self._running = True
        while self._running:
            await asyncio.sleep(self.interval_s)
            now = time.monotonic()
            for agent_id, session in list(self.registry.agents.items()):
                if not session.is_online:
                    continue
                try:
                    await session.websocket.send(json.dumps({
                        "type": "ping",
                        "server_time": _now_iso(),
                    }))
                except Exception:
                    session.missed_pings += 1
                else:
                    elapsed = now - session.last_pong_time
                    expected_pings = int(elapsed // self.interval_s)
                    if expected_pings > session.missed_pings:
                        session.missed_pings = expected_pings

                if session.missed_pings >= self.max_missed:
                    await self._declare_offline(agent_id)

    async def stop(self):
        self._running = False

    async def _declare_offline(self, agent_id: str):
        session = self.registry.agents.get(agent_id)
        if session is None or not session.is_online:
            return
        self.logger.info(f"Agent {agent_id} declared offline (missed {session.missed_pings} pings)")
        session.is_online = False
        if self.presence is not None:
            await self.presence.update(agent_id, "offline", session.metadata)

    async def on_pong(self, agent_id: str, frame: dict):
        session = self.registry.agents.get(agent_id)
        if session is None:
            return
        session.last_pong_time = time.monotonic()
        session.missed_pings = 0


# ---------------------------------------------------------------------------
# Message router
# ---------------------------------------------------------------------------

class MessageRouter:
    """Routes messages with end-to-end ACK, per-pair ordering, loop prevention."""

    def __init__(self, registry: Registry, logger: logging.Logger = None,
                 max_message_bytes: int = 1_048_576,
                 rate_limit_per_agent: int = 60,
                 rate_limit_per_pair: int = 20,
                 delivery_timeout: float = 30.0,
                 max_conversation_depth: int = 20,
                 circuit_breaker_window: float = 60.0,
                 circuit_breaker_max: int = 30,
                 circuit_breaker_cooldown: float = 120.0,
                 msg_record_ttl: float = 300.0,
                 msg_id_cache_ttl: float = 300.0):
        self.registry = registry
        self.logger = logger or logging.getLogger("router")
        self.max_message_bytes = max_message_bytes
        self.rate_limit_per_agent = rate_limit_per_agent
        self.rate_limit_per_pair = rate_limit_per_pair
        self.delivery_timeout = delivery_timeout
        self.max_conversation_depth = max_conversation_depth
        self.circuit_breaker_window = circuit_breaker_window
        self.circuit_breaker_max = circuit_breaker_max
        self.circuit_breaker_cooldown = circuit_breaker_cooldown
        self.msg_record_ttl = msg_record_ttl
        self.msg_id_cache_ttl = msg_id_cache_ttl

    # ------------------------------------------------------------------
    # Main route method
    # ------------------------------------------------------------------

    async def route(self, sender_id: str, frame: dict) -> None:
        """Route a message from sender to target."""
        target_id = frame.get("to")
        body = frame.get("body")

        # --- Validation ---
        if not target_id or not isinstance(target_id, str):
            await self._send_error(sender_id, "BAD_REQUEST",
                                   "Missing or invalid 'to' field", frame)
            return
        if body is None:
            await self._send_error(sender_id, "BAD_REQUEST",
                                   "Missing 'body' field", frame)
            return

        # Size check
        body_json = json.dumps(body)
        if len(body_json) > self.max_message_bytes:
            await self._send_error(sender_id, "MESSAGE_TOO_LARGE", frame)
            return

        # Rate limit check (per-agent — token bucket, O(1))
        if not self._check_rate_limit_agent(sender_id):
            await self._send_error(sender_id, "RATE_LIMITED", frame)
            return

        # Rate limit check (per-pair — sliding window deque, O(1) amortized)
        if not self._check_rate_limit_pair(sender_id, target_id):
            await self._send_error(sender_id, "RATE_LIMITED", frame)
            return

        # Target existence check
        target = self.registry.agents.get(target_id)
        if target is None:
            await self._send_error(sender_id, "AGENT_NOT_FOUND", frame)
            return

        # Block check
        if sender_id in target.blocked_agents:
            await self._send_error(sender_id, "BLOCKED", frame)
            return

        # Circuit breaker check
        if self._is_circuit_open(sender_id, target_id):
            await self._send_error(sender_id, "CIRCUIT_BREAKER", frame)
            return

        # --- Message ID & Deduplication ---
        msg_id = frame.get("msg_id", str(uuid.uuid4()))
        if msg_id in self.registry.msg_id_cache:
            cached = self.registry.messages.get(msg_id)
            if cached:
                await self._send_ack(sender_id, msg_id, cached.status, target_id)
            return
        self.registry.msg_id_cache[msg_id] = time.monotonic()

        # --- Conversation ID & Depth ---
        conversation_id = frame.get("conversation_id")
        reply_to = frame.get("reply_to")

        if reply_to:
            original = self.registry.messages.get(reply_to)
            if original:
                # Reply authorization (design §2.6 / §6.4): the sender of a reply must be
                # the original message's recipient. Prevents forged replies.
                if original.recipient_id != sender_id:
                    await self._send_error(sender_id, "FORGED_REPLY", frame)
                    return
                conversation_id = original.conversation_id
                depth = original.depth + 1
            else:
                conversation_id = conversation_id or str(uuid.uuid4())
                depth = 1
        else:
            conversation_id = conversation_id or str(uuid.uuid4())
            depth = 1

        # Depth check
        if depth > self.max_conversation_depth:
            await self._send_error(sender_id, "DEPTH_EXCEEDED", frame)
            return

        # Conversation ended check (reopen resets the ended state — design §2.9)
        conv = self.registry.conversations.get(conversation_id)
        if conv and conv.is_ended:
            if not frame.get("reopen"):
                await self._send_error(sender_id, "CONVERSATION_ENDED", frame)
                return
            # Reopen: clear the ended state so the conversation can continue.
            conv.is_ended = False
            conv.ended_by = None
            conv.ended_at = None

        # --- Per-pair sequence number ---
        pair_key = (sender_id, target_id)
        seq = self.registry.pair_seqs.get(pair_key, 0) + 1
        self.registry.pair_seqs[pair_key] = seq

        # --- Build delivery frame ---
        delivery = {
            "type": "message",
            "from": sender_id,
            "body": body,
            "msg_id": msg_id,
            "reply_to": reply_to,
            "conversation_id": conversation_id,
            "seq": seq,
            "depth": depth,
            "ts": _now_iso(),
        }

        # --- Store message record ---
        ttl = min(frame.get("ttl", 0), 300)
        record = MessageRecord(
            msg_id=msg_id,
            sender_id=sender_id,
            recipient_id=target_id,
            body=body,
            reply_to=reply_to,
            conversation_id=conversation_id,
            seq=seq,
            depth=depth,
            status="pending",
            created_at=time.monotonic(),
            ttl=ttl,
        )
        self.registry.messages[msg_id] = record

        # --- Update conversation record ---
        if conversation_id not in self.registry.conversations:
            self.registry.conversations[conversation_id] = ConversationRecord(
                conversation_id=conversation_id,
                participants={sender_id, target_id},
                depth=depth,
                created_at=time.monotonic(),
            )
        else:
            self.registry.conversations[conversation_id].participants.add(sender_id)
            self.registry.conversations[conversation_id].participants.add(target_id)
            if depth > self.registry.conversations[conversation_id].depth:
                self.registry.conversations[conversation_id].depth = depth

        # Add to recent messages deque for loop detection
        self.registry.conversations[conversation_id].recent_messages.append(record)

        # --- Loop detection ---
        if self._detect_loop(conversation_id):
            self._open_circuit_breaker(sender_id, target_id)
            self._open_circuit_breaker(target_id, sender_id)
            await self._send_error(sender_id, "CIRCUIT_BREAKER", frame)
            return

        # --- Update circuit breaker ---
        self._update_circuit_breaker(sender_id, target_id)

        # --- Create delivery event for end-to-end ACK ---
        self.registry.delivery_events[msg_id] = asyncio.Event()

        # --- Send "pending" ACK to sender ---
        await self._send_ack(sender_id, msg_id, "pending", target_id)

        # --- Deliver or queue ---
        if target.is_online:
            try:
                await target.websocket.send(json.dumps(delivery))
                delivered = await self._wait_for_receipt(msg_id, target_id)
                if delivered:
                    record.status = "delivered"
                    await self._send_ack(sender_id, msg_id, "delivered", target_id)
                else:
                    record.status = "unconfirmed"
                    await self._send_ack(sender_id, msg_id, "unconfirmed", target_id)
            except Exception as e:
                await self._handle_delivery_failure(msg_id, sender_id, target_id, str(e))
        elif ttl > 0:
            record.status = "queued"
            target.pending_messages.append(delivery)
            await self._send_ack(sender_id, msg_id, "queued", target_id, ttl=ttl)
        else:
            record.status = "offline"
            await self._send_ack(sender_id, msg_id, "offline", target_id)

    # ------------------------------------------------------------------
    # End-to-end ACK
    # ------------------------------------------------------------------

    async def _wait_for_receipt(self, msg_id: str, target_id: str) -> bool:
        """Wait for target to send 'received' frame. Returns True if received."""
        event = self.registry.delivery_events.get(msg_id)
        if event is None:
            return False
        try:
            await asyncio.wait_for(event.wait(), timeout=self.delivery_timeout)
            return True
        except asyncio.TimeoutError:
            return False
        finally:
            self.registry.delivery_events.pop(msg_id, None)

    async def _handle_delivery_failure(self, msg_id: str, sender_id: str,
                                       target_id: str, error: str):
        """Handle a delivery failure (connection dropped during send)."""
        record = self.registry.messages.get(msg_id)
        if record:
            record.status = "failed"
        await self._send_ack(sender_id, msg_id, "failed", target_id)
        self.logger.warning(f"Delivery failed for msg {msg_id} to {target_id}: {error}")

    async def on_received(self, agent_id: str, frame: dict):
        """Called when a 'received' frame arrives from the target agent."""
        msg_id = frame.get("msg_id")
        if not msg_id:
            return
        record = self.registry.messages.get(msg_id)
        if record is None:
            return
        if record.recipient_id != agent_id:
            await self._send_error(agent_id, "FORGED_REPLY", frame)
            return
        event = self.registry.delivery_events.get(msg_id)
        if event:
            event.set()

    # ------------------------------------------------------------------
    # Reconnect — deliver queued messages
    # ------------------------------------------------------------------

    async def on_reconnect(self, agent_id: str):
        """Deliver queued messages when an agent reconnects.

        Checks TTL on each queued message — expired messages are skipped
        and reported as TTL_EXPIRED to the original sender.

        After each successful delivery, sends a "delivered" ACK to the
        original sender and updates the message record status.
        """
        session = self.registry.agents.get(agent_id)
        if session is None:
            return

        now = time.monotonic()
        still_valid = []

        for msg in session.pending_messages:
            msg_id = msg.get("msg_id")
            record = self.registry.messages.get(msg_id)

            # Check TTL
            if record and record.ttl > 0:
                elapsed = now - record.created_at
                if elapsed > record.ttl:
                    record.status = "ttl_expired"
                    await self._send_ack(record.sender_id, msg_id, "ttl_expired", agent_id)
                    continue

            # Deliver the message
            try:
                await session.websocket.send(json.dumps(msg))

                # Update record status (v5.0 fix — was missing in v4.0)
                if record:
                    record.status = "delivered"

                # Send "delivered" ACK to original sender (v5.0 addition)
                if record:
                    await self._send_ack(record.sender_id, msg_id, "delivered", agent_id)
                # Success → drop from the queue (do NOT re-deliver on next reconnect).
            except Exception:
                # Delivery failed — keep in queue for the next reconnect attempt.
                still_valid.append(msg)

        session.pending_messages = still_valid

    # ------------------------------------------------------------------
    # Conversation end
    # ------------------------------------------------------------------

    async def on_conversation_end(self, agent_id: str, frame: dict):
        """Handle an end-of-conversation signal."""
        conversation_id = frame.get("conversation_id")
        if not conversation_id:
            await self._send_error(agent_id, "BAD_REQUEST",
                                   "Missing 'conversation_id'", frame)
            return
        conv = self.registry.conversations.get(conversation_id)
        if conv is None:
            await self._send_error(agent_id, "CONVERSATION_NOT_FOUND", frame)
            return
        if agent_id not in conv.participants:
            await self._send_error(agent_id, "NOT_CONVERSATION_PARTICIPANT", frame)
            return

        conv.is_ended = True
        conv.ended_by = agent_id
        conv.ended_at = time.monotonic()

        end_msg = json.dumps({
            "type": "conversation_ended",
            "conversation_id": conversation_id,
            "reason": frame.get("reason", "ended"),
            "by": agent_id,
            "ts": _now_iso(),
        })
        for participant in conv.participants:
            session = self.registry.agents.get(participant)
            if session and session.is_online:
                try:
                    await session.websocket.send(end_msg)
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Block / unblock
    # ------------------------------------------------------------------

    async def on_block(self, agent_id: str, frame: dict):
        """Block messages from a specific agent."""
        target_id = frame.get("agent_id")
        if not target_id:
            await self._send_error(agent_id, "BAD_REQUEST",
                                   "Missing 'agent_id'", frame)
            return
        session = self.registry.agents.get(agent_id)
        if session is None:
            return
        session.blocked_agents.add(target_id)
        self.logger.info(f"{agent_id} blocked {target_id}")

    async def on_unblock(self, agent_id: str, frame: dict):
        """Unblock a previously blocked agent."""
        target_id = frame.get("agent_id")
        if not target_id:
            await self._send_error(agent_id, "BAD_REQUEST",
                                   "Missing 'agent_id'", frame)
            return
        session = self.registry.agents.get(agent_id)
        if session is None:
            return
        session.blocked_agents.discard(target_id)
        self.logger.info(f"{agent_id} unblocked {target_id}")

    # ------------------------------------------------------------------
    # Circuit breaker
    # ------------------------------------------------------------------

    def _is_circuit_open(self, sender_id: str, target_id: str) -> bool:
        pair_key = (sender_id, target_id)
        breaker = self.registry.circuit_breakers.get(pair_key)
        if breaker is None:
            return False
        if not breaker.is_open:
            return False
        if breaker.cooldown_until and time.monotonic() > breaker.cooldown_until:
            breaker.is_open = False
            breaker.timestamps.clear()
            return False
        return True

    def _update_circuit_breaker(self, sender_id: str, target_id: str):
        pair_key = (sender_id, target_id)
        now = time.monotonic()
        breaker = self.registry.circuit_breakers.get(pair_key)
        if breaker is None:
            breaker = CircuitBreakerRecord(pair_key=pair_key, timestamps=deque())
            self.registry.circuit_breakers[pair_key] = breaker
        while breaker.timestamps and now - breaker.timestamps[0] > self.circuit_breaker_window:
            breaker.timestamps.popleft()
        breaker.timestamps.append(now)
        if len(breaker.timestamps) >= self.circuit_breaker_max:
            breaker.is_open = True
            breaker.cooldown_until = now + self.circuit_breaker_cooldown

    def _open_circuit_breaker(self, sender_id: str, target_id: str):
        pair_key = (sender_id, target_id)
        now = time.monotonic()
        breaker = self.registry.circuit_breakers.get(pair_key)
        if breaker is None:
            breaker = CircuitBreakerRecord(pair_key=pair_key, timestamps=deque())
            self.registry.circuit_breakers[pair_key] = breaker
        breaker.is_open = True
        breaker.cooldown_until = now + self.circuit_breaker_cooldown

    # ------------------------------------------------------------------
    # Loop detection
    # ------------------------------------------------------------------

    def _detect_loop(self, conversation_id: str) -> bool:
        """Detect A→B→A→B loop pattern in a conversation."""
        conv = self.registry.conversations.get(conversation_id)
        if conv is None:
            return False
        recent = conv.recent_messages
        if len(recent) < 6:
            return False
        participants = set(r.sender_id for r in recent)
        if len(participants) != 2:
            return False
        for i in range(len(recent) - 1):
            if recent[i].sender_id == recent[i + 1].sender_id:
                return False
        return True

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _check_rate_limit_agent(self, agent_id: str) -> bool:
        """Per-agent token bucket. O(1)."""
        session = self.registry.agents.get(agent_id)
        if session is None:
            return False
        now = time.monotonic()
        elapsed = now - session._rate_last_refill
        session.rate_budget = min(
            self.rate_limit_per_agent,
            session.rate_budget + elapsed * (self.rate_limit_per_agent / 60.0),
        )
        session._rate_last_refill = now
        if session.rate_budget >= 1.0:
            session.rate_budget -= 1.0
            return True
        return False

    def _check_rate_limit_pair(self, sender_id: str, target_id: str) -> bool:
        """Per-pair sliding window deque. O(1) amortized."""
        session = self.registry.agents.get(sender_id)
        if session is None:
            return False
        pair_key = (sender_id, target_id)
        now = time.monotonic()
        if pair_key not in session._pair_counters:
            session._pair_counters[pair_key] = deque()
        timestamps = session._pair_counters[pair_key]
        while timestamps and now - timestamps[0] > 60.0:
            timestamps.popleft()
        if len(timestamps) >= self.rate_limit_per_pair:
            return False
        timestamps.append(now)
        return True

    # ------------------------------------------------------------------
    # Pruning
    # ------------------------------------------------------------------

    async def prune_old_records(self):
        """Background task: periodically prune old message records and msg_id cache."""
        while True:
            await asyncio.sleep(60)
            self._prune_once()

    def _prune_once(self):
        """Single pruning pass — split out so tests can run it without the loop."""
        now = time.monotonic()

        # Prune message records
        expired = [
            mid for mid, rec in self.registry.messages.items()
            if now - rec.created_at > self.msg_record_ttl
        ]
        for mid in expired:
            del self.registry.messages[mid]

        # Prune msg_id cache (per-entry TTL, not bulk clear)
        expired_cache = [
            mid for mid, ts in self.registry.msg_id_cache.items()
            if now - ts > self.msg_id_cache_ttl
        ]
        for mid in expired_cache:
            del self.registry.msg_id_cache[mid]

        # Prune stale delivery events
        stale = [
            mid for mid, evt in self.registry.delivery_events.items()
            if evt.is_set()
        ]
        for mid in stale:
            self.registry.delivery_events.pop(mid, None)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _send_ack(self, sender_id: str, msg_id: str, status: str,
                        target_id: str, ttl: int = None):
        """Send a message_ack to the sender."""
        ack = {
            "type": "message_ack",
            "msg_id": msg_id,
            "status": status,
            "to": target_id,
            "ts": _now_iso(),
        }
        if ttl is not None:
            ack["ttl"] = ttl
        session = self.registry.agents.get(sender_id)
        if session and session.is_online:
            try:
                await session.websocket.send(json.dumps(ack))
            except Exception:
                pass

    async def _send_error(self, agent_id: str, code: str, frame: dict = None,
                          message: str = None):
        """Send an error frame to an agent."""
        err = {
            "type": "error",
            "code": code,
            "message": message or _ERROR_MESSAGES.get(code, code),
        }
        if frame and "msg_id" in frame:
            err["msg_id"] = frame["msg_id"]
        session = self.registry.agents.get(agent_id)
        if session and session.is_online:
            try:
                await session.websocket.send(json.dumps(err))
            except Exception:
                pass


_ERROR_MESSAGES = {
    "AGENT_NOT_FOUND": "Target agent not found in registry",
    "AGENT_OFFLINE": "Target agent is currently offline",
    "ALREADY_REGISTERED": "Agent ID already registered from another connection",
    "BAD_REGISTRATION": "Invalid registration fields",
    "RATE_LIMITED": "Rate limit exceeded",
    "MESSAGE_TOO_LARGE": "Message body exceeds maximum size",
    "FORGED_REPLY": "Reply authorization failed",
    "TTL_EXPIRED": "Message TTL expired before delivery",
    "DEPTH_EXCEEDED": "Conversation depth limit exceeded",
    "CIRCUIT_BREAKER": "Circuit breaker active — cooldown in progress",
    "CONVERSATION_ENDED": "Conversation has been ended",
    "BLOCKED": "Sender is blocked by the target agent",
    "DUPLICATE_MSG_ID": "Duplicate message ID",
    "INTERNAL_ERROR": "Internal server error",
    "CONVERSATION_NOT_FOUND": "Conversation not found",
    "NOT_CONVERSATION_PARTICIPANT": "Not a participant in this conversation",
    "BAD_REQUEST": "Bad request",
}


# ---------------------------------------------------------------------------
# CLI renderer (ANSI escape codes, no threading)
# ---------------------------------------------------------------------------

class CliRenderer:
    """ANSI escape code terminal UI running inside the asyncio event loop."""

    def __init__(self, registry: Registry):
        self.registry = registry
        self.messages: list[tuple[str, str, str]] = []
        self.scroll_offset = 0
        self.input_buffer = ""
        self.search_term = ""
        self.is_searching = False
        self.running = True

    def add_message(self, msg_type: str, text: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self.messages.append((ts, msg_type, text))
        self.scroll_offset = 0

    def render(self):
        sys.stdout.write("\033[H\033[J")
        online = sum(1 for s in self.registry.agents.values() if s.is_online)
        offline = sum(1 for s in self.registry.agents.values() if not s.is_online)
        sys.stdout.write(f"\033[1m NEUROGOSSIP \u2014 Online: {online}  Offline: {offline}\033[0m\n")
        sys.stdout.write("\u2500" * 80 + "\n")

        visible_height = 20
        filtered = self.messages
        if self.search_term:
            filtered = [m for m in self.messages if self.search_term.lower() in m[2].lower()]

        start = max(0, len(filtered) - visible_height - self.scroll_offset)
        end = len(filtered) - self.scroll_offset
        for ts, msg_type, text in filtered[start:end]:
            color = {"send": "34", "recv": "32", "server": "33", "error": "31"}.get(msg_type, "0")
            sys.stdout.write(f"\033[{color}m[{ts}] {text}\033[0m\n")

        for _ in range(visible_height - (end - start)):
            sys.stdout.write("\n")

        sys.stdout.write("\u2500" * 80 + "\n")
        if self.is_searching:
            sys.stdout.write(f"\033[7m /search: {self.search_term}\033[0m")
        else:
            sys.stdout.write(f"\033[7m {self.input_buffer}\033[0m")
        sys.stdout.write("\033[K")
        sys.stdout.flush()

    async def read_stdin(self):
        data = os.read(sys.stdin.fileno(), 1024)
        for char in data.decode("utf-8", errors="replace"):
            self._process_char(char)
        self.render()

    def _process_char(self, char: str):
        if self.is_searching:
            if char == "\n":
                self.is_searching = False
            elif char == "\x1b":
                self.is_searching = False
                self.search_term = ""
            elif char == "\x7f":
                self.search_term = self.search_term[:-1]
            else:
                self.search_term += char
            return

        if char == "\n":
            self._process_input(self.input_buffer)
            self.input_buffer = ""
        elif char == "\x7f":
            self.input_buffer = self.input_buffer[:-1]
        elif char == "\x1b":
            seq = sys.stdin.read(2)
            if seq == "[5~":
                self.scroll_offset = min(self.scroll_offset + 1, len(self.messages))
            elif seq == "[6~":
                self.scroll_offset = max(self.scroll_offset - 1, 0)
        else:
            self.input_buffer += char

    def _process_input(self, line: str):
        line = line.strip()
        if not line:
            return
        if line.startswith("/"):
            self._process_command(line)
        elif line.startswith("@"):
            self._process_send(line)
        else:
            self.add_message("server", f"Unknown input. Type @handle: message or /help")

    def _process_command(self, line: str):
        parts = line.split()
        cmd = parts[0].lower()

        if cmd == "/list":
            for aid, s in self.registry.agents.items():
                status = "online" if s.is_online else "offline"
                meta = s.metadata
                dn = meta.get("display_name", aid)
                self.add_message("server", f"  {aid} ({dn}) \u2014 {status}")
        elif cmd == "/status" and len(parts) > 1:
            aid = parts[1]
            s = self.registry.agents.get(aid)
            if s:
                self.add_message("server",
                    f"  {aid}: online={s.is_online}, "
                    f"pending={len(s.pending_messages)}, "
                    f"blocked={list(s.blocked_agents)}")
            else:
                self.add_message("error", f"Agent '{aid}' not found")
        elif cmd == "/broadcast" and len(parts) > 1:
            body = " ".join(parts[1:])
            count = 0
            for aid, s in self.registry.agents.items():
                if s.is_online:
                    try:
                        asyncio.ensure_future(s.websocket.send(json.dumps({
                            "type": "message",
                            "from": "__server__",
                            "body": body,
                            "msg_id": str(uuid.uuid4()),
                            "ts": _now_iso(),
                        })))
                        count += 1
                    except Exception:
                        pass
            self.add_message("server", f"Broadcast sent to {count} agents")
        elif cmd == "/block" and len(parts) > 1:
            self.add_message("server", "Use /block <agent> from an agent connection, not CLI")
        elif cmd == "/unblock" and len(parts) > 1:
            self.add_message("server", "Use /unblock <agent> from an agent connection, not CLI")
        elif cmd == "/search" and len(parts) > 1:
            self.is_searching = True
            self.search_term = " ".join(parts[1:])
        elif cmd == "/export":
            self._export_log()
        elif cmd == "/help":
            self.add_message("server", "Commands: /list, /status <agent>, /broadcast <msg>, "
                                       "/search <term>, /export, /help")
        else:
            self.add_message("server", f"Unknown command: {cmd}")

    def _process_send(self, line: str):
        """Parse @handle: message format."""
        if ":" not in line:
            self.add_message("error", "Format: @handle: message")
            return
        colon_pos = line.index(":")
        handle = line[1:colon_pos].strip()
        body = line[colon_pos + 1:].strip()
        if not handle or not body:
            self.add_message("error", "Format: @handle: message")
            return
        target = self.registry.agents.get(handle)
        if target is None:
            self.add_message("error", f"Agent '{handle}' not found")
            return
        if not target.is_online:
            self.add_message("error", f"Agent '{handle}' is offline")
            return
        msg_id = str(uuid.uuid4())
        try:
            asyncio.ensure_future(target.websocket.send(json.dumps({
                "type": "message",
                "from": "__cli__",
                "body": body,
                "msg_id": msg_id,
                "ts": _now_iso(),
            })))
            self.add_message("send", f"\u2192 {handle}: {body}")
        except Exception as e:
            self.add_message("error", f"Failed to send to {handle}: {e}")

    def _export_log(self):
        path = f"neurogossip_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        try:
            with open(path, "w") as f:
                for ts, mt, text in self.messages:
                    f.write(f"[{ts}] [{mt}] {text}\n")
            self.add_message("server", f"Log exported to {path}")
        except Exception as e:
            self.add_message("error", f"Export failed: {e}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Main server
# ---------------------------------------------------------------------------

class NeurogossipServer:
    """Main WebSocket server for Neurogossip."""

    def __init__(self, host: str = "0.0.0.0", port: int = 8765,
                 heartbeat_interval: float = 15.0, max_missed: int = 3,
                 cli_mode: bool = False,
                 require_auth: bool = False, secret: Optional[str] = None,
                 max_message_bytes: int = 1_048_576,
                 rate_limit_agent: int = 60, rate_limit_pair: int = 20,
                 delivery_timeout: float = 30.0,
                 max_conversation_depth: int = 20,
                 circuit_breaker_window: float = 60.0,
                 circuit_breaker_max: int = 30,
                 circuit_breaker_cooldown: float = 120.0,
                 presence_debounce: float = 1.0,
                 log_level: str = "INFO"):
        self.host = host
        self.port = port
        self.cli_mode = cli_mode
        self.require_auth = require_auth
        self.secret = secret
        self.logger = logging.getLogger("neurogossip")
        self.logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

        self.registry = Registry()
        self.presence = PresenceBroadcaster(
            self.registry, debounce_s=presence_debounce, logger=self.logger,
        )
        self.heartbeat = HeartbeatEngine(
            self.registry, interval_s=heartbeat_interval, max_missed=max_missed,
            logger=self.logger, presence=self.presence,
        )
        self.router = MessageRouter(
            self.registry, logger=self.logger,
            max_message_bytes=max_message_bytes,
            rate_limit_per_agent=rate_limit_agent,
            rate_limit_per_pair=rate_limit_pair,
            delivery_timeout=delivery_timeout,
            max_conversation_depth=max_conversation_depth,
            circuit_breaker_window=circuit_breaker_window,
            circuit_breaker_max=circuit_breaker_max,
            circuit_breaker_cooldown=circuit_breaker_cooldown,
        )
        self.cli = CliRenderer(self.registry) if cli_mode else None
        self.server = None
        self._running = False
        self._bg_tasks: list[asyncio.Task] = []
        self.grace_period_s: float = 5.0  # shutdown grace period (design §3.6)

    async def serve(self):
        """Start listening and launch background tasks (non-blocking).

        Returns once the server is listening. Use ``stop()`` to tear down, or
        ``start()`` to additionally install signal handlers and block until the
        server stops.
        """
        if self._running:
            return
        self._running = True
        self.server = await serve(
            self._handle_connection,
            self.host,
            self.port,
        )
        self.logger.info(f"Neurogossip server listening on {self.host}:{self.port}")

        # Start background tasks
        self._bg_tasks = [
            asyncio.create_task(self.heartbeat.start()),
            asyncio.create_task(self.router.prune_old_records()),
        ]

        if self.cli_mode:
            loop = asyncio.get_event_loop()
            loop.add_reader(sys.stdin.fileno(), lambda: asyncio.ensure_future(self.cli.read_stdin()))
            self.cli.add_message("server", f"Server started on {self.host}:{self.port}")
            self.cli.add_message("server", "Type @handle: message to send. /help for commands.")
            self.cli.render()

    async def start(self):
        """Entry point: serve, install signal handlers, run until stopped.

        On SIGTERM the registered handler triggers a graceful ``shutdown()``. On SIGINT,
        ``asyncio.run`` cancels the main task; we catch that cancellation and still run a
        graceful shutdown so in-flight messages are drained and agents are notified
        before the process exits.
        """
        await self.serve()

        # SIGTERM → graceful shutdown (asyncio.run owns SIGINT).
        try:
            loop = asyncio.get_event_loop()
            loop.add_signal_handler(signal.SIGTERM,
                                     lambda: asyncio.create_task(self.shutdown()))
        except NotImplementedError:
            pass  # Windows doesn't support add_signal_handler

        try:
            await self.server.serve_forever()
        except asyncio.CancelledError:
            # SIGINT (asyncio.run cancelled the main task) → shut down gracefully.
            await self.shutdown()

    async def shutdown(self):
        """Graceful shutdown sequence (6 steps, design §3.6)."""
        self.logger.info("Shutting down...")
        self._running = False

        # 1. Stop accepting new connections
        self.server.close()

        # 2. Notify all connected agents
        shutdown_msg = json.dumps({
            "type": "shutdown",
            "reason": "server_going_down",
            "grace_period_s": self.grace_period_s,
        })
        notify_tasks = []
        for agent_id, session in list(self.registry.agents.items()):
            if session.is_online:
                notify_tasks.append(
                    self._send_with_timeout(session.websocket, shutdown_msg, timeout=2.0)
                )
        await asyncio.gather(*notify_tasks, return_exceptions=True)

        # 3. Wait for agents to acknowledge (max grace period)
        await asyncio.sleep(self.grace_period_s)

        # 4. Drain in-flight messages
        for msg_id, record in list(self.registry.messages.items()):
            if record.status == "pending":
                record.status = "failed"
                await self.router._send_ack(record.sender_id, msg_id, "failed", record.recipient_id)

        # 5. Close all connections
        close_tasks = []
        for agent_id, session in list(self.registry.agents.items()):
            if session.is_online:
                close_tasks.append(
                    self._close_connection(session.websocket)
                )
        await asyncio.gather(*close_tasks, return_exceptions=True)

        # 6. Stop background tasks + presence timer and wait for the listener to close.
        await self.stop()

    async def stop(self):
        """Tear down the server: cancel background tasks and close the listener."""
        self._running = False
        for t in self._bg_tasks:
            t.cancel()
        if self._bg_tasks:
            await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        self._bg_tasks = []
        # Cancel any pending presence debounce timer.
        if self.presence._flush_task is not None and not self.presence._flush_task.done():
            self.presence._flush_task.cancel()
            self.presence._flush_task = None
        if self.server is not None:
            self.server.close()
            try:
                await self.server.wait_closed()
            except Exception:
                pass

    async def _send_with_timeout(self, websocket, message: str, timeout: float = 2.0):
        try:
            await asyncio.wait_for(websocket.send(message), timeout=timeout)
        except Exception:
            pass

    async def _close_connection(self, websocket):
        try:
            await asyncio.wait_for(websocket.close(), timeout=2.0)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Connection handler
    # ------------------------------------------------------------------

    async def _handle_connection(self, websocket: ServerConnection):
        """Handle a new WebSocket connection."""
        agent_id = None
        try:
            async for raw in websocket:
                try:
                    frame = json.loads(raw)
                except json.JSONDecodeError:
                    await websocket.send(json.dumps({
                        "type": "error", "code": "BAD_REQUEST",
                        "message": "Invalid JSON",
                    }))
                    continue

                msg_type = frame.get("type")
                if msg_type == "register":
                    agent_id = await self._handle_register(websocket, frame)
                elif agent_id is None:
                    await websocket.send(json.dumps({
                        "type": "error", "code": "BAD_REGISTRATION",
                        "message": "Must register first",
                    }))
                elif msg_type == "pong":
                    await self.heartbeat.on_pong(agent_id, frame)
                elif msg_type == "message":
                    await self.router.route(agent_id, frame)
                elif msg_type == "received":
                    await self.router.on_received(agent_id, frame)
                elif msg_type == "conversation_end":
                    await self.router.on_conversation_end(agent_id, frame)
                elif msg_type == "block":
                    await self.router.on_block(agent_id, frame)
                elif msg_type == "unblock":
                    await self.router.on_unblock(agent_id, frame)
                elif msg_type == "list_agents":
                    await self._handle_list_agents(websocket)
                else:
                    await websocket.send(json.dumps({
                        "type": "error", "code": "BAD_REQUEST",
                        "message": f"Unknown message type: {msg_type}",
                    }))
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            self.logger.error(f"Connection error: {e}")
        finally:
            if agent_id:
                await self._handle_disconnect(agent_id)

    async def _handle_register(self, websocket: ServerConnection, frame: dict) -> Optional[str]:
        """Handle agent registration."""
        agent_id = frame.get("agent_id")
        if not isinstance(agent_id, str) or not AGENT_ID_RE.match(agent_id):
            await websocket.send(json.dumps({
                "type": "error", "code": "BAD_REGISTRATION",
                "message": "Invalid or missing agent_id (lowercase alphanumeric + hyphens, max 64)",
            }))
            return None

        # Auth check
        if self.require_auth:
            token = frame.get("auth_token")
            if token != self.secret:
                await websocket.send(json.dumps({
                    "type": "error", "code": "BAD_REGISTRATION",
                    "message": "Invalid auth token",
                }))
                return None

        # Metadata validation
        metadata = frame.get("metadata", {})
        err = _validate_metadata(metadata)
        if err:
            await websocket.send(json.dumps({
                "type": "error", "code": "BAD_REGISTRATION",
                "message": err,
            }))
            return None

        # Check if already registered
        existing = self.registry.agents.get(agent_id)
        if existing and existing.is_online:
            await websocket.send(json.dumps({
                "type": "error", "code": "ALREADY_REGISTERED",
                "message": f"Agent '{agent_id}' is already connected",
            }))
            return None

        # Register. If an old (offline) session exists for this agent_id, carry over its
        # queued offline messages and block list so reconnect delivery (§2.5.1) can fire,
        # and drop the stale session_id mapping.
        pending_messages = []
        blocked_agents = set()
        if existing is not None:
            pending_messages = list(existing.pending_messages)
            blocked_agents = set(existing.blocked_agents)
            self.registry.sessions.pop(existing.session_id, None)

        session_id = _generate_session_id()
        session = AgentSession(
            agent_id=agent_id,
            websocket=websocket,
            metadata=metadata,
            session_id=session_id,
            connected_at=time.monotonic(),
            last_pong_time=time.monotonic(),
            is_online=True,
            pending_messages=pending_messages,
            blocked_agents=blocked_agents,
        )
        self.registry.agents[agent_id] = session
        self.registry.sessions[session_id] = agent_id

        await websocket.send(json.dumps({
            "type": "registered",
            "agent_id": agent_id,
            "session_id": session_id,
            "heartbeat_interval_s": self.heartbeat.interval_s,
        }))

        self.logger.info(f"Agent {agent_id} registered (session {session_id})")

        if self.cli:
            self.cli.add_message("server", f"{agent_id} registered")

        # Broadcast presence (debounced batch)
        await self.presence.update(agent_id, "online", metadata)

        # Deliver queued messages
        await self.router.on_reconnect(agent_id)

        return agent_id

    async def _handle_list_agents(self, websocket: ServerConnection):
        """Handle list_agents request."""
        agents_list = []
        for aid, session in self.registry.agents.items():
            agents_list.append({
                "agent_id": aid,
                "status": "online" if session.is_online else "offline",
                "metadata": session.metadata,
            })
        await websocket.send(json.dumps({
            "type": "agent_list",
            "agents": agents_list,
        }))

    async def _handle_disconnect(self, agent_id: str):
        """Handle agent disconnection."""
        session = self.registry.agents.get(agent_id)
        if session is None:
            return
        session.is_online = False
        self.logger.info(f"Agent {agent_id} disconnected")
        if self.cli:
            self.cli.add_message("server", f"{agent_id} disconnected")
        await self.presence.update(agent_id, "offline", session.metadata)


def _generate_session_id() -> str:
    import secrets
    return secrets.token_urlsafe(16)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Neurogossip v5.0 — Agent Registry & DM Server")
    parser.add_argument("--port", type=int, default=8765, help="Server listen port")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Server bind address")
    parser.add_argument("--cli", action="store_true", help="Enable ANSI-based CLI message view")
    parser.add_argument("--heartbeat", type=float, default=15.0, help="Heartbeat interval (seconds)")
    parser.add_argument("--max-missed", type=int, default=3, help="Missed pings before offline")
    parser.add_argument("--require-auth", action="store_true", help="Require auth token")
    parser.add_argument("--secret", type=str, default=None, help="Shared auth secret")
    parser.add_argument("--max-message-bytes", type=int, default=1_048_576, help="Max message size")
    parser.add_argument("--rate-limit-agent", type=int, default=60, help="Per-agent msg/min")
    parser.add_argument("--rate-limit-pair", type=int, default=20, help="Per-pair msg/min")
    parser.add_argument("--delivery-timeout", type=float, default=30.0, help="Delivery ACK timeout")
    parser.add_argument("--max-conversation-depth", type=int, default=20, help="Max reply depth")
    parser.add_argument("--circuit-breaker-window", type=float, default=60.0, help="CB window (s)")
    parser.add_argument("--circuit-breaker-max", type=int, default=30, help="CB max msgs")
    parser.add_argument("--circuit-breaker-cooldown", type=float, default=120.0, help="CB cooldown (s)")
    parser.add_argument("--presence-debounce", type=float, default=1.0,
                         help="Seconds to debounce presence broadcasts (0 = immediate)")
    parser.add_argument("--log-level", type=str, default="INFO", help="Log level")
    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        level=getattr(logging, args.log_level.upper(), logging.INFO),
    )

    server = NeurogossipServer(
        host=args.host,
        port=args.port,
        heartbeat_interval=args.heartbeat,
        max_missed=args.max_missed,
        cli_mode=args.cli,
        require_auth=args.require_auth,
        secret=args.secret,
        max_message_bytes=args.max_message_bytes,
        rate_limit_agent=args.rate_limit_agent,
        rate_limit_pair=args.rate_limit_pair,
        delivery_timeout=args.delivery_timeout,
        max_conversation_depth=args.max_conversation_depth,
        circuit_breaker_window=args.circuit_breaker_window,
        circuit_breaker_max=args.circuit_breaker_max,
        circuit_breaker_cooldown=args.circuit_breaker_cooldown,
        presence_debounce=args.presence_debounce,
        log_level=args.log_level,
    )

    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
