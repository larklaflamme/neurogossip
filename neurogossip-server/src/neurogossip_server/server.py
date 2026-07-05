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
import atexit
import io
import json
import logging
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
import os
import queue
import re
import shutil
import signal
import sys
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import websockets
from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed

# Optional markdown rendering for the CLI. ``rich`` renders message bodies (which
# may be markdown) to styled terminal text; if it isn't installed we fall back to
# plain text so ``--cli`` still works without the optional dependency.
try:
    from rich.console import Console as _RichConsole
    from rich.markdown import Markdown as _RichMarkdown
    _RICH_AVAILABLE = True
except Exception:  # pragma: no cover - exercised only when rich is absent
    _RichConsole = None
    _RichMarkdown = None
    _RICH_AVAILABLE = False


# ---------------------------------------------------------------------------
# Logging filter — silence benign websockets handshake/close noise
# ---------------------------------------------------------------------------

class _SuppressConnectionClosedFilter(logging.Filter):
    """Downgrade benign websockets handshake/close ERROR logs to DEBUG.

    The library's ``conn_handler`` catches *every* handshake exception and logs
    ``"opening handshake failed"`` at ERROR with a full traceback. The causes are all
    client-side / network issues — a client that opens a socket then drops it
    mid-upgrade (``ConnectionClosedError: no close frame received or sent``), a
    garbage/short HTTP request (``InvalidMessage`` / ``EOFError``), a disconnect
    while the server is writing the response, etc. None are actionable server bugs,
    yet the traceback reads like an unhandled exception.

    When either:

    - the record's message is ``"opening handshake failed"`` (the library's own
      catch-all for any handshake exception — covers ``ConnectionClosedError``,
      ``InvalidMessage``, ``EOFError``, decode errors, …), or
    - the record's exception is a ``ConnectionClosed`` subclass (mid-stream
      disconnects, keepalive-close — expected churn, not errors),

    we downgrade the record to DEBUG *and* strip its ``exc_info`` so the handler can
    no longer format the multi-line traceback. The exception summary is folded into a
    single concise line so the event stays observable (e.g. ``"opening handshake
    failed: no close frame received or sent"``) without the noise.

    Result:

    - At the default INFO/WARNING level: nothing is printed (DEBUG is suppressed by
      the handler level gate).
    - At DEBUG level: a single one-line note, no traceback.
    - Genuine internal errors (e.g. ``"unexpected internal error"``) carry a
      non-``ConnectionClosed`` exception and a different message, so they stay at
      ERROR with their traceback intact.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno < logging.ERROR:
            return True
        exc = record.exc_info[1] if record.exc_info else None
        if record.getMessage() != "opening handshake failed" and not (
                exc is not None and isinstance(exc, ConnectionClosed)):
            return True
        original_msg = record.getMessage()
        record.levelno = logging.DEBUG
        record.levelname = "DEBUG"
        # Drop the attached exception so the handler can't render the traceback.
        record.exc_info = None
        record.exc_text = None
        # Fold the exception summary into a single informative line.
        if exc is not None:
            record.msg = f"{original_msg}: {exc}"
            record.args = ()
        return True


def _install_websockets_log_filter() -> None:
    """Attach the close-noise filter to the websockets loggers (idempotent)."""
    for name in ("websockets", "websockets.server"):
        logger = logging.getLogger(name)
        if not any(isinstance(f, _SuppressConnectionClosedFilter)
                   for f in logger.filters):
            logger.addFilter(_SuppressConnectionClosedFilter())


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

DEFAULT_OUTBOUND_QUEUE_SIZE = 10000  # per-connection outbound frame queue ("large")


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
    # Reliable-delivery plumbing: a bounded per-connection outbound queue drained by a
    # dedicated writer task. All post-registration frames go through ``enqueue`` so the
    # router/heartbeat/presence never block on ``websocket.send`` (a stalled/dead socket
    # blocks only the writer task, not the event loop), and bursts are absorbed by the
    # queue with explicit drop-oldest backpressure instead of head-of-line blocking.
    outbound_queue_size: int = DEFAULT_OUTBOUND_QUEUE_SIZE
    outbound: Optional[asyncio.Queue] = None
    writer_task: Optional[asyncio.Task] = None
    writer_failed: bool = False
    # Router callback ``(session, frames, error)`` invoked when the writer task fails;
    # requeues ttl>0 message frames for reconnect redelivery, fails the rest.
    on_send_failure: Optional[Callable] = None

    # -- outbound queue + writer ------------------------------------------
    def enqueue(self, frame: dict) -> None:
        """Queue a frame for the writer task to send (non-blocking, backpressure).

        Lazily starts the writer task. On overflow drops the oldest frame and logs an
        ERROR (so drops are diagnosable). No-ops once the writer has failed.
        """
        if self.writer_failed:
            return
        if self.outbound is None:
            self.outbound = asyncio.Queue(maxsize=self.outbound_queue_size)
        if self.writer_task is None or self.writer_task.done():
            self.writer_task = asyncio.create_task(self._writer_loop())
        try:
            self.outbound.put_nowait(frame)
        except asyncio.QueueFull:
            try:
                dropped = self.outbound.get_nowait()
            except asyncio.QueueEmpty:
                dropped = None
            if dropped is not None:
                logging.getLogger("neurogossip").error(
                    "outbound queue overflow agent=%s dropped oldest frame type=%s",
                    self.agent_id, dropped.get("type"))
            try:
                self.outbound.put_nowait(frame)
            except asyncio.QueueFull:
                logging.getLogger("neurogossip").error(
                    "outbound queue still full agent=%s dropped new frame type=%s",
                    self.agent_id, frame.get("type"))

    async def _writer_loop(self) -> None:
        """Drain the outbound queue onto the socket. On a send failure, hand the
        failing frame + everything still queued to the router's failure callback."""
        frame: Optional[dict] = None
        try:
            while True:
                frame = await self.outbound.get()
                await self.websocket.send(json.dumps(frame))
        except Exception as e:
            await self._on_writer_failure(e, frame)

    async def _on_writer_failure(self, error: BaseException,
                                 failing_frame: Optional[dict] = None) -> None:
        self.writer_failed = True
        self.is_online = False
        # Drain whatever is still queued so the failure callback can requeue/fail it.
        # Include the failing frame (already dequeued before the send raised).
        drained: list[dict] = []
        if failing_frame is not None:
            drained.append(failing_frame)
        if self.outbound is not None:
            while True:
                try:
                    drained.append(self.outbound.get_nowait())
                except asyncio.QueueEmpty:
                    break
        cb = self.on_send_failure
        if cb is not None:
            try:
                await cb(self, drained, error)
            except Exception:
                logging.getLogger("neurogossip").debug(
                    "on_send_failure callback raised", exc_info=True)

    async def drain_outbound_to_pending(self) -> None:
        """On a clean disconnect/eviction, move ttl-bearing message frames still queued
        for send into ``pending_messages`` so they're redelivered on reconnect; drop
        the rest (ephemeral pings/presence/acks). Best-effort: no router lookup needed
        because the delivery frame already carries the same fields as a queued entry.
        """
        if self.outbound is None:
            return
        moved = 0
        while True:
            try:
                frame = self.outbound.get_nowait()
            except asyncio.QueueEmpty:
                break
            if frame.get("type") == "message":
                self.pending_messages.append(frame)
                moved += 1
        if moved:
            logging.getLogger("neurogossip").info(
                "agent %s: moved %d queued message(s) to pending for reconnect",
                self.agent_id, moved)

    def cancel_writer(self) -> None:
        """Cancel the writer task (disconnect/eviction/shutdown)."""
        t = self.writer_task
        if t is not None and not t.done():
            t.cancel()
        self.writer_task = None


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
        # Non-blocking receipt watchers: msg_id -> {"sender_id","target_id",
        # "timer_handle","event"}. ``route()`` starts one per accepted message and
        # returns immediately; ``on_received``/timeout completes it. See §2.5.
        self.pending_watchers: dict[str, dict] = {}


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
        frame = {"type": "presence", "changes": changes}
        for session in list(self.registry.agents.values()):
            if session.is_online and not session.writer_failed:
                session.enqueue(frame)


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
                # Enqueue the ping (non-blocking); a dead socket is detected by the
                # writer-failure path (writer_failed → is_online=False) and by the
                # timestamp-based missed-ping check below.
                if not session.writer_failed:
                    session.enqueue({"type": "ping", "server_time": _now_iso()})
                elapsed = now - session.last_pong_time
                expected_pings = int(elapsed // self.interval_s)
                if expected_pings > session.missed_pings:
                    session.missed_pings = expected_pings
                if session.writer_failed:
                    session.missed_pings = self.max_missed

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
                 msg_id_cache_ttl: float = 300.0,
                 on_message_routed: Callable[[str, str, Any, Optional[str]], None] = None):
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
        # Optional sink notified once per accepted message
        # (sender_id, target_id, body, reply_to). Used by the CLI to log every
        # message that flows through the server. Invoked after all rejection
        # checks so duplicates / errors aren't logged.
        self.on_message_routed = on_message_routed

    # ------------------------------------------------------------------
    # Main route method
    # ------------------------------------------------------------------

    async def route(self, sender_id: str, frame: dict) -> None:
        """Route a message from sender to target."""
        target_id = frame.get("to")
        body = frame.get("body")
        self.logger.debug("message from=%s to=%s msg_id=%s reply_to=%s ttl=%s body=%s",
                          sender_id, target_id, frame.get("msg_id"), frame.get("reply_to"),
                          frame.get("ttl"), _truncate(body))

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

        # --- Notify the message sink (CLI logger) ---
        if self.on_message_routed is not None:
            try:
                self.on_message_routed(sender_id, target_id, body, reply_to)
            except Exception:
                self.logger.debug("on_message_routed hook failed", exc_info=True)

        # --- Create delivery event for end-to-end ACK ---
        self.registry.delivery_events[msg_id] = asyncio.Event()

        # --- Send "pending" ACK to sender ---
        await self._send_ack(sender_id, msg_id, "pending", target_id)

        # --- Deliver or queue (non-blocking) ---
        # A target may be flagged offline by the app heartbeat (missed pongs) while
        # its websocket is still open — e.g. a busy agent that isn't processing app
        # pings but is still connected and actively sending. Deliver to the open
        # socket in that case rather than dropping the message; only take the
        # offline/queue path when the socket is actually closed. ``close_code`` is
        # None while a websockets connection is OPEN.
        ws_open = False
        try:
            ws_open = target.websocket is not None and target.websocket.close_code is None
        except Exception:
            ws_open = False
        if target.is_online or ws_open:
            # Enqueue the delivery to the target's writer (non-blocking) and start a
            # timer-based receipt watcher. route() returns immediately — the sender's
            # connection coroutine is never blocked by a 30s ACK wait. The watcher
            # completes on the target's "received" frame (→ "delivered") or on timeout
            # (→ "unconfirmed"); a writer failure requeues/fails it. See §2.5.
            target.enqueue(delivery)
            self._start_watcher(msg_id, sender_id, target_id, record)
        elif ttl > 0:
            record.status = "queued"
            target.pending_messages.append(delivery)
            self.logger.debug("queued msg_id=%s to=%s ttl=%s (target offline)", msg_id, target_id, ttl)
            await self._send_ack(sender_id, msg_id, "queued", target_id, ttl=ttl)
        else:
            record.status = "offline"
            self.logger.error(
                "DROPPED msg_id=%s from=%s to=%s: target offline and no ttl — "
                "message not delivered", msg_id, sender_id, target_id)
            await self._send_ack(sender_id, msg_id, "offline", target_id)

    # ------------------------------------------------------------------
    # End-to-end ACK (non-blocking, timer-based)
    # ------------------------------------------------------------------

    def _start_watcher(self, msg_id: str, sender_id: str, target_id: str,
                      record: MessageRecord) -> None:
        """Start a timer-based receipt watcher for one in-flight message.

        ``on_received`` completes it with "delivered"; the timer fires "unconfirmed"
        after ``delivery_timeout``. The sender's connection coroutine is never parked.
        """
        event = asyncio.Event()
        self.registry.delivery_events[msg_id] = event
        loop = asyncio.get_event_loop()
        timer_handle = loop.call_later(self.delivery_timeout,
                                       self._on_receipt_timeout, msg_id)
        self.registry.pending_watchers[msg_id] = {
            "sender_id": sender_id,
            "target_id": target_id,
            "timer_handle": timer_handle,
            "event": event,
        }

    async def _complete_receipt(self, msg_id: str, status: str) -> None:
        """Complete a watcher: cancel its timer, update the record, ack the sender.

        Idempotent: only acts while the record is still ``pending``. Called from
        ``on_received`` (status="delivered") and ``_on_receipt_timeout`` ("unconfirmed").
        """
        w = self.registry.pending_watchers.pop(msg_id, None)
        if w is None:
            return
        handle = w.get("timer_handle")
        if handle is not None:
            try:
                handle.cancel()
            except Exception:
                pass
        record = self.registry.messages.get(msg_id)
        if record is not None and record.status == "pending":
            record.status = status
            self.logger.debug("%s msg_id=%s to=%s", status, msg_id, w["target_id"])
            await self._send_ack(w["sender_id"], msg_id, status, w["target_id"])
        self.registry.delivery_events.pop(msg_id, None)

    def _on_receipt_timeout(self, msg_id: str) -> None:
        """Timer fired with no receipt → mark unconfirmed + ack the sender."""
        w = self.registry.pending_watchers.get(msg_id)
        if w is None:
            return
        record = self.registry.messages.get(msg_id)
        if record is not None and record.status == "pending":
            record.status = "unconfirmed"
            self.logger.debug("unconfirmed msg_id=%s to=%s (no receipt)",
                              msg_id, w["target_id"])
            # Enqueue the ack from the loop (this runs in a call_later callback, no
            # running coroutine to await _send_ack — schedule it).
            asyncio.ensure_future(
                self._send_ack(w["sender_id"], msg_id, "unconfirmed", w["target_id"]))
        self.registry.pending_watchers.pop(msg_id, None)
        self.registry.delivery_events.pop(msg_id, None)

    async def _handle_delivery_failure(self, msg_id: str, sender_id: str,
                                       target_id: str, error: str,
                                       delivery: dict = None, ttl: int = 0,
                                       target: AgentSession = None):
        """Handle a delivery failure (a direct send raised — used by on_reconnect).

        Marks the target offline, cancels the watcher, and either requeues the message
        for reconnect redelivery (ttl>0) with a "queued" ack, or marks it failed with a
        "failed" ack + ERROR. The routing path no longer calls this directly — writer
        send failures go through ``_handle_writer_failure``.
        """
        record = self.registry.messages.get(msg_id)
        self._cancel_watcher(msg_id)
        if target is not None and target.is_online:
            target.is_online = False

        if ttl > 0 and delivery is not None and target is not None:
            if record:
                record.status = "queued"
            target.pending_messages.append(delivery)
            await self._send_ack(sender_id, msg_id, "queued", target_id, ttl=ttl)
            self.logger.warning(
                f"Send to {target_id} failed ({error}); "
                f"queued msg {msg_id} for reconnect delivery")
        else:
            if record:
                record.status = "failed"
            await self._send_ack(sender_id, msg_id, "failed", target_id)
            self.logger.error(
                "DROPPED msg_id=%s from=%s to=%s: delivery failed (%s) — "
                "message not delivered", msg_id, sender_id, target_id, error)

    async def _handle_writer_failure(self, session: "AgentSession",
                                     frames: list, error: BaseException) -> None:
        """Writer task failed: the target socket is dead. Mark it offline and dispose
        of every message frame still queued for it: ttl>0 → requeue into
        ``pending_messages`` for reconnect redelivery + "queued" ack to the original
        sender; no ttl → "failed" ack + ERROR. Non-message frames (ping/presence/ack)
        are dropped (ephemeral). Uses the message record (``registry.messages``) for
        the ttl — the delivery frame itself carries no ``ttl``.
        """
        target_id = session.agent_id
        for frame in frames:
            if frame.get("type") != "message":
                continue
            msg_id = frame.get("msg_id")
            record = self.registry.messages.get(msg_id)
            self._cancel_watcher(msg_id)  # no-op if Phase-C watcher absent
            if record is None:
                continue
            if record.ttl > 0:
                record.status = "queued"
                session.pending_messages.append(frame)
                await self._send_ack(record.sender_id, msg_id, "queued", target_id,
                                     ttl=record.ttl)
                self.logger.warning(
                    "writer failed for %s (%s); queued msg %s for reconnect delivery",
                    target_id, error, msg_id)
            else:
                record.status = "failed"
                await self._send_ack(record.sender_id, msg_id, "failed", target_id)
                self.logger.error(
                    "DROPPED msg_id=%s from=%s to=%s: writer failed (%s) — "
                    "message not delivered", msg_id, record.sender_id, target_id, error)

    def _cancel_watcher(self, msg_id: str) -> None:
        """Cancel a pending receipt watcher (Phase C). No-op before watchers exist."""
        w = self.registry.pending_watchers.pop(msg_id, None)
        if w is not None:
            handle = w.get("timer_handle")
            if handle is not None:
                try:
                    handle.cancel()
                except Exception:
                    pass
            evt = self.registry.delivery_events.pop(msg_id, None)
            if evt is not None:
                try:
                    evt.set()
                except Exception:
                    pass

    async def on_received(self, agent_id: str, frame: dict):
        """Called when a 'received' frame arrives from the target agent."""
        msg_id = frame.get("msg_id")
        if not msg_id:
            return
        self.logger.debug("received msg_id=%s from=%s", msg_id, agent_id)
        record = self.registry.messages.get(msg_id)
        if record is None:
            self.logger.debug("received msg_id=%s unknown (ignored)", msg_id)
            return
        if record.recipient_id != agent_id:
            await self._send_error(agent_id, "FORGED_REPLY", frame)
            return
        # Complete the watcher → cancel timer, mark delivered, ack the sender.
        await self._complete_receipt(msg_id, "delivered")

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
        self.logger.debug("reconnect delivery for %s: %d queued message(s)",
                          agent_id, len(session.pending_messages))

        for msg in session.pending_messages:
            msg_id = msg.get("msg_id")
            record = self.registry.messages.get(msg_id)

            # Check TTL
            if record and record.ttl > 0:
                elapsed = now - record.created_at
                if elapsed > record.ttl:
                    record.status = "ttl_expired"
                    self.logger.error(
                        "DROPPED msg_id=%s from=%s to=%s: ttl expired before delivery — "
                        "message not delivered", msg_id,
                        record.sender_id if record else "?", agent_id)
                    await self._send_ack(record.sender_id, msg_id, "ttl_expired", agent_id)
                    continue

            # Deliver the message. Reconnect redelivery is low-volume on a freshly
            # registered socket, so send directly (synchronous, clean outcome) rather
            # than via the writer queue — this avoids optimistic-mark/duplicate-ack
            # issues and keeps ttl-bearing messages in pending on a real send failure.
            try:
                await session.websocket.send(json.dumps(msg))
                if record:
                    record.status = "delivered"
                    self.logger.debug("delivered queued msg_id=%s to=%s", msg_id, agent_id)
                    await self._send_ack(record.sender_id, msg_id, "delivered", agent_id)
                # Success → drop from the pending list (do NOT re-deliver next reconnect).
            except Exception as e:
                # Delivery failed — keep in queue for the next reconnect attempt.
                self.logger.debug("reconnect delivery failed msg_id=%s to=%s: %s",
                                  msg_id, agent_id, e)
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
        self.logger.info("conversation %s ended by %s (reason=%s)",
                         conversation_id, agent_id, frame.get("reason", "ended"))

        end_msg = {
            "type": "conversation_ended",
            "conversation_id": conversation_id,
            "reason": frame.get("reason", "ended"),
            "by": agent_id,
            "ts": _now_iso(),
        }
        for participant in conv.participants:
            session = self.registry.agents.get(participant)
            if session and session.is_online and not session.writer_failed:
                session.enqueue(end_msg)

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
        """Detect an A↔B echo loop in a conversation.

        A strict 2-partner alternating sequence (A→B→A→B→…) is the shape of BOTH a
        legitimate multi-turn request/reply conversation AND a runaway echo loop. To
        avoid killing normal reply chains (which left replies undelivered after ~5
        turns), we only flag a loop when the participants are actually cycling the
        *same content* — i.e. at least one body repeats within the recent window.
        Distinct-content conversations keep flowing; ``max_conversation_depth`` still
        caps total conversation length.
        """
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
        # Strict alternation of 2 participants — only a loop if content repeats.
        bodies = [_body_key(r.body) for r in recent]
        return len(set(bodies)) < len(bodies)

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

        # Cancel orphaned receipt watchers whose message record has been pruned.
        orphan_watchers = [
            mid for mid in self.registry.pending_watchers
            if mid not in self.registry.messages
        ]
        for mid in orphan_watchers:
            self._cancel_watcher(mid)

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
        if session is not None and not session.writer_failed:
            session.enqueue(ack)
            self.logger.debug("send ack to=%s msg_id=%s status=%s", sender_id, msg_id, status)

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
        self.logger.debug("send error to=%s code=%s msg_id=%s",
                          agent_id, code, err.get("msg_id"))
        session = self.registry.agents.get(agent_id)
        if session is not None and not session.writer_failed:
            session.enqueue(err)


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

    def __init__(self, registry: Registry, quit_callback: Callable[[], None] = None):
        self.registry = registry
        self.quit_callback = quit_callback
        self.messages: list[tuple[str, str, str]] = []
        self.scroll_offset = 0
        self.input_buffer = ""
        self.search_term = ""
        self.is_searching = False
        self.running = True
        self._width = 78  # render width for markdown bodies (fits the 80-col frame)
        # Cache of rendered markdown lines per body, so a re-render of the visible
        # window (on every keystroke/message) doesn't re-parse markdown each time.
        self._md_cache: dict[str, list[str]] = {}
        # CLI output (the TUI) is painted by a dedicated daemon thread so a stalled
        # terminal / SSH session can't block ``sys.stdout.write`` and freeze the
        # asyncio event loop (which was causing multi-minute stalls → mass agent
        # disconnects). The event loop only signals "please repaint" (non-blocking);
        # the render thread does the (potentially blocking) stdout writes.
        self._render_event = threading.Event()
        self._render_thread: Optional[threading.Thread] = None

    def _start_render_thread(self):
        if self._render_thread is None:
            t = threading.Thread(target=self._render_loop, daemon=True,
                                  name="cli-render")
            t.start()
            self._render_thread = t

    def _render_loop(self):
        while True:
            self._render_event.wait()
            time.sleep(0.05)  # coalesce a burst of repaint requests into one paint
            self._render_event.clear()
            try:
                self.render()
            except Exception:
                pass

    def add_message(self, msg_type: str, text: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self.messages.append((ts, msg_type, text))
        self.scroll_offset = 0

    def log_routed_message(self, from_id: str, to_id: str, body: Any,
                           reply_to: Optional[str] = None):
        """Log an agent\u2192agent message that flowed through the server.

        Stored as a ``"msg"`` entry whose first field is the plain header line
        ``[HH:MM:SS] <from> -> <to> | REQUEST`` or ``| REPLY`` (depending on
        ``reply_to``) and whose body is the markdown text. ``_render_entry_lines``
        applies the color styling when painting.

        Repaints the screen (coalesced) when attached to a real terminal so live
        message flow is visible without waiting for a keystroke — without re-painting
        on every single message under burst load (which would stall the event loop).
        """
        ts = datetime.now().strftime("%H:%M:%S")
        kind = "REPLY" if reply_to else "REQUEST"
        header = f"[{ts}] {from_id} -> {to_id} | {kind}"
        body_str = body if isinstance(body, str) else json.dumps(body)
        self.messages.append((header, "msg", body_str))
        self.scroll_offset = 0
        if sys.stdout.isatty():
            self._schedule_render()

    def _schedule_render(self):
        """Signal the render thread to repaint (non-blocking). Coalesces a burst of
        repaint requests into one paint per ~50ms, and crucially keeps the
        (potentially blocking) stdout write off the asyncio event loop."""
        self._start_render_thread()
        self._render_event.set()

    def _flush_render(self):
        # Back-compat shim; rendering is driven by the render thread now.
        self._schedule_render()

    def _render_markdown(self, body: str) -> list[str]:
        """Render a markdown body to a list of colored terminal lines (cached)."""
        if not body:
            return []
        cached = self._md_cache.get(body)
        if cached is not None:
            return list(cached)
        if _RICH_AVAILABLE:
            buf = io.StringIO()
            console = _RichConsole(
                file=buf, width=self._width, force_terminal=True,
                color_system="auto", highlight=False, soft_wrap=False,
            )
            console.print(_RichMarkdown(body))
            rendered = buf.getvalue().rstrip("\n")
            lines = rendered.split("\n") if rendered else []
        else:
            # Fallback: plain text, no markdown styling.
            lines = body.split("\n")
        # Bound the cache so a long-running server doesn't hold every body forever.
        if len(self._md_cache) >= 256:
            self._md_cache.clear()
        self._md_cache[body] = list(lines)
        return lines

    def _render_entry_lines(self, ts: str, msg_type: str, text: str) -> list[str]:
        """Expand one log entry into the styled terminal lines it occupies."""
        if msg_type == "msg":
            # `ts` is the plain header "[HH:MM:SS] from -> to | REQUEST|REPLY";
            # color the REQUEST/REPLY tag, bold the rest.
            base, sep, tag = ts.partition(" | ")
            tag_color = "35" if tag == "REPLY" else "36"  # magenta REPLY, cyan REQUEST
            styled_header = f"\033[1m{base}\033[0m{sep}\033[{tag_color}m{tag}\033[0m"
            lines = [styled_header]
            lines.extend(self._render_markdown(text))  # markdown body, colored
            lines.extend(["", "\033[2m---\033[0m"])    # blank line + separator
            return lines
        color = {"send": "34", "recv": "32", "server": "33", "error": "31"}.get(msg_type, "0")
        return [f"\033[{color}m[{ts}] {text}\033[0m"]

    def render(self):
        """Paint a single window: a 1-line status header, the scrollable markdown
        message log filling the middle, and the bottom line reserved for command
        entry. Sizes to the terminal so the command line always lands on the last
        row.
        """
        cols, rows = shutil.get_terminal_size((80, 24))
        self._width = max(20, cols - 1)
        # Snapshot the shared state the event loop mutates, so rendering from the
        # render thread doesn't race with concurrent appends/registrations.
        agents = list(self.registry.agents.values())
        search_term = self.search_term
        scroll_offset = self.scroll_offset
        input_buffer = self.input_buffer
        is_searching = self.is_searching
        messages = list(self.messages)
        sys.stdout.write("\033[H\033[J")

        # Top status line (1 row).
        online = sum(1 for s in agents if s.is_online)
        offline = sum(1 for s in agents if not s.is_online)
        sys.stdout.write(
            f"\033[1m NEUROGOSSIP \u2014 Online: {online}  Offline: {offline}\033[0m\n")

        # Message area: everything between the status line and the bottom command
        # line (rows - 2). Flatten entries to terminal lines so multi-line markdown
        # bodies window correctly.
        msg_height = max(1, rows - 2)
        if search_term:
            needle = search_term.lower()
            filtered = [m for m in messages if needle in (m[0] + " " + m[2]).lower()]
        else:
            filtered = messages

        all_lines: list[str] = []
        for ts, msg_type, text in filtered:
            all_lines.extend(self._render_entry_lines(ts, msg_type, text))

        start = max(0, len(all_lines) - msg_height - scroll_offset)
        end = max(start, len(all_lines) - scroll_offset)
        for line in all_lines[start:end]:
            sys.stdout.write(line + "\n")
        for _ in range(msg_height - (end - start)):
            sys.stdout.write("\n")

        # Bottom line: command entry (reserved).
        if is_searching:
            sys.stdout.write(f"\033[7m /search: {search_term}\033[0m")
        else:
            sys.stdout.write(f"\033[7m {input_buffer}\033[0m")
        sys.stdout.write("\033[K")
        sys.stdout.flush()

    async def read_stdin(self):
        try:
            data = os.read(sys.stdin.fileno(), 1024)
            for char in data.decode("utf-8", errors="replace"):
                self._process_char(char)
            self._schedule_render()
        except (KeyboardInterrupt, asyncio.CancelledError):
            # Shutdown in progress (second Ctrl-C raises KeyboardInterrupt in
            # whatever callback is running — often this reader task). Swallow it
            # so the task doesn't surface an "exception was never retrieved"
            # traceback; the main loop drives the graceful shutdown.
            pass
        except Exception as e:
            self.add_message("error", f"stdin read error: {e}")

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
            # Escape / arrow-key intro. The line-buffered (cooked) terminal doesn't
            # deliver escape sequences char-by-char, and a blocking read here would
            # stall the event loop — so just clear the input buffer and ignore.
            self.input_buffer = ""
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
                if s.is_online and not s.writer_failed:
                    s.enqueue({
                        "type": "message",
                        "from": "__server__",
                        "body": body,
                        "msg_id": str(uuid.uuid4()),
                        "ts": _now_iso(),
                    })
                    count += 1
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
        elif cmd == "/quit":
            self.add_message("server", "Shutting down gracefully...")
            if self.quit_callback is not None:
                self.quit_callback()
            else:
                self.add_message("error", "No shutdown handler attached")
        elif cmd == "/help":
            self.add_message("server", "Commands: /list, /status <agent>, /broadcast <msg>, "
                                       "/search <term>, /export, /quit, /help")
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
        if target.writer_failed:
            self.add_message("error", f"Failed to send to {handle}: connection closed")
            return
        target.enqueue({
            "type": "message",
            "from": "__cli__",
            "body": body,
            "msg_id": msg_id,
            "ts": _now_iso(),
        })
        self.add_message("send", f"\u2192 {handle}: {body}")

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


def _body_key(body: Any) -> str:
    """Normalize a message body to a hashable key for loop / dedup comparison."""
    if isinstance(body, str):
        return body
    return json.dumps(body, sort_keys=True)


def _truncate(text: Any, limit: int = 2000) -> str:
    """Render ``text`` for logging, truncating long values (e.g. message bodies)."""
    s = text if isinstance(text, str) else json.dumps(text, ensure_ascii=False)
    if len(s) > limit:
        return s[:limit] + f"…<+{len(s) - limit} bytes>"
    return s


def _redact_frame(frame: dict) -> dict:
    """Copy a frame with secrets masked, for safe debug logging."""
    redacted = dict(frame)
    if "auth_token" in redacted and redacted["auth_token"] is not None:
        redacted["auth_token"] = "***"
    return redacted


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
                 log_level: str = "INFO",
                 ws_ping_interval: float = 20.0,
                 ws_ping_timeout: float = 45.0,
                 liveness_timeout: float = 2.0,
                 outbound_queue_size: int = DEFAULT_OUTBOUND_QUEUE_SIZE):
        self.host = host
        self.port = port
        self.cli_mode = cli_mode
        self.require_auth = require_auth
        self.secret = secret
        self.logger = logging.getLogger("neurogossip")
        # Silence benign websockets handshake/close ERROR tracebacks (client dropped
        # the connection mid-upgrade, etc.) without hiding genuine internal errors.
        _install_websockets_log_filter()
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
        if self.cli is not None:
            # /quit requests a graceful shutdown by closing the listener; start()
            # then runs the full shutdown() sequence to completion before exiting.
            self.cli.quit_callback = self.request_shutdown
            # Log every agent→agent message that flows through the router as
            # "[HH:MM:SS] <from> -> <to> | REQUEST|REPLY" + markdown-rendered body.
            self.router.on_message_routed = self._on_message_routed
        self.server = None
        self._running = False
        self._shutting_down = False
        self._bg_tasks: list[asyncio.Task] = []
        self.grace_period_s: float = 5.0  # shutdown grace period (design §3.6)
        # websockets protocol keepalive — reaps half-open sockets in ~ping_interval+ping_timeout
        # instead of waiting for the 45s app heartbeat (design §2.2).
        self.ws_ping_interval = ws_ping_interval
        self.ws_ping_timeout = ws_ping_timeout
        # How long to wait for a pong when probing a stale "online" session on reconnect
        # before evicting it (design §2.1 — evict confirmed-dead sessions, never a live one).
        self.liveness_timeout = liveness_timeout
        # Per-connection outbound frame queue size ("large message queues", with
        # explicit drop-oldest backpressure).
        self.outbound_queue_size = outbound_queue_size

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
            ping_interval=self.ws_ping_interval,
            ping_timeout=self.ws_ping_timeout,
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
            self.cli._schedule_render()

    def request_shutdown(self):
        """Request a graceful shutdown from within the event loop.

        Called by the CLI ``/quit`` command and the SIGTERM handler. Closing the
        listener ends ``serve_forever()`` in ``start()``; ``start()`` then runs the
        full ``shutdown()`` sequence to completion before returning, so the process
        doesn't exit mid-drain of in-flight messages.
        """
        if self.server is not None:
            self.server.close()

    def _on_message_routed(self, sender_id: str, target_id: str, body: Any,
                           reply_to: Optional[str] = None) -> None:
        """Router hook: forward each accepted message to the CLI logger."""
        if self.cli is not None:
            self.cli.log_routed_message(sender_id, target_id, body, reply_to=reply_to)

    async def start(self):
        """Entry point: serve, install signal handlers, run until stopped.

        Blocks on ``serve_forever()`` until shutdown is requested — by ``/quit``, by
        SIGTERM (both call ``request_shutdown()``, which closes the listener), or by
        SIGINT (``asyncio.run`` cancels the main task). In every case we then run the
        full graceful ``shutdown()`` to completion before returning, so in-flight
        messages are drained and agents are notified before the process exits.
        """
        await self.serve()

        # SIGTERM → graceful shutdown (asyncio.run owns SIGINT).
        try:
            loop = asyncio.get_event_loop()
            loop.add_signal_handler(signal.SIGTERM, self.request_shutdown)
        except NotImplementedError:
            pass  # Windows doesn't support add_signal_handler

        try:
            await self.server.serve_forever()
        except asyncio.CancelledError:
            # SIGINT (asyncio.run cancelled the main task) → fall through to shutdown.
            pass

        # Run the full graceful-shutdown sequence to completion regardless of how
        # serve_forever() ended, so the process doesn't exit mid-drain.
        await self.shutdown()

    async def shutdown(self):
        """Graceful shutdown sequence (6 steps, design §3.6).

        Idempotent: a second call (e.g. SIGTERM during a SIGINT-driven shutdown) is a
        no-op so the sequence runs exactly once to completion.
        """
        if self._shutting_down:
            return
        self._shutting_down = True
        self.logger.info("Shutting down...")
        self._running = False

        # 0. Stop reading stdin so no more read_stdin tasks are scheduled mid-teardown.
        if self.cli_mode:
            try:
                asyncio.get_event_loop().remove_reader(sys.stdin.fileno())
            except Exception:
                pass

        # 1. Stop accepting new connections
        self.server.close()

        # 2. Notify all connected agents (non-blocking via the per-connection queue).
        shutdown_msg = {
            "type": "shutdown",
            "reason": "server_going_down",
            "grace_period_s": self.grace_period_s,
        }
        for agent_id, session in list(self.registry.agents.items()):
            if session.is_online and not session.writer_failed:
                session.enqueue(shutdown_msg)
        # Let the writer tasks drain the shutdown frames before we tear down.
        await asyncio.sleep(0)

        # 3. Wait for agents to acknowledge (max grace period)
        await asyncio.sleep(self.grace_period_s)

        # 4. Drain in-flight messages + cancel pending receipt watchers.
        for mid in list(self.router.registry.pending_watchers):
            self.router._cancel_watcher(mid)
        for msg_id, record in list(self.registry.messages.items()):
            if record.status == "pending":
                record.status = "failed"
                await self.router._send_ack(record.sender_id, msg_id, "failed", record.recipient_id)

        # 5. Close all connections (stop their writer tasks first).
        close_tasks = []
        for agent_id, session in list(self.registry.agents.items()):
            if session.is_online:
                session.cancel_writer()
                close_tasks.append(
                    self._close_connection(session.websocket)
                )
        await asyncio.gather(*close_tasks, return_exceptions=True)

        # 6. Stop background tasks + presence timer and wait for the listener to close.
        await self.stop()
        _stop_log_listeners()  # stop the off-loop logging thread

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

    async def _is_session_live(self, session: AgentSession) -> bool:
        """Probe whether a session's websocket is genuinely alive.

        Used on reconnect when the registry still marks an agent online: a live
        session pongs quickly (so we keep it and reject the new connection — design
        §2.1, no hijacking); a half-open socket doesn't, and is evicted. Fast path:
        if the websockets library already knows the socket is closed, skip the probe.
        """
        ws = session.websocket
        try:
            if ws.close_code is not None:
                return False
        except Exception:
            return False
        try:
            await asyncio.wait_for(ws.ping(), timeout=self.liveness_timeout)
            return True
        except Exception:
            return False

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
                self.logger.debug("recv frame agent=%s type=%s: %s",
                                  agent_id, msg_type, _truncate(_redact_frame(frame)))
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
                    await self._handle_list_agents(agent_id)
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
                await self._handle_disconnect(agent_id, websocket)

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
        if existing is not None and existing.is_online:
            # The registry thinks this agent is still online. That's either a
            # genuinely live session (reject — design §2.1 forbids hijacking) or a
            # half-open socket the server hasn't reaped yet. Probe it: evict and
            # replace only if confirmed dead, so a reconnecting agent isn't blocked
            # for the 45s app-heartbeat timeout (or the keepalive timeout).
            if await self._is_session_live(existing):
                self.logger.debug("register rejected: %s already online (live)", agent_id)
                await websocket.send(json.dumps({
                    "type": "error", "code": "ALREADY_REGISTERED",
                    "message": f"Agent '{agent_id}' is already connected",
                }))
                return None
            self.logger.info(
                f"Agent {agent_id} reconnect evicted stale (half-open) session")
            existing.is_online = False
            # Salvage any message frames still queued for the dead socket so they're
            # carried over and redelivered on the fresh connection, then stop its writer.
            try:
                await existing.drain_outbound_to_pending()
            except Exception:
                pass
            existing.cancel_writer()
            await self._close_connection(existing.websocket)  # wind down the dead socket

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
            outbound_queue_size=self.outbound_queue_size,
            on_send_failure=self.router._handle_writer_failure,
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

    async def _handle_list_agents(self, agent_id: str):
        """Handle list_agents request (reply via the agent's outbound queue)."""
        agents_list = []
        for aid, session in self.registry.agents.items():
            agents_list.append({
                "agent_id": aid,
                "status": "online" if session.is_online else "offline",
                "metadata": session.metadata,
            })
        session = self.registry.agents.get(agent_id)
        if session is not None and not session.writer_failed:
            session.enqueue({"type": "agent_list", "agents": agents_list})

    async def _handle_disconnect(self, agent_id: str,
                                 websocket: Optional[ServerConnection] = None):
        """Handle agent disconnection.

        Only acts for the session that owns ``websocket``. If the agent has since
        reconnected (eviction replaced the session with a new websocket), the old
        connection's teardown must not mark the fresh session offline.
        """
        session = self.registry.agents.get(agent_id)
        if session is None:
            return
        if websocket is not None and session.websocket is not websocket:
            return
        session.is_online = False
        # Move any message frames still queued for the writer into pending so they're
        # redelivered on reconnect, then stop the writer task.
        try:
            await session.drain_outbound_to_pending()
        except Exception:
            pass
        session.cancel_writer()
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

def _load_env(path: str = ".env") -> dict[str, str]:
    """Load ``KEY=VALUE`` pairs from a .env file into a dict (missing file → empty).

    Minimal parser: strips whitespace, optional ``export`` prefix, surrounding
    quotes, and inline ``# comments``; ignores blank/comment lines. Existing
    process environment variables are NOT overridden by the file (so real env
    vars win), matching the common .env convention.
    """
    env: dict[str, str] = {}
    if not os.path.isfile(path):
        return env
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export "):].lstrip()
            if "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            # Strip an inline comment that's preceded by whitespace.
            if " #" in val:
                val = val.split(" #", 1)[0]
            val = val.strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                val = val[1:-1]
            env[key] = val
    return env


def _parse_bytes(value: str, default: int) -> int:
    """Parse a size string like ``250MB`` / ``250M`` / ``262144000`` into bytes."""
    if not value:
        return default
    s = str(value).strip().upper()
    if not s:
        return default
    units = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "K": 1024,
             "M": 1024**2, "G": 1024**3}
    for suffix in ("KB", "MB", "GB", "B", "K", "M", "G"):
        if s.endswith(suffix):
            num = s[:-len(suffix)].strip()
            try:
                return int(float(num) * units[suffix])
            except ValueError:
                return default
    try:
        return int(s)
    except ValueError:
        return default


def _parse_bool(value, default: bool = False) -> bool:
    if value is None:
        return default
    s = str(value).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off", ""):
        return False
    return default


_LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"

# Active QueueListener threads, so shutdown()/tests can stop them.
_ACTIVE_LOG_LISTENERS: list[QueueListener] = []


def _setup_logging(log_path: str, log_level: str, max_bytes: int,
                   backup_count: int, cli_mode: bool) -> RotatingFileHandler:
    """Configure rotating file logging at DEBUG + an optional console handler.

    A :class:`RotatingFileHandler` captures the neurogossip loggers (neurogossip,
    router, heartbeat, presence) at DEBUG — all communications and technical
    details. When the file reaches ``max_bytes`` it is archived (``.log`` →
    ``.log.1`` → …) and a fresh file is started, keeping up to ``backup_count``
    archives. In CLI mode no console handler is attached so log output doesn't
    corrupt the TUI (logs go to the file only).

    Logging is decoupled from the event loop: the file/console handlers run inside a
    :class:`QueueListener` thread, fed by a :class:`QueueHandler` on the root logger.
    Log calls become non-blocking ``queue.put_nowait``; the disk write (and any disk
    hiccup) happens in the listener thread and can never stall handshakes/keepalives.

    Chatty third-party loggers are quieted to WARNING so they don't flood the log
    (``markdown_it`` emits thousands of DEBUG lines per parse — see the congestion
    postmortem). The app's own message/frame logging is unaffected.
    """
    log_dir = os.path.dirname(log_path) or "."
    os.makedirs(log_dir, exist_ok=True)
    file_handler = RotatingFileHandler(
        log_path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(_LOG_FORMAT))

    target_handlers = [file_handler]
    if not cli_mode:
        console = logging.StreamHandler()   # stderr
        console.setLevel(getattr(logging, str(log_level).upper(), logging.INFO))
        console.setFormatter(logging.Formatter(_LOG_FORMAT))
        target_handlers.append(console)

    # Off-loop: QueueHandler on root → QueueListener thread → real handlers.
    log_queue: queue.Queue = queue.Queue()
    listener = QueueListener(log_queue, *target_handlers, respect_handler_level=True)
    listener.start()
    _ACTIVE_LOG_LISTENERS.append(listener)
    atexit.register(listener.stop)

    queue_handler = QueueHandler(log_queue)
    queue_handler.setLevel(logging.DEBUG)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)            # neurogossip loggers inherit DEBUG
    for h in list(root.handlers):            # drop prior handlers (no double-log)
        root.removeHandler(h)
    root.addHandler(queue_handler)

    # Quiet chatty third-party loggers: their DEBUG is pure noise (markdown_it rule
    # traces, rich internals, websockets byte-level frames, asyncio loop debug) and
    # flooding the log stalls the event loop → handshake/keepalive timeouts.
    for noisy in ("markdown_it", "markdown_it.tree", "rich", "websockets", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return file_handler


def _stop_log_listeners() -> None:
    """Stop any active QueueListener threads (called from shutdown + atexit)."""
    while _ACTIVE_LOG_LISTENERS:
        lst = _ACTIVE_LOG_LISTENERS.pop()
        try:
            lst.stop()
        except Exception:
            pass


def main():
    env = _load_env(os.environ.get("NEUROGOSSIP_SERVER_ENV", ".env"))

    def env_str(key, default):
        return os.environ.get(key, env.get(key, default))

    parser = argparse.ArgumentParser(description="Neurogossip v5.0 — Agent Registry & DM Server")
    parser.add_argument("--host", type=str,
                         default=env_str("NEUROGOSSIP_SERVER_BIND", "0.0.0.0"),
                         help="Server bind address (env: NEUROGOSSIP_SERVER_BIND)")
    parser.add_argument("--port", type=int,
                         default=int(env_str("NEUROGOSSIP_SERVER_PORT", "8765") or 8765),
                         help="Server listen port (env: NEUROGOSSIP_SERVER_PORT)")
    parser.add_argument("--cli", action="store_true",
                         default=_parse_bool(env_str("NEUROGOSSIP_SERVER_CLI", "false")),
                         help="Enable ANSI-based CLI message view (env: NEUROGOSSIP_SERVER_CLI)")
    parser.add_argument("--heartbeat", type=float,
                         default=float(env_str("NEUROGOSSIP_SERVER_HEARTBEAT_INTERVAL", "15") or 15),
                         help="Heartbeat interval seconds (env: NEUROGOSSIP_SERVER_HEARTBEAT_INTERVAL)")
    parser.add_argument("--max-missed", type=int,
                         default=int(env_str("NEUROGOSSIP_SERVER_MAX_MISSED", "3") or 3),
                         help="Missed pings before offline (env: NEUROGOSSIP_SERVER_MAX_MISSED)")
    parser.add_argument("--require-auth", action="store_true",
                         default=_parse_bool(env_str("NEUROGOSSIP_SERVER_AUTH_REQUIRED", "false")),
                         help="Require auth token (env: NEUROGOSSIP_SERVER_AUTH_REQUIRED)")
    parser.add_argument("--secret", type=str,
                         default=env_str("NEUROGOSSIP_SERVER_AUTH_SECRET", None),
                         help="Shared auth secret (env: NEUROGOSSIP_SERVER_AUTH_SECRET)")
    parser.add_argument("--max-message-bytes", type=int,
                         default=int(env_str("NEUROGOSSIP_SERVER_MAX_MESSAGE_BYTES", "1048576") or 1048576),
                         help="Max message size (env: NEUROGOSSIP_SERVER_MAX_MESSAGE_BYTES)")
    parser.add_argument("--rate-limit-agent", type=int,
                         default=int(env_str("NEUROGOSSIP_SERVER_RATE_LIMIT_AGENT", "60") or 60),
                         help="Per-agent msg/min (env: NEUROGOSSIP_SERVER_RATE_LIMIT_AGENT)")
    parser.add_argument("--rate-limit-pair", type=int,
                         default=int(env_str("NEUROGOSSIP_SERVER_RATE_LIMIT_PAIR", "20") or 20),
                         help="Per-pair msg/min (env: NEUROGOSSIP_SERVER_RATE_LIMIT_PAIR)")
    parser.add_argument("--delivery-timeout", type=float,
                         default=float(env_str("NEUROGOSSIP_SERVER_DELIVERY_TIMEOUT", "30") or 30),
                         help="Delivery ACK timeout (env: NEUROGOSSIP_SERVER_DELIVERY_TIMEOUT)")
    parser.add_argument("--max-conversation-depth", type=int,
                         default=int(env_str("NEUROGOSSIP_SERVER_MAX_CONVERSATION_DEPTH", "20") or 20),
                         help="Max reply depth (env: NEUROGOSSIP_SERVER_MAX_CONVERSATION_DEPTH)")
    parser.add_argument("--circuit-breaker-window", type=float,
                         default=float(env_str("NEUROGOSSIP_SERVER_CIRCUIT_BREAKER_WINDOW", "60") or 60),
                         help="CB window s (env: NEUROGOSSIP_SERVER_CIRCUIT_BREAKER_WINDOW)")
    parser.add_argument("--circuit-breaker-max", type=int,
                         default=int(env_str("NEUROGOSSIP_SERVER_CIRCUIT_BREAKER_MAX", "30") or 30),
                         help="CB max msgs (env: NEUROGOSSIP_SERVER_CIRCUIT_BREAKER_MAX)")
    parser.add_argument("--circuit-breaker-cooldown", type=float,
                         default=float(env_str("NEUROGOSSIP_SERVER_CIRCUIT_BREAKER_COOLDOWN", "120") or 120),
                         help="CB cooldown s (env: NEUROGOSSIP_SERVER_CIRCUIT_BREAKER_COOLDOWN)")
    parser.add_argument("--presence-debounce", type=float,
                         default=float(env_str("NEUROGOSSIP_SERVER_PRESENCE_DEBOUNCE", "1") or 1),
                         help="Presence debounce seconds (env: NEUROGOSSIP_SERVER_PRESENCE_DEBOUNCE)")
    parser.add_argument("--ws-ping-interval", type=float,
                         default=float(env_str("NEUROGOSSIP_SERVER_WS_PING_INTERVAL", "20") or 20),
                         help="websockets keepalive ping interval s (env: NEUROGOSSIP_SERVER_WS_PING_INTERVAL)")
    parser.add_argument("--ws-ping-timeout", type=float,
                         default=float(env_str("NEUROGOSSIP_SERVER_WS_PING_TIMEOUT", "45") or 45),
                         help="websockets keepalive pong timeout s — keep generous so busy "
                              "agents aren't reaped (env: NEUROGOSSIP_SERVER_WS_PING_TIMEOUT)")
    parser.add_argument("--liveness-timeout", type=float,
                         default=float(env_str("NEUROGOSSIP_SERVER_LIVENESS_TIMEOUT", "2") or 2),
                         help="Stale-session pong probe timeout s (env: NEUROGOSSIP_SERVER_LIVENESS_TIMEOUT)")
    parser.add_argument("--outbound-queue-size", type=int,
                         default=int(env_str("NEUROGOSSIP_SERVER_OUTBOUND_QUEUE_SIZE",
                                             str(DEFAULT_OUTBOUND_QUEUE_SIZE))
                                    or DEFAULT_OUTBOUND_QUEUE_SIZE),
                         help="Per-connection outbound frame queue size (drop-oldest on overflow) "
                              "(env: NEUROGOSSIP_SERVER_OUTBOUND_QUEUE_SIZE)")
    parser.add_argument("--log", type=str,
                         default=env_str("NEUROGOSSIP_SERVER_LOG", "logs/neurogossip-server.log"),
                         help="Log file path (env: NEUROGOSSIP_SERVER_LOG)")
    parser.add_argument("--log-level", type=str,
                         default=env_str("NEUROGOSSIP_SERVER_LOG_LEVEL", "debug"),
                         help="Console log level; file always logs DEBUG (env: NEUROGOSSIP_SERVER_LOG_LEVEL)")
    parser.add_argument("--log-size", type=str,
                         default=env_str("NEUROGOSSIP_SERVER_LOG_SIZE",
                                         env_str("NUEROGOSSIP_SERVER_LOG_SIZE", "250MB")),
                         help="Rotating log size before archive, e.g. 250MB "
                              "(env: NEUROGOSSIP_SERVER_LOG_SIZE)")
    parser.add_argument("--log-backup-count", type=int,
                         default=int(env_str("NEUROGOSSIP_SERVER_LOG_BACKUP_COUNT", "5") or 5),
                         help="Number of archived log files to keep (env: NEUROGOSSIP_SERVER_LOG_BACKUP_COUNT)")
    args = parser.parse_args()

    file_handler = _setup_logging(
        log_path=args.log,
        log_level=args.log_level,
        max_bytes=_parse_bytes(args.log_size, 250 * 1024 * 1024),
        backup_count=args.log_backup_count,
        cli_mode=args.cli,
    )
    log = logging.getLogger("neurogossip")
    log.info("Neurogossip server starting — host=%s port=%s cli=%s log=%s level=%s",
             args.host, args.port, args.cli, args.log, args.log_level)
    log.debug("Logging to %s (maxBytes=%d, backupCount=%d, handler=%s)",
              args.log, file_handler.maxBytes, file_handler.backupCount,
              type(file_handler).__name__)

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
        ws_ping_interval=args.ws_ping_interval,
        ws_ping_timeout=args.ws_ping_timeout,
        liveness_timeout=args.liveness_timeout,
        outbound_queue_size=args.outbound_queue_size,
    )

    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
