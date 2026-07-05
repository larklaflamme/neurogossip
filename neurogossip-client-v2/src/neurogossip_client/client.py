"""NeuroGossipClient — a durable-by-default Redis Streams + Pub/Sub client for
multi-turn AI-agent markdown conversations.

Reliability model (see design.md):

- Every publish/``send_direct`` does ``XADD`` (durable stream) **and** ``PUBLISH``
  (live notification). The Stream is the source of truth; Pub/Sub is a low-latency
  wake-up that triggers a Stream read — so each message is delivered exactly once,
  with no gap between catch-up and live.
- Direct (1:1) messages use a per-recipient consumer group; the recipient ``XACK``s
  after processing, so a crash before ack → the message is reclaimed and redelivered
  (at-least-once). The sender can confirm delivery via the ack.
- Rooms are broadcast: every member reads the room stream by cursor, so a member that
  was briefly disconnected catches up on what it missed on (re)connect.
- The client auto-reconnects (backoff), re-subscribes to its inbox + all joined rooms,
  and re-runs catch-up — a Redis blip or agent restart does not silently lose messages.

Configuration: the Redis URL is read from the ``REDIS_URL`` environment variable unless
an explicit ``redis_url=`` is passed. If neither is set, construction raises
``RedisUrlNotConfiguredError``.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from collections.abc import AsyncIterator
from typing import Any, Literal

import redis.asyncio as aioredis

from . import durability, presence
from .channels import (
    cursor_key,
    inbox_channel,
    inbox_consumer_group,
    inbox_stream,
    room_channel,
    room_stream,
)
from .errors import (
    MessageDecodeError,
    NotConnectedError,
    RedisUrlNotConfiguredError,
)
from .models import (
    AgentIdentity,
    AgentPresence,
    GossipMessage,
    MessageContent,
    MessageMetadata,
    Recipient,
)
from .validators import validate_payload_size

log = logging.getLogger("neurogossip_client")

DeliveryMode = Literal["durable", "live"]

# Redis's own ceiling for a single bulk string / stream field value is the
# `proto-max-bulk-len` config, which defaults to 512 MiB (512 * 1024 * 1024).
# We mirror that here so we never reject a payload Redis would accept.
_DEFAULT_MAX_PAYLOAD = 512 * 1024 * 1024


class NeuroGossipClient:
    """Async Redis client for reliable agent-to-agent markdown messaging."""

    def __init__(
        self,
        agent: AgentIdentity,
        namespace: str = "default",
        redis_url: str | None = None,
        *,
        redis_client: aioredis.Redis | None = None,  # test/injection seam
        max_payload_bytes: int = _DEFAULT_MAX_PAYLOAD,
        delivery_mode: DeliveryMode = "durable",
        ignore_self: bool = True,
        heartbeat_interval_s: float = 10.0,
        presence_ttl_s: float = 30.0,
        reconnect_backoff_s: float = 1.0,
        reconnect_max_backoff_s: float = 30.0,
    ) -> None:
        # Resolve the Redis URL: explicit arg wins, else the REDIS_URL env var.
        # Raise immediately (at construction, before any I/O) if neither is set and no
        # pre-built client was injected.
        if redis_client is not None:
            self._injected_redis = redis_client
            self.redis_url = redis_url or os.environ.get("REDIS_URL") or "(injected)"
        else:
            self._injected_redis = None
            url = redis_url if redis_url is not None else os.environ.get("REDIS_URL")
            if not url:
                raise RedisUrlNotConfiguredError(
                    "REDIS_URL is not configured. Define the REDIS_URL environment "
                    "variable (e.g. REDIS_URL=redis://localhost:6379/0) or pass "
                    "redis_url=... to NeuroGossipClient."
                )
            self.redis_url = url

        self.agent = agent
        self.namespace = namespace
        self.max_payload_bytes = max_payload_bytes
        self.delivery_mode = delivery_mode
        self.ignore_self = ignore_self
        self.heartbeat_interval_s = heartbeat_interval_s
        self.presence_ttl_s = presence_ttl_s
        self.reconnect_backoff_s = reconnect_backoff_s
        self.reconnect_max_backoff_s = reconnect_max_backoff_s

        self._redis: aioredis.Redis | None = None
        self._pubsub: aioredis.client.PubSub | None = None
        # Inbound queue holds (ack_id, message) tuples. ack_id is the inbox stream
        # entry id for direct messages (for XACK) and None for room messages.
        self._inbox: asyncio.Queue[tuple[str | None, GossipMessage]] = asyncio.Queue()
        # message_id -> inbox stream entry id, for the public ack() API.
        self._stream_ids: dict[str, str] = {}
        self._joined_rooms: set[str] = set()
        self._listen_task: asyncio.Task | None = None
        self._reconnect_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._online = asyncio.Event()

    # ------------------------------------------------------------------ lifecycle
    async def __aenter__(self) -> "NeuroGossipClient":
        await self._connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        self._stop.set()
        for t in (self._listen_task, self._reconnect_task, self._heartbeat_task):
            if t is not None and not t.done():
                t.cancel()
        for t in (self._listen_task, self._reconnect_task, self._heartbeat_task):
            if t is not None:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
        if self._pubsub is not None:
            try:
                aclose = getattr(self._pubsub, "aclose", None)
                if aclose is not None:
                    await aclose()
                else:
                    await self._pubsub.close()
            except Exception:
                pass
            self._pubsub = None
        if self._redis is not None and self._injected_redis is None:
            try:
                await self._redis.aclose()
            except Exception:
                pass
            self._redis = None

    async def _connect(self) -> None:
        """Open Redis, subscribe to own inbox, catch up, go live, start presence."""
        self._stop.clear()
        if self._injected_redis is not None:
            self._redis = self._injected_redis
        else:
            self._redis = aioredis.from_url(
                self.redis_url, encoding="utf-8", decode_responses=True
            )
        await self._redis.ping()
        self._pubsub = self._redis.pubsub()

        # Subscribe to own inbox (direct messages), then catch up on anything missed.
        inbox_ch = inbox_channel(self.namespace, self.agent.agent_id)
        inbox_st = inbox_stream(self.namespace, self.agent.agent_id)
        group = inbox_consumer_group(self.namespace, self.agent.agent_id)
        await self._pubsub.subscribe(inbox_ch)
        await durability.ensure_group(self._redis, inbox_st, group)  # id=0 → all history
        await self._drain_inbox_stream(reclaim_pending=True)  # catch up + reclaim unacked

        # Subscribe to + catch up on each previously-joined room.
        for room_id in list(self._joined_rooms):
            await self._pubsub.subscribe(room_channel(self.namespace, room_id))
            await self._drain_room_stream(room_id)

        self._listen_task = asyncio.create_task(self._listen_loop())
        self._reconnect_task = asyncio.create_task(self._watch_connection())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._online.set()

    # ------------------------------------------------------------------ publishing
    async def publish(
        self,
        room_id: str,
        markdown: str,
        *,
        thread_id: str | None = None,
        reply_to: str | None = None,
        trace_id: str | None = None,
        blueprint_id: str | None = None,
        tags: list[str] | None = None,
    ) -> GossipMessage:
        """Broadcast a markdown message to a room (durable + live)."""
        self._require_connected()
        msg = self._build_message(
            room_id=room_id,
            recipient=Recipient(mode="room"),
            markdown=markdown,
            thread_id=thread_id,
            reply_to=reply_to,
            trace_id=trace_id,
            blueprint_id=blueprint_id,
            tags=tags,
        )
        await self._emit(
            room_channel(self.namespace, room_id),
            room_stream(self.namespace, room_id),
            msg,
        )
        return msg

    async def send_direct(
        self,
        to_agent_id: str,
        markdown: str,
        *,
        thread_id: str | None = None,
        reply_to: str | None = None,
        trace_id: str | None = None,
        blueprint_id: str | None = None,
        tags: list[str] | None = None,
    ) -> GossipMessage:
        """Send a 1:1 markdown message to a specific agent (durable + acknowledged)."""
        self._require_connected()
        msg = self._build_message(
            room_id=None,
            recipient=Recipient(mode="direct", agent_ids=[to_agent_id]),
            markdown=markdown,
            thread_id=thread_id,
            reply_to=reply_to,
            trace_id=trace_id,
            blueprint_id=blueprint_id,
            tags=tags,
        )
        await self._emit(
            inbox_channel(self.namespace, to_agent_id),
            inbox_stream(self.namespace, to_agent_id),
            msg,
        )
        return msg

    async def _emit(self, channel: str, stream: str, msg: GossipMessage) -> None:
        payload = msg.model_dump_json(by_alias=True)
        validate_payload_size(payload, self.max_payload_bytes)
        assert self._redis is not None
        if self.delivery_mode == "durable":
            await durability.xadd_message(self._redis, stream, payload)
        await self._redis.publish(channel, payload)

    def _build_message(
        self,
        *,
        room_id: str | None,
        recipient: Recipient,
        markdown: str,
        thread_id: str | None,
        reply_to: str | None,
        trace_id: str | None,
        blueprint_id: str | None,
        tags: list[str] | None,
    ) -> GossipMessage:
        msg_id = str(uuid.uuid4())
        # thread_id/reply_to chain propagation is enforced by GossipMessage's validator
        # (original -> thread_id = message_id; response -> thread_id = reply_to unless
        # an explicit root thread_id is passed).
        return GossipMessage(
            message_id=msg_id,
            room_id=room_id,
            thread_id=thread_id,
            reply_to=reply_to,
            sender=self.agent,
            recipient=recipient,
            content=MessageContent(body=markdown),
            metadata=MessageMetadata(
                trace_id=trace_id, blueprint_id=blueprint_id, tags=tags or []
            ),
        )

    # ------------------------------------------------------------------ rooms
    async def join_room(self, room_id: str) -> None:
        self._joined_rooms.add(room_id)
        if self._pubsub is not None:
            await self._pubsub.subscribe(room_channel(self.namespace, room_id))
            await self._drain_room_stream(room_id)  # catch up on missed room messages

    async def leave_room(self, room_id: str) -> None:
        self._joined_rooms.discard(room_id)
        if self._pubsub is not None:
            await self._pubsub.unsubscribe(room_channel(self.namespace, room_id))

    # ------------------------------------------------------------------ receiving
    async def listen(self) -> AsyncIterator[GossipMessage]:
        """Yield every incoming message (direct inbox + all joined rooms).

        Missed-while-disconnected messages are delivered first (catch-up), then live.
        ``ignore_self`` suppresses this agent's own echoed room messages. Direct
        messages are ``XACK``ed after the app sees them (at-least-once with crash
        recovery: a crash before the next iteration leaves them unacked → redelivered).
        """
        while not self._stop.is_set():
            ack_id, msg = await self._inbox.get()
            if self.ignore_self and msg.sender.agent_id == self.agent.agent_id:
                # Still ack direct so the PEL doesn't grow; rooms have no ack.
                if ack_id is not None:
                    await self._ack_direct(ack_id)
                continue
            yield msg
            if ack_id is not None:  # direct: ack after the app has seen it
                await self._ack_direct(ack_id)

    async def ack(self, msg: GossipMessage) -> None:
        """Explicitly ack a direct message (idempotent). Auto-acked by ``listen()``."""
        sid = self._stream_ids.get(msg.message_id)
        if sid is not None:
            await self._ack_direct(sid)

    async def _ack_direct(self, stream_id: str) -> None:
        if self._redis is None:
            return
        group = inbox_consumer_group(self.namespace, self.agent.agent_id)
        try:
            await durability.ack(
                self._redis,
                inbox_stream(self.namespace, self.agent.agent_id),
                group,
                stream_id,
            )
        except Exception as e:
            log.debug("XACK failed for %s: %s", stream_id, e)

    async def _listen_loop(self) -> None:
        """One loop over the shared PubSub object. Each notification triggers a Stream
        read (the Stream is the source of truth — exactly-once, no catch-up/live gap)."""
        try:
            async for raw in self._pubsub.listen():  # type: ignore[union-attr]
                if self._stop.is_set():
                    break
                if raw.get("type") != "message":
                    continue
                channel = raw.get("channel")
                stream, mode, room_id = self._channel_to_stream(channel)
                if stream is None:
                    continue
                if mode == "inbox":
                    await self._drain_inbox_stream()
                else:
                    await self._drain_room_stream(room_id)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("listen loop ended: %s", e)

    async def _drain_inbox_stream(self, *, reclaim_pending: bool = False) -> None:
        """Read never-delivered direct messages → enqueue.

        On (re)connect (``reclaim_pending=True``) also reclaim ALL this consumer's
        unacked entries — the previous incarnation is gone, so messages it read but
        never acked must be redelivered (at-least-once). On a normal pubsub-triggered
        drain (``reclaim_pending=False``) only new (">") entries are read, to avoid
        re-delivering messages the live agent is still processing.

        Ordering (reclaim_pending=True): claim the previous incarnation's pending
        PEL **before** reading new (">") entries. If ``read_new_for_group`` ran
        first, every entry it delivered would land in this consumer's PEL and
        ``claim_pending`` (min_idle=0) would immediately re-claim it —
        double-delivering every catch-up message. Claiming first means ">" then
        returns only truly-never-delivered entries (the claimed set is already
        this consumer's, which ">" skips), so each message is delivered once.

        Self-healing: if the consumer group has been deleted (e.g. Redis flush),
        ``read_new_for_group`` raises a NOGROUP error. We catch it, recreate the
        group via ``ensure_group``, and retry once — so a transient group loss
        never kills the listen loop.
        """
        if self._redis is None:
            return
        st = inbox_stream(self.namespace, self.agent.agent_id)
        group = inbox_consumer_group(self.namespace, self.agent.agent_id)
        consumer = self.agent.agent_id
        entries: list[tuple[str, str]] = []
        try:
            if reclaim_pending:
                entries += await durability.claim_pending(self._redis, st, group, consumer)
            entries += await durability.read_new_for_group(self._redis, st, group, consumer)
            if not reclaim_pending:
                entries += await durability.claim_stale(self._redis, st, group, consumer)
        except Exception as e:
            err_str = str(e)
            if "NOGROUP" in err_str:
                log.warning("consumer group missing for inbox stream, recreating: %s", e)
                await durability.ensure_group(self._redis, st, group)
                # Retry once after recreating the group
                entries = []
                if reclaim_pending:
                    entries += await durability.claim_pending(self._redis, st, group, consumer)
                entries += await durability.read_new_for_group(self._redis, st, group, consumer)
                if not reclaim_pending:
                    entries += await durability.claim_stale(self._redis, st, group, consumer)
            else:
                raise
        for entry_id, payload in entries:
            msg = self._parse(payload)
            if msg is not None:
                self._stream_ids[msg.message_id] = entry_id
                await self._inbox.put((entry_id, msg))

    async def _drain_room_stream(self, room_id: str) -> None:
        """Read new room messages since the cursor → enqueue + advance the cursor.

        On first join the cursor is ``"$"``: resolve it to the latest concrete stream ID
        (or ``"0-0"`` if the stream is empty) so only messages published AFTER the join
        are delivered — then advance the cursor as messages are read.
        """
        if self._redis is None:
            return
        st = room_stream(self.namespace, room_id)
        ckey = cursor_key(self.namespace, self.agent.agent_id, st)
        cursor = await durability.get_cursor(self._redis, ckey)
        if cursor == "$":
            last = await self._redis.xrevrange(st, count=1)
            cursor = last[0][0] if last else "0-0"
            await durability.set_cursor(self._redis, ckey, cursor)
        entries = await durability.read_room_from_cursor(self._redis, st, cursor)
        for entry_id, payload in entries:
            msg = self._parse(payload)
            if msg is not None:
                await self._inbox.put((None, msg))
        if entries:
            await durability.set_cursor(self._redis, ckey, entries[-1][0])

    def _parse(self, payload: str) -> GossipMessage | None:
        try:
            return GossipMessage.model_validate_json(payload)
        except Exception as e:
            log.warning("dropping undecodable message: %s", e)
            return None

    def _channel_to_stream(self, channel: Any) -> tuple[str | None, str, str | None]:
        """Map a live channel name back to its (stream, mode, room_id)."""
        if isinstance(channel, bytes):
            channel = channel.decode("utf-8", "replace")
        if not isinstance(channel, str):
            return None, "", None
        parts = channel.split(":")
        # neurogossip:{ns}:inbox:{agent} | neurogossip:{ns}:room:{room}
        if len(parts) != 4 or parts[0] != "neurogossip" or parts[1] != self.namespace:
            return None, "", None
        kind, name = parts[2], parts[3]
        if kind == "inbox" and name == self.agent.agent_id:
            return inbox_stream(self.namespace, name), "inbox", None
        if kind == "room":
            return room_stream(self.namespace, name), "room", name
        return None, "", None

    # ------------------------------------------------------------------ reconnect
    async def _watch_connection(self) -> None:
        backoff = self.reconnect_backoff_s
        while not self._stop.is_set():
            await asyncio.sleep(self.heartbeat_interval_s)
            if self._stop.is_set():
                break
            try:
                assert self._redis is not None
                await self._redis.ping()
                backoff = self.reconnect_backoff_s
                self._online.set()
            except Exception:
                self._online.clear()
                log.warning("Redis connection lost; reconnecting in %.1fs", backoff)
                try:
                    await self._reconnect(backoff)
                except Exception as e:
                    log.warning("reconnect failed: %s", e)
                backoff = min(backoff * 2, self.reconnect_max_backoff_s)

    async def _reconnect(self, backoff: float) -> None:
        await asyncio.sleep(backoff)
        # Tear down the old pubsub + tasks, then re-connect (re-subscribe + catch up).
        for t in (self._listen_task, self._heartbeat_task):
            if t is not None and not t.done():
                t.cancel()
        for t in (self._listen_task, self._heartbeat_task):
            if t is not None:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
        if self._pubsub is not None:
            try:
                try:
                    await self._pubsub.aclose()
                except AttributeError:
                    await (self._pubsub.aclose() if hasattr(self._pubsub, 'aclose') else self._pubsub.close())
            except Exception:
                pass
        if self._redis is not None and self._injected_redis is None:
            try:
                await self._redis.aclose()
            except Exception:
                pass
        self._listen_task = self._heartbeat_task = None
        await self._connect()

    # ------------------------------------------------------------------ presence
    async def _heartbeat_loop(self) -> None:
        try:
            while not self._stop.is_set():
                if self._redis is not None:
                    try:
                        await presence.heartbeat_once(
                            self._redis, self.namespace, self.agent, self.presence_ttl_s
                        )
                    except Exception as e:
                        log.debug("heartbeat failed: %s", e)
                await asyncio.sleep(self.heartbeat_interval_s)
        except asyncio.CancelledError:
            raise

    async def list_online_agents(self, *, include_self: bool = False) -> list[AgentPresence]:
        """Return the other agents currently present (excludes self by default)."""
        self._require_connected()
        assert self._redis is not None
        return await presence.list_online_agents(
            self._redis, self.namespace, self.agent.agent_id, include_self=include_self
        )

    # ------------------------------------------------------------------ helpers
    @property
    def is_online(self) -> bool:
        return self._online.is_set()

    def _require_connected(self) -> None:
        if self._redis is None or self._pubsub is None:
            raise NotConnectedError("Client is not connected (use 'async with client:')")
