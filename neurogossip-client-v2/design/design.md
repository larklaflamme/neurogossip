# neurogossip-client v2 — Design

A practical design and implementation plan for **neurogossip-client v2**, a Redis-based
client library that multi-turn AI agents use to exchange markdown messages reliably.

> **Why v2.** v1 used a custom WebSocket relay server. That server was the single point
> of failure: its asyncio event loop stalled under load, so handshakes timed out and
> keepalives dropped every agent at once ("multiple disconnects", "replies not
> delivered"). v2 removes the custom server from the critical path entirely and puts a
> battle-tested broker (Redis) in its place. Agents talk to Redis directly; there is no
> neurogossip server process to stall.

## Transport decision: durable-by-default (Redis Streams + Pub/Sub), not pure Pub/Sub

Pure Redis Pub/Sub is **at-most-once**: if a subscriber is disconnected at the moment of
publish, the message is gone forever — no queue, no replay. That is exactly the failure
mode that killed v1 (an agent restarts for a few seconds → every reply published in that
window is lost). Pure pub/sub would reproduce the lost-reply problem at the broker layer.

So v2 is **durable-by-default**:

- Every publish writes the message to a **Redis Stream** (durable, replayable) **and**
  `PUBLISH`es it to the matching channel (low-latency live fan-out).
- Each client keeps a per-channel read cursor (last Stream ID), persisted in Redis. On
  (re)connect it replays the Stream from its cursor, then consumes live from Pub/Sub. A
  briefly-disconnected agent catches up on everything it missed.
- Direct (1:1) messages use a **consumer group** per recipient so delivery is
  **acknowledged** (`XACK` after the agent processes the message) — the sender can know
  a reply landed, and unacked messages are reclaimed on crash.

Pub/Sub remains the live notification path; Streams are the durability/replay/ack path.
A pure `LIVE` mode (pub/sub only, no Stream) is available as an opt-in for ephemeral
chatter, but it is **not** the default and agents that need replies to land should not
use it.

Future backends (Postgres/SQLite for queryable history, vector DB for semantic recall)
are out of scope for v2 and layered on top of the Stream log later.

---

## 1. Goal

Build a Python client library, **neurogossip-client v2**, that each AI agent embeds to
hold reliable, multi-turn markdown conversations with other agents through Redis.

Core properties:

| Concern | Choice |
|---|---|
| Transport | Redis Streams (durable) + Redis Pub/Sub (live fan-out) |
| Payload | Markdown text, max 5 KB |
| Envelope | JSON, `neurogossip.message.v1` |
| Patterns | Room (broadcast) and Direct (1:1, acknowledged) |
| Identity/ presence | Redis keys with TTL |
| Runtime | asyncio-first (`redis.asyncio`) |
| Deployment | Reuses the existing Redis Docker instance — no new infra |

---

## 2. Messaging model

Two addressing modes, both durable:

**Room** — broadcast to every member of a room.
- Live channel: `neurogossip:{namespace}:room:{room_id}`
- Durable stream: `neurogossip:{namespace}:stream:room:{room_id}`
- Each member reads the stream by cursor (every member sees every message).

**Direct** — 1:1 to a specific agent (replaces v1's direct messaging + offline queue).
- Live channel: `neurogossip:{namespace}:inbox:{recipient_agent_id}`
- Durable stream: `neurogossip:{namespace}:stream:inbox:{recipient_agent_id}`
- The recipient owns a consumer group on its inbox stream → `XACK` on process.

Every agent, on connect, automatically subscribes to **its own inbox** (so it can receive
direct messages) and may `join_room(...)` to subscribe to rooms. An agent can list the
other agents currently present (§11) and choose which to address next — either directly
(`send_direct`) or by publishing to a room.

---

## 3. Message envelope

```json
{
  "schema": "neurogossip.message.v1",
  "message_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "room_id": "rh-proof-discussion",
  "thread_id": "a1b2c3d4-...",
  "reply_to": "d4c3b2a1-...",
  "sender": {
    "agent_id": "skye",
    "agent_name": "SKYE",
    "role": "research_lead"
  },
  "recipient": {
    "mode": "room",
    "agent_ids": []
  },
  "content": {
    "type": "text/markdown",
    "body": "I think the proof obligation fails at the uniform lower bound assumption."
  },
  "metadata": {
    "trace_id": "run_2026_06_23_001",
    "blueprint_id": "ift_rh_review",
    "tags": ["math", "lean", "critique"]
  },
  "created_at": "2026-06-23T20:00:00Z"
}
```

### Identity, chain, and threading

- **`message_id`** — a **UUID** (`uuid.uuid4`), unique per message.
- **`reply_to`** — the `message_id` this message is **in response to** (the immediate
  parent in the chain). `null` for an original (first) message. Walking `reply_to` → … →
  `null` reconstructs the full path from a response back to the original.
- **`thread_id`** — the `message_id` of the **original** message in the chain (the
  root). The first message sets `thread_id = <its own message_id>`. Every response
  propagates the root's `thread_id`. This lets you fetch **all messages in a chain in one
  query** (by `thread_id`), while `reply_to` gives the exact parent each step.

So a chain looks like:

```
M1 (original):  message_id=M1, reply_to=null,   thread_id=M1
M2 (reply):     message_id=M2, reply_to=M1,     thread_id=M1
M3 (reply):     message_id=M3, reply_to=M2,     thread_id=M1
M4 (reply):     message_id=M4, reply_to=M1,     thread_id=M1   # branches are fine
```

Ordering for replay comes from the **Redis Stream ID** (monotonic, time-ordered),
independent of client clocks.

### Size

Serialized envelope ≤ **5 KB**, enforced before publish. Markdown body alone should stay
well under that to leave room for the envelope.

---

## 4. Package API

The Redis URL is read from the **`REDIS_URL`** environment variable. You usually don't
pass it explicitly:

```python
import asyncio
from neurogossip_client import NeuroGossipClient, AgentIdentity

async def main():
    # redis_url defaults to os.environ["REDIS_URL"]; if REDIS_URL is unset and no
    # redis_url= is passed, construction raises RedisUrlNotConfiguredError.
    client = NeuroGossipClient(
        namespace="ravennest",
        agent=AgentIdentity(agent_id="skye", agent_name="SKYE", role="research_lead"),
    )
    async with client:                       # connects, subscribes to own inbox, starts presence
        await client.join_room("rh-proof-discussion")

        # Who can I talk to right now? (other present agents, not myself)
        peers = await client.list_online_agents()        # -> [AgentPresence(agent_id="axioma", ...), ...]

        # Address a specific agent directly (durable + acknowledged):
        await client.send_direct(
            to_agent_id="axioma",
            markdown="Axioma, please test whether the eta lower-bound assumption is empirically stable.",
            thread_id="rh-proof",
            tags=["request", "eta"],
        )
        # Or broadcast to a room:
        await client.publish(
            room_id="rh-proof-discussion",
            markdown="Heads-up everyone: re-running the eta checks.",
        )

        # One stream of everything this agent receives (rooms + direct), catch-up first:
        async for msg in client.listen():
            if msg.sender.agent_id == client.agent.agent_id:
                continue
            print(f"[{msg.sender.agent_id}] ({msg.recipient.mode}) {msg.content.body}")
            # reply, continuing the chain:
            await client.send_direct(
                to_agent_id=msg.sender.agent_id,
                markdown="On it.",
                reply_to=msg.message_id,      # in response to this message
                thread_id=msg.thread_id,      # same chain root
            )

asyncio.run(main())
```

---

## 5. Package layout

```
neurogossip-client-v2/
  pyproject.toml
  README.md
  design/design.md
  src/neurogossip_client/
    __init__.py
    client.py          # NeuroGossipClient: connect, publish, send_direct, listen, presence, reconnect
    models.py          # AgentIdentity, AgentPresence, Recipient, GossipMessage, ...
    channels.py        # channel + stream name helpers + validation
    codecs.py          # JSON encode/decode (or rely on pydantic)
    validators.py      # 5 KB payload check
    errors.py
    presence.py        # heartbeat + list_online_agents
    durability.py      # Stream XADD/XREAD/XREADGROUP/XACK + cursor persistence + catch-up
    logging.py
    cli.py             # `neurogossip send|listen|peers|ping`
  tests/
    test_models.py
    test_chain_tracking.py
    test_payload_size.py
    test_channel_names.py
    test_pubsub_integration.py
    test_durability_replay.py
    test_presence.py
  docker/docker-compose.redis.yml
  examples/
    agent_skye.py
    agent_axioma.py
    multi_agent_chat.py
```

---

## 6. Data models (Pydantic v2)

```python
# src/neurogossip_client/models.py
from __future__ import annotations
import uuid
from datetime import datetime, timezone
from typing import Literal
from pydantic import BaseModel, Field

class AgentIdentity(BaseModel):
    agent_id: str
    agent_name: str | None = None
    role: str | None = None

class AgentPresence(BaseModel):
    agent_id: str
    agent_name: str | None = None
    role: str | None = None
    status: Literal["online", "away", "offline"] = "online"
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class Recipient(BaseModel):
    mode: Literal["room", "direct"] = "room"   # "group" is a later addition
    agent_ids: list[str] = Field(default_factory=list)  # [recipient] for direct

class MessageContent(BaseModel):
    type: Literal["text/markdown"] = "text/markdown"
    body: str

class MessageMetadata(BaseModel):
    trace_id: str | None = None
    blueprint_id: str | None = None
    tags: list[str] = Field(default_factory=list)

class GossipMessage(BaseModel):
    schema: Literal["neurogossip.message.v1"] = "neurogossip.message.v1"
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    room_id: str | None = None              # set for room messages; None for direct
    thread_id: str | None = None            # root message_id of the chain
    reply_to: str | None = None             # parent message_id this is in response to
    sender: AgentIdentity
    recipient: Recipient = Field(default_factory=Recipient)
    content: MessageContent
    metadata: MessageMetadata = Field(default_factory=MessageMetadata)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
```

**Chain construction helpers** (on the client):

- `publish(...)` / `send_direct(...)` with no `reply_to` → an original message; the
  client sets `thread_id = message_id` if `thread_id` is not supplied.
- with `reply_to` set → a response; the client requires `thread_id` (or looks up the
  parent's `thread_id` from the Stream/record) so the chain root is propagated.
- `get_thread(thread_id)` → all messages in the chain (read from the relevant Stream(s)
  filtered by `thread_id`), and `walk_chain(message_id)` → the parent path via
  `reply_to`.

---

## 7. Client interface (durable + reconnect)

```python
# src/neurogossip_client/client.py  (sketch — real impl in client.py + durability.py)
from __future__ import annotations
import asyncio, json, os, uuid
from collections.abc import AsyncIterator
from typing import Literal
import redis.asyncio as aioredis
from .models import AgentIdentity, AgentPresence, GossipMessage, MessageContent, MessageMetadata, Recipient
from .channels import room_channel, room_stream, inbox_channel, inbox_stream, cursor_key, presence_key
from .validators import validate_payload_size
from .errors import NeuroGossipError, PayloadTooLargeError, MessageDecodeError, RedisUrlNotConfiguredError

DeliveryMode = Literal["durable", "live"]   # default "durable"

class NeuroGossipClient:
    def __init__(
        self,
        agent: AgentIdentity,
        namespace: str = "default",
        redis_url: str | None = None,       # defaults to the REDIS_URL env var
        max_payload_bytes: int = 5 * 1024,
        delivery_mode: DeliveryMode = "durable",
        ignore_self: bool = True,
        heartbeat_interval_s: float = 10.0,
        presence_ttl_s: float = 30.0,
        reconnect_backoff_s: float = 1.0,
        reconnect_max_backoff_s: float = 30.0,
    ):
        # Resolve the Redis URL: explicit arg wins, else the REDIS_URL env var.
        # Raise immediately (at construction, before any I/O) if neither is set.
        url = redis_url if redis_url is not None else os.environ.get("REDIS_URL")
        if not url:
            raise RedisUrlNotConfiguredError(
                "REDIS_URL is not configured. Define the REDIS_URL environment variable "
                "(e.g. REDIS_URL=redis://localhost:6379/0) or pass redis_url=... to "
                "NeuroGossipClient."
            )
        self.redis_url = url
        self.agent = agent
        self.namespace = namespace
        self.max_payload_bytes = max_payload_bytes
        self.delivery_mode = delivery_mode
        self.ignore_self = ignore_self
        self.heartbeat_interval_s = heartbeat_interval_s
        self.presence_ttl_s = presence_ttl_s
        self._redis: aioredis.Redis | None = None
        self._pubsub: aioredis.client.PubSub | None = None
        self._inbox: asyncio.Queue[GossipMessage] = asyncio.Queue()
        self._joined_rooms: set[str] = set()
        self._listen_task: asyncio.Task | None = None
        self._reconnect_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._online = asyncio.Event()

    # -- lifecycle ------------------------------------------------------
    async def __aenter__(self) -> "NeuroGossipClient":
        await self._connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._stop.set()
        for t in (self._listen_task, self._reconnect_task, self._heartbeat_task):
            if t and not t.done():
                t.cancel()
        if self._pubsub is not None:
            await self._pubsub.close()
        if self._redis is not None:
            await self._redis.aclose()

    async def _connect(self) -> None:
        """Open Redis, subscribe to own inbox, start presence, catch up, go live."""
        self._redis = aioredis.from_url(self.redis_url, encoding="utf-8", decode_responses=True)
        await self._redis.ping()
        self._pubsub = self._redis.pubsub()
        await self._pubsub.subscribe(inbox_channel(self.namespace, self.agent.agent_id))
        # Catch up on missed direct messages (consumer group) + each joined room (cursor).
        await self._catch_up_inbox()
        for room_id in list(self._joined_rooms):
            await self._catch_up_room(room_id)
            await self._pubsub.subscribe(room_channel(self.namespace, room_id))
        self._listen_task = asyncio.create_task(self._listen_loop())
        self._reconnect_task = asyncio.create_task(self._watch_connection())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._online.set()

    # -- publishing -----------------------------------------------------
    async def publish(self, room_id: str, markdown: str, *, thread_id: str | None = None,
                      reply_to: str | None = None, tags: list[str] | None = None) -> GossipMessage:
        msg = self._build_message(room_id=room_id, recipient=Recipient(mode="room"),
                                  markdown=markdown, thread_id=thread_id, reply_to=reply_to, tags=tags)
        await self._emit(room_channel(self.namespace, room_id),
                         room_stream(self.namespace, room_id), msg)
        return msg

    async def send_direct(self, to_agent_id: str, markdown: str, *, thread_id: str | None = None,
                          reply_to: str | None = None, tags: list[str] | None = None) -> GossipMessage:
        msg = self._build_message(room_id=None, recipient=Recipient(mode="direct", agent_ids=[to_agent_id]),
                                  markdown=markdown, thread_id=thread_id, reply_to=reply_to, tags=tags)
        await self._emit(inbox_channel(self.namespace, to_agent_id),
                         inbox_stream(self.namespace, to_agent_id), msg)
        return msg

    async def _emit(self, channel: str, stream: str, msg: GossipMessage) -> None:
        payload = msg.model_dump_json()
        validate_payload_size(payload, self.max_payload_bytes)
        if self.delivery_mode == "durable":
            await self._redis.xadd(stream, {"payload": payload}, id="*", maxlen=10000, approximate=True)
        await self._redis.publish(channel, payload)

    def _build_message(self, *, room_id, recipient, markdown, thread_id, reply_to, tags) -> GossipMessage:
        msg_id = str(uuid.uuid4())
        # Chain root propagation: an original sets thread_id = its own id; a response
        # keeps the supplied thread_id (the root's message_id).
        if thread_id is None:
            thread_id = msg_id if reply_to is None else None
        return GossipMessage(
            message_id=msg_id, room_id=room_id, thread_id=thread_id, reply_to=reply_to,
            sender=self.agent, recipient=recipient,
            content=MessageContent(body=markdown),
            metadata=MessageMetadata(tags=tags or []),
        )

    # -- rooms ----------------------------------------------------------
    async def join_room(self, room_id: str) -> None:
        self._joined_rooms.add(room_id)
        if self._pubsub is not None:
            await self._catch_up_room(room_id)              # replay missed room messages
            await self._pubsub.subscribe(room_channel(self.namespace, room_id))

    async def leave_room(self, room_id: str) -> None:
        self._joined_rooms.discard(room_id)
        if self._pubsub is not None:
            await self._pubsub.unsubscribe(room_channel(self.namespace, room_id))

    # -- receiving: single dispatch loop --------------------------------
    async def listen(self) -> AsyncIterator[GossipMessage]:
        """Yield every incoming message (direct inbox + all joined rooms).
        Catch-up (missed while disconnected) is delivered first, then live."""
        while not self._stop.is_set():
            msg = await self._inbox.get()
            if self.ignore_self and msg.sender.agent_id == self.agent.agent_id:
                continue
            yield msg
            # For direct messages, ack after the app has seen it (see durability.py).
            if msg.recipient.mode == "direct":
                await self._ack_direct(msg)

    async def _listen_loop(self) -> None:
        """One loop reading the shared PubSub object -> parse -> enqueue."""
        try:
            async for raw in self._pubsub.listen():
                if raw.get("type") != "message" or not raw.get("data"):
                    continue
                try:
                    msg = GossipMessage.model_validate_json(raw["data"])
                except Exception:
                    continue
                await self._inbox.put(msg)
        except asyncio.CancelledError:
            raise

    # -- durability: catch-up + ack (see durability.py) -----------------
    async def _catch_up_inbox(self) -> None: ...     # XREADGROUP from this agent's group, push to _inbox
    async def _catch_up_room(self, room_id: str) -> None: ...  # XREAD from cursor, push to _inbox
    async def _ack_direct(self, msg: GossipMessage) -> None: ...  # XACK on the inbox stream

    # -- reconnect ------------------------------------------------------
    async def _watch_connection(self) -> None:
        """On Redis connection loss: back off, reconnect, re-subscribe (inbox + rooms),
        and catch up on missed messages. Never silently die."""
        backoff = self.reconnect_backoff_s
        while not self._stop.is_set():
            try:
                await self._redis.ping()
            except Exception:
                self._online.clear()
                await self._reconnect(backoff)
                backoff = min(backoff * 2, self.reconnect_max_backoff_s)
            else:
                backoff = self.reconnect_backoff_s
                self._online.set()
            await asyncio.sleep(self.heartbeat_interval_s)

    async def _reconnect(self, backoff: float) -> None:
        await asyncio.sleep(backoff)
        try:
            if self._pubsub is not None:
                await self._pubsub.close()
        except Exception:
            pass
        await self._connect()      # re-open, re-subscribe to inbox + joined rooms, catch up

    # -- presence (see presence.py) -------------------------------------
    async def _heartbeat_loop(self) -> None: ...
    async def list_online_agents(self, *, include_self: bool = False) -> list[AgentPresence]: ...
```

Key points:

- **One** `PubSub` object, **one** `_listen_loop` task, dispatching into a single
  `asyncio.Queue`. `listen()` is the single async stream the agent consumes — supports
  multi-room + direct correctly (v1's per-room `listen(room_id)` could not run
  concurrently on a shared PubSub).
- `ignore_self` suppresses the agent's own echoed publishes (pub/sub delivers them back).
- **Reconnect** re-subscribes to the inbox and all joined rooms and runs catch-up, so a
  Redis blip or agent restart does not lose messages.
- **Direct delivery is acknowledged** (`XACK`); room delivery is broadcast-by-cursor
  (every member sees every message; ack optional).

---

## 8. Channel & stream naming

```python
# src/neurogossip_client/channels.py
import re
_SAFE = re.compile(r"^[a-zA-Z0-9_.-]+$")   # no ':' inside namespace/room/agent ids

def validate_name(value: str, label: str) -> str:
    if not value:
        raise ValueError(f"{label} cannot be empty")
    if not _SAFE.match(value):
        raise ValueError(f"{label} invalid; allowed: letters, numbers, _ . -")
    return value

def room_channel(ns: str, room_id: str) -> str:
    return f"neurogossip:{validate_name(ns,'namespace')}:room:{validate_name(room_id,'room_id')}"
def room_stream(ns: str, room_id: str) -> str:
    return f"neurogossip:{validate_name(ns,'namespace')}:stream:room:{validate_name(room_id,'room_id')}"
def inbox_channel(ns: str, agent_id: str) -> str:
    return f"neurogossip:{validate_name(ns,'namespace')}:inbox:{validate_name(agent_id,'agent_id')}"
def inbox_stream(ns: str, agent_id: str) -> str:
    return f"neurogossip:{validate_name(ns,'namespace')}:stream:inbox:{validate_name(agent_id,'agent_id')}"
def cursor_key(ns: str, agent_id: str, stream: str) -> str:
    return f"neurogossip:{validate_name(ns,'namespace')}:cursor:{validate_name(agent_id,'agent_id')}:{stream}"
def presence_key(ns: str, agent_id: str) -> str:
    return f"neurogossip:{validate_name(ns,'namespace')}:presence:{validate_name(agent_id,'agent_id')}"
```

`:` is reserved as the namespace separator, so namespace/room/agent ids disallow `:`
(unlike v1's draft, which allowed it and made channels ambiguous).

---

## 9. Durability, catch-up, and acknowledgement (`durability.py`)

- **Room streams (broadcast + replay).** Each agent tracks its last-read Stream ID per
  room in `cursor_key`. On `join_room` / reconnect: `XREAD` from the cursor (count
  capped, e.g. 500 at a time) and push each parsed message to `self._inbox` **before**
  subscribing to the live channel — so catch-up is delivered first, in order, then live.
  After reading, `SET cursor_key <last_id>`. No consumer group needed for rooms (every
  member gets every message).

- **Inbox streams (direct + acknowledged).** Each agent owns a consumer group
  `neurogossip:{namespace}:cg:inbox:{agent_id}` on its inbox stream. On connect /
  reconnect: `XREADGROUP group <cg> <agent_id> ">"` to read new, and reclaim stale
  unacked entries (`XPENDING`/`XCLAIM` older than N seconds) in case of a prior crash.
  The client pushes each to `self._inbox`; `listen()` calls `XACK` after the app sees
  the message. The sender can query `XPENDING`/`XINFO` to confirm a direct message was
  acked → "delivered and processed" (the v1 `delivered` ack, broker-side).

- **Stream trimming.** `XADD ... MAXLEN ~ 10000` per stream keeps memory bounded while
  preserving a long recent window for catch-up; long-term history moves to a recorder /
  Postgres later (§13).

- **`LIVE` mode.** `delivery_mode="live"` skips the `XADD` (pub/sub only, at-most-once)
  for ephemeral chatter. Documented as **not reliable**; not the default.

---

## 10. Payload validation & errors

```python
# src/neurogossip_client/validators.py
from .errors import PayloadTooLargeError
def validate_payload_size(payload: str, max_bytes: int) -> None:
    size = len(payload.encode("utf-8"))
    if size > max_bytes:
        raise PayloadTooLargeError(f"Message payload is {size} bytes; max is {max_bytes}")
```

```python
# src/neurogossip_client/errors.py
class NeuroGossipError(Exception): pass
class PayloadTooLargeError(NeuroGossipError): pass
class ConnectionError(NeuroGossipError): pass
class MessageDecodeError(NeuroGossipError): pass
class NotConnectedError(NeuroGossipError): pass
class RedisUrlNotConfiguredError(NeuroGossipError):
    """Raised at construction when no Redis URL is available — neither an explicit
    redis_url= argument nor a REDIS_URL environment variable was provided."""
    pass
```

---

## 11. Presence — listing other agents (`presence.py`)

So each agent can choose who to address next. Lightweight Redis keys with TTL; no DB.

- Each agent's heartbeat writes `presence_key` → `AgentPresence` JSON, TTL `presence_ttl_s`
  (default 30s), every `heartbeat_interval_s` (default 10s).
- `list_online_agents(include_self=False)` scans `neurogossip:{ns}:presence:*` (`SCAN`),
  parses each value, drops expired/stale (TTL handles expiry; also filter `updated_at`
  within ~2× TTL to be safe), and returns the **other** present agents.

```python
async def _heartbeat_loop(self):
    try:
        while not self._stop.is_set():
            p = AgentPresence(agent_id=self.agent.agent_id, agent_name=self.agent.agent_name,
                              role=self.agent.role, status="online")
            await self._redis.set(presence_key(self.namespace, self.agent.agent_id),
                                  p.model_dump_json(), ex=int(self.presence_ttl_s))
            await asyncio.sleep(self.heartbeat_interval_s)
    except asyncio.CancelledError:
        raise

async def list_online_agents(self, *, include_self: bool = False) -> list[AgentPresence]:
    pattern = f"neurogossip:{self.namespace}:presence:*"
    out: list[AgentPresence] = []
    async for key in self._redis.scan_iter(match=pattern, count=100):
        raw = await self._redis.get(key)
        if not raw:
            continue
        try:
            p = AgentPresence.model_validate_json(raw)
        except Exception:
            continue
        if not include_self and p.agent_id == self.agent.agent_id:
            continue
        out.append(p)
    return out
```

`start_heartbeat()` / `stop_heartbeat()` are folded into connect/disconnect (the
heartbeat task starts in `__aenter__`, cancels in `__aexit__`).

---

## 12. Conversation state & history

Redis Streams are the durable event log, not the long-term queryable memory.

- **MVP/v2**: Stream log + client catch-up + `get_thread(thread_id)` / `walk_chain(message_id)` (read from Streams, filter by `thread_id` / walk `reply_to`).
- **Later**: a recorder subscribes to all `neurogossip:*` channels and writes to
  Postgres/SQLite for indexed history; a vector DB for semantic recall.

| Layer | Role |
|---|---|
| Pub/Sub | live fan-out notification |
| Redis Streams | durable, replayable, acknowledged delivery log |
| Postgres/SQLite | queryable conversation memory (later) |
| Vector DB | semantic retrieval over prior discussions (later) |

---

## 13. Backend evolution (unchanged shape, durable first)

- **v2 (now)**: Streams + Pub/Sub, durable-by-default, reconnect + catch-up + direct ack.
- **Next**: recorder service → Postgres/SQLite history.
- **Later**: cross-namespace routing, multi-region Redis, per-room ACLs.

---

## 14. Delivery modes

| Mode | Behavior | Use |
|---|---|---|
| `durable` (default) | XADD + PUBLISH; catch-up on reconnect; direct acked | real conversations / replies |
| `live` | PUBLISH only (at-most-once) | ephemeral chatter only |

The interface is shaped so `group` mode and additional backends can be added later
without changing `publish`/`send_direct`/`listen`.

---

## 15. CLI

`--redis` defaults to the `REDIS_URL` environment variable; if neither is set the command
exits with the same "REDIS_URL is not configured" error as the library.

```
neurogossip send    [--redis URL] --namespace ... --agent skye --room ift-rh --message "..."
neurogossip send    [--redis URL] --namespace ... --agent skye --to axioma --message "..."   # direct
neurogossip listen  [--redis URL] --namespace ... --agent axioma
neurogossip peers   [--redis URL] --namespace ... --agent skye        # list online agents
neurogossip ping    [--redis URL]
```

Built with Typer; deps below.

---

## 16. pyproject.toml

```toml
[project]
name = "neurogossip-client"
version = "0.1.0"
description = "Redis Streams + Pub/Sub client for reliable multi-turn AI-agent discussions"
requires-python = ">=3.11"
dependencies = [
  "redis>=5.0.0",
  "pydantic>=2.0.0",
  "typer>=0.12.0",
  "rich>=13.0.0",
]
[project.optional-dependencies]
dev = ["pytest>=8.0.0", "pytest-asyncio>=0.23.0", "fakeredis>=2.20.0", "ruff>=0.5.0", "mypy>=1.10.0"]
[project.scripts]
neurogossip = "neurogossip_client.cli:app"
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

`uuid` is stdlib (no `ulid-py` dependency). `fakeredis` is used for fast unit tests
without a live Redis.

---

## 17. Testing plan

**Unit (fakeredis, no Docker):**
- `REDIS_URL` resolution: construction raises `RedisUrlNotConfiguredError` when neither
  `redis_url=` nor the `REDIS_URL` env var is set (use `monkeypatch.delenv("REDIS_URL")`);
  an explicit `redis_url=` overrides a missing env var; the env var is used when no arg
  is passed.
- message schema validation; UUID `message_id` uniqueness; chain tracking
  (`reply_to`/`thread_id` propagation; `walk_chain`, `get_thread`); 5 KB enforcement;
  channel/stream name validation (reject `:`, empty, bad chars); JSON roundtrip; ignore_self.

**Integration (Docker Redis):**
- Agent A `send_direct` → Agent B receives; B acks; A observes ack.
- Room publish → multiple members receive; catch-up after a member disconnects/reconnects
  (no lost messages).
- `list_online_agents` returns peers, excludes self, expires stale agents.
- Reconnect: kill the Redis link, verify re-subscribe + catch-up + no silent death.
- Subscribe/unsubscribe; payload limit enforced pre-publish; connection cleanup.

```python
@pytest.mark.asyncio
async def test_direct_delivery_and_ack(redis_url):
    skye = NeuroGossipClient(redis_url=redis_url, namespace="t", agent=AgentIdentity(agent_id="skye"))
    axioma = NeuroGossipClient(redis_url=redis_url, namespace="t", agent=AgentIdentity(agent_id="axioma"))
    async with skye, axioma:
        await skye.send_direct("axioma", "hello", thread_id="rh")
        msg = await anext(axioma.listen())
        assert msg.content.body == "hello"
        assert msg.recipient.mode == "direct"
        assert msg.thread_id is not None
        # ack recorded for skye to observe
```

---

## 18. Operations

- Redis URL: the client reads **`REDIS_URL`** from the environment (e.g.
  `REDIS_URL=redis://localhost:6379/0` for dev, `redis://redis:6379/0` for compose
  service-to-service, `redis://:<password>@redis-host:6379/0` for prod, TLS if remote).
  An explicit `redis_url=` argument overrides it. If `REDIS_URL` is unset and no
  `redis_url=` is passed, the client raises `RedisUrlNotConfiguredError` at construction.
- Other env vars: `NEUROGOSSIP_NAMESPACE`, `NEUROGOSSIP_AGENT_ID`,
  `NEUROGOSSIP_AGENT_NAME`, `NEUROGOSSIP_AGENT_ROLE`, `NEUROGOSSIP_DELIVERY_MODE=durable`.
- Docker Compose Redis: `redis:8 --appendonly yes` (AOF useful for Streams/presence keys
  even though Pub/Sub itself isn't durable); persistent volume; `restart: unless-stopped`.

---

## 19. Security

Local trusted Docker: unauthenticated Redis acceptable. Production: Redis AUTH, private
network/VPC, TLS if remote, namespace isolation, pre-publish payload + channel-name
validation, no secrets in markdown. Later: message signing, agent identity tokens,
per-room ACLs, encrypted bodies, audit log (Stream is a natural audit trail).

---

## 20. v2 MVP scope (signed off)

In:

- asyncio Redis client, **durable-by-default** (Streams + Pub/Sub).
- UUID `message_id`; `reply_to` (in-response-to) + `thread_id` chain tracking;
  `get_thread` / `walk_chain`.
- `publish` (room), `send_direct` (1:1, acked), `join_room`/`leave_room`, single
  `listen()` stream (rooms + direct), `ignore_self`.
- Auto-reconnect + re-subscribe + Stream catch-up.
- Presence: heartbeat + `list_online_agents` (peers, excludes self).
- 5 KB payload + channel-name validation; Pydantic v2 models; errors.
- CLI `send`/`listen`/`peers`/`ping`; Docker Redis compose; unit + integration tests.

Out (later): `group` mode, Postgres/SQLite recorder, vector memory, cross-namespace
routing, agent auth/tokens, per-room ACLs, multi-region Redis.

---

## 21. Architecture summary

```
AI Agent  ──neurogossip-client v2──▶  Redis
                                        │
                 publish/send_direct:   │
                   XADD  stream ──────▶ │ stream (durable log, replay, ack)
                   PUBLISH channel ───▶ │ pubsub (live fan-out)
                                        │
                 on (re)connect:        │
                   XREAD/XREADGROUP ◀── │ catch up missed messages
                   SUBSCRIBE      ◀── │ live notifications
                                        │
                 presence: SET key ◀──▶ │ presence registry (TTL)
                 peers: SCAN keys  ◀── │
                                        ▼
                              Other AI Agents (same path)
```

The custom WebSocket server is gone. Reliability comes from Redis Streams (durable,
replayable, acknowledged) plus a client that reconnects and catches up — so a restart
or a Redis blip no longer loses replies.