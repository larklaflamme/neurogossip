# Integrating neurogossip-client-v2 into your agent architecture

This document is for engineers embedding **neurogossip-client-v2** (the Redis-based
messaging client) into an AI agent. It tells you exactly what to do, the patterns for
the common agent architectures, and how to test your integration.

> One-line summary: each agent creates a `NeuroGossipClient`, connects it inside an
> `async with`, joins the rooms it cares about, calls `listen()` in a loop to receive
> messages, and calls `send_direct()` / `publish()` to send. The client handles
> durability, reconnection, and presence — **your agent must not run its own WebSocket
> server or reconnect logic.**

---

## 1. What the library gives you

- **Reliable delivery without a custom server.** Agents talk to Redis directly. There is
  no neurogossip server process to run, deploy, or stall.
- **Durable-by-default.** Every send is written to a Redis Stream **and** published live.
  If your agent is disconnected/restarting when a message arrives, it catches up on
  reconnect — replies are not silently lost (the failure that killed v1).
- **Two addressing modes:**
  - `send_direct(to_agent_id, ...)` — 1:1, **acknowledged** (`XACK`). Use this for
    replies and targeted requests.
  - `publish(room_id, ...)` — broadcast to every member of a room.
- **Presence.** `list_online_agents()` returns the *other* agents currently present, so
  your agent can decide who to address next.
- **Chain tracking.** Every message has a UUID `message_id`, a `reply_to` (the parent
  message it responds to) and a `thread_id` (the root of the chain). You can reconstruct
  the full conversation path.
- **Auto-reconnect + catch-up.** A Redis blip or agent restart re-subscribes and replays
  missed messages; you do not write reconnect code.

## 2. Prerequisites

1. **A Redis instance** reachable from the agent. For local dev use the bundled
   `docker/docker-compose.redis.yml` (`docker compose -f docker/docker-compose.redis.yml
   up -d`); in existing infrastructure, point at your Redis.
2. **The `REDIS_URL` environment variable**, e.g. `export REDIS_URL=redis://localhost:6379/0`.
   If it is missing and you don't pass `redis_url=...` to the constructor, the client
   raises `RedisUrlNotConfiguredError` immediately.
3. **Install the library:** `pip install -e .` (editable) or `pip install neurogossip-client`.
4. **Python ≥ 3.11**, asyncio.

Optional env vars: `NEUROGOSSIP_NAMESPACE` (isolation between deployments/projects),
`NEUROGOSSIP_AGENT_ID`, `NEUROGOSSIP_AGENT_NAME`, `NEUROGOSSIP_AGENT_ROLE`.

---

## 3. The integration contract (what your agent MUST do)

These are non-negotiable — violating them is the source of nearly every integration bug.

1. **Use the async context manager** (`async with client:`) to connect and disconnect.
   Never call methods before connecting (raises `NotConnectedError`) or after closing.
2. **Run one `listen()` loop** per agent and handle every message it yields. This single
   loop receives **both** direct messages and room messages. Do not start multiple
   `listen()` loops on one client.
3. **Do not block the listen loop.** `listen()` yields a message; if your handler does a
   long LLM call *inside* the loop body, no further messages are delivered until it
   returns. Offload slow work (see §5).
4. **Set `reply_to` and `thread_id` on responses** so chains are trackable. `reply_to =
   parent_message_id`; `thread_id = root_message_id` (propagated from the message you're
   replying to). For a brand-new conversation, omit both — the library auto-roots
   `thread_id` to the message's own id.
5. **Let the library ack direct messages** (it does so automatically after `listen()`
   yields them). Only call `await client.ack(msg)` explicitly if you want to ack *after*
   your own processing completes (advanced; see §6).
6. **Pick a namespace** per deployment/project and keep it consistent across agents that
   must talk to each other. Agents in different namespaces are isolated.
7. **Keep messages ≤ 5 KB** serialized (enforced pre-send; oversized raises
   `PayloadTooLargeError`).

---

## 4. Minimal integration (pure-asyncio agent)

If your agent is already an asyncio program, this is the whole integration:

```python
import asyncio
from neurogossip_client import NeuroGossipClient, AgentIdentity

async def main():
    client = NeuroGossipClient(
        namespace="myteam",                          # from NEUROGOSSIP_NAMESPACE or literal
        agent=AgentIdentity(agent_id="skye", agent_name="SKYE", role="research_lead"),
        # redis_url defaults to $REDIS_URL; omit it in production
    )
    async with client:                               # connect + subscribe to own inbox + presence
        await client.join_room("planning")           # optional: also hear room broadcasts
        # optional: discover peers before addressing
        peers = await client.list_online_agents()    # -> [AgentPresence(agent_id="axioma", ...), ...]

        async for msg in client.listen():            # ONE loop: direct + room messages
            if msg.sender.agent_id == client.agent.agent_id:
                continue                             # ignore_self handles this, but be defensive
            print(f"[{msg.sender.agent_id}] ({msg.recipient.mode}) {msg.content.body}")

            # Respond, continuing the chain:
            await client.send_direct(
                to_agent_id=msg.sender.agent_id,
                markdown="Got it — working on it.",
                reply_to=msg.message_id,             # in response to this message
                thread_id=msg.thread_id,             # same chain root
            )

asyncio.run(main())
```

**To send without waiting for a message** (e.g. kick off a conversation, or broadcast):

```python
# 1:1, acknowledged:
await client.send_direct("axioma", "please review the draft", thread_id="draft-review")
# broadcast to a room:
await client.publish("planning", "standup in 5 minutes")
```

---

## 5. Architecture patterns

### 5a. Threaded agent — comms in a daemon thread, sync agent logic (the common case)

Many agent frameworks run their turn loop in a normal thread with synchronous calls
(especially when the LLM client is synchronous). The pattern: run the asyncio client in a
dedicated daemon thread with its own event loop, expose a thread-safe `send`/`peers`
handle, and drain inbound messages into a queue the agent thread reads. This mirrors how
the v1 agents wrapped the old client.

```python
import asyncio, threading, queue
from neurogossip_client import NeuroGossipClient, AgentIdentity

class CommsHandle:
    """Thread-safe handle to a NeuroGossipClient running in a background asyncio thread."""
    def __init__(self, agent_id, namespace="myteam"):
        self.agent_id = agent_id
        self.namespace = namespace
        self._loop = None
        self._client = None
        self.inbox: queue.Queue = queue.Queue()        # agent thread reads from here
        self._thread = threading.Thread(target=self._run, name=f"ng-{agent_id}", daemon=True)
        self._ready = threading.Event()
        self._stop = False

    def start(self, timeout=10.0):
        self._thread.start()
        self._ready.wait(timeout=timeout)
        if self._client is None:
            raise RuntimeError("neurogossip client failed to connect")

    def stop(self):
        self._stop = True
        if self._loop:
            asyncio.run_coroutine_threadsafe(self._client.close(), self._loop).result(timeout=5)
        self._thread.join(timeout=5)

    # -- called from the agent (sync) thread --
    def send_direct(self, to, markdown, **kw):
        fut = asyncio.run_coroutine_threadsafe(
            self._client.send_direct(to, markdown, **kw), self._loop)
        return fut.result(timeout=30)

    def publish(self, room, markdown, **kw):
        fut = asyncio.run_coroutine_threadsafe(
            self._client.publish(room, markdown, **kw), self._loop)
        return fut.result(timeout=30)

    def list_online_agents(self):
        fut = asyncio.run_coroutine_threadsafe(self._client.list_online_agents(), self._loop)
        return fut.result(timeout=10)

    # -- background asyncio thread --
    def _run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._client = NeuroGossipClient(
            namespace=self.namespace,
            agent=AgentIdentity(agent_id=self.agent_id, agent_name=self.agent_id.upper()),
        )
        loop.run_until_complete(self._main())

    async def _main(self):
        try:
            async with self._client:
                await self._client.join_room("planning")
                self._ready.set()
                async for msg in self._client.listen():
                    if msg.sender.agent_id == self.agent_id:
                        continue
                    self.inbox.put(msg)               # hand to the agent thread
        except Exception as e:
            self.inbox.put(e)                          # surface connect/listen errors
        finally:
            self._ready.set()                          # unblock start() on failure too

# ---- agent (synchronous) side ----
comms = CommsHandle("skye"); comms.start()
try:
    peers = comms.list_online_agents()
    comms.send_direct("axioma", "hello", thread_id="greet")
    while True:
        msg = comms.inbox.get()                        # blocks until a message arrives
        if isinstance(msg, Exception):
            raise msg
        print(f"from {msg.sender.agent_id}: {msg.content.body}")
        # ... your (possibly slow, synchronous) turn logic ...
        comms.send_direct(msg.sender.agent_id, "reply",
                          reply_to=msg.message_id, thread_id=msg.thread_id)
finally:
    comms.stop()
```

**Why this works:** the listen loop runs in the background thread and never blocks on your
turn logic — it just pushes messages into a `queue.Queue`. Your agent thread can do long
synchronous LLM calls freely; incoming messages buffer in the queue. The client's
auto-reconnect keeps the listen loop alive across Redis blips. Direct messages are acked
by the listen loop automatically; if the agent crashes before processing a queued message,
it's redelivered on next start (at-least-once).

### 5b. Asyncio agent with long LLM turns — offload the turn, keep the loop free

If your agent is asyncio but a turn is a long `await llm.generate(...)`, do **not** await
it inline in the `listen()` body (it would stall delivery of other messages). Dispatch each
message to its own task and let the listen loop continue:

```python
async with client:
    await client.join_room("planning")
    async for msg in client.listen():
        if msg.sender.agent_id == client.agent.agent_id:
            continue
        asyncio.create_task(handle_turn(client, msg))   # don't await — keep listening

async def handle_turn(client, msg):
    reply = await my_llm(msg.content.body)              # long, but in its own task
    await client.send_direct(msg.sender.agent_id, reply,
                             reply_to=msg.message_id, thread_id=msg.thread_id)
```

Track the spawned tasks (e.g. a `set` with a done-callback) so exceptions surface and you
can await them on shutdown.

### 5c. Multi-process agents

Each process creates its own `NeuroGossipClient` with a distinct `agent_id` (or the same
`agent_id` if you want them to share an inbox — but then they compete for the consumer
group, so prefer distinct ids). They communicate through the same Redis + namespace. No
shared state between processes is required — Redis is the shared state.

### 5d. Observer / recorder

A non-replying observer (logger, dashboard, supervisor) just connects, joins rooms, and
reads `listen()` without sending. Set `ignore_self=False` if it has no agent_id of its
own to suppress. Presence is optional (you can disable the heartbeat with a long
`heartbeat_interval_s`, or simply not call `list_online_agents`).

---

## 6. Acknowledgement semantics (read if you care about exactly-once vs at-least-once)

Direct messages are **at-least-once**:

- The client's `listen()` auto-acks a direct message **after yielding it to your loop**.
  If your process crashes *after* receiving but *before* fully processing, the message
  was already acked → it is **not** redelivered (this is "ack-on-deliver").
- For stricter "ack-only-after-I-finished-processing", set `auto_receipt`-style explicit
  acking: the library provides `await client.ack(msg)`. To use it, you would take over
  acking — but note `listen()` already auto-acks, so the common path is fine for most
  agents. (If you need ack-after-process as the default, file an issue; it's a small
  config change.)

Room messages are **broadcast + replay-on-reconnect**: every member sees every message;
a member that was offline catches up on reconnect from its cursor. There is no per-message
ack for rooms (by design — rooms are live broadcast).

---

## 7. Chain tracking

The library populates chain fields for you:

- **Original message** (you send with no `reply_to`): `thread_id` is auto-set to the
  message's own `message_id`; `reply_to` is `None`.
- **Response** (you send with `reply_to=parent_id`): pass `thread_id=root_id` (the
  original's `message_id`). If you omit `thread_id`, it defaults to `reply_to` as a
  best-effort root — but always pass the true root for correct chain grouping.

To reconstruct a chain in your agent: keep a local map `message_id -> message` (or query
the stream later). Walk `reply_to` from any message back to `None` for the path; group by
`thread_id` for the whole thread. (A `get_thread(thread_id)` / `walk_chain(message_id)`
helper is on the roadmap; today you track chains from the fields.)

---

## 8. Configuration reference

| Setting | How | Default |
|---|---|---|
| Redis URL | `REDIS_URL` env var or `redis_url=` arg | **required** (else `RedisUrlNotConfiguredError`) |
| Namespace | `NEUROGOSSIP_NAMESPACE` env or `namespace=` arg | `"default"` |
| Delivery mode | `delivery_mode="durable"` / `"live"` | `"durable"` (use `"live"` only for ephemeral chatter — not reliable) |
| Max payload | `max_payload_bytes=` | 512 MiB (Redis `proto-max-bulk-len` default) |
| Ignore own echoes | `ignore_self=` | `True` |
| Heartbeat interval | `heartbeat_interval_s=` | 10 s |
| Presence TTL | `presence_ttl_s=` | 30 s |
| Reconnect backoff | `reconnect_backoff_s=` / `reconnect_max_backoff_s=` | 1 s → 30 s (exponential) |

Keep `delivery_mode="durable"` for anything where a missed reply matters.

---

## 9. Common pitfalls

- **Forgetting `async with`** → `NotConnectedError` on first send. Always use the context
  manager (or `await client.close()` in a finally).
- **Long work inside `async for msg in client.listen()`** → stalls delivery. Offload
  (§5a/§5b).
- **Not passing `thread_id` on replies** → chains split. Propagate `msg.thread_id`.
- **Multiple `listen()` loops on one client** → undefined; use one.
- **Different namespaces across agents that should talk** → they're isolated; align the
  namespace.
- **`REDIS_URL` unset in the agent's environment** → construction raises. Set it in the
  service env / systemd unit / container env.
- **Sending > 5 KB** → `PayloadTooLargeError`; chunk or summarize.

---

## 10. How to test your integration

### 10.1 Unit tests with fakeredis (no Docker, fast)

Use the `redis_client=` injection seam with a shared `fakeredis.FakeServer` so multiple
clients see the same state. Template:

```python
# tests/test_my_agent.py
import asyncio, fakeredis, pytest
from fakeredis import FakeAsyncRedis
from neurogossip_client import NeuroGossipClient, AgentIdentity

@pytest.fixture
def server(): return fakeredis.FakeServer()

def client(agent_id, server, **kw):
    return NeuroGossipClient(
        agent=AgentIdentity(agent_id=agent_id, agent_name=agent_id.upper()),
        namespace="test",
        redis_client=FakeAsyncRedis(server=server, decode_responses=True),
        heartbeat_interval_s=999, presence_ttl_s=999, **kw)

async def test_my_agent_replies(server):
    a = client("axioma", server); b = client("skye", server)
    async with a, b:
        await a.send_direct("skye", "ping", thread_id="t")
        msg = await asyncio.wait_for(anext(b.listen()), timeout=3)
        assert msg.content.body == "ping"
        # ... call your agent's handler with msg, assert it sends the right reply ...
```

### 10.2 Integration tests with a real Redis (Docker)

Spin up `docker/docker-compose.redis.yml`, then point tests at `REDIS_URL`. Test the
real wire behavior:

```python
import asyncio, os, pytest
from neurogossip_client import NeuroGossipClient, AgentIdentity

REDIS_URL = os.getenv("REDIS_URL_TEST", "redis://localhost:6379/15")  # use a test DB

async def test_two_agents_over_real_redis():
    a = NeuroGossipClient(agent=AgentIdentity(agent_id="a"), namespace="it", redis_url=REDIS_URL)
    b = NeuroGossipClient(agent=AgentIdentity(agent_id="b"), namespace="it", redis_url=REDIS_URL)
    async with a, b:
        await a.send_direct("b", "hello-over-wire", thread_id="w")
        msg = await asyncio.wait_for(anext(b.listen()), timeout=5)
        assert msg.content.body == "hello-over-wire"
        assert msg.recipient.mode == "direct"
```

### 10.3 Reliability tests (the important ones)

These prove your integration survives the v1 failure modes. Add them to your agent's test
suite:

**a) Offline direct delivery** — a message sent while the recipient is down is delivered
on connect:
```python
async def test_offline_direct_delivered_on_connect(server):
    a = client("a", server)
    async with a:
        await a.send_direct("b", "while-offline", thread_id="o")
    # b connects AFTER a sent:
    b = client("b", server)
    async with b:
        msg = await asyncio.wait_for(anext(b.listen()), timeout=3)
        assert msg.content.body == "while-offline"
```

**b) Catch-up after reconnect** — a room member that disconnects, then reconnects, gets the
messages it missed:
```python
async def test_room_catchup(server):
    a = client("a", server); b = client("b", server)
    async with a, b:
        await a.join_room("r"); await b.join_room("r")
        await asyncio.sleep(0.1)
        await a.publish("r", "before")
        await asyncio.wait_for(anext(b.listen()), timeout=3)
    # b is down; a posts again
    async with client("a", server) as a2:
        await a2.join_room("r"); await asyncio.sleep(0.1)
        await a2.publish("r", "while-away")
    # b reconnects and catches up:
    async with client("b", server) as b2:
        await b2.join_room("r")
        msg = await asyncio.wait_for(anext(b2.listen()), timeout=3)
        assert msg.content.body == "while-away"
```

**c) Crash-redelivery** — a direct message received but not acked (process killed
mid-turn) is redelivered on next start:
```python
async def test_unacked_redelivered(server):
    async with client("a", server) as a:
        await a.send_direct("b", "must-redeliver", thread_id="c")
    async with client("b", server) as b:
        msg = await asyncio.wait_for(anext(b.listen()), timeout=3)
        assert msg.content.body == "must-redeliver"
        # deliberately do NOT ack / do NOT process fully, then exit (simulated crash)
    async with client("b", server) as b2:
        msg2 = await asyncio.wait_for(anext(b2.listen()), timeout=3)
        assert msg2.content.body == "must-redeliver"
        await b2.ack(msg2)
```

**d) Presence** — peers are listed and self is excluded:
```python
async def test_peers(server):
    a = client("a", server, heartbeat_interval_s=0.05, presence_ttl_s=10)
    b = client("b", server, heartbeat_interval_s=0.05, presence_ttl_s=10)
    async with a, b:
        await asyncio.sleep(0.2)
        peers = await a.list_online_agents()
        assert {p.agent_id for p in peers} == {"b"}
```

### 10.4 Soak / reliability run

Run your integrated agents against a real Redis for an extended period and assert zero
losses. Use `examples/multi_agent_chat.py` as a template (4 agents, all-to-all + replies)
or wrap your own agents in a loop that sends N messages and asserts all are received.
A 10-minute run with 4 agents exchanging thousands of messages and zero losses is a good
sign-off gate.

### 10.5 Integration checklist

- [ ] `REDIS_URL` set in the agent's runtime environment.
- [ ] Agent connects via `async with NeuroGossipClient(...)`.
- [ ] One `listen()` loop; slow work offloaded (not inline).
- [ ] Replies carry `reply_to` + `thread_id`.
- [ ] Namespace consistent across agents that talk.
- [ ] Offline-delivery test passes (recipient down → catches up on connect).
- [ ] Reconnect/catch-up test passes (room messages missed → redelivered).
- [ ] Crash-redelivery test passes (unacked direct → redelivered).
- [ ] Presence test passes (`list_online_agents` shows peers, excludes self).
- [ ] Soak run: N minutes, 4+ agents, zero lost messages.
- [ ] Graceful shutdown: `close()` on SIGTERM/Ctrl-C (the context manager handles it).

---

## 11. Lifecycle & shutdown

- **Connect:** `async with client:` (or `await client._connect()` + manual `close()`).
  On connect the client subscribes to its own inbox, ensures its consumer group, catches
  up on missed direct messages, starts presence, and starts the reconnect watcher.
- **Reconnect:** automatic. On Redis connection loss the client backs off, re-opens,
  re-subscribes to its inbox + all joined rooms, and re-runs catch-up. You do nothing.
- **Shutdown:** exit the `async with` block (or `await client.close()`). It cancels the
  listen/reconnect/heartbeat tasks, closes the Pub/Sub and Redis connection. For a
  threaded integration (§5a), call `handle.stop()` in your SIGTERM handler.

## 12. Where to look

- **API reference:** `src/neurogossip_client/client.py` (`NeuroGossipClient`).
- **Design & rationale:** `design/design.md`.
- **Examples:** `examples/agent_skye.py`, `examples/agent_axioma.py`,
  `examples/multi_agent_chat.py` (4 agents all-to-all).
- **CLI (smoke testing):** `neurogossip ping`, `neurogossip peers --agent X`,
  `neurogossip send --agent Y --to X --message "hi"`, `neurogossip listen --agent X`.

If your integration hits a failure mode not covered here, check `design/design.md` §9
(durability) and §20 (scope) — and file an issue. The library is deliberately small;
the reliability lives in Redis Streams + the client's reconnect/catch-up, not in code you
have to write.