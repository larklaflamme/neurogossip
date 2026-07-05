# neurogossip-client v2

A Redis Streams + Pub/Sub client library for reliable, multi-turn AI-agent markdown
conversations. Each AI agent embeds this client to talk to other agents through Redis —
**no custom server**; agents connect to Redis directly.

## Why v2

v1 used a custom WebSocket relay server that was unreliable (event-loop stalls → mass
disconnects → lost replies). v2 removes the custom server and puts a battle-tested
broker (Redis) in the critical path.

## Reliability model

- **Durable-by-default**: every publish does `XADD` (Redis Stream) **and** `PUBLISH`
  (live fan-out). The Stream is the source of truth; Pub/Sub is a low-latency wake-up.
- **Direct (1:1) messages** use a per-recipient consumer group with `XACK` — at-least-once
  delivery with crash recovery (unacked messages are reclaimed and redelivered).
- **Rooms (broadcast)**: every member reads the room stream by cursor; a briefly
  disconnected member catches up on what it missed on (re)connect.
- **Auto-reconnect + catch-up**: a Redis blip or agent restart re-subscribes and replays
  missed messages — replies are not silently lost.

## Quick start

```bash
export REDIS_URL=redis://localhost:6379/0
```

```python
import asyncio
from neurogossip_client import NeuroGossipClient, AgentIdentity

async def main():
    client = NeuroGossipClient(
        namespace="ravennest",
        agent=AgentIdentity(agent_id="skye", agent_name="SKYE", role="research_lead"),
    )
    async with client:
        await client.join_room("rh-proof-discussion")
        peers = await client.list_online_agents()              # other present agents
        await client.send_direct("axioma", "please run the eta checks.",
                                 thread_id="rh", tags=["request"])
        async for msg in client.listen():
            if msg.sender.agent_id == client.agent.agent_id:
                continue
            print(f"[{msg.sender.agent_id}] {msg.content.body}")
            await client.send_direct(msg.sender.agent_id, "on it",
                                     reply_to=msg.message_id, thread_id=msg.thread_id)

asyncio.run(main())
```

## Messages

Each message has a UUID `message_id`, a `reply_to` (the parent message_id it responds
to) and a `thread_id` (the root message_id of the chain), so you can track the full chain
from any response back to the original.

See `design/design.md` for the full design.

## Install

```bash
pip install -e .
pytest            # unit tests use fakeredis; integration tests use Docker Redis
```