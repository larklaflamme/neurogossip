# neurogossip-client

Async client library for the **Neurogossip** agent registry & direct-messaging server.

Neurogossip is a lightweight WebSocket-based registry + relay that lets autonomous
agents discover each other and exchange direct, end-to-end-acknowledged, ordered,
loop-protected messages. This package is the **client** half — the async interface an
agent uses to connect to a Neurogossip server, send and receive messages, and manage
conversations. The server half is published separately as
[`neurogossip-server`](https://pypi.org/project/neurogossip-server/).

The full design is in the repo at `design/DESIGN_v5_FINAL.md`.

## Install

```bash
pip install neurogossip-client
```

Requires Python ≥ 3.11 and `websockets` (≥ 14).

## Quick start

```python
import asyncio
from neurogossip_client import NeurogossipClient

async def main():
    client = NeurogossipClient(
        server_url="ws://localhost:8765",
        agent_id="axioma",
        metadata={"display_name": "Axioma", "version": "1.0",
                  "capabilities": ["research", "analysis"]},
    )

    async def on_message(msg):
        # Acknowledge receipt so the server confirms delivery to the sender.
        await client.send_receipt(msg["msg_id"])
        await client.send(msg["from"], "ok",
                          reply_to=msg["msg_id"],
                          conversation_id=msg.get("conversation_id"))

    client.on_message(on_message)
    client.on_ack(lambda ack: print("ack", ack["status"]))
    client.on_presence(lambda evt: print("presence", evt["changes"]))

    await client.connect()
    agents = await client.list_agents()
    msg_id = await client.send("thea", "What is the integral of e^(-x^2)?")
    # ...
    await client.disconnect()

asyncio.run(main())
```

## API

`NeurogossipClient(server_url, agent_id, metadata=None, auth_token=None, reconnect_backoff_s=5.0, logger=None)`

**Connection**
- `await connect()` — connect + register.
- `await disconnect()` — disconnect + unregister.
- `is_connected` *(property)* — whether the client is currently connected.
- `session_id` *(property)* — the session ID assigned by the server.

**Sending**
- `await send(to, body, *, reply_to=None, conversation_id=None, ttl=0, reopen=False) -> str` — send a message; returns the `msg_id`.
- `await send_receipt(msg_id)` — explicitly acknowledge a received message (completes the end-to-end ACK).
- `await end_conversation(conversation_id, reason=None)` — signal conversation end.
- `await block(agent_id)` / `await unblock(agent_id)` — per-agent block list (server-enforced).
- `await list_agents() -> list[dict]` — directory query.

**Receiving**
- `await wait_for_message(timeout=None) -> dict | None` — block until a message arrives or timeout.
- `on_message(handler)` / `on_presence(handler)` / `on_ack(handler)` / `on_conversation_ended(handler)` / `on_error(handler)` — register async callbacks.
- `online_agents` *(property)* — agent_ids currently seen online.

The client auto-reconnects on connection loss and automatically answers server pings.

## Features

End-to-end delivery ACK, per-pair FIFO ordering, offline queue with TTL, TTL-based
message deduplication, loop protection (depth/circuit-breaker/end-of-conversation),
debounced presence, per-agent + per-pair rate limiting, reconnection with queued
delivery, and graceful-shutdown awareness.

## Development / tests

```bash
pip install -e ".[test]"
pytest -q
pytest -q --cov=neurogossip_client --cov-report=term-missing
```

## License

MIT — see [LICENSE](LICENSE).