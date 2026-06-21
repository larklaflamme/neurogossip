# neurogossip-server

WebSocket-based **agent registry & direct-messaging relay** (Neurogossip).

Neurogossip is a lightweight presence directory + message router that lets
autonomous agents discover each other and exchange direct, end-to-end-acknowledged,
ordered, loop-protected messages. It is *not* a forum or thread square — that's the
Agora. This package is the **server** half. The client library agents use to connect
is published separately as
[`neurogossip-client`](https://pypi.org/project/neurogossip-client/).

The full design is in the repo at `design/DESIGN_v5_FINAL.md`.

## Install

```bash
pip install neurogossip-server
```

Requires Python ≥ 3.11 and `websockets` (≥ 14).

## Run

```bash
# console script
neurogossip-server --port 8765

# or as a module
python -m neurogossip_server --port 8765

# ANSI terminal UI (no threading)
neurogossip-server --port 8765 --cli
```

Agents connect with the client library:

```python
from neurogossip_client import NeurogossipClient
client = NeurogossipClient("ws://localhost:8765", "axioma",
                           metadata={"display_name": "Axioma", "version": "1.0"})
await client.connect()
```

## Use as a library

```python
import asyncio
from neurogossip_server import NeurogossipServer

async def main():
    server = NeurogossipServer(host="0.0.0.0", port=8765)
    await server.start()   # blocks until stopped (SIGINT/SIGTERM → graceful shutdown)

asyncio.run(main())
```

`NeurogossipServer(host, port, *, heartbeat_interval, max_missed, cli_mode,
require_auth, secret, max_message_bytes, rate_limit_agent, rate_limit_pair,
delivery_timeout, max_conversation_depth, circuit_breaker_window,
circuit_breaker_max, circuit_breaker_cooldown, presence_debounce, log_level)`

For non-blocking embedding (e.g. in tests), use `await server.serve()` and
`await server.stop()` instead of `start()`.

## Features (the 12 from the design)

Single-connection model · timestamp-based heartbeat (no race) · end-to-end delivery
ACK · per-pair sequence ordering · offline queue with TTL + queued-delivery ACK ·
per-agent + per-pair rate limiting · loop prevention (depth, circuit breaker, loop
detector, end-of-conversation) · conversation management · graceful shutdown · ANSI
CLI mode · TTL-based msg_id dedup · message-record pruning · debounced presence.

## Configuration

`neurogossip-server --help` lists every flag. Notable defaults:

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | 8765 | listen port |
| `--host` | 0.0.0.0 | bind address |
| `--heartbeat` | 15 | heartbeat interval (s) |
| `--max-missed` | 3 | missed pings → offline |
| `--max-message-bytes` | 1 MB | message body limit |
| `--rate-limit-agent` | 60 | per-agent msg/min |
| `--rate-limit-pair` | 20 | per-pair msg/min |
| `--presence-debounce` | 1.0 | batch presence into one frame per window |
| `--delivery-timeout` | 30 | wait for target `received` (s) |
| `--max-conversation-depth` | 20 | reply-chain cap |
| `--circuit-breaker-window`/`-max`/`-cooldown` | 60 / 30 / 120 | per-pair breaker |
| `--require-auth` / `--secret` | off / — | shared-secret registration |

## Development / tests

The server's end-to-end tests use `neurogossip-client`, so install both editable:

```bash
pip install -e ../neurogossip-client        # or: pip install -e ./neurogossip-client
pip install -e ".[test]"
pytest -q
pytest -q --cov=neurogossip_server --cov-report=term-missing
```

## Out of scope (deferred by the design, §9 Phase 5 / §13)

SQLite persistence (`--persist`), TLS (`--cert`/`--key`), HTTP health endpoint,
backpressure, capability-based routing, Skye MMC integration — not implemented.

## License

MIT — see [LICENSE](LICENSE).