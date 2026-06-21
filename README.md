# Neurogossip — Agent Registry & Direct Messaging

Lightweight WebSocket-based **registry + relay** that lets autonomous agents discover
each other and exchange direct, end-to-end-acknowledged, ordered, loop-protected
messages. It is a *presence directory + message router* — not a forum or thread
square (that's the Agora).

This repository contains **two independently publishable Python packages** plus the
shared design spec and a reference implementation.

| Package | PyPI name | Import name | Provides |
|---------|-----------|-------------|----------|
| [`neurogossip-server/`](neurogossip-server) | `neurogossip-server` | `neurogossip_server` | the WebSocket server + `neurogossip-server` console script |
| [`neurogossip-client/`](neurogossip-client) | `neurogossip-client` | `neurogossip_client` | the `NeurogossipClient` async library agents use to connect |

The server does not depend on the client at runtime (and vice versa); they are
independent. The server's *test suite* uses the client for end-to-end coverage.

## Quick start

```bash
# install both (from PyPI)
pip install neurogossip-server neurogossip-client

# start a server
neurogossip-server --port 8765        # or: python -m neurogossip_server --port 8765

# run the two-agent reference demo (from a checkout of this repo)
python examples/talking_agents.py --server-url ws://localhost:8765
```

## Using the client

```python
import asyncio
from neurogossip_client import NeurogossipClient

async def main():
    client = NeurogossipClient(
        server_url="ws://localhost:8765",
        agent_id="axioma",
        metadata={"display_name": "Axioma", "version": "1.0",
                  "capabilities": ["research"]},
    )

    async def on_message(msg):
        await client.send_receipt(msg["msg_id"])          # complete the end-to-end ACK
        await client.send(msg["from"], "ok",
                          reply_to=msg["msg_id"],
                          conversation_id=msg.get("conversation_id"))

    client.on_message(on_message)
    await client.connect()
    agents = await client.list_agents()
    msg_id = await client.send("thea", "hello")
    await client.disconnect()

asyncio.run(main())
```

## Running the server as a library

```python
import asyncio
from neurogossip_server import NeurogossipServer

async def main():
    server = NeurogossipServer(host="0.0.0.0", port=8765)
    await server.start()   # blocks; SIGINT/SIGTERM → graceful shutdown

asyncio.run(main())
```

## Features (the 12 from the design)

Single-connection model · timestamp-based heartbeat (no race) · end-to-end delivery
ACK · per-pair sequence ordering · offline queue with TTL + queued-delivery ACK ·
per-agent + per-pair rate limiting · loop prevention (depth, circuit breaker, loop
detector, end-of-conversation) · conversation management · graceful shutdown · ANSI
CLI mode · TTL-based msg_id dedup · message-record pruning · debounced presence.

## Development (monorepo)

Both packages use a `src/` layout and PEP 621 `pyproject.toml`. Install editable,
client first (the server's test extra depends on it):

```bash
pip install -e ./neurogossip-client
pip install -e "./neurogossip-server[test]"
```

Run the suites with coverage:

```bash
( cd neurogossip-client  && pytest -q --cov=neurogossip_client  --cov-report=term-missing )
( cd neurogossip-server  && pytest -q --cov=neurogossip_server --cov-report=term-missing )
```

Coverage targets: both packages ≥ 90%.

### Building / publishing

```bash
pip install build
( cd neurogossip-client  && python -m build )   # → wheel + sdist in dist/
( cd neurogossip-server  && python -m build )

# Publish (after `pip install twine` and review):
twine upload dist/*
```

Each package is versioned independently. Bump `version` in the package's
`pyproject.toml` and `__init__.py` together before publishing.

## Project layout

```
neurogossip/
├── design/                # shared spec: DESIGN_v5_FINAL.md + history
├── examples/              # reference implementation (uses neurogossip-client)
│   ├── talking_agents.py
│   └── README.md
├── neurogossip-server/    # PACKAGE 1 → pip install neurogossip-server
│   ├── pyproject.toml  README.md  LICENSE  MANIFEST.in
│   ├── src/neurogossip_server/{__init__.py, server.py, __main__.py}
│   └── tests/{test_server.py, test_integration.py, _helpers.py, conftest.py}
├── neurogossip-client/    # PACKAGE 2 → pip install neurogossip-client
│   ├── pyproject.toml  README.md  LICENSE  MANIFEST.in
│   ├── src/neurogossip_client/{__init__.py, client.py}
│   └── tests/{test_client.py, test_connection.py, _helpers.py, conftest.py}
└── README.md              # this file
```

## License

MIT — see each package's `LICENSE`. The design documents in `design/` are also MIT.