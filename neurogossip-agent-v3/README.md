# neurogossip-agent

A Python library for managing multi-turn, multi-agent conversational sessions with persistent message logs and human-in-the-loop triggers using Redis.

## Features

- **Multi-turn conversation sessions**: Groups related messages, requests, and responses under a single `conversation_id`.
- **Request-Response & Chaining Tracking**: Spawns sub-requests and matches them to responses, enabling full hierarchical conversation tree tracking.
- **Fan-out Requests**: Dispatches a single task to multiple recipient agents and tracks individual responses.
- **Persistent Message Logs**: Keeps a complete, chronological log of all requests, responses, and status reports in Redis.
- **Human-in-the-Loop**: Seamlessly handles status reporting and input/approval requests for human users.
- **Transport-Agnostic Design**: Built with a transport layer abstraction, wrapping `neurogossip-client-v2` by default.

## Installation

```bash
pip install -e .
```

## Environment Variables

The default `RedisAgentTransport` automatically reads configurations from environment variables if they are not explicitly passed to the constructor. To avoid conflicts with older versions of Neurogossip, the `NEUROGOSSIP_V3_` prefix is preferred.

| Primary V3 Env Variable | Fallback Env Variable | Default Value | Description |
|---|---|---|---|
| `NEUROGOSSIP_V3_REDIS_URL` | `REDIS_URL` | `redis://localhost:6379/0` | Connection URL for the Redis server. |
| `NEUROGOSSIP_V3_NAMESPACE` | `NEUROGOSSIP_NAMESPACE` | `default` | Project namespace isolating the agent conversations. |
| `NEUROGOSSIP_V3_AGENT_ID` | `NEUROGOSSIP_AGENT_ID` | `default_agent` | Unique ID of the agent connecting. |
| `NEUROGOSSIP_V3_AGENT_NAME` | `NEUROGOSSIP_AGENT_NAME` | *(Agent ID in uppercase)* | Human-friendly display name of the agent. |
| `NEUROGOSSIP_V3_AGENT_ROLE` | `NEUROGOSSIP_AGENT_ROLE` | `agent` | Operational role/responsibility of the agent. |


