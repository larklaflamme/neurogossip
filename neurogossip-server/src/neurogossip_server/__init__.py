"""Neurogossip server — WebSocket agent registry & direct-messaging relay.

Public API::

    from neurogossip_server import NeurogossipServer

    server = NeurogossipServer(host="0.0.0.0", port=8765)
    asyncio.run(server.start())

Or run it as a console script / module::

    neurogossip-server --port 8765
    python -m neurogossip_server --port 8765
"""

from .server import (
    AGENT_ID_RE,
    AgentSession,
    CircuitBreakerRecord,
    ConversationRecord,
    HeartbeatEngine,
    METADATA_SCHEMA,
    MessageRecord,
    MessageRouter,
    NeurogossipServer,
    PresenceBroadcaster,
    Registry,
)

__version__ = "0.1.0"
__all__ = [
    "NeurogossipServer",
    "Registry",
    "HeartbeatEngine",
    "MessageRouter",
    "PresenceBroadcaster",
    "AgentSession",
    "MessageRecord",
    "ConversationRecord",
    "CircuitBreakerRecord",
    "METADATA_SCHEMA",
    "AGENT_ID_RE",
    "__version__",
]