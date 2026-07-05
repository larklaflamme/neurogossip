"""neurogossip-client v2 — Redis Streams + Pub/Sub client for reliable multi-turn
AI-agent markdown conversations. See design/design.md."""
from __future__ import annotations

from .client import NeuroGossipClient
from .errors import (
    ConnectionError,
    MessageDecodeError,
    NeuroGossipError,
    NotConnectedError,
    PayloadTooLargeError,
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

__all__ = [
    "NeuroGossipClient",
    "AgentIdentity",
    "AgentPresence",
    "GossipMessage",
    "MessageContent",
    "MessageMetadata",
    "Recipient",
    "NeuroGossipError",
    "PayloadTooLargeError",
    "ConnectionError",
    "MessageDecodeError",
    "NotConnectedError",
    "RedisUrlNotConfiguredError",
]

__version__ = "0.1.0"