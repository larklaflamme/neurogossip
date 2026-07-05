"""Channel, stream, cursor and presence key helpers + name validation.

Channel name scheme:

    neurogossip:{namespace}:room:{room_id}                       live room channel
    neurogossip:{namespace}:stream:room:{room_id}                durable room stream
    neurogossip:{namespace}:inbox:{agent_id}                     live direct channel
    neurogossip:{namespace}:stream:inbox:{agent_id}              durable direct stream
    neurogossip:{namespace}:cg:inbox:{agent_id}                  consumer group (direct)
    neurogossip:{namespace}:cursor:{agent_id}:{stream}           per-agent read cursor
    neurogossip:{namespace}:presence:{agent_id}                  presence key (TTL)

``:`` is reserved as the namespace separator, so namespace/room/agent ids may not
contain it (otherwise channels become ambiguous).
"""
from __future__ import annotations

import re

_SAFE = re.compile(r"^[A-Za-z0-9_.-]+$")


def validate_name(value: str, label: str) -> str:
    """Validate a namespace/room/agent id: non-empty, allowed chars only (no ``:``)."""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} cannot be empty")
    if not _SAFE.match(value):
        raise ValueError(
            f"{label} contains invalid characters. "
            "Allowed: letters, numbers, underscore, dot, dash (no colon)."
        )
    return value


def room_channel(namespace: str, room_id: str) -> str:
    return f"neurogossip:{validate_name(namespace, 'namespace')}:room:{validate_name(room_id, 'room_id')}"


def room_stream(namespace: str, room_id: str) -> str:
    return f"neurogossip:{validate_name(namespace, 'namespace')}:stream:room:{validate_name(room_id, 'room_id')}"


def inbox_channel(namespace: str, agent_id: str) -> str:
    return f"neurogossip:{validate_name(namespace, 'namespace')}:inbox:{validate_name(agent_id, 'agent_id')}"


def inbox_stream(namespace: str, agent_id: str) -> str:
    return f"neurogossip:{validate_name(namespace, 'namespace')}:stream:inbox:{validate_name(agent_id, 'agent_id')}"


def inbox_consumer_group(namespace: str, agent_id: str) -> str:
    return f"neurogossip:{validate_name(namespace, 'namespace')}:cg:inbox:{validate_name(agent_id, 'agent_id')}"


def cursor_key(namespace: str, agent_id: str, stream: str) -> str:
    # ``stream`` is a fully-qualified key already validated via the channel helpers, but
    # it contains ':' — so we don't run validate_name on it (only on namespace/agent_id).
    validate_name(namespace, "namespace")
    validate_name(agent_id, "agent_id")
    return f"neurogossip:{namespace}:cursor:{agent_id}:{stream}"


def presence_key(namespace: str, agent_id: str) -> str:
    return f"neurogossip:{validate_name(namespace, 'namespace')}:presence:{validate_name(agent_id, 'agent_id')}"