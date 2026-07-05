"""Presence: heartbeat keys + listing the other agents currently online."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .channels import presence_key
from .models import AgentIdentity, AgentPresence

# A presence entry is considered stale (treat as offline) if older than this.
PRESENCE_STALE_S = 70.0


async def heartbeat_once(redis: Any, namespace: str, agent: AgentIdentity,
                         ttl_s: float) -> None:
    """Write/refresh this agent's presence key with a TTL."""
    p = AgentPresence(
        agent_id=agent.agent_id,
        agent_name=agent.agent_name,
        role=agent.role,
        status="online",
        updated_at=datetime.now(timezone.utc),
    )
    await redis.set(presence_key(namespace, agent.agent_id),
                    p.model_dump_json(), ex=int(max(1, ttl_s)))


async def list_online_agents(
    redis: Any,
    namespace: str,
    self_id: str,
    *,
    include_self: bool = False,
    stale_s: float = PRESENCE_STALE_S,
) -> list[AgentPresence]:
    """Scan presence keys and return the agents currently present.

    Stale entries (not refreshed within ``stale_s``) are dropped; the TTL also expires
    them in Redis. ``self_id`` is excluded unless ``include_self`` is True.
    """
    pattern = f"neurogossip:{namespace}:presence:*"
    now = datetime.now(timezone.utc)
    out: list[AgentPresence] = []
    async for key in redis.scan_iter(match=pattern, count=200):
        raw = await redis.get(key)
        if not raw:
            continue
        try:
            p = AgentPresence.model_validate_json(raw)
        except Exception:
            continue
        age = (now - p.updated_at).total_seconds()
        if age > stale_s:
            continue
        if not include_self and p.agent_id == self_id:
            continue
        out.append(p)
    return out