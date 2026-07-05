"""Redis Streams durability helpers: XADD, cursor-based room replay, consumer-group
direct delivery with XACK, and stale-PEL reclaim.

- Room streams: every member reads by cursor (broadcast + replay missed-on-reconnect).
- Inbox streams: per-recipient consumer group → XREADGROUP ">" for new, reclaim stale
  PEL entries on (re)connect, XACK after the app processes the message.
"""
from __future__ import annotations

from typing import Any

# Stream trim: keep a long recent window for catch-up while bounding memory.
STREAM_MAXLEN = 10000
CATCHUP_BATCH = 500
STALE_MIN_IDLE_MS = 30_000  # reclaim pending entries older than 30s on reconnect


async def xadd_message(redis: Any, stream: str, payload: str) -> str:
    """Append a message payload to a stream; return the new Stream ID."""
    return await redis.xadd(stream, {"payload": payload}, id="*",
                            maxlen=STREAM_MAXLEN, approximate=True)


async def ensure_group(redis: Any, stream: str, group: str) -> None:
    """Create the consumer group on the stream (idempotent; MKSTREAM).

    Start at id ``"0"`` so direct messages sent while the agent was offline (before its
    first connect) are delivered on first connect — the durability guarantee.
    """
    try:
        await redis.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception as e:  # redis.exceptions.ResponseError BUSYGROUP is expected
        if "BUSYGROUP" not in str(e):
            raise


async def read_new_for_group(redis: Any, stream: str, group: str, consumer: str,
                             count: int = CATCHUP_BATCH) -> list[tuple[str, str]]:
    """XREADGROUP '>' — never-delivered messages for this consumer group (non-blocking)."""
    resp = await redis.xreadgroup(group, consumer, {stream: ">"},
                                  count=count, block=None)
    return _entries(resp)


async def claim_stale(redis: Any, stream: str, group: str, consumer: str,
                      min_idle_ms: int = STALE_MIN_IDLE_MS,
                      count: int = CATCHUP_BATCH) -> list[tuple[str, str]]:
    """Reclaim pending (unacked) entries that have been idle longer than min_idle_ms —
    i.e. messages a previous incarnation of this agent read but never acked (crash)."""
    claimed: list[tuple[str, str]] = []
    try:
        pending = await redis.xpending_range(stream, group, min="-", max="+",
                                             count=count, idle=min_idle_ms)
    except Exception:
        pending = []
    ids = [p["message_id"] for p in pending] if pending else []
    if not ids:
        return claimed
    try:
        got = await redis.xclaim(stream, group, consumer, min_idle_time=min_idle_ms,
                                 message_ids=ids)
        claimed = _entries([[stream, got]]) if got else []
    except Exception:
        pass
    return claimed


async def claim_pending(redis: Any, stream: str, group: str, consumer: str,
                        count: int = CATCHUP_BATCH) -> list[tuple[str, str]]:
    """Reclaim ALL pending (unacked) entries for this consumer (min_idle=0).

    Used on (re)connect: the previous incarnation is gone, so any messages it read but
    never acked must be redelivered (at-least-once). Distinct from ``claim_stale``
    (which only reclaims long-idle entries and is for cross-consumer cleanup).
    """
    claimed: list[tuple[str, str]] = []
    try:
        pending = await redis.xpending_range(stream, group, min="-", max="+",
                                             count=count, consumername=consumer)
    except Exception:
        pending = []
    ids = [p["message_id"] for p in pending] if pending else []
    if not ids:
        return claimed
    try:
        got = await redis.xclaim(stream, group, consumer, min_idle_time=0,
                                 message_ids=ids)
        claimed = _entries([[stream, got]]) if got else []
    except Exception:
        pass
    return claimed


async def ack(redis: Any, stream: str, group: str, *ids: str) -> int:
    """Acknowledge delivery of message IDs (idempotent)."""
    if not ids:
        return 0
    return await redis.xack(stream, group, *ids)


async def read_room_from_cursor(redis: Any, stream: str, cursor: str,
                                count: int = CATCHUP_BATCH) -> list[tuple[str, str]]:
    """Non-blocking XREAD after ``cursor`` for room broadcast replay. ``"$"`` (a fresh
    join) yields nothing — there's nothing to catch up; the live subscription handles
    new messages."""
    if cursor == "$":
        return []
    resp = await redis.xread({stream: cursor}, count=count, block=None)
    return _entries(resp)


async def get_cursor(redis: Any, key: str) -> str:
    val = await redis.get(key)
    return val if val else "$"


async def set_cursor(redis: Any, key: str, last_id: str) -> None:
    await redis.set(key, last_id)


def _entries(resp: Any) -> list[tuple[Any, str]]:
    """Normalize redis-py XREAD/XREADGROUP/XCLAIM responses to [(id, payload_str)].

    Handles both ``decode_responses=True`` (str keys/values) and raw bytes.
    """
    out: list[tuple[Any, str]] = []
    if not resp:
        return out
    for _stream, entries in resp:
        for entry_id, fields in entries:
            payload = ""
            if isinstance(fields, dict):
                payload = fields.get("payload", fields.get(b"payload", ""))
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8", "replace")
            out.append((entry_id, payload))
    return out