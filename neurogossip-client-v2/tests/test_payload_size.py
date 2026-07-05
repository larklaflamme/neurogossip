"""Payload size + channel-name validation tests."""
import pytest

from neurogossip_client import AgentIdentity, GossipMessage, MessageContent, PayloadTooLargeError
from neurogossip_client.channels import (
    inbox_channel, room_channel, room_stream, validate_name,
)
from neurogossip_client.validators import validate_payload_size


def test_payload_under_limit_ok():
    validate_payload_size("x" * 100, 512)


def test_payload_over_limit_raises():
    with pytest.raises(PayloadTooLargeError):
        validate_payload_size("x" * 600, 512)


def test_payload_uses_utf8_byte_length():
    # "€" (U+20AC) is 3 bytes in UTF-8.
    validate_payload_size("€" * 10, 40)  # 30 bytes, under limit
    with pytest.raises(PayloadTooLargeError):
        validate_payload_size("€" * 20, 40)  # 60 bytes, over limit


def test_publish_enforces_size(make_client):
    # A client with a tiny limit rejects an oversized publish before hitting Redis.
    import asyncio
    c = make_client("skye", max_payload_bytes=120)
    big = "x" * 500

    async def run():
        async with c:
            with pytest.raises(PayloadTooLargeError):
                await c.publish("room1", big)
    asyncio.run(run())


def test_validate_name_rejects_colon_empty_badchars():
    with pytest.raises(ValueError):
        validate_name("has:colon", "room_id")
    with pytest.raises(ValueError):
        validate_name("", "room_id")
    with pytest.raises(ValueError):
        validate_name("bad space", "room_id")
    with pytest.raises(ValueError):
        validate_name("bad/slash", "room_id")


def test_validate_name_accepts_allowed():
    assert validate_name("good-id", "room_id") == "good-id"
    assert validate_name("a.b_c-1", "ns") == "a.b_c-1"


def test_channel_helpers_format():
    assert room_channel("ns", "room1") == "neurogossip:ns:room:room1"
    assert room_stream("ns", "room1") == "neurogossip:ns:stream:room:room1"
    assert inbox_channel("ns", "skye") == "neurogossip:ns:inbox:skye"


def test_channel_helpers_reject_bad_ids():
    with pytest.raises(ValueError):
        room_channel("n:s", "room1")
    with pytest.raises(ValueError):
        inbox_channel("ns", "a:b")