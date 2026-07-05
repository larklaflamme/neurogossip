"""Integration tests over a shared fakeredis server: direct delivery + ack, room
broadcast, multi-agent fan-out, ignore_self, chain fields on the wire."""
import asyncio

import pytest

from neurogossip_client import AgentIdentity


async def _recv(client, timeout=3.0):
    return await asyncio.wait_for(anext(client.listen()), timeout=timeout)


async def test_direct_delivery_and_chain_fields(make_client):
    skye = make_client("skye")
    axioma = make_client("axioma")
    async with skye, axioma:
        msg = await skye.send_direct("axioma", "hello-axioma", thread_id="rh",
                                     tags=["request"])
        got = await _recv(axioma)
        assert got.content.body == "hello-axioma"
        assert got.sender.agent_id == "skye"
        assert got.recipient.mode == "direct"
        assert got.recipient.agent_ids == ["axioma"]
        # chain: an explicit thread_id is propagated; reply_to None for an original
        assert msg.thread_id == "rh"
        assert got.thread_id == "rh"
        assert msg.reply_to is None
        # explicit ack clears the pending entry
        await axioma.ack(got)
        await asyncio.sleep(0.1)
        from neurogossip_client.channels import inbox_stream, inbox_consumer_group
        pend = await axioma._redis.xpending_range(
            inbox_stream("test", "axioma"), inbox_consumer_group("test", "axioma"),
            "-", "+", count=10)
        assert len(pend) == 0


async def test_reply_carries_reply_to_and_thread_id(make_client):
    skye = make_client("skye")
    axioma = make_client("axioma")
    async with skye, axioma:
        orig = await skye.send_direct("axioma", "please check the eta bound.",
                                      thread_id="rh")
        got = await _recv(axioma)
        await axioma.ack(got)
        # axioma replies, continuing the chain
        reply = await axioma.send_direct("skye", "on it.",
                                         reply_to=orig.message_id, thread_id=orig.thread_id)
        assert reply.reply_to == orig.message_id
        assert reply.thread_id == orig.thread_id
        replied = await _recv(skye)
        assert replied.reply_to == orig.message_id
        assert replied.thread_id == orig.thread_id
        await skye.ack(replied)


async def test_room_broadcast_to_multiple_members(make_client):
    skye = make_client("skye")
    axioma = make_client("axioma")
    thea = make_client("thea")
    async with skye, axioma, thea:
        await axioma.join_room("room1")
        await thea.join_room("room1")
        await skye.join_room("room1")
        await asyncio.sleep(0.1)  # let subscriptions settle
        await skye.publish("room1", "hi everyone")
        a = await _recv(axioma)
        t = await _recv(thea)
        assert a.content.body == "hi everyone" and a.recipient.mode == "room"
        assert t.content.body == "hi everyone" and t.recipient.mode == "room"


async def test_ignore_self_suppresses_own_room_message(make_client):
    skye = make_client("skye")
    async with skye:
        await skye.join_room("room1")
        await asyncio.sleep(0.1)
        await skye.publish("room1", "self message")
        # skye's own echo should be suppressed; nothing arrives within a short window
        with pytest.raises(asyncio.TimeoutError):
            await _recv(skye, timeout=0.5)


async def test_leave_room_stops_delivery(make_client):
    skye = make_client("skye")
    axioma = make_client("axioma")
    async with skye, axioma:
        await axioma.join_room("room1")
        await asyncio.sleep(0.1)
        await axioma.leave_room("room1")
        await asyncio.sleep(0.1)
        await skye.publish("room1", "after leave")
        with pytest.raises(asyncio.TimeoutError):
            await _recv(axioma, timeout=0.5)