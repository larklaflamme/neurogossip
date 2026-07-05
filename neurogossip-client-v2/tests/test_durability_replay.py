"""Durability tests: messages published while a recipient is offline are delivered when
it (re)connects; room members catch up on what they missed."""
import asyncio

from neurogossip_client import AgentIdentity, NeuroGossipClient
from fakeredis import FakeAsyncRedis


async def test_direct_message_delivered_after_offline(make_client, fake_server):
    # axioma is NOT connected yet. skye sends direct messages to axioma while it's
    # offline. When axioma connects, it must receive them (durable direct delivery).
    skye = make_client("skye")
    async with skye:
        await skye.send_direct("axioma", "while-offline-1", thread_id="rh")
        await skye.send_direct("axioma", "while-offline-2", thread_id="rh")

    # axioma now connects for the first time and should catch up on both.
    axioma = NeuroGossipClient(
        agent=AgentIdentity(agent_id="axioma", agent_name="AXIOMA"),
        namespace="test",
        redis_client=FakeAsyncRedis(server=fake_server, decode_responses=True),
        heartbeat_interval_s=999.0, presence_ttl_s=999.0,
    )
    async with axioma:
        m1 = await asyncio.wait_for(anext(axioma.listen()), timeout=3.0)
        m2 = await asyncio.wait_for(anext(axioma.listen()), timeout=3.0)
        await axioma.ack(m1)
        await axioma.ack(m2)
        bodies = {m1.content.body, m2.content.body}
        assert bodies == {"while-offline-1", "while-offline-2"}
        assert m1.thread_id == "rh" and m2.thread_id == "rh"


async def test_room_catchup_after_reconnect(make_client, fake_server):
    # skye + axioma in a room. axioma disconnects; skye posts; axioma reconnects and
    # catches up on the missed room message (cursor-based replay).
    skye = make_client("skye")
    axioma = make_client("axioma")
    async with skye, axioma:
        await skye.join_room("room1")
        await axioma.join_room("room1")
        await asyncio.sleep(0.1)
        # drain the join-time empty state
        await skye.publish("room1", "before-disconnect")
        await asyncio.wait_for(anext(axioma.listen()), timeout=3.0)

    # axioma is now disconnected (context exited). skye posts while axioma is away.
    skye2 = make_client("skye")
    async with skye2:
        await skye2.join_room("room1")
        await asyncio.sleep(0.1)
        await skye2.publish("room1", "while-away")

    # axioma reconnects and should catch up on "while-away".
    axioma2 = NeuroGossipClient(
        agent=AgentIdentity(agent_id="axioma", agent_name="AXIOMA"),
        namespace="test",
        redis_client=FakeAsyncRedis(server=fake_server, decode_responses=True),
        heartbeat_interval_s=999.0, presence_ttl_s=999.0,
    )
    async with axioma2:
        await axioma2.join_room("room1")
        got = await asyncio.wait_for(anext(axioma2.listen()), timeout=3.0)
        assert got.content.body == "while-away"


async def test_unacked_direct_redelivered_on_reconnect(make_client, fake_server):
    # axioma receives a direct message but does NOT ack (simulating a crash mid-process).
    # On reconnect it should get the message again (at-least-once).
    skye = make_client("skye")
    async with skye:
        await skye.send_direct("axioma", "must-redeliver", thread_id="rh")

    axioma = make_client("axioma")
    async with axioma:
        got = await asyncio.wait_for(anext(axioma.listen()), timeout=3.0)
        assert got.content.body == "must-redeliver"
        # deliberately do NOT ack, then exit (simulated crash)

    # reconnect — the unacked message should be reclaimed and redelivered.
    axioma2 = NeuroGossipClient(
        agent=AgentIdentity(agent_id="axioma", agent_name="AXIOMA"),
        namespace="test",
        redis_client=FakeAsyncRedis(server=fake_server, decode_responses=True),
        heartbeat_interval_s=999.0, presence_ttl_s=999.0,
    )
    async with axioma2:
        got2 = await asyncio.wait_for(anext(axioma2.listen()), timeout=3.0)
        assert got2.content.body == "must-redeliver"
        await axioma2.ack(got2)