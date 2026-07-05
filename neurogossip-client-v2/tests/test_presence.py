"""Presence tests: heartbeat writes a TTL key; list_online_agents returns peers
(other agents), excludes self, and drops stale entries."""
import asyncio
from datetime import datetime, timedelta, timezone

from neurogossip_client import AgentPresence
from neurogossip_client.channels import presence_key
from neurogossip_client import presence as presence_mod


async def test_list_online_agents_excludes_self_and_lists_peers(make_client, fake_server):
    skye = make_client("skye", heartbeat_interval_s=0.05, presence_ttl_s=10)
    axioma = make_client("axioma", heartbeat_interval_s=0.05, presence_ttl_s=10)
    thea = make_client("thea", heartbeat_interval_s=0.05, presence_ttl_s=10)
    async with skye, axioma, thea:
        await asyncio.sleep(0.2)  # let heartbeats write presence keys
        peers = await skye.list_online_agents()
        ids = {p.agent_id for p in peers}
        assert ids == {"axioma", "thea"}  # excludes self (skye)
        # include_self adds skye
        all_ids = {p.agent_id for p in await skye.list_online_agents(include_self=True)}
        assert all_ids == {"skye", "axioma", "thea"}


async def test_stale_presence_dropped(make_client, fake_server):
    skye = make_client("skye", heartbeat_interval_s=999, presence_ttl_s=999)
    async with skye:
        # write a stale axioma presence manually (updated long ago)
        stale = AgentPresence(agent_id="axioma", agent_name="AXIOMA",
                              updated_at=datetime.now(timezone.utc) - timedelta(minutes=5))
        await skye._redis.set(presence_key("test", "axioma"), stale.model_dump_json(),
                              ex=999)
        peers = await skye.list_online_agents()
        assert all(p.agent_id != "axioma" for p in peers)  # stale axioma dropped