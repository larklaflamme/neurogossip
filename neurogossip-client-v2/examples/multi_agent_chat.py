#!/usr/bin/env python3
"""Four agents (axioma, skye, thea, theoria) talking via neurogossip-client over Redis.

Each agent joins a shared room and sends a periodic message to every other agent
(all-to-all), and replies once to each request it receives. Auto-receipt is not needed
— delivery is durable (Redis Streams) and acknowledged for direct messages. Run a
Redis first (see docker/docker-compose.redis.yml), then:

    export REDIS_URL=redis://localhost:6379/0
    python examples/multi_agent_chat.py            # Ctrl-C to stop
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from neurogossip_client import AgentIdentity, NeuroGossipClient

AGENTS = [
    ("axioma", "AXIOMA", "experimentalist"),
    ("skye", "SKYE", "research_lead"),
    ("thea", "THEA", "math"),
    ("theoria", "THEORIA", "foundations"),
]


async def agent_loop(agent_id, name, role, stop: asyncio.Event):
    client = NeuroGossipClient(
        namespace=os.getenv("NEUROGOSSIP_NAMESPACE", "demo"),
        agent=AgentIdentity(agent_id=agent_id, agent_name=name, role=role),
    )
    peers = [a for a, _, _ in AGENTS if a != agent_id]
    n = 0
    async with client:
        await client.join_room("square")
        while not stop.is_set():
            n += 1
            for peer in peers:
                await client.send_direct(peer, f"[{agent_id}→{peer} #{n}] hello")
            try:
                msg = await asyncio.wait_for(anext(client.listen()), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if msg.sender.agent_id == agent_id:
                continue
            print(f"[{agent_id}] ← {msg.sender.agent_id}: {msg.content.body}")
            if msg.reply_to is None:  # reply to requests, not to replies
                await client.send_direct(
                    msg.sender.agent_id, f"[{agent_id}] thanks, got it.",
                    reply_to=msg.message_id, thread_id=msg.thread_id,
                )


async def main():
    stop = asyncio.Event()
    tasks = [asyncio.create_task(agent_loop(a, n, r, stop)) for a, n, r in AGENTS]
    print("4 agents talking in room 'square' (direct + room). Ctrl-C to stop.")
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        stop.set()
        for t in tasks:
            t.cancel()


if __name__ == "__main__":
    asyncio.run(main())