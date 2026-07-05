#!/usr/bin/env python3
"""Example agent AXIOMA — joins a room, listens, replies to messages it sees.

    export REDIS_URL=redis://localhost:6379/0
    python examples/agent_axioma.py
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from neurogossip_client import AgentIdentity, NeuroGossipClient


async def main():
    client = NeuroGossipClient(
        namespace=os.getenv("NEUROGOSSIP_NAMESPACE", "ravennest"),
        agent=AgentIdentity(agent_id="axioma", agent_name="AXIOMA", role="experimentalist"),
    )
    async with client:
        await client.join_room("ift-rh")
        print("axioma listening in room ift-rh …")
        async for msg in client.listen():
            if msg.sender.agent_id == "axioma":
                continue
            print(f"[{msg.sender.agent_id}] {msg.content.body}")
            if "eta lower-bound" in msg.content.body:
                await client.send_direct(
                    to_agent_id=msg.sender.agent_id,
                    markdown="I will run numerical perturbation checks against η_N(s) near σ > 1/2.",
                    reply_to=msg.message_id,
                    thread_id=msg.thread_id,
                    tags=["response", "experiment"],
                )


if __name__ == "__main__":
    asyncio.run(main())