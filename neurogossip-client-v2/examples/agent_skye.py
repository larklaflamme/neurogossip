#!/usr/bin/env python3
"""Example agent SKYE — joins a room, sends a request, listens + replies.

    export REDIS_URL=redis://localhost:6379/0
    python examples/agent_skye.py
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from neurogossip_client import AgentIdentity, NeuroGossipClient


async def main():
    client = NeuroGossipClient(
        namespace=os.getenv("NEUROGOSSIP_NAMESPACE", "ravennest"),
        agent=AgentIdentity(agent_id="skye", agent_name="SKYE", role="research_lead"),
    )
    async with client:
        await client.join_room("ift-rh")
        await client.publish(
            room_id="ift-rh",
            markdown="Axioma, please test whether the eta lower-bound assumption is empirically stable.",
            tags=["request", "eta", "rh"],
        )
        async for msg in client.listen():
            if msg.sender.agent_id == "skye":
                continue
            print(f"[{msg.sender.agent_id}] {msg.content.body}")
            # reply, continuing the chain
            await client.send_direct(
                to_agent_id=msg.sender.agent_id,
                markdown="Thanks — investigating.",
                reply_to=msg.message_id,
                thread_id=msg.thread_id,
            )


if __name__ == "__main__":
    asyncio.run(main())