#!/usr/bin/env python3
"""Neurogossip reference implementation — two agents talking through the server.

This demonstrates how an agent uses the :class:`NeurogossipClient` library to
communicate with another agent via the Neurogossip WebSocket server. It runs two
agents (``axioma`` and ``thea``) in a single process and walks through a complete
exchange:

  1. Both agents connect + register.
  2. ``axioma`` queries the directory (``list_agents``) and sees ``thea``.
  3. ``axioma`` sends a message to ``thea``.
  4. ``thea`` receives it, sends an explicit receipt (completing the end-to-end ACK),
     and replies.
  5. ``axioma`` receives the reply, receipts it, and the conversation ends.

Run a server first, then this example:

    # terminal 1
    python server.py --port 8765
    # terminal 2
    python examples/talking_agents.py --server-url ws://localhost:8765
"""

import argparse
import asyncio
import os
import sys

# Prefer the installed `neurogossip-client` package. As a convenience for running
# directly from a source checkout without installing, also put the client package's
# ``src`` directory on the path.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CLIENT_SRC = os.path.join(_REPO_ROOT, "neurogossip-client", "src")
if os.path.isdir(_CLIENT_SRC) and _CLIENT_SRC not in sys.path:
    sys.path.insert(0, _CLIENT_SRC)

from neurogossip_client import NeurogossipClient  # noqa: E402


def log(agent: str, text: str) -> None:
    print(f"[{agent}] {text}", flush=True)


async def run_thea(server_url: str, ready: asyncio.Event, done: asyncio.Event) -> None:
    """Thea: register, answer one question, reply, then end the conversation."""
    client = NeurogossipClient(
        server_url=server_url,
        agent_id="thea",
        metadata={"display_name": "Thea", "version": "1.0",
                  "capabilities": ["math", "wit"]},
    )

    async def on_message(msg: dict) -> None:
        log("thea", f"recv from {msg['from']}: {msg['body']!r} "
                   f"(conv={msg.get('conversation_id')}, seq={msg.get('seq')})")
        # 1. Acknowledge receipt so the server can tell the sender the message was delivered.
        await client.send_receipt(msg["msg_id"])
        # 2. Reply in the same conversation.
        if "integral" in msg["body"].lower():
            reply = "sqrt(pi)"
            log("thea", f"replying with {reply!r}")
            await client.send(msg["from"], reply, reply_to=msg["msg_id"],
                             conversation_id=msg.get("conversation_id"))
            # 3. End the conversation gracefully.
            await client.end_conversation(msg.get("conversation_id"), reason="resolved")
            log("thea", "ended the conversation")
            done.set()

    client.on_message(on_message)
    await client.connect()
    log("thea", f"registered (session {client.session_id})")
    ready.set()
    try:
        await done.wait()
    finally:
        await client.disconnect()
        log("thea", "disconnected")


async def run_axioma(server_url: str, thea_ready: asyncio.Event,
                     got_reply: asyncio.Event) -> None:
    """Axioma: register, discover Thea, ask a question, await the reply."""
    client = NeurogossipClient(
        server_url=server_url,
        agent_id="axioma",
        metadata={"display_name": "Axioma", "version": "1.9.1",
                  "capabilities": ["research", "creativity", "analysis"]},
    )
    acks: list[dict] = []

    async def on_message(msg: dict) -> None:
        log("axioma", f"recv from {msg['from']}: {msg['body']!r} "
                      f"(conv={msg.get('conversation_id')}, seq={msg.get('seq')})")
        await client.send_receipt(msg["msg_id"])
        got_reply.set()

    async def on_ack(ack: dict) -> None:
        acks.append(ack)
        log("axioma", f"ack msg={ack['msg_id']} status={ack['status']}")

    async def on_conversation_ended(event: dict) -> None:
        log("axioma", f"conversation {event.get('conversation_id')} ended "
                     f"(reason={event.get('reason')}, by={event.get('by')})")

    client.on_message(on_message)
    client.on_ack(on_ack)
    client.on_conversation_ended(on_conversation_ended)

    await client.connect()
    log("axioma", f"registered (session {client.session_id})")

    # Wait until Thea is online, then ask.
    await thea_ready.wait()
    agents = await client.list_agents()
    log("axioma", f"directory: {[(a['agent_id'], a['status']) for a in agents]}")

    msg_id = await client.send(
        "thea", "What is the integral of e^(-x^2) from -inf to inf?")
    log("axioma", f"sent question (msg_id={msg_id})")

    await got_reply.wait()
    # Give the delivered ACK + conversation_ended a moment to arrive.
    await asyncio.sleep(0.3)
    delivered = any(a["msg_id"] == msg_id and a["status"] == "delivered" for a in acks)
    log("axioma", f"question delivered={delivered}")

    await client.disconnect()
    log("axioma", "disconnected")


async def main(server_url: str) -> None:
    thea_ready = asyncio.Event()
    got_reply = asyncio.Event()
    done = asyncio.Event()
    await asyncio.gather(
        run_thea(server_url, thea_ready, done),
        run_axioma(server_url, thea_ready, got_reply),
    )
    print("\nReference exchange complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Neurogossip two-agent reference demo")
    parser.add_argument("--server-url", default="ws://localhost:8765",
                        help="Neurogossip server URL (default ws://localhost:8765)")
    args = parser.parse_args()
    try:
        asyncio.run(main(args.server_url))
    except KeyboardInterrupt:
        pass