#!/usr/bin/env python3
"""Reliability demo — 4 agents talking to each other through neurogossip-server.

Each of the four agents (axioma, skye, thea, theoria) sends a message to every other
agent on a timer (all-to-all) AND replies once to each request it receives, forming
ongoing request/reply conversations. Auto-receipt is on so the server's end-to-end ACK
completes promptly. The run tracks sent / delivered / lost and asserts **zero losses**
at the end.

Run a (headless) server first, then this demo:

    # terminal 1
    neurogossip-server --port 8765
    # terminal 2
    python examples/reliable_demo.py --server-url ws://localhost:8765 --duration 600

A shorter run for a quick check:

    python examples/reliable_demo.py --duration 60

Exit code 0 = zero losses; non-zero = some messages were not delivered.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time

# Allow running straight from a source checkout without installing the client.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CLIENT_SRC = os.path.join(_REPO_ROOT, "neurogossip-client", "src")
if os.path.isdir(_CLIENT_SRC) and _CLIENT_SRC not in sys.path:
    sys.path.insert(0, _CLIENT_SRC)

from neurogossip_client import NeurogossipClient  # noqa: E402

AGENTS = [
    ("axioma", "Axioma", "research, creativity, analysis"),
    ("skye", "Skye", "cognition, tools, vision"),
    ("thea", "Thea", "math, wit"),
    ("theoria", "Theoria", "foundations, axiomatics"),
]


class Agent:
    """One demo agent: periodic all-to-all sends + one reply per request."""

    def __init__(self, agent_id: str, display_name: str, capabilities: str,
                 server_url: str, stats: dict):
        self.id = agent_id
        self.peers: list[str] = []
        self.stats = stats
        self.client = NeurogossipClient(
            server_url=server_url,
            agent_id=agent_id,
            metadata={"display_name": display_name, "version": "1.0",
                      "capabilities": [c.strip() for c in capabilities.split(",")]},
            auto_receipt=True,
            open_timeout_s=20.0,
        )
        self.client.on_message(self._on_message)
        self.client.on_ack(self._on_ack)
        self.client.on_error(self._on_error)
        self._counter = 0

    def set_peers(self, peers: list[str]) -> None:
        self.peers = [p for p in peers if p != self.id]

    async def start(self) -> None:
        await self.client.connect()

    async def stop(self) -> None:
        try:
            await self.client.disconnect()
        except Exception:
            pass

    async def send_all(self) -> None:
        """Send one request to each peer (all-to-all)."""
        for peer in self.peers:
            self._counter += 1
            body = f"[{self.id}→{peer} #{self._counter}] hello, please ack."
            try:
                mid = await self.client.send(peer, body, ttl=60)
                self.stats["sent"][mid] = (self.id, peer)
            except Exception as e:
                self.stats["send_errors"] += 1
                self.client.logger.warning("send to %s failed: %s", peer, e)

    async def _on_message(self, frame: dict) -> None:
        """Reply once to each request (not to replies — keeps conversations bounded)."""
        if frame.get("reply_to") is not None:
            return  # it's a reply; don't reply back (avoids an infinite chain)
        peer = frame.get("from")
        msg_id = frame.get("msg_id")
        if not peer or not msg_id:
            return
        body = f"[{self.id}→{peer} reply] received your message, thanks."
        try:
            rid = await self.client.send(peer, body, reply_to=msg_id, ttl=60)
            self.stats["sent"][rid] = (self.id, peer)  # track replies for loss check
            self.stats["replies"] += 1
        except Exception as e:
            self.stats["send_errors"] += 1
            self.client.logger.warning("reply to %s failed: %s", peer, e)

    async def _on_ack(self, ack: dict) -> None:
        status = ack.get("status")
        mid = ack.get("msg_id")
        if mid is None:
            return
        self.stats["acks"][mid] = status  # track the latest ack per message
        if status == "delivered":
            self.stats["delivered"] += 1
        elif status in ("failed", "offline", "unconfirmed", "ttl_expired"):
            self.stats["lost_detail"].append((mid, status))
        # "queued" is not a loss — it'll be redelivered and acked "delivered" later.

    async def _on_error(self, frame: dict) -> None:
        self.stats["server_errors"] += 1
        self.client.logger.warning("server error: %s %s", frame.get("code"), frame.get("message"))


async def run(server_url: str, duration: float, rate: float) -> int:
    stats: dict = {
        "sent": {}, "acks": {}, "delivered": 0, "replies": 0,
        "send_errors": 0, "server_errors": 0, "lost_detail": [],
    }
    agents = [Agent(aid, dn, cap, server_url, stats) for aid, dn, cap in AGENTS]
    peer_ids = [a.id for a in agents]
    for a in agents:
        a.set_peers(peer_ids)

    print(f"Connecting {len(agents)} agents to {server_url} …", flush=True)
    await asyncio.gather(*(a.start() for a in agents))
    print(f"All {len(agents)} agents connected. Running for {duration:.0f}s "
          f"(send every {rate:.1f}s, all-to-all + reply).", flush=True)

    async def ticker():
        end = time.monotonic() + duration
        n = 0
        while time.monotonic() < end:
            await asyncio.gather(*(a.send_all() for a in agents))
            n += 1
            await asyncio.sleep(rate)
            if n % 5 == 0:
                print(f"  [t+{n*rate:.0f}s] sent={len(stats['sent'])} "
                      f"delivered={stats['delivered']} replies={stats['replies']} "
                      f"send_errs={stats['send_errors']} server_errs={stats['server_errors']}",
                      flush=True)

    try:
        await ticker()
    except KeyboardInterrupt:
        print("\nInterrupted; finishing…", flush=True)

    # Drain in-flight ACKs: poll until every sent message has a final ack (or 15s).
    print("Draining in-flight ACKs (up to 15s)…", flush=True)
    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        pending = [m for m in stats["sent"] if stats["acks"].get(m) is None]
        if not pending:
            break
        await asyncio.sleep(0.2)

    await asyncio.gather(*(a.stop() for a in agents), return_exceptions=True)

    # Reconcile: every sent message should have ended "delivered".
    lost = []
    for mid, (frm, to) in stats["sent"].items():
        st = stats["acks"].get(mid)
        if st != "delivered":
            lost.append((mid, frm, to, st))
    print(f"\n=== RESULT ===")
    print(f"sent={len(stats['sent'])} delivered={stats['delivered']} "
          f"replies={stats['replies']}")
    print(f"send_errors={stats['send_errors']} server_errors={stats['server_errors']}")
    print(f"lost={len(lost)}")
    if lost:
        print("Lost messages (msg_id, from, to, last_ack):")
        for mid, frm, to, st in lost[:20]:
            print(f"  {mid} {frm}->{to} last_ack={st}")
    ok = len(lost) == 0 and stats["server_errors"] == 0
    print("PASS — zero losses" if ok else "FAIL — some messages were not delivered")
    return 0 if ok else 1


def main() -> None:
    p = argparse.ArgumentParser(description="4-agent neurogossip reliability demo")
    p.add_argument("--server-url", default=os.getenv("NEUROGOSSIP_SERVER_URL",
                                                     "ws://localhost:8765"))
    p.add_argument("--duration", type=float, default=600.0,
                   help="Run duration in seconds (default 600 = 10 min)")
    p.add_argument("--rate", type=float, default=2.0,
                   help="Seconds between all-to-all send rounds (default 2)")
    args = p.parse_args()
    try:
        rc = asyncio.run(run(args.server_url, args.duration, args.rate))
    except KeyboardInterrupt:
        rc = 130
    sys.exit(rc)


if __name__ == "__main__":
    main()