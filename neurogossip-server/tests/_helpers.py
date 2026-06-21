"""Shared helpers for the Neurogossip test suite (importable as a normal module)."""

import asyncio

from neurogossip_client import NeurogossipClient


async def make_client(server, agent_id, metadata=None, **kwargs):
    """Create + connect a real NeurogossipClient to the in-process server."""
    if metadata is None:
        metadata = {"display_name": agent_id.title(), "version": "1.0",
                    "capabilities": ["test"]}
    client = NeurogossipClient(
        server_url=f"ws://127.0.0.1:{server.port}",
        agent_id=agent_id,
        metadata=metadata,
        **kwargs,
    )
    await client.connect()
    return client


class Listener:
    """Awaitable collector for a single client callback (acks / errors / messages)."""

    def __init__(self):
        self.frames = []
        self._evt = asyncio.Event()

    async def __call__(self, frame):
        self.frames.append(frame)
        self._evt.set()

    async def wait_for(self, pred, timeout=3.0):
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while True:
            for f in list(self.frames):
                if pred(f):
                    return f
            remaining = deadline - loop.time()
            if remaining <= 0:
                return None
            self._evt.clear()
            try:
                await asyncio.wait_for(self._evt.wait(), remaining)
            except asyncio.TimeoutError:
                return None