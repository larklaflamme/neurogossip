"""Shared helpers for the neurogossip-client test suite (importable as a module)."""

from neurogossip_client import NeurogossipClient


def make_client(stub, agent_id="axioma", **kw):
    """Build a NeurogossipClient pointed at the in-process stub server."""
    return NeurogossipClient(
        server_url=stub.url,
        agent_id=agent_id,
        metadata={"display_name": agent_id.title(), "version": "1.0",
                  "capabilities": ["test"]},
        **kw,
    )