"""Neurogossip client library — async interface to the Neurogossip server.

Public API::

    from neurogossip_client import NeurogossipClient

    client = NeurogossipClient(
        server_url="ws://localhost:8765",
        agent_id="axioma",
        metadata={"display_name": "Axioma", "version": "1.0",
                  "capabilities": ["research"]},
    )
    await client.connect()
    msg_id = await client.send("thea", "hello")
    ...
    await client.disconnect()
"""

from .client import NeurogossipClient

__version__ = "0.1.0"
__all__ = ["NeurogossipClient", "__version__"]