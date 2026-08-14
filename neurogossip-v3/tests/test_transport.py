"""Unit tests for WebSocketAgentTransport."""

import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from neurogossip_v3.models import Contribution, ContributionKind, VoteSession
from neurogossip_v3.transport import WebSocketAgentTransport


@pytest.mark.asyncio
async def test_transport_loopback_and_handlers():
    transport = WebSocketAgentTransport("ws://localhost:8765", "skye")

    # Mock client methods
    transport._client.connect = AsyncMock()
    transport._client.disconnect = AsyncMock()
    transport._client.send = AsyncMock(return_value="msg-123")

    delib_received = []
    inv_received = []
    vote_received = []

    async def delib_handler(sender_id, body, msg_id, reply_to, conv_id):
        delib_received.append((sender_id, body))

    async def inv_handler(sender_id, body, msg_id, conv_id):
        inv_received.append((sender_id, body))

    async def vote_handler(sender_id, body, msg_id, conv_id):
        vote_received.append((sender_id, body))

    transport.set_deliberation_handler(delib_handler)
    transport.set_invitation_handler(inv_handler)
    transport.set_vote_handler(vote_handler)

    await transport.connect()

    del_id = uuid4()
    contrib = Contribution(
        deliberation_id=del_id,
        sender_id="skye",
        kind=ContributionKind.STATEMENT,
        content="Loopback test message",
    )

    # Loopback send to self
    await transport.send_contribution("skye", del_id, contrib)
    await asyncio.sleep(0.05)

    assert len(delib_received) == 1
    assert delib_received[0][0] == "skye"
    assert delib_received[0][1]["contribution"]["content"] == "Loopback test message"

    # Send to other agent (calls mock _client.send)
    await transport.send_contribution("thea", del_id, contrib)
    transport._client.send.assert_called_once()

    await transport.disconnect()


@pytest.mark.asyncio
async def test_transport_frame_dispatch():
    transport = WebSocketAgentTransport("ws://localhost:8765", "thea")
    received_invites = []

    async def inv_handler(sender_id, body, msg_id, conv_id):
        received_invites.append(body["goal"])

    transport.set_invitation_handler(inv_handler)
    transport._client.connect = AsyncMock()
    transport._client.disconnect = AsyncMock()

    await transport.connect()

    del_id = uuid4()
    # Simulate receiving a frame from the WebSocket server
    frame = {
        "from": "skye",
        "body": f'{{"type": "deliberation_invitation", "deliberation_id": "{del_id}", "goal": "Solve RH", "initiator_id": "skye"}}',
        "msg_id": "msg-456",
        "conversation_id": str(del_id),
    }
    transport._dispatch_incoming(frame)

    await asyncio.sleep(0.05)
    assert len(received_invites) == 1
    assert received_invites[0] == "Solve RH"

    await transport.disconnect()
