"""Tests for fan-out request and multi-agent tracking logic."""
from __future__ import annotations

import pytest

from neurogossip_agent.models import RequestStatus, SessionStatus


@pytest.mark.asyncio
async def test_fanout_request_flow(session_manager, mock_transport):
    session = await session_manager.create_conversation(initiator_id="skye")
    cid = session.conversation_id

    # Create root request
    root_req = await session_manager.create_request(
        conversation_id=cid,
        sender_id="human",
        recipient_ids=["skye"],
        payload="Run full check."
    )

    # Dispatched sub-request as fan-out to axioma and thea
    fanout_req = await session_manager.create_request(
        conversation_id=cid,
        sender_id="skye",
        recipient_ids=["axioma", "thea"],
        payload="Check data stability",
        parent_request_id=root_req.request_id
    )

    assert fanout_req.recipient_ids == ["axioma", "thea"]
    assert fanout_req.status == RequestStatus.PENDING

    # Verify transport sent messages to both recipients
    # First message was root_req to skye (index 0)
    # Next two should be the fan-out to axioma and thea
    assert len(mock_transport.sent_messages) == 3
    recipients_sent = [m["recipient_id"] for m in mock_transport.sent_messages[1:]]
    assert "axioma" in recipients_sent
    assert "thea" in recipients_sent

    # Verify pending recipients in Redis
    pending_recipients_key = session_manager._request_pending_recipients_key(fanout_req.request_id)
    pending_count = await session_manager.redis.scard(pending_recipients_key)
    assert pending_count == 2

    # Agent axioma responds
    await session_manager.send_response(
        request_id=fanout_req.request_id,
        sender_id="axioma",
        payload="Axioma: stable"
    )

    # Verify request is STILL pending because thea hasn't responded
    fetched_req = await session_manager.get_request(fanout_req.request_id)
    assert fetched_req.status == RequestStatus.PENDING

    # Check pending recipients set
    pending_count = await session_manager.redis.scard(pending_recipients_key)
    assert pending_count == 1
    is_thea_pending = await session_manager.redis.sismember(pending_recipients_key, "thea")
    assert is_thea_pending

    # Agent thea responds
    await session_manager.send_response(
        request_id=fanout_req.request_id,
        sender_id="thea",
        payload="Thea: stable"
    )

    # Now verify request IS completed
    fetched_req = await session_manager.get_request(fanout_req.request_id)
    assert fetched_req.status == RequestStatus.COMPLETED

    # Verify pending set is empty
    pending_count = await session_manager.redis.scard(pending_recipients_key)
    assert pending_count == 0

    # Retrieve all responses for the request
    responses = await session_manager.get_all_responses(fanout_req.request_id)
    assert len(responses) == 2
    response_payloads = {r.sender_id: r.payload for r in responses}
    assert response_payloads["axioma"] == "Axioma: stable"
    assert response_payloads["thea"] == "Thea: stable"
