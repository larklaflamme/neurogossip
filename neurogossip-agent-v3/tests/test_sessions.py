"""Tests for conversation sessions and basic request-response logic."""
from __future__ import annotations

import pytest

from neurogossip_agent.errors import SessionNotFoundError, RequestNotFoundError
from neurogossip_agent.models import RequestStatus, SessionStatus


@pytest.mark.asyncio
async def test_create_and_get_conversation(session_manager):
    # Create conversation
    session = await session_manager.create_conversation(initiator_id="skye", metadata={"topic": "proofs"})
    assert session.initiator_id == "skye"
    assert session.status == SessionStatus.ACTIVE
    assert session.metadata == {"topic": "proofs"}

    # Fetch conversation
    fetched = await session_manager.get_conversation(session.conversation_id)
    assert fetched.conversation_id == session.conversation_id
    assert fetched.initiator_id == "skye"
    assert fetched.status == SessionStatus.ACTIVE
    assert fetched.metadata == {"topic": "proofs"}


@pytest.mark.asyncio
async def test_get_nonexistent_conversation(session_manager):
    with pytest.raises(SessionNotFoundError):
        await session_manager.get_conversation("nonexistent")


@pytest.mark.asyncio
async def test_request_response_flow(session_manager, mock_transport):
    # Setup conversation
    session = await session_manager.create_conversation(initiator_id="skye")
    cid = session.conversation_id

    # Create root request
    req = await session_manager.create_request(
        conversation_id=cid,
        sender_id="human",
        recipient_ids=["skye"],
        payload="What is the RH proof status?"
    )

    assert req.conversation_id == cid
    assert req.sender_id == "human"
    assert req.recipient_ids == ["skye"]
    assert req.status == RequestStatus.PENDING

    # Verify transport message was dispatched
    assert len(mock_transport.sent_messages) == 1
    msg = mock_transport.sent_messages[0]
    assert msg["recipient_id"] == "skye"
    assert msg["payload"]["request_id"] == req.request_id
    assert msg["payload"]["payload"] == "What is the RH proof status?"

    # Check request in Redis
    fetched_req = await session_manager.get_request(req.request_id)
    assert fetched_req.status == RequestStatus.PENDING

    # Reply to request
    resp = await session_manager.send_response(
        request_id=req.request_id,
        sender_id="skye",
        payload="RH is still unproved."
    )

    assert resp.request_id == req.request_id
    assert resp.sender_id == "skye"
    assert resp.recipient_id == "human"
    assert resp.payload == "RH is still unproved."

    # Verify request is now COMPLETED
    fetched_req = await session_manager.get_request(req.request_id)
    assert fetched_req.status == RequestStatus.COMPLETED
    assert fetched_req.completed_at is not None

    # Since it was root request, verify conversation status is COMPLETED
    fetched_session = await session_manager.get_conversation(cid)
    assert fetched_session.status == SessionStatus.COMPLETED


@pytest.mark.asyncio
async def test_persistent_history_log(session_manager):
    session = await session_manager.create_conversation(initiator_id="skye")
    cid = session.conversation_id

    # Add logs & status reports
    await session_manager.log_status_report(cid, agent_id="skye", message="Starting research...")
    
    req = await session_manager.create_request(
        conversation_id=cid,
        sender_id="skye",
        recipient_ids=["axioma"],
        payload="Check checks"
    )
    
    await session_manager.send_response(
        request_id=req.request_id,
        sender_id="axioma",
        payload="Checked"
    )

    # Fetch history
    history = await session_manager.get_history(cid)
    
    # Verify events
    assert len(history) == 5  # session_started, status_report, request_sent, response_received, session_completed (since parent_request_id was None)
    
    assert history[0].type == "session_started"
    assert history[1].type == "status_report"
    assert history[1].content == {"message": "Starting research..."}
    assert history[2].type == "request_sent"
    assert history[2].ref_id == req.request_id
    assert history[3].type == "response_received"
    assert history[3].content["payload"] == "Checked"
    assert history[4].type == "session_completed"
