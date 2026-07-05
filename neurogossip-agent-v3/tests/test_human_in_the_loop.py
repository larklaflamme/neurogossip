"""Tests for human-in-the-loop and blocking response mechanics."""
from __future__ import annotations

import asyncio
import pytest

from neurogossip_agent.models import RequestStatus, SessionStatus


@pytest.mark.asyncio
async def test_human_in_the_loop_transition(session_manager):
    session = await session_manager.create_conversation(initiator_id="skye")
    cid = session.conversation_id

    # Create root request (Agent skye processes human request)
    root_req = await session_manager.create_request(
        conversation_id=cid,
        sender_id="human",
        recipient_ids=["skye"],
        payload="Research project A."
    )

    assert (await session_manager.get_conversation(cid))
    fetched_session = await session_manager.get_conversation(cid)
    assert fetched_session.status == SessionStatus.ACTIVE

    # skye creates a request targeting the "human" for input
    human_req = await session_manager.create_request(
        conversation_id=cid,
        sender_id="skye",
        recipient_ids=["human"],
        payload="What is the budget?",
        parent_request_id=root_req.request_id
    )

    # Verify session status transitions to WAITING_FOR_HUMAN
    fetched_session = await session_manager.get_conversation(cid)
    assert fetched_session.status == SessionStatus.WAITING_FOR_HUMAN

    # Human provides input
    await session_manager.send_response(
        request_id=human_req.request_id,
        sender_id="human",
        payload="$50k budget"
    )

    # Verify session status transitions back to ACTIVE
    fetched_session = await session_manager.get_conversation(cid)
    assert fetched_session.status == SessionStatus.ACTIVE

    # Verify human request is completed
    fetched_human_req = await session_manager.get_request(human_req.request_id)
    assert fetched_human_req.status == RequestStatus.COMPLETED


@pytest.mark.asyncio
async def test_blocking_wait_for_response(session_manager):
    session = await session_manager.create_conversation(initiator_id="skye")
    cid = session.conversation_id

    req = await session_manager.create_request(
        conversation_id=cid,
        sender_id="skye",
        recipient_ids=["axioma"],
        payload="Run check."
    )

    # Task to wait for the response
    async def _waiter():
        responses = await session_manager.wait_for_response(req.request_id, timeout=2.0)
        return responses

    wait_task = asyncio.create_task(_waiter())
    await asyncio.sleep(0.1)  # Allow waiter task to subscribe

    # Send response
    await session_manager.send_response(
        request_id=req.request_id,
        sender_id="axioma",
        payload="Check completed successfully."
    )

    responses = await wait_task
    assert len(responses) == 1
    assert responses[0].payload == "Check completed successfully."
    assert responses[0].sender_id == "axioma"
