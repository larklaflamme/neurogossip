"""Tests for local session archiving (memory management) and dynamic un-archiving."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import pytest

from neurogossip_agent.models import SessionStatus, AgentSessionContext


@pytest.mark.asyncio
async def test_session_archiving_idle(session_manager):
    # 1. Create a conversation and populate it locally
    session = await session_manager.create_conversation(initiator_id="skye")
    cid = session.conversation_id

    # Simulate it being in memory
    ctx = AgentSessionContext(conversation_id=cid)
    # Set last accessed to be 10 seconds ago
    ctx.last_accessed = datetime.now(timezone.utc) - timedelta(seconds=10)
    session_manager.active_sessions[cid] = ctx

    assert cid in session_manager.active_sessions

    # 2. Run sweep with a timeout of 5 seconds (should archive)
    archived = await session_manager.sweep_sessions(idle_timeout_s=5.0)
    
    assert cid in archived
    assert cid not in session_manager.active_sessions


@pytest.mark.asyncio
async def test_session_archiving_not_idle(session_manager):
    # 1. Create a conversation and populate it locally
    session = await session_manager.create_conversation(initiator_id="skye")
    cid = session.conversation_id

    # Simulate it being in memory
    ctx = AgentSessionContext(conversation_id=cid)
    # Set last accessed to now (not idle)
    ctx.last_accessed = datetime.now(timezone.utc)
    session_manager.active_sessions[cid] = ctx

    assert cid in session_manager.active_sessions

    # 2. Run sweep with a timeout of 5 seconds (should NOT archive)
    archived = await session_manager.sweep_sessions(idle_timeout_s=5.0)
    
    assert cid not in archived
    assert cid in session_manager.active_sessions


@pytest.mark.asyncio
async def test_session_archiving_completed(session_manager):
    # 1. Create a conversation and populate it locally
    session = await session_manager.create_conversation(initiator_id="skye")
    cid = session.conversation_id

    # Simulate it being in memory and NOT idle
    ctx = AgentSessionContext(conversation_id=cid)
    ctx.last_accessed = datetime.now(timezone.utc)
    session_manager.active_sessions[cid] = ctx

    assert cid in session_manager.active_sessions

    # 2. Mark session completed in Redis
    await session_manager._update_session_status(cid, SessionStatus.COMPLETED)

    # 3. Run sweep (completed sessions are archived immediately regardless of idle time)
    archived = await session_manager.sweep_sessions(idle_timeout_s=999.0)
    
    assert cid in archived
    assert cid not in session_manager.active_sessions


@pytest.mark.asyncio
async def test_dynamic_unarchiving_on_message(session_manager):
    # 1. Create a conversation in Redis and log history events
    session = await session_manager.create_conversation(initiator_id="skye")
    cid = session.conversation_id
    
    await session_manager.log_status_report(cid, "skye", "Init simulation")

    # Clear it from local memory to simulate being archived/purged
    session_manager.active_sessions.pop(cid, None)
    assert cid not in session_manager.active_sessions

    # 2. Receive a message for this session (simulating un-archiving)
    ctx = await session_manager.process_incoming_message(
        conversation_id=cid,
        message_id="msg_xyz",
        reply_to=None,
        tags=["status_report"],
        sender_id="skye",
        payload={"message": "Init simulation"}
    )

    # Assert it is loaded into memory
    assert cid in session_manager.active_sessions
    assert ctx.conversation_id == cid
    
    # Verify the history was reloaded into the local state
    history = ctx.local_state.get("history", [])
    assert len(history) == 2  # session_started, status_report
    assert history[0].type == "session_started"
    assert history[1].type == "status_report"
    assert history[1].content == {"message": "Init simulation"}
