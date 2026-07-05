"""Comprehensive tests emulating multi-agent conversation switching across concurrent sessions."""
from __future__ import annotations

import pytest

from neurogossip_agent.models import RequestStatus, SessionStatus


@pytest.mark.asyncio
async def test_concurrent_conversations_switching(session_manager, mock_transport):
    # --- 1. Setup Conversation 1 (C1) ---
    c1 = await session_manager.create_conversation(initiator_id="coordinator", metadata={"name": "Job 1"})
    cid1 = c1.conversation_id
    
    # Root request for C1
    req1_root = await session_manager.create_request(
        conversation_id=cid1,
        sender_id="human",
        recipient_ids=["coordinator"],
        payload="Analyze dataset 1"
    )
    
    # Coordinator delegates C1 to worker_b
    req1_sub = await session_manager.create_request(
        conversation_id=cid1,
        sender_id="coordinator",
        recipient_ids=["worker_b"],
        payload="Check dataset 1 anomalies",
        parent_request_id=req1_root.request_id
    )

    # Verify C1 context is active in memory
    assert cid1 in session_manager.active_sessions
    ctx1 = session_manager.active_sessions[cid1]
    assert ctx1.conversation_id == cid1

    # --- 2. Setup Conversation 2 (C2) concurrently ---
    c2 = await session_manager.create_conversation(initiator_id="coordinator", metadata={"name": "Job 2"})
    cid2 = c2.conversation_id
    
    # Root request for C2
    req2_root = await session_manager.create_request(
        conversation_id=cid2,
        sender_id="human",
        recipient_ids=["coordinator"],
        payload="Analyze dataset 2"
    )
    
    # Coordinator delegates C2 to both worker_b and worker_c via fan-out request
    req2_sub = await session_manager.create_request(
        conversation_id=cid2,
        sender_id="coordinator",
        recipient_ids=["worker_b", "worker_c"],
        payload="Check dataset 2 anomalies (fan-out)",
        parent_request_id=req2_root.request_id
    )

    # Verify both contexts are now open in memory
    assert cid1 in session_manager.active_sessions
    assert cid2 in session_manager.active_sessions
    
    # Check distinct metadata
    conv1 = await session_manager.get_conversation(cid1)
    conv2 = await session_manager.get_conversation(cid2)
    assert conv1.metadata["name"] == "Job 1"
    assert conv2.metadata["name"] == "Job 2"

    # --- 3. Process replies out of order to simulate context switching ---
    
    # a) worker_b replies to C2 first
    await session_manager.send_response(
        request_id=req2_sub.request_id,
        sender_id="worker_b",
        payload="Dataset 2 anomalies found in column A"
    )
    
    # Verify C2 request is still pending (needs worker_c response)
    fetched_req2 = await session_manager.get_request(req2_sub.request_id)
    assert fetched_req2.status == RequestStatus.PENDING

    # b) worker_b replies to C1
    await session_manager.send_response(
        request_id=req1_sub.request_id,
        sender_id="worker_b",
        payload="Dataset 1 anomaly-free"
    )
    
    # Verify C1 request is completed (single recipient replied)
    fetched_req1 = await session_manager.get_request(req1_sub.request_id)
    assert fetched_req1.status == RequestStatus.COMPLETED

    # c) coordinator finalizes C1 (completes the root request)
    await session_manager.send_response(
        request_id=req1_root.request_id,
        sender_id="coordinator",
        payload="Dataset 1 is anomaly-free. Verified."
    )
    
    # Verify Conversation 1 is completed
    fetched_conv1 = await session_manager.get_conversation(cid1)
    assert fetched_conv1.status == SessionStatus.COMPLETED

    # --- 4. Sweep Idle & Completed Sessions ---
    # C1 is completed, so it should be archived. C2 is still active, so it should remain.
    archived = await session_manager.sweep_sessions(idle_timeout_s=300.0)
    assert cid1 in archived
    assert cid2 not in archived
    assert cid1 not in session_manager.active_sessions
    assert cid2 in session_manager.active_sessions

    # --- 5. Finish Conversation 2 ---
    # worker_c replies to C2, which will trigger dynamic un-archiving of C2 if it was archived
    # (In this case it is still in memory, but we can verify it works fine)
    await session_manager.send_response(
        request_id=req2_sub.request_id,
        sender_id="worker_c",
        payload="Dataset 2 anomalies found in column B"
    )
    
    # Verify C2 request is now completed (both responded)
    fetched_req2 = await session_manager.get_request(req2_sub.request_id)
    assert fetched_req2.status == RequestStatus.COMPLETED

    # coordinator finalizes C2
    await session_manager.send_response(
        request_id=req2_root.request_id,
        sender_id="coordinator",
        payload="Dataset 2 has anomalies in columns A and B."
    )

    # Verify Conversation 2 is completed
    fetched_conv2 = await session_manager.get_conversation(cid2)
    assert fetched_conv2.status == SessionStatus.COMPLETED

    # Sweep should now archive C2 as well
    archived_c2 = await session_manager.sweep_sessions(idle_timeout_s=300.0)
    assert cid2 in archived_c2
    assert cid2 not in session_manager.active_sessions

    # --- 6. Verify Isolated Histories ---
    hist1 = await session_manager.get_history(cid1)
    hist2 = await session_manager.get_history(cid2)

    # Assert history events are completely separate and contain correct event types
    assert len(hist1) > 0
    assert len(hist2) > 0

    c1_types = [h.type for h in hist1]
    c2_types = [h.type for h in hist2]

    assert "session_started" in c1_types
    assert "session_completed" in c1_types
    assert "session_started" in c2_types
    assert "session_completed" in c2_types

    # Ensure no cross-pollution of request IDs in logs
    c1_refs = {h.ref_id for h in hist1 if h.ref_id}
    c2_refs = {h.ref_id for h in hist2 if h.ref_id}
    assert c1_refs.isdisjoint(c2_refs)
