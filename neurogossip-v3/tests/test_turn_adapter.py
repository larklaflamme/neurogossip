"""Unit tests for TurnBasedAdapter and SuspendTurn exception."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from neurogossip_v3.manager import DeliberationManager
from neurogossip_v3.models import ContributionKind, ExecutionMode
from neurogossip_v3.state_store import InMemoryStateStore
from neurogossip_v3.transport import WebSocketAgentTransport
from neurogossip_v3.turn_adapter import SuspendTurn, TurnBasedAdapter


@pytest.fixture
def mock_transport():
    transport = WebSocketAgentTransport("ws://localhost:8765", "skye")
    transport._client.send = AsyncMock(return_value="msg-123")
    return transport


@pytest.mark.asyncio
async def test_turn_based_mode_raises_suspend_turn(mock_transport):
    store = InMemoryStateStore()
    manager = DeliberationManager(
        "skye", store, mock_transport, execution_mode=ExecutionMode.TURN_BASED
    )
    adapter = TurnBasedAdapter(manager)

    deliberation = await manager.start_deliberation(
        goal="Turn Based Test",
        participants={"thea"},
    )

    await manager.contribute(
        deliberation_id=deliberation.deliberation_id,
        kind=ContributionKind.QUESTION,
        content="Is proof ready?",
        expects_response_from={"thea"},
    )

    with pytest.raises(SuspendTurn) as exc_info:
        await manager.wait_for_responses(deliberation.deliberation_id)

    suspend = exc_info.value
    assert suspend.deliberation_id == deliberation.deliberation_id
    assert suspend.expected_responders == {"thea"}

    # Test serialization roundtrip
    serialized = adapter.serialize_context(suspend)
    deserialized = adapter.deserialize_context(serialized)

    assert deserialized.deliberation_id == deliberation.deliberation_id
    assert deserialized.expected_responders == {"thea"}
