"""Unit tests for GroupManager."""

from unittest.mock import AsyncMock

import pytest

from neurogossip_v3.errors import AlreadyMemberError, NotMemberError
from neurogossip_v3.group_manager import GroupManager
from neurogossip_v3.state_store import InMemoryStateStore
from neurogossip_v3.transport import WebSocketAgentTransport


@pytest.fixture
def mock_transport():
    transport = WebSocketAgentTransport("ws://localhost:8765", "skye")
    transport._client.send = AsyncMock(return_value="msg-123")
    return transport


@pytest.mark.asyncio
async def test_group_lifecycle(mock_transport):
    store = InMemoryStateStore()
    gm = GroupManager("skye", store, mock_transport)

    group = await gm.create_group("Research Team", members={"thea"})
    assert group.name == "Research Team"
    assert group.members == {"skye", "thea"}

    with pytest.raises(AlreadyMemberError):
        await gm.join_group(group.group_id)

    # Thea leaves group
    thea_gm = GroupManager("thea", store, mock_transport)
    updated_group = await thea_gm.leave_group(group.group_id)
    assert updated_group.members == {"skye"}

    with pytest.raises(NotMemberError):
        await thea_gm.leave_group(group.group_id)
