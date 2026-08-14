"""Unit tests for VotingSubsystem."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from neurogossip_v3.errors import InvalidChoiceError, VoteClosedError
from neurogossip_v3.manager import DeliberationManager
from neurogossip_v3.models import DeliberationStatus, VoteStatus
from neurogossip_v3.state_store import InMemoryStateStore
from neurogossip_v3.transport import WebSocketAgentTransport
from neurogossip_v3.voting import VotingSubsystem


@pytest.fixture
def mock_transport():
    transport = WebSocketAgentTransport("ws://localhost:8765", "skye")
    transport._client.send = AsyncMock(return_value="msg-123")
    return transport


@pytest.mark.asyncio
async def test_voting_lifecycle(mock_transport):
    store = InMemoryStateStore()
    manager = DeliberationManager("skye", store, mock_transport)
    voting = VotingSubsystem("skye", store, mock_transport)

    deliberation = await manager.start_deliberation(
        goal="Vote proposal test",
        participants={"skye", "thea"},
    )

    # Propose vote
    session = await voting.propose_vote(
        deliberation_id=deliberation.deliberation_id,
        proposal="Accept design v3",
        options=["yes", "no"],
        threshold=0.5,
    )
    assert session.status == VoteStatus.OPEN

    d = await store.get_deliberation(deliberation.deliberation_id)
    assert d.status == DeliberationStatus.VOTING

    # Cast invalid choice
    with pytest.raises(InvalidChoiceError):
        await voting.cast_vote(session.vote_id, "maybe")

    # Skye votes yes
    session = await voting.cast_vote(session.vote_id, "yes")
    assert "skye" in session.votes

    # Thea votes yes (subsystem with thea's agent_id)
    thea_voting = VotingSubsystem("thea", store, mock_transport)
    session = await thea_voting.cast_vote(session.vote_id, "yes")

    # All voted -> closed and passes
    assert session.status == VoteStatus.CLOSED
    assert session.result.passed is True
    assert session.result.outcome == "yes"

    d = await store.get_deliberation(deliberation.deliberation_id)
    assert d.status == DeliberationStatus.ACTIVE
