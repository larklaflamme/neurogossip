"""Unit tests for DeliberationManager."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from neurogossip_v3.errors import (
    DeliberationNotFoundError,
    InvalidKindError,
    InvalidStatusError,
    NotParticipantError,
    TimeoutError,
)
from neurogossip_v3.manager import DeliberationManager
from neurogossip_v3.models import (
    ContributionKind,
    DeliberationStatus,
    ExecutionMode,
    ResolutionKind,
)
from neurogossip_v3.state_store import InMemoryStateStore
from neurogossip_v3.transport import WebSocketAgentTransport


@pytest.fixture
def mock_transport():
    transport = WebSocketAgentTransport("ws://localhost:8765", "skye")
    transport._client.connect = AsyncMock()
    transport._client.disconnect = AsyncMock()
    transport._client.send = AsyncMock(return_value="msg-123")
    return transport


@pytest.mark.asyncio
async def test_start_and_contribute(mock_transport):
    store = InMemoryStateStore()
    manager = DeliberationManager("skye", store, mock_transport)

    deliberation = await manager.start_deliberation(
        goal="Verify RH lemma",
        participants={"thea"},
    )
    assert deliberation.status == DeliberationStatus.ACTIVE
    assert deliberation.participants == {"skye", "thea"}

    contrib = await manager.contribute(
        deliberation_id=deliberation.deliberation_id,
        kind=ContributionKind.STATEMENT,
        content="Lemma 1 holds under assumption A.",
    )
    assert contrib.sequence_number == 1
    assert contrib.sender_id == "skye"


@pytest.mark.asyncio
async def test_vote_kind_restriction(mock_transport):
    store = InMemoryStateStore()
    manager = DeliberationManager("skye", store, mock_transport)

    deliberation = await manager.start_deliberation(
        goal="Test Vote Restriction",
        participants={"thea"},
    )

    # In ACTIVE status, kind=VOTE must be rejected with InvalidKindError
    with pytest.raises(InvalidKindError):
        await manager.contribute(
            deliberation_id=deliberation.deliberation_id,
            kind=ContributionKind.VOTE,
            content="I vote yes",
        )


@pytest.mark.asyncio
async def test_wait_for_responses_async_mode(mock_transport):
    store = InMemoryStateStore()
    skye_mgr = DeliberationManager("skye", store, mock_transport)
    thea_mgr = DeliberationManager("thea", store, mock_transport)

    deliberation = await skye_mgr.start_deliberation(
        goal="Ask Question",
        participants={"thea"},
    )

    # Skye contributes a question expecting response from thea
    await skye_mgr.contribute(
        deliberation_id=deliberation.deliberation_id,
        kind=ContributionKind.QUESTION,
        content="Is lemma 1 valid?",
        expects_response_from={"thea"},
    )

    d = await store.get_deliberation(deliberation.deliberation_id)
    assert d.status == DeliberationStatus.WAITING

    # Background task for Thea's reply
    async def thea_reply():
        await asyncio.sleep(0.05)
        await thea_mgr.contribute(
            deliberation_id=deliberation.deliberation_id,
            kind=ContributionKind.STATEMENT,
            content="Yes, lemma 1 is valid.",
        )

    reply_task = asyncio.create_task(thea_reply())
    responses = await skye_mgr.wait_for_responses(
        deliberation_id=deliberation.deliberation_id,
        timeout_seconds=2.0,
    )
    await reply_task

    assert len(responses) == 1
    assert responses[0].sender_id == "thea"
    assert responses[0].content == "Yes, lemma 1 is valid."


@pytest.mark.asyncio
async def test_resolve_and_interrupt(mock_transport):
    store = InMemoryStateStore()
    manager = DeliberationManager("skye", store, mock_transport)

    deliberation = await manager.start_deliberation(
        goal="Resolve test",
        participants={"thea"},
    )

    res = await manager.resolve(
        deliberation_id=deliberation.deliberation_id,
        kind=ResolutionKind.RESOLVED,
        summary="Goal achieved.",
        conclusion="Proved.",
    )
    assert res.kind == ResolutionKind.RESOLVED

    d = await store.get_deliberation(deliberation.deliberation_id)
    assert d.status == DeliberationStatus.RESOLVED


@pytest.mark.asyncio
async def test_catch_up_and_summarize(mock_transport):
    store = InMemoryStateStore()
    manager = DeliberationManager("skye", store, mock_transport)

    deliberation = await manager.start_deliberation(
        goal="Summarize test",
        participants={"thea"},
    )
    await manager.contribute(
        deliberation_id=deliberation.deliberation_id,
        kind=ContributionKind.STATEMENT,
        content="First point.",
    )
    await manager.contribute(
        deliberation_id=deliberation.deliberation_id,
        kind=ContributionKind.STATEMENT,
        content="Second point.",
    )

    missed = await manager.catch_up(deliberation.deliberation_id)
    assert len(missed) == 2

    summary = await manager.summarize(deliberation.deliberation_id)
    assert "Summarize test" in summary
    assert "First point." in summary
