"""Integration and scenario tests for Neurogossip v3 deliberation protocol."""

import asyncio
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from neurogossip_v3.errors import InterruptedError, TimeoutError
from neurogossip_v3.group_manager import GroupManager
from neurogossip_v3.manager import DeliberationManager
from neurogossip_v3.models import (
    ContributionKind,
    DeliberationBounds,
    DeliberationStatus,
    ResolutionKind,
    VoteStatus,
)
from neurogossip_v3.state_store import InMemoryStateStore
from neurogossip_v3.transport import WebSocketAgentTransport
from neurogossip_v3.voting import VotingSubsystem


def create_agent(agent_id: str, store: InMemoryStateStore):
    transport = WebSocketAgentTransport("ws://localhost:8765", agent_id)
    transport._client.send = AsyncMock(return_value="msg-id")
    manager = DeliberationManager(agent_id, store, transport)
    voting = VotingSubsystem(agent_id, store, transport)
    group_mgr = GroupManager(agent_id, store, transport)
    return manager, voting, group_mgr, transport


@pytest.mark.asyncio
async def test_scenario_1_simple_deliberation():
    store = InMemoryStateStore()
    skye_mgr, _, _, _ = create_agent("skye", store)
    thea_mgr, _, _, _ = create_agent("thea", store)

    deliberation = await skye_mgr.start_deliberation(
        goal="Simple Deliberation Scenario",
        participants={"thea"},
    )
    assert deliberation.status == DeliberationStatus.ACTIVE

    # Skye makes statement
    c1 = await skye_mgr.contribute(
        deliberation_id=deliberation.deliberation_id,
        kind=ContributionKind.STATEMENT,
        content="Proposal A is submitted.",
    )
    # Thea critiques
    c2 = await thea_mgr.contribute(
        deliberation_id=deliberation.deliberation_id,
        kind=ContributionKind.CRITIQUE,
        content="Proposal A lacks error handling.",
        reply_to=c1.contribution_id,
    )
    # Skye clarifies
    c3 = await skye_mgr.contribute(
        deliberation_id=deliberation.deliberation_id,
        kind=ContributionKind.CLARIFICATION,
        content="Added try-catch block to Proposal A.",
        reply_to=c2.contribution_id,
    )

    # Skye resolves
    res = await skye_mgr.resolve(
        deliberation_id=deliberation.deliberation_id,
        kind=ResolutionKind.RESOLVED,
        summary="Proposal A accepted with error handling.",
    )
    assert res.kind == ResolutionKind.RESOLVED

    d = await store.get_deliberation(deliberation.deliberation_id)
    assert d.status == DeliberationStatus.RESOLVED


@pytest.mark.asyncio
async def test_scenario_2_response_waiting():
    store = InMemoryStateStore()
    skye_mgr, _, _, _ = create_agent("skye", store)
    thea_mgr, _, _, _ = create_agent("thea", store)

    deliberation = await skye_mgr.start_deliberation(
        goal="Response Waiting Scenario",
        participants={"thea"},
    )

    await skye_mgr.contribute(
        deliberation_id=deliberation.deliberation_id,
        kind=ContributionKind.QUESTION,
        content="Do you agree with Lemma 2?",
        expects_response_from={"thea"},
    )

    d = await store.get_deliberation(deliberation.deliberation_id)
    assert d.status == DeliberationStatus.WAITING

    async def delayed_response():
        await asyncio.sleep(0.05)
        await thea_mgr.contribute(
            deliberation_id=deliberation.deliberation_id,
            kind=ContributionKind.STATEMENT,
            content="Yes, I agree.",
        )

    task = asyncio.create_task(delayed_response())
    responses = await skye_mgr.wait_for_responses(
        deliberation_id=deliberation.deliberation_id,
        timeout_seconds=2.0,
    )
    await task

    assert len(responses) == 1
    assert responses[0].sender_id == "thea"

    d_after = await store.get_deliberation(deliberation.deliberation_id)
    assert d_after.status == DeliberationStatus.ACTIVE


@pytest.mark.asyncio
async def test_scenario_3_group_deliberation():
    store = InMemoryStateStore()
    skye_mgr, skye_voting, skye_gm, _ = create_agent("skye", store)
    thea_mgr, thea_voting, _, _ = create_agent("thea", store)
    theoria_mgr, theoria_voting, _, _ = create_agent("theoria", store)

    group = await skye_gm.create_group("Sisters", members={"thea", "theoria"})

    deliberation = await skye_mgr.start_deliberation(
        goal="Group Deliberation Scenario",
        participants=group.members,
    )
    assert deliberation.participants == {"skye", "thea", "theoria"}

    # Propose vote
    session = await skye_voting.propose_vote(
        deliberation_id=deliberation.deliberation_id,
        proposal="Publish finding",
        options=["yes", "no"],
    )

    await skye_voting.cast_vote(session.vote_id, "yes")
    await thea_voting.cast_vote(session.vote_id, "yes")
    session = await theoria_voting.cast_vote(session.vote_id, "no")

    assert session.status == VoteStatus.CLOSED
    assert session.result.passed is True
    assert session.result.outcome == "yes"


@pytest.mark.asyncio
async def test_scenario_4_interruption():
    store = InMemoryStateStore()
    skye_mgr, _, _, _ = create_agent("skye", store)
    thea_mgr, _, _, _ = create_agent("thea", store)

    deliberation = await skye_mgr.start_deliberation(
        goal="Interruption Scenario",
        participants={"thea"},
    )

    await thea_mgr.interrupt(
        deliberation_id=deliberation.deliberation_id,
        reason="Diverging from original goal",
    )

    d = await store.get_deliberation(deliberation.deliberation_id)
    assert d.status == DeliberationStatus.INTERRUPTED
    assert "Diverging" in d.resolution.summary


@pytest.mark.asyncio
async def test_scenario_5_bounds_max_turns():
    store = InMemoryStateStore()
    bounds = DeliberationBounds(max_turns=3)
    skye_mgr, _, _, _ = create_agent("skye", store)
    thea_mgr, _, _, _ = create_agent("thea", store)

    deliberation = await skye_mgr.start_deliberation(
        goal="Max Turns Scenario",
        participants={"thea"},
        bounds=bounds,
    )

    await skye_mgr.contribute(deliberation.deliberation_id, ContributionKind.STATEMENT, "Msg 1")
    await thea_mgr.contribute(deliberation.deliberation_id, ContributionKind.STATEMENT, "Msg 2")
    await skye_mgr.contribute(deliberation.deliberation_id, ContributionKind.STATEMENT, "Msg 3")

    # Turn 4 exceeds max_turns=3 -> auto-interrupt
    await thea_mgr.contribute(deliberation.deliberation_id, ContributionKind.STATEMENT, "Msg 4")

    d = await store.get_deliberation(deliberation.deliberation_id)
    assert d.status == DeliberationStatus.INTERRUPTED
    assert "max_turns" in d.resolution.summary


@pytest.mark.asyncio
async def test_scenario_6_bounds_circularity():
    store = InMemoryStateStore()
    bounds = DeliberationBounds(
        max_turns=10,
        auto_interrupt_on_loop=True,
        min_contribution_length_for_circularity_check=10,
    )
    skye_mgr, _, _, _ = create_agent("skye", store)

    deliberation = await skye_mgr.start_deliberation(
        goal="Circularity Scenario",
        participants={"skye"},
        bounds=bounds,
    )

    # Repeating same long statement
    await skye_mgr.contribute(deliberation.deliberation_id, ContributionKind.STATEMENT, "Identical content for circularity test")
    await skye_mgr.contribute(deliberation.deliberation_id, ContributionKind.STATEMENT, "Identical content for circularity test")

    d = await store.get_deliberation(deliberation.deliberation_id)
    assert d.status == DeliberationStatus.INTERRUPTED
    assert "circularity" in d.resolution.summary


@pytest.mark.asyncio
async def test_scenario_7_catch_up():
    store = InMemoryStateStore()
    skye_mgr, _, _, _ = create_agent("skye", store)
    thea_mgr, _, _, _ = create_agent("thea", store)

    deliberation = await skye_mgr.start_deliberation(
        goal="Catch up Scenario",
        participants={"thea"},
    )

    await skye_mgr.contribute(deliberation.deliberation_id, ContributionKind.STATEMENT, "Item A")
    await skye_mgr.contribute(deliberation.deliberation_id, ContributionKind.STATEMENT, "Item B")

    # Thea catches up
    missed = await thea_mgr.catch_up(deliberation.deliberation_id)
    assert len(missed) == 2

    # Second catch up -> nothing missed
    missed_again = await thea_mgr.catch_up(deliberation.deliberation_id)
    assert len(missed_again) == 0


@pytest.mark.asyncio
async def test_scenario_8_timeout_return_partial():
    store = InMemoryStateStore()
    skye_mgr, _, _, _ = create_agent("skye", store)

    deliberation = await skye_mgr.start_deliberation(
        goal="Timeout Scenario",
        participants={"thea"},
    )

    await skye_mgr.contribute(
        deliberation_id=deliberation.deliberation_id,
        kind=ContributionKind.QUESTION,
        content="Waiting for offline agent",
        expects_response_from={"thea"},
    )

    # Thea never replies, Skye waits with return_partial
    responses = await skye_mgr.wait_for_responses(
        deliberation_id=deliberation.deliberation_id,
        timeout_seconds=0.1,
        timeout_policy="return_partial",
    )
    assert len(responses) == 0
    d = await store.get_deliberation(deliberation.deliberation_id)
    assert d.status == DeliberationStatus.ACTIVE


@pytest.mark.asyncio
async def test_scenario_9_voting_deadlock():
    store = InMemoryStateStore()
    skye_mgr, skye_voting, _, _ = create_agent("skye", store)
    thea_mgr, thea_voting, _, _ = create_agent("thea", store)

    deliberation = await skye_mgr.start_deliberation(
        goal="Voting Deadlock Scenario",
        participants={"thea"},
    )

    session = await skye_voting.propose_vote(
        deliberation_id=deliberation.deliberation_id,
        proposal="50-50 Split",
        options=["yes", "no"],
        threshold=0.9,  # Requires high consensus
    )

    await skye_voting.cast_vote(session.vote_id, "yes")
    session = await thea_voting.cast_vote(session.vote_id, "no")

    assert session.status == VoteStatus.CLOSED
    assert session.result.passed is False


@pytest.mark.asyncio
async def test_scenario_10_decline_invitation():
    store = InMemoryStateStore()
    skye_mgr, _, _, _ = create_agent("skye", store)
    thea_mgr, _, _, _ = create_agent("thea", store)

    deliberation = await skye_mgr.start_deliberation(
        goal="Decline Scenario",
        participants={"thea"},
    )

    await thea_mgr.decline_invitation(deliberation.deliberation_id, "Busy with other proof")

    d = await store.get_deliberation(deliberation.deliberation_id)
    assert "thea" not in d.participants
