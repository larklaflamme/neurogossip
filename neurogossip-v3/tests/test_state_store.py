"""Unit tests for StateStore and InMemoryStateStore."""

import asyncio
from uuid import uuid4

import pytest

from neurogossip_v3.errors import (
    AlreadyExistsError,
    DeliberationNotFoundError,
    GroupAlreadyExistsError,
)
from neurogossip_v3.models import (
    Contribution,
    ContributionKind,
    Deliberation,
    DeliberationStatus,
    Group,
    VoteSession,
)
from neurogossip_v3.state_store import InMemoryStateStore


@pytest.mark.asyncio
async def test_deliberation_crud():
    store = InMemoryStateStore()
    del_id = uuid4()
    deliberation = Deliberation(
        deliberation_id=del_id,
        goal="Test Goal",
        initiator_id="skye",
        participants={"skye", "thea"},
    )

    # Create
    await store.create_deliberation(deliberation)
    fetched = await store.get_deliberation(del_id)
    assert fetched is not None
    assert fetched.goal == "Test Goal"

    # Already exists error
    with pytest.raises(AlreadyExistsError):
        await store.create_deliberation(deliberation)

    # Update
    deliberation.status = DeliberationStatus.ACTIVE
    await store.update_deliberation(deliberation)
    fetched_updated = await store.get_deliberation(del_id)
    assert fetched_updated.status == DeliberationStatus.ACTIVE

    # List
    active_dels = await store.list_deliberations(agent_id="skye", status=DeliberationStatus.ACTIVE)
    assert len(active_dels) == 1

    # Delete
    await store.delete_deliberation(del_id)
    assert await store.get_deliberation(del_id) is None


@pytest.mark.asyncio
async def test_contribution_log_and_pagination():
    store = InMemoryStateStore()
    del_id = uuid4()
    deliberation = Deliberation(
        deliberation_id=del_id,
        goal="Test Log",
        initiator_id="skye",
        participants={"skye"},
    )
    await store.create_deliberation(deliberation)

    contrib_ids = []
    for i in range(5):
        c = Contribution(
            deliberation_id=del_id,
            sender_id="skye",
            kind=ContributionKind.STATEMENT,
            content=f"Message {i}",
        )
        await store.append_contribution(c)
        contrib_ids.append(c.contribution_id)
        assert c.sequence_number == i + 1

    # Fetch all
    all_contribs = await store.get_contributions(del_id, limit=10)
    assert len(all_contribs) == 5

    # Fetch with limit
    sub_contribs = await store.get_contributions(del_id, limit=3)
    assert len(sub_contribs) == 3
    assert sub_contribs[-1].content == "Message 4"

    # Fetch before_contribution_id
    before_c3 = await store.get_contributions(
        del_id, limit=10, before_contribution_id=contrib_ids[3]
    )
    assert len(before_c3) == 3
    assert [c.content for c in before_c3] == ["Message 0", "Message 1", "Message 2"]


@pytest.mark.asyncio
async def test_vote_session_crud():
    store = InMemoryStateStore()
    del_id = uuid4()
    vote_id = uuid4()
    vote_session = VoteSession(
        vote_id=vote_id,
        deliberation_id=del_id,
        proposal="Pass motion",
        options=["yes", "no"],
    )

    await store.create_vote_session(vote_session)
    fetched = await store.get_vote_session(vote_id)
    assert fetched is not None
    assert fetched.proposal == "Pass motion"

    vote_session.proposal = "Updated motion"
    await store.update_vote_session(vote_session)
    fetched_updated = await store.get_vote_session(vote_id)
    assert fetched_updated.proposal == "Updated motion"


@pytest.mark.asyncio
async def test_group_crud():
    store = InMemoryStateStore()
    group_id = uuid4()
    group = Group(
        group_id=group_id,
        name="Sisters",
        members={"skye", "thea", "theoria", "axioma"},
    )

    await store.create_group(group)
    fetched = await store.get_group(group_id)
    assert fetched is not None
    assert fetched.name == "Sisters"

    # Name collision error
    group_col = Group(name="Sisters")
    with pytest.raises(GroupAlreadyExistsError):
        await store.create_group(group_col)

    groups = await store.list_groups()
    assert len(groups) == 1

    await store.delete_group(group_id)
    assert await store.get_group(group_id) is None


@pytest.mark.asyncio
async def test_cursor_tracking():
    store = InMemoryStateStore()
    del_id = uuid4()
    c_id = uuid4()

    assert await store.get_agent_cursor("skye", del_id) is None
    await store.set_agent_cursor("skye", del_id, c_id)
    assert await store.get_agent_cursor("skye", del_id) == c_id


@pytest.mark.asyncio
async def test_concurrent_appends():
    store = InMemoryStateStore()
    del_id = uuid4()
    deliberation = Deliberation(
        deliberation_id=del_id,
        goal="Concurrent Test",
        initiator_id="skye",
        participants={"skye"},
    )
    await store.create_deliberation(deliberation)

    async def worker(worker_id: int):
        for i in range(10):
            c = Contribution(
                deliberation_id=del_id,
                sender_id=f"worker_{worker_id}",
                kind=ContributionKind.STATEMENT,
                content=f"Worker {worker_id} msg {i}",
            )
            await store.append_contribution(c)

    await asyncio.gather(worker(1), worker(2), worker(3))
    contribs = await store.get_contributions(del_id, limit=100)
    assert len(contribs) == 30
    seqs = [c.sequence_number for c in contribs]
    assert seqs == list(range(1, 31))
