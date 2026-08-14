"""Unit tests for BoundsEnforcer."""

from uuid import uuid4

import pytest

from neurogossip_v3.bounds import BoundsEnforcer
from neurogossip_v3.models import (
    Contribution,
    ContributionKind,
    Deliberation,
    DeliberationBounds,
)
from neurogossip_v3.state_store import InMemoryStateStore


@pytest.mark.asyncio
async def test_bounds_turn_limit_and_circularity():
    store = InMemoryStateStore()
    del_id = uuid4()

    bounds = DeliberationBounds(
        max_turns=3,
        auto_interrupt_on_loop=True,
        min_contribution_length_for_circularity_check=10,
    )
    deliberation = Deliberation(
        deliberation_id=del_id,
        goal="Test bounds",
        initiator_id="skye",
        participants={"skye"},
        bounds=bounds,
    )
    await store.create_deliberation(deliberation)

    enforcer = BoundsEnforcer(store)

    # 1. Turn 1
    c1 = Contribution(
        deliberation_id=del_id,
        sender_id="skye",
        kind=ContributionKind.STATEMENT,
        content="This is a long statement content for testing bounds.",
    )
    await store.append_contribution(c1)
    assert await enforcer.check_bounds(del_id) == []

    # 2. Turn 2 - Circular (exact same long statement)
    c2 = Contribution(
        deliberation_id=del_id,
        sender_id="skye",
        kind=ContributionKind.STATEMENT,
        content="This is a long statement content for testing bounds.",
    )
    await store.append_contribution(c2)
    violations = await enforcer.check_bounds(del_id)
    assert any("circularity" in v for v in violations)

    # 3. Turn 3 & 4 - Max turns
    c3 = Contribution(
        deliberation_id=del_id,
        sender_id="skye",
        kind=ContributionKind.STATEMENT,
        content="Different content completely.",
    )
    await store.append_contribution(c3)
    c4 = Contribution(
        deliberation_id=del_id,
        sender_id="skye",
        kind=ContributionKind.STATEMENT,
        content="Another unique message.",
    )
    await store.append_contribution(c4)

    violations_turns = await enforcer.check_bounds(del_id)
    assert any("max_turns" in v for v in violations_turns)


@pytest.mark.asyncio
async def test_short_contributions_ignored_by_circularity():
    store = InMemoryStateStore()
    del_id = uuid4()
    bounds = DeliberationBounds(
        max_turns=10,
        auto_interrupt_on_loop=True,
        min_contribution_length_for_circularity_check=20,
    )
    deliberation = Deliberation(
        deliberation_id=del_id,
        goal="Test short msgs",
        initiator_id="skye",
        participants={"skye"},
        bounds=bounds,
    )
    await store.create_deliberation(deliberation)
    enforcer = BoundsEnforcer(store)

    # Short repeated messages (< 20 chars)
    for _ in range(3):
        c = Contribution(
            deliberation_id=del_id,
            sender_id="skye",
            kind=ContributionKind.STATEMENT,
            content="Vote yes",  # 8 chars < 20 chars
        )
        await store.append_contribution(c)

    violations = await enforcer.check_bounds(del_id)
    assert not any("circularity" in v for v in violations)
