"""Tests for Pydantic v2 data models."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from neurogossip_v3.models import (
    Contribution,
    ContributionKind,
    Deliberation,
    DeliberationBounds,
    DeliberationGoal,
    DeliberationStatus,
    DissentingOpinion,
    ExecutionMode,
    Group,
    Reference,
    Resolution,
    ResolutionKind,
    Vote,
    VoteResult,
    VoteSession,
    VoteStatus,
)


def test_deliberation_bounds_defaults():
    bounds = DeliberationBounds()
    assert bounds.max_turns == 100
    assert bounds.max_duration_s == 3600.0
    assert bounds.max_divergence == 0.7
    assert bounds.require_consensus is False
    assert bounds.voting_threshold == 0.5
    assert bounds.auto_interrupt_on_loop is True
    assert bounds.min_contribution_length_for_circularity_check == 20


def test_deliberation_model_roundtrip():
    del_id = uuid4()
    deliberation = Deliberation(
        deliberation_id=del_id,
        goal="Determine RH proof validity",
        initiator_id="skye",
        participants={"skye", "thea", "theoria"},
        status=DeliberationStatus.FORMING,
    )
    dumped = deliberation.model_dump(mode="json")
    loaded = Deliberation.model_validate(dumped)
    assert loaded.deliberation_id == del_id
    assert loaded.goal == "Determine RH proof validity"
    assert loaded.initiator_id == "skye"
    assert loaded.participants == {"skye", "thea", "theoria"}
    assert loaded.status == DeliberationStatus.FORMING


def test_contribution_model_roundtrip():
    del_id = uuid4()
    contrib_id = uuid4()
    ref = Reference(ref_type="noema", ref_id="NOEMA-123", description="Math proof artifact")
    contrib = Contribution(
        contribution_id=contrib_id,
        deliberation_id=del_id,
        sender_id="theoria",
        kind=ContributionKind.STATEMENT,
        content="Lemma 1 is verified.",
        addressed_to={"skye"},
        expects_response_from={"skye"},
        confidence=0.95,
        references=[ref],
        sequence_number=1,
    )
    dumped = contrib.model_dump(mode="json")
    loaded = Contribution.model_validate(dumped)
    assert loaded.contribution_id == contrib_id
    assert loaded.deliberation_id == del_id
    assert loaded.sender_id == "theoria"
    assert loaded.kind == ContributionKind.STATEMENT
    assert loaded.content == "Lemma 1 is verified."
    assert loaded.addressed_to == {"skye"}
    assert loaded.expects_response_from == {"skye"}
    assert loaded.confidence == 0.95
    assert len(loaded.references) == 1
    assert loaded.references[0].ref_id == "NOEMA-123"


def test_vote_session_roundtrip():
    del_id = uuid4()
    vote_id = uuid4()
    vote = Vote(agent_id="axioma", choice="agree", rationale="Solid reasoning")
    session = VoteSession(
        vote_id=vote_id,
        deliberation_id=del_id,
        proposal="Accept proposed resolution",
        options=["agree", "disagree", "abstain"],
        threshold=0.5,
        status=VoteStatus.OPEN,
        votes={"axioma": vote},
    )
    dumped = session.model_dump(mode="json")
    loaded = VoteSession.model_validate(dumped)
    assert loaded.vote_id == vote_id
    assert loaded.proposal == "Accept proposed resolution"
    assert loaded.votes["axioma"].choice == "agree"


def test_extra_fields_forbidden():
    with pytest.raises(ValidationError):
        DeliberationBounds(max_turns=50, invalid_extra_field=123)
