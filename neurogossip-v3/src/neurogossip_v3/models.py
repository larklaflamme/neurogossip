"""Pydantic v2 data models for Neurogossip v3 deliberation protocol."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class DeliberationStatus(str, Enum):
    FORMING = "forming"
    ACTIVE = "active"
    WAITING = "waiting"
    VOTING = "voting"
    RESOLVING = "resolving"
    # Terminal states:
    RESOLVED = "resolved"
    DEADLOCKED = "deadlocked"
    TIMED_OUT = "timed_out"
    INTERRUPTED = "interrupted"
    ABANDONED = "abandoned"


class ContributionKind(str, Enum):
    STATEMENT = "statement"
    QUESTION = "question"
    PROPOSAL = "proposal"
    COUNTERPROPOSAL = "counterproposal"
    CRITIQUE = "critique"
    CLARIFICATION = "clarification"
    VOTE = "vote"
    SUMMARY = "summary"
    INTERRUPT = "interrupt"
    ACK = "ack"
    DECLINE = "decline"


class ResolutionKind(str, Enum):
    RESOLVED = "resolved"
    DEADLOCKED = "deadlocked"
    TIMED_OUT = "timed_out"
    INTERRUPTED = "interrupted"
    ABANDONED = "abandoned"


class VoteStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"
    TIMED_OUT = "timed_out"


class ExecutionMode(str, Enum):
    ASYNC = "async"
    TURN_BASED = "turn_based"


class Reference(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ref_type: str  # "noema" | "file" | "url" | "ere"
    ref_id: str  # actual id/path/url
    description: Optional[str] = None


class DeliberationBounds(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_turns: int = 100
    max_duration_s: float = 3600.0
    max_divergence: float = 0.7
    require_consensus: bool = False
    voting_threshold: float = 0.5
    auto_interrupt_on_loop: bool = True
    min_contribution_length_for_circularity_check: int = 20


class DeliberationGoal(BaseModel):
    model_config = ConfigDict(extra="forbid")

    description: str
    success_criteria: Optional[str] = None
    require_consensus: bool = False


class Contribution(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contribution_id: UUID = Field(default_factory=uuid4)
    deliberation_id: UUID
    sender_id: str
    kind: ContributionKind
    content: str
    reply_to: Optional[UUID] = None
    thread_id: Optional[UUID] = None
    addressed_to: set[str] = Field(default_factory=set)
    expects_response_from: set[str] = Field(default_factory=set)
    confidence: Optional[float] = None
    references: list[Reference] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    sequence_number: int = 0


class DissentingOpinion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent_id: str
    statement: str
    contribution_id: Optional[UUID] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Resolution(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resolution_id: UUID = Field(default_factory=uuid4)
    deliberation_id: UUID
    kind: ResolutionKind
    summary: str
    conclusion: Optional[str] = None
    dissenting_opinions: list[DissentingOpinion] = Field(default_factory=list)
    resolved_by: str
    resolved_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Group(BaseModel):
    model_config = ConfigDict(extra="forbid")

    group_id: UUID = Field(default_factory=uuid4)
    name: str
    description: Optional[str] = None
    members: set[str] = Field(default_factory=set)
    created_by: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = Field(default_factory=dict)


class Vote(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent_id: str
    choice: str
    rationale: Optional[str] = None
    cast_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class VoteResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    outcome: str
    tally: dict[str, int] = Field(default_factory=dict)
    passed: bool = False


class VoteSession(BaseModel):
    model_config = ConfigDict(extra="forbid")

    vote_id: UUID = Field(default_factory=uuid4)
    deliberation_id: UUID
    proposal: str
    options: list[str]
    threshold: float = 0.5
    timeout_s: float = 300.0
    status: VoteStatus = VoteStatus.OPEN
    votes: dict[str, Vote] = Field(default_factory=dict)
    result: Optional[VoteResult] = None
    timeout_policy: str = "fail"  # "fail" | "pass" | "extend"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    closed_at: Optional[datetime] = None


class Deliberation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    deliberation_id: UUID = Field(default_factory=uuid4)
    goal: str
    initiator_id: str
    participants: set[str] = Field(default_factory=set)
    status: DeliberationStatus = DeliberationStatus.FORMING
    bounds: DeliberationBounds = Field(default_factory=DeliberationBounds)
    resolution: Optional[Resolution] = None
    parent_deliberation_id: Optional[UUID] = None
    async_child: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
