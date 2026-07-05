"""Pydantic v2 data models for neurogossip-agent session management."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal
from pydantic import BaseModel, Field


class SessionStatus(str, Enum):
    ACTIVE = "active"
    WAITING_FOR_HUMAN = "waiting_for_human"
    COMPLETED = "completed"
    FAILED = "failed"


class RequestStatus(str, Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


class ConversationSession(BaseModel):
    conversation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    initiator_id: str
    status: SessionStatus = SessionStatus.ACTIVE
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AgentRequest(BaseModel):
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    conversation_id: str
    parent_request_id: str | None = None
    sender_id: str
    recipient_ids: list[str] = Field(default_factory=list)
    status: RequestStatus = RequestStatus.PENDING
    payload: Any
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None


class AgentResponse(BaseModel):
    response_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    request_id: str
    conversation_id: str
    sender_id: str  # The agent replying
    recipient_id: str  # The original request sender
    payload: Any
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class HistoryEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    conversation_id: str
    type: Literal["session_started", "request_sent", "response_received", "status_report", "session_completed", "session_failed"]
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    sender_id: str | None = None
    recipient_id: str | None = None
    ref_id: str | None = None  # Reference to request_id or response_id
    content: Any = None


class AgentSessionContext(BaseModel):
    conversation_id: str
    last_accessed: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    local_state: dict[str, Any] = Field(default_factory=dict)

