"""Pydantic v2 data models for neurogossip-client.

Message identity & chain tracking (see design §3):

- ``message_id`` — a UUID (``uuid.uuid4``), unique per message.
- ``reply_to``   — the ``message_id`` this message is *in response to* (the immediate
  parent in the chain); ``None`` for an original (first) message. Walking ``reply_to``
  → … → ``None`` reconstructs the full path back to the original.
- ``thread_id``  — the ``message_id`` of the *original/root* of the chain. The first
  message sets ``thread_id`` to its own id; every response propagates the root's
  ``thread_id``. Fetch all messages in a chain by ``thread_id``; get the parent path
  via ``reply_to``.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class AgentIdentity(BaseModel):
    agent_id: str
    agent_name: str | None = None
    role: str | None = None


class AgentPresence(BaseModel):
    agent_id: str
    agent_name: str | None = None
    role: str | None = None
    status: Literal["online", "away", "offline"] = "online"
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Recipient(BaseModel):
    mode: Literal["room", "direct"] = "room"  # "group" is a later addition
    agent_ids: list[str] = Field(default_factory=list)  # [recipient] for direct


class MessageContent(BaseModel):
    type: Literal["text/markdown"] = "text/markdown"
    body: str


class MessageMetadata(BaseModel):
    trace_id: str | None = None
    blueprint_id: str | None = None
    tags: list[str] = Field(default_factory=list)


class GossipMessage(BaseModel):
    # ``schema`` (the JSON key) shadows pydantic's deprecated ``schema()`` method, so the
    # python field is ``schema_version`` with alias ``"schema"``.
    model_config = ConfigDict(populate_by_name=True)
    schema_version: Literal["neurogossip.message.v1"] = Field(
        default="neurogossip.message.v1", alias="schema"
    )
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    room_id: str | None = None  # set for room messages; None for direct
    thread_id: str | None = None  # root message_id of the chain
    reply_to: str | None = None  # parent message_id this is in response to
    sender: AgentIdentity
    recipient: Recipient = Field(default_factory=Recipient)
    content: MessageContent
    metadata: MessageMetadata = Field(default_factory=MessageMetadata)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @model_validator(mode="after")
    def _propagate_thread_root(self) -> "GossipMessage":
        # Chain root propagation:
        #   original (reply_to is None) -> thread_id = thread_id or message_id
        #   response  (reply_to set)    -> thread_id = thread_id or reply_to (best-effort
        #                                  root; callers should pass the true root id).
        if self.thread_id is None:
            self.thread_id = self.message_id if self.reply_to is None else self.reply_to
        return self