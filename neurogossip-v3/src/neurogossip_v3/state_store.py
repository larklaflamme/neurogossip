"""Abstract state store interface and InMemoryStateStore implementation."""

import asyncio
from abc import ABC, abstractmethod
from typing import Optional
from uuid import UUID

from neurogossip_v3.errors import (
    AlreadyExistsError,
    ContributionNotFoundError,
    DeliberationNotFoundError,
    GroupAlreadyExistsError,
    GroupNotFoundError,
    VoteNotFoundError,
)
from neurogossip_v3.models import (
    Contribution,
    Deliberation,
    DeliberationStatus,
    Group,
    VoteSession,
)


class StateStore(ABC):
    """Abstract interface for deliberation state storage."""

    # Deliberation CRUD
    @abstractmethod
    async def create_deliberation(self, deliberation: Deliberation) -> None:
        pass

    @abstractmethod
    async def get_deliberation(self, deliberation_id: UUID) -> Optional[Deliberation]:
        pass

    @abstractmethod
    async def update_deliberation(self, deliberation: Deliberation) -> None:
        pass

    @abstractmethod
    async def delete_deliberation(self, deliberation_id: UUID) -> None:
        pass

    @abstractmethod
    async def list_deliberations(
        self, agent_id: Optional[str] = None, status: Optional[DeliberationStatus] = None
    ) -> list[Deliberation]:
        pass

    # Contribution Log
    @abstractmethod
    async def append_contribution(self, contribution: Contribution) -> None:
        pass

    @abstractmethod
    async def get_contributions(
        self,
        deliberation_id: UUID,
        limit: int = 100,
        before_contribution_id: Optional[UUID] = None,
    ) -> list[Contribution]:
        pass

    @abstractmethod
    async def get_contribution(self, contribution_id: UUID) -> Optional[Contribution]:
        pass

    # Vote Sessions
    @abstractmethod
    async def create_vote_session(self, vote_session: VoteSession) -> None:
        pass

    @abstractmethod
    async def get_vote_session(self, vote_id: UUID) -> Optional[VoteSession]:
        pass

    @abstractmethod
    async def update_vote_session(self, vote_session: VoteSession) -> None:
        pass

    # Group CRUD
    @abstractmethod
    async def create_group(self, group: Group) -> None:
        pass

    @abstractmethod
    async def get_group(self, group_id: UUID) -> Optional[Group]:
        pass

    @abstractmethod
    async def update_group(self, group: Group) -> None:
        pass

    @abstractmethod
    async def delete_group(self, group_id: UUID) -> None:
        pass

    @abstractmethod
    async def list_groups(self) -> list[Group]:
        pass

    # Cursor Tracking
    @abstractmethod
    async def set_agent_cursor(
        self, agent_id: str, deliberation_id: UUID, last_seen_contribution_id: UUID
    ) -> None:
        pass

    @abstractmethod
    async def get_agent_cursor(
        self, agent_id: str, deliberation_id: UUID
    ) -> Optional[UUID]:
        pass


class InMemoryStateStore(StateStore):
    """Thread-safe in-memory implementation of StateStore using asyncio.Lock."""

    def __init__(self, namespace: str = "default"):
        self.namespace = namespace
        self._lock = asyncio.Lock()

        # Storage dicts
        self._deliberations: dict[UUID, Deliberation] = {}
        self._contributions_by_deliberation: dict[UUID, list[Contribution]] = {}
        self._contributions_by_id: dict[UUID, Contribution] = {}
        self._vote_sessions: dict[UUID, VoteSession] = {}
        self._groups: dict[UUID, Group] = {}
        self._agent_cursors: dict[tuple[str, UUID], UUID] = {}

    # Deliberation CRUD
    async def create_deliberation(self, deliberation: Deliberation) -> None:
        async with self._lock:
            if deliberation.deliberation_id in self._deliberations:
                raise AlreadyExistsError(
                    f"Deliberation {deliberation.deliberation_id} already exists"
                )
            self._deliberations[deliberation.deliberation_id] = deliberation
            self._contributions_by_deliberation[deliberation.deliberation_id] = []

    async def get_deliberation(self, deliberation_id: UUID) -> Optional[Deliberation]:
        async with self._lock:
            return self._deliberations.get(deliberation_id)

    async def update_deliberation(self, deliberation: Deliberation) -> None:
        async with self._lock:
            if deliberation.deliberation_id not in self._deliberations:
                raise DeliberationNotFoundError(
                    f"Deliberation {deliberation.deliberation_id} not found"
                )
            self._deliberations[deliberation.deliberation_id] = deliberation

    async def delete_deliberation(self, deliberation_id: UUID) -> None:
        async with self._lock:
            if deliberation_id in self._deliberations:
                del self._deliberations[deliberation_id]
            self._contributions_by_deliberation.pop(deliberation_id, None)

    async def list_deliberations(
        self, agent_id: Optional[str] = None, status: Optional[DeliberationStatus] = None
    ) -> list[Deliberation]:
        async with self._lock:
            results = list(self._deliberations.values())
            if agent_id is not None:
                results = [d for d in results if agent_id in d.participants]
            if status is not None:
                results = [d for d in results if d.status == status]
            return results

    # Contribution Log
    async def append_contribution(self, contribution: Contribution) -> None:
        async with self._lock:
            del_id = contribution.deliberation_id
            if del_id not in self._deliberations:
                raise DeliberationNotFoundError(f"Deliberation {del_id} not found")

            log = self._contributions_by_deliberation.setdefault(del_id, [])
            contribution.sequence_number = len(log) + 1
            log.append(contribution)
            self._contributions_by_id[contribution.contribution_id] = contribution

    async def get_contributions(
        self,
        deliberation_id: UUID,
        limit: int = 100,
        before_contribution_id: Optional[UUID] = None,
    ) -> list[Contribution]:
        async with self._lock:
            if deliberation_id not in self._deliberations:
                raise DeliberationNotFoundError(f"Deliberation {deliberation_id} not found")

            log = self._contributions_by_deliberation.get(deliberation_id, [])
            if not log:
                return []

            if before_contribution_id is not None:
                # Find index of before_contribution_id
                target_idx = None
                for i, c in enumerate(log):
                    if c.contribution_id == before_contribution_id:
                        target_idx = i
                        break
                if target_idx is None:
                    raise ContributionNotFoundError(
                        f"Contribution {before_contribution_id} not found"
                    )
                sub_log = log[:target_idx]
            else:
                sub_log = log

            return sub_log[-limit:] if len(sub_log) > limit else sub_log

    async def get_contribution(self, contribution_id: UUID) -> Optional[Contribution]:
        async with self._lock:
            return self._contributions_by_id.get(contribution_id)

    # Vote Sessions
    async def create_vote_session(self, vote_session: VoteSession) -> None:
        async with self._lock:
            if vote_session.vote_id in self._vote_sessions:
                raise AlreadyExistsError(f"Vote session {vote_session.vote_id} already exists")
            self._vote_sessions[vote_session.vote_id] = vote_session

    async def get_vote_session(self, vote_id: UUID) -> Optional[VoteSession]:
        async with self._lock:
            return self._vote_sessions.get(vote_id)

    async def update_vote_session(self, vote_session: VoteSession) -> None:
        async with self._lock:
            if vote_session.vote_id not in self._vote_sessions:
                raise VoteNotFoundError(f"Vote session {vote_session.vote_id} not found")
            self._vote_sessions[vote_session.vote_id] = vote_session

    # Group CRUD
    async def create_group(self, group: Group) -> None:
        async with self._lock:
            if group.group_id in self._groups:
                raise GroupAlreadyExistsError(f"Group {group.group_id} already exists")
            # Check name collision
            for g in self._groups.values():
                if g.name == group.name:
                    raise GroupAlreadyExistsError(f"Group named '{group.name}' already exists")
            self._groups[group.group_id] = group

    async def get_group(self, group_id: UUID) -> Optional[Group]:
        async with self._lock:
            return self._groups.get(group_id)

    async def update_group(self, group: Group) -> None:
        async with self._lock:
            if group.group_id not in self._groups:
                raise GroupNotFoundError(f"Group {group.group_id} not found")
            self._groups[group.group_id] = group

    async def delete_group(self, group_id: UUID) -> None:
        async with self._lock:
            if group_id in self._groups:
                del self._groups[group_id]

    async def list_groups(self) -> list[Group]:
        async with self._lock:
            return list(self._groups.values())

    # Cursor Tracking
    async def set_agent_cursor(
        self, agent_id: str, deliberation_id: UUID, last_seen_contribution_id: UUID
    ) -> None:
        async with self._lock:
            self._agent_cursors[(agent_id, deliberation_id)] = last_seen_contribution_id

    async def get_agent_cursor(
        self, agent_id: str, deliberation_id: UUID
    ) -> Optional[UUID]:
        async with self._lock:
            return self._agent_cursors.get((agent_id, deliberation_id))
