"""GroupManager for Neurogossip v3 deliberation protocol."""

from typing import Optional, Set
from uuid import UUID

from neurogossip_v3.errors import (
    AlreadyMemberError,
    GroupNotFoundError,
    NotMemberError,
)
from neurogossip_v3.models import Group
from neurogossip_v3.state_store import StateStore
from neurogossip_v3.transport import WebSocketAgentTransport


class GroupManager:
    """Manages persistent agent groups for shared deliberations."""

    def __init__(self, agent_id: str, state_store: StateStore, transport: WebSocketAgentTransport):
        self.agent_id = agent_id
        self.store = state_store
        self.transport = transport

    async def create_group(
        self, name: str, members: Optional[Set[str]] = None, description: Optional[str] = None
    ) -> Group:
        """Create a new group."""
        group_members = set(members or []) | {self.agent_id}
        group = Group(
            name=name,
            description=description,
            members=group_members,
            created_by=self.agent_id,
        )
        await self.store.create_group(group)
        return group

    async def get_group(self, group_id: UUID) -> Group:
        """Fetch group by ID."""
        group = await self.store.get_group(group_id)
        if not group:
            raise GroupNotFoundError(f"Group {group_id} not found")
        return group

    async def join_group(self, group_id: UUID) -> Group:
        """Add current agent to group."""
        group = await self.get_group(group_id)
        if self.agent_id in group.members:
            raise AlreadyMemberError(f"Agent {self.agent_id} is already in group {group_id}")
        group.members.add(self.agent_id)
        await self.store.update_group(group)
        return group

    async def leave_group(self, group_id: UUID) -> Group:
        """Remove current agent from group."""
        group = await self.get_group(group_id)
        if self.agent_id not in group.members:
            raise NotMemberError(f"Agent {self.agent_id} is not in group {group_id}")
        group.members.remove(self.agent_id)
        await self.store.update_group(group)
        return group

    async def list_groups(self) -> list[Group]:
        """List all groups."""
        return await self.store.list_groups()

    async def delete_group(self, group_id: UUID) -> None:
        """Delete a group."""
        group = await self.get_group(group_id)
        await self.store.delete_group(group.group_id)
