"""DeliberationManager core engine for Neurogossip v3 deliberation protocol.

Implements all core operations: start_deliberation, contribute, wait_for_responses,
resolve, interrupt, decline_invitation, catch_up, summarize, list_deliberations.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Literal, Optional, Set
from uuid import UUID

from neurogossip_v3.bounds import BoundsEnforcer
from neurogossip_v3.errors import (
    ContributionNotFoundError,
    DeliberationNotFoundError,
    InterruptedError,
    InvalidKindError,
    InvalidStatusError,
    NotParticipantError,
    TimeoutError,
)
from neurogossip_v3.models import (
    Contribution,
    ContributionKind,
    Deliberation,
    DeliberationBounds,
    DeliberationStatus,
    DissentingOpinion,
    ExecutionMode,
    Reference,
    Resolution,
    ResolutionKind,
)
from neurogossip_v3.state_store import StateStore
from neurogossip_v3.transport import WebSocketAgentTransport
from neurogossip_v3.turn_adapter import SuspendTurn


class DeliberationManager:
    """Core deliberation engine managing multi-agent structured conversations."""

    def __init__(
        self,
        agent_id: str,
        state_store: StateStore,
        transport: WebSocketAgentTransport,
        execution_mode: ExecutionMode = ExecutionMode.ASYNC,
        default_bounds: Optional[DeliberationBounds] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.agent_id = agent_id
        self.store = state_store
        self.transport = transport
        self.execution_mode = execution_mode
        self.default_bounds = default_bounds or DeliberationBounds()
        self.logger = logger or logging.getLogger(f"v3_manager.{agent_id}")
        self.bounds_enforcer = BoundsEnforcer(self.store)

        # Register transport handlers
        self.transport.set_deliberation_handler(self._handle_incoming_contribution)
        self.transport.set_invitation_handler(self._handle_incoming_invitation)
        self.transport.set_vote_handler(self._handle_incoming_vote)

        # Active waiting events for ASYNC mode: dict[deliberation_id, asyncio.Event]
        self._wait_events: dict[UUID, asyncio.Event] = {}
        # Received responses per deliberation during wait
        self._received_responses: dict[UUID, list[Contribution]] = {}
        # Expected responders per deliberation
        self._pending_responders: dict[UUID, Set[str]] = {}

    # ------------------------------------------------------------------
    # Handlers for incoming transport messages
    # ------------------------------------------------------------------

    async def _handle_incoming_contribution(
        self, sender_id: str, payload: dict, msg_id: str, reply_to: Optional[str], conv_id: Optional[str]
    ) -> None:
        """Handler for incoming contribution payloads."""
        contrib_data = payload.get("contribution")
        if not contrib_data:
            return

        del_id_str = payload.get("deliberation_id") or conv_id
        if not del_id_str:
            return

        del_id = UUID(del_id_str)
        deliberation = await self.store.get_deliberation(del_id)
        if not deliberation:
            return

        try:
            contribution = Contribution.model_validate(contrib_data)
        except Exception as e:
            self.logger.error(f"Failed to parse contribution: {e}")
            return

        # Check if already stored (idempotency check)
        existing = await self.store.get_contribution(contribution.contribution_id)
        if not existing:
            await self.store.append_contribution(contribution)

        # Check waiting event for this deliberation
        if del_id in self._pending_responders:
            if sender_id in self._pending_responders[del_id]:
                self._pending_responders[del_id].remove(sender_id)
                self._received_responses.setdefault(del_id, []).append(contribution)

            if not self._pending_responders[del_id]:
                # All expected responders have replied
                event = self._wait_events.get(del_id)
                if event:
                    event.set()

        # Check bounds
        await self._check_and_enforce_bounds(del_id)

    async def _handle_incoming_invitation(
        self, sender_id: str, payload: dict, msg_id: str, conv_id: Optional[str]
    ) -> None:
        """Handler for incoming deliberation invitations."""
        del_id_str = payload.get("deliberation_id") or conv_id
        if not del_id_str:
            return

        del_id = UUID(del_id_str)
        goal = payload.get("goal", "")
        initiator_id = payload.get("initiator_id", sender_id)

        existing = await self.store.get_deliberation(del_id)
        if not existing:
            deliberation = Deliberation(
                deliberation_id=del_id,
                goal=goal,
                initiator_id=initiator_id,
                participants={initiator_id, self.agent_id},
                status=DeliberationStatus.FORMING,
            )
            await self.store.create_deliberation(deliberation)

    async def _handle_incoming_vote(
        self, sender_id: str, payload: dict, msg_id: str, conv_id: Optional[str]
    ) -> None:
        """Handler for incoming vote notifications (handled in voting module)."""
        pass

    # ------------------------------------------------------------------
    # Deliberation Operations
    # ------------------------------------------------------------------

    async def start_deliberation(
        self,
        goal: str,
        participants: Set[str],
        bounds: Optional[DeliberationBounds] = None,
        parent_deliberation_id: Optional[UUID] = None,
        async_child: bool = False,
        formation_timeout_seconds: float = 60.0,
    ) -> Deliberation:
        """Start a new deliberation with a set of participants."""
        all_participants = set(participants) | {self.agent_id}

        deliberation = Deliberation(
            deliberation_id=uuid4(),
            goal=goal,
            initiator_id=self.agent_id,
            participants=all_participants,
            status=DeliberationStatus.FORMING,
            bounds=bounds or self.default_bounds,
            parent_deliberation_id=parent_deliberation_id,
            async_child=async_child,
        )
        await self.store.create_deliberation(deliberation)

        # Send invitations to external participants
        for p in all_participants:
            if p != self.agent_id:
                await self.transport.send_invitation(
                    to=p,
                    deliberation_id=deliberation.deliberation_id,
                    goal=goal,
                    initiator_id=self.agent_id,
                )

        # Wait for formation timeout or transition immediately
        # In v3.0, transition to ACTIVE
        deliberation.status = DeliberationStatus.ACTIVE
        deliberation.updated_at = datetime.now(timezone.utc)
        await self.store.update_deliberation(deliberation)

        # If parent_deliberation_id is set and not async_child, handle parent state
        if parent_deliberation_id and not async_child:
            parent = await self.store.get_deliberation(parent_deliberation_id)
            if parent and parent.status == DeliberationStatus.ACTIVE:
                parent.status = DeliberationStatus.WAITING
                parent.updated_at = datetime.now(timezone.utc)
                await self.store.update_deliberation(parent)

        return deliberation

    async def contribute(
        self,
        deliberation_id: UUID,
        kind: ContributionKind,
        content: str,
        reply_to: Optional[UUID] = None,
        addressed_to: Optional[Set[str]] = None,
        expects_response_from: Optional[Set[str]] = None,
        confidence: Optional[float] = None,
        references: Optional[list[Reference]] = None,
    ) -> Contribution:
        """Add a contribution to an active deliberation."""
        deliberation = await self.store.get_deliberation(deliberation_id)
        if not deliberation:
            raise DeliberationNotFoundError(f"Deliberation {deliberation_id} not found")

        if self.agent_id not in deliberation.participants:
            raise NotParticipantError(f"Agent {self.agent_id} is not a participant")

        # Status validation
        if deliberation.status in (
            DeliberationStatus.RESOLVED,
            DeliberationStatus.DEADLOCKED,
            DeliberationStatus.TIMED_OUT,
            DeliberationStatus.INTERRUPTED,
            DeliberationStatus.ABANDONED,
        ):
            raise InvalidStatusError(f"Deliberation is in terminal status: {deliberation.status}")

        # VOTE kind validation (Addresses Thea F4 / Theoria M4)
        if kind == ContributionKind.VOTE and deliberation.status != DeliberationStatus.VOTING:
            raise InvalidKindError("Contribution kind VOTE is only allowed in VOTING status")

        # Reply validation
        if reply_to:
            parent_c = await self.store.get_contribution(reply_to)
            if not parent_c:
                raise ContributionNotFoundError(f"Parent contribution {reply_to} not found")

        contribution = Contribution(
            deliberation_id=deliberation_id,
            sender_id=self.agent_id,
            kind=kind,
            content=content,
            reply_to=reply_to,
            addressed_to=addressed_to or set(),
            expects_response_from=expects_response_from or set(),
            confidence=confidence,
            references=references or [],
        )

        await self.store.append_contribution(contribution)

        # Update deliberation status if expects_response_from is set
        if expects_response_from:
            deliberation.status = DeliberationStatus.WAITING
        else:
            deliberation.status = DeliberationStatus.ACTIVE

        deliberation.updated_at = datetime.now(timezone.utc)
        await self.store.update_deliberation(deliberation)

        # Broadcast contribution to participants
        payload = {
            "type": "deliberation",
            "deliberation_id": str(deliberation_id),
            "contribution": contribution.model_dump(mode="json"),
        }
        await self.transport.broadcast_to_participants(
            participants=deliberation.participants,
            deliberation_id=deliberation_id,
            message=payload,
        )

        # Bounds enforcement
        await self._check_and_enforce_bounds(deliberation_id)

        return contribution

    async def wait_for_responses(
        self,
        deliberation_id: UUID,
        timeout_seconds: float = 300.0,
        timeout_policy: Literal["fail", "return_partial", "extend"] = "fail",
    ) -> list[Contribution]:
        """Wait for expected responders to reply."""
        deliberation = await self.store.get_deliberation(deliberation_id)
        if not deliberation:
            raise DeliberationNotFoundError(f"Deliberation {deliberation_id} not found")

        # Fetch last contribution by this agent to find expected responders
        contribs = await self.store.get_contributions(deliberation_id, limit=10)
        last_my_contrib = None
        for c in reversed(contribs):
            if c.sender_id == self.agent_id and c.expects_response_from:
                last_my_contrib = c
                break

        if not last_my_contrib or not last_my_contrib.expects_response_from:
            return []

        expected = set(last_my_contrib.expects_response_from)

        # Handle TURN_BASED mode
        if self.execution_mode == ExecutionMode.TURN_BASED:
            raise SuspendTurn(
                deliberation_id=deliberation_id,
                expected_responders=expected,
                timeout_s=timeout_seconds,
            )

        # ASYNC mode using asyncio.Event
        event = asyncio.Event()
        self._wait_events[deliberation_id] = event
        self._pending_responders[deliberation_id] = expected.copy()
        self._received_responses[deliberation_id] = []

        try:
            await asyncio.wait_for(event.wait(), timeout=timeout_seconds)
            responses = self._received_responses.get(deliberation_id, [])

            # Reset deliberation status to ACTIVE
            deliberation.status = DeliberationStatus.ACTIVE
            deliberation.updated_at = datetime.now(timezone.utc)
            await self.store.update_deliberation(deliberation)

            return responses

        except asyncio.TimeoutError:
            if timeout_policy == "fail":
                deliberation.status = DeliberationStatus.TIMED_OUT
                deliberation.updated_at = datetime.now(timezone.utc)
                await self.store.update_deliberation(deliberation)
                raise TimeoutError(f"Wait for responses timed out after {timeout_seconds}s")
            elif timeout_policy == "return_partial":
                deliberation.status = DeliberationStatus.ACTIVE
                deliberation.updated_at = datetime.now(timezone.utc)
                await self.store.update_deliberation(deliberation)
                return self._received_responses.get(deliberation_id, [])
            elif timeout_policy == "extend":
                # Double timeout once
                try:
                    await asyncio.wait_for(event.wait(), timeout=timeout_seconds)
                    responses = self._received_responses.get(deliberation_id, [])
                    deliberation.status = DeliberationStatus.ACTIVE
                    await self.store.update_deliberation(deliberation)
                    return responses
                except asyncio.TimeoutError:
                    deliberation.status = DeliberationStatus.TIMED_OUT
                    await self.store.update_deliberation(deliberation)
                    raise TimeoutError(f"Extended wait timed out after {timeout_seconds * 2}s")

        finally:
            self._wait_events.pop(deliberation_id, None)
            self._pending_responders.pop(deliberation_id, None)

    async def resolve(
        self,
        deliberation_id: UUID,
        kind: ResolutionKind,
        summary: str,
        conclusion: Optional[str] = None,
        dissenting_opinions: Optional[list[DissentingOpinion]] = None,
    ) -> Resolution:
        """Resolve a deliberation with a conclusion."""
        deliberation = await self.store.get_deliberation(deliberation_id)
        if not deliberation:
            raise DeliberationNotFoundError(f"Deliberation {deliberation_id} not found")

        if self.agent_id not in deliberation.participants:
            raise NotParticipantError(f"Agent {self.agent_id} is not a participant")

        resolution = Resolution(
            deliberation_id=deliberation_id,
            kind=kind,
            summary=summary,
            conclusion=conclusion,
            dissenting_opinions=dissenting_opinions or [],
            resolved_by=self.agent_id,
        )

        deliberation.status = DeliberationStatus(kind.value)
        deliberation.resolution = resolution
        deliberation.updated_at = datetime.now(timezone.utc)
        await self.store.update_deliberation(deliberation)

        # Unblock any waiting events
        event = self._wait_events.get(deliberation_id)
        if event:
            event.set()

        # If parent deliberation was waiting on this child, unblock parent
        if deliberation.parent_deliberation_id and not deliberation.async_child:
            parent = await self.store.get_deliberation(deliberation.parent_deliberation_id)
            if parent and parent.status == DeliberationStatus.WAITING:
                parent.status = DeliberationStatus.ACTIVE
                parent.updated_at = datetime.now(timezone.utc)
                await self.store.update_deliberation(parent)

        # Broadcast resolution
        payload = {
            "type": "deliberation_resolution",
            "deliberation_id": str(deliberation_id),
            "resolution": resolution.model_dump(mode="json"),
        }
        await self.transport.broadcast_to_participants(
            participants=deliberation.participants,
            deliberation_id=deliberation_id,
            message=payload,
        )

        return resolution

    async def interrupt(self, deliberation_id: UUID, reason: str) -> Resolution:
        """Interrupt a running deliberation."""
        deliberation = await self.store.get_deliberation(deliberation_id)
        if not deliberation:
            raise DeliberationNotFoundError(f"Deliberation {deliberation_id} not found")

        if self.agent_id not in deliberation.participants:
            raise NotParticipantError(f"Agent {self.agent_id} is not a participant")

        resolution = Resolution(
            deliberation_id=deliberation_id,
            kind=ResolutionKind.INTERRUPTED,
            summary=f"Interrupted: {reason}",
            resolved_by=self.agent_id,
        )

        deliberation.status = DeliberationStatus.INTERRUPTED
        deliberation.resolution = resolution
        deliberation.updated_at = datetime.now(timezone.utc)
        await self.store.update_deliberation(deliberation)

        # Unblock waiting events
        event = self._wait_events.get(deliberation_id)
        if event:
            event.set()

        # Broadcast interruption
        payload = {
            "type": "deliberation_interrupted",
            "deliberation_id": str(deliberation_id),
            "reason": reason,
        }
        await self.transport.broadcast_to_participants(
            participants=deliberation.participants,
            deliberation_id=deliberation_id,
            message=payload,
        )

        return resolution

    async def decline_invitation(
        self, deliberation_id: UUID, reason: Optional[str] = None
    ) -> None:
        """Decline a deliberation invitation."""
        deliberation = await self.store.get_deliberation(deliberation_id)
        if not deliberation:
            raise DeliberationNotFoundError(f"Deliberation {deliberation_id} not found")

        if self.agent_id in deliberation.participants:
            deliberation.participants.remove(self.agent_id)

        if not deliberation.participants or self.agent_id == deliberation.initiator_id:
            deliberation.status = DeliberationStatus.ABANDONED

        deliberation.updated_at = datetime.now(timezone.utc)
        await self.store.update_deliberation(deliberation)

    async def catch_up(
        self,
        deliberation_id: UUID,
        limit: int = 100,
        before_contribution_id: Optional[UUID] = None,
    ) -> list[Contribution]:
        """Catch up on missed contributions since last cursor."""
        deliberation = await self.store.get_deliberation(deliberation_id)
        if not deliberation:
            raise DeliberationNotFoundError(f"Deliberation {deliberation_id} not found")

        cursor = await self.store.get_agent_cursor(self.agent_id, deliberation_id)
        contribs = await self.store.get_contributions(
            deliberation_id=deliberation_id,
            limit=limit,
            before_contribution_id=before_contribution_id,
        )

        if cursor:
            # Filter contributions after cursor
            missed = []
            found_cursor = False
            for c in contribs:
                if found_cursor:
                    missed.append(c)
                elif c.contribution_id == cursor:
                    found_cursor = True
            contribs_to_return = missed if found_cursor else contribs
        else:
            contribs_to_return = contribs

        if contribs_to_return:
            new_cursor = contribs_to_return[-1].contribution_id
            await self.store.set_agent_cursor(self.agent_id, deliberation_id, new_cursor)

        return contribs_to_return

    async def summarize(self, deliberation_id: UUID, max_length: int = 500) -> str:
        """Generate a natural language summary of the deliberation."""
        deliberation = await self.store.get_deliberation(deliberation_id)
        if not deliberation:
            raise DeliberationNotFoundError(f"Deliberation {deliberation_id} not found")

        contribs = await self.store.get_contributions(deliberation_id, limit=50)
        lines = [
            f"Deliberation Summary (ID: {deliberation_id}):",
            f"Goal: {deliberation.goal}",
            f"Status: {deliberation.status.value}",
            f"Participants: {', '.join(sorted(deliberation.participants))}",
            f"Total Contributions: {len(contribs)}",
            "Key Messages:",
        ]
        for c in contribs[-5:]:
            lines.append(f"  - [{c.sender_id} ({c.kind.value})]: {c.content[:100]}")

        summary_text = "\n".join(lines)
        return summary_text[:max_length]

    async def list_deliberations(
        self, status: Optional[DeliberationStatus] = None
    ) -> list[Deliberation]:
        """List active and recent deliberations for this agent."""
        return await self.store.list_deliberations(agent_id=self.agent_id, status=status)

    # ------------------------------------------------------------------
    # Helper bounds enforcement
    # ------------------------------------------------------------------

    async def _check_and_enforce_bounds(self, deliberation_id: UUID) -> None:
        """Check bounds and auto-interrupt if exceeded."""
        violated_bounds = await self.bounds_enforcer.check_bounds(deliberation_id)
        if violated_bounds:
            reason = f"Bounds exceeded: {', '.join(violated_bounds)}"
            await self.interrupt(deliberation_id, reason)
