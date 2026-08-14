"""Voting subsystem for Neurogossip v3 deliberation protocol."""

from datetime import datetime, timezone
from typing import Literal, Optional
from uuid import UUID, uuid4

from neurogossip_v3.errors import (
    DeliberationNotFoundError,
    InvalidChoiceError,
    InvalidStatusError,
    NotParticipantError,
    VoteAlreadyActiveError,
    VoteClosedError,
    VoteNotFoundError,
)
from neurogossip_v3.models import (
    DeliberationStatus,
    Vote,
    VoteResult,
    VoteSession,
    VoteStatus,
)
from neurogossip_v3.state_store import StateStore
from neurogossip_v3.transport import WebSocketAgentTransport


class VotingSubsystem:
    """Manages voting proposals, casting votes, and tallying within deliberations."""

    def __init__(self, agent_id: str, state_store: StateStore, transport: WebSocketAgentTransport):
        self.agent_id = agent_id
        self.store = state_store
        self.transport = transport

    async def propose_vote(
        self,
        deliberation_id: UUID,
        proposal: str,
        options: list[str],
        threshold: float = 0.5,
        timeout_s: float = 300.0,
        on_timeout: Literal["fail", "pass", "extend"] = "fail",
    ) -> VoteSession:
        """Initiate a voting proposal in a deliberation."""
        deliberation = await self.store.get_deliberation(deliberation_id)
        if not deliberation:
            raise DeliberationNotFoundError(f"Deliberation {deliberation_id} not found")

        if self.agent_id not in deliberation.participants:
            raise NotParticipantError(f"Agent {self.agent_id} is not a participant")

        if deliberation.status not in (DeliberationStatus.ACTIVE, DeliberationStatus.WAITING):
            raise InvalidStatusError(
                f"Cannot propose vote in status {deliberation.status}"
            )

        vote_session = VoteSession(
            vote_id=uuid4(),
            deliberation_id=deliberation_id,
            proposal=proposal,
            options=options,
            threshold=threshold,
            timeout_s=timeout_s,
            timeout_policy=on_timeout,
            status=VoteStatus.OPEN,
        )

        await self.store.create_vote_session(vote_session)

        # Transition deliberation to VOTING
        deliberation.status = DeliberationStatus.VOTING
        deliberation.updated_at = datetime.now(timezone.utc)
        await self.store.update_deliberation(deliberation)

        # Broadcast vote notification
        payload = {
            "type": "vote_session",
            "deliberation_id": str(deliberation_id),
            "vote_session": vote_session.model_dump(mode="json"),
        }
        await self.transport.broadcast_to_participants(
            participants=deliberation.participants,
            deliberation_id=deliberation_id,
            message=payload,
        )

        return vote_session

    async def cast_vote(
        self,
        vote_id: UUID,
        choice: str,
        rationale: Optional[str] = None,
    ) -> VoteSession:
        """Cast a vote in an open vote session."""
        vote_session = await self.store.get_vote_session(vote_id)
        if not vote_session:
            raise VoteNotFoundError(f"Vote session {vote_id} not found")

        if vote_session.status != VoteStatus.OPEN:
            raise VoteClosedError(f"Vote session {vote_id} is closed")

        if choice not in vote_session.options:
            raise InvalidChoiceError(
                f"Choice '{choice}' not in valid options: {vote_session.options}"
            )

        deliberation = await self.store.get_deliberation(vote_session.deliberation_id)
        if not deliberation:
            raise DeliberationNotFoundError(f"Deliberation {vote_session.deliberation_id} not found")

        if self.agent_id not in deliberation.participants:
            raise NotParticipantError(f"Agent {self.agent_id} is not a participant")

        # Record vote
        vote = Vote(
            agent_id=self.agent_id,
            choice=choice,
            rationale=rationale,
        )
        vote_session.votes[self.agent_id] = vote

        # Check if all participants have voted
        if len(vote_session.votes) >= len(deliberation.participants):
            await self._close_vote(vote_session, deliberation)
        else:
            await self.store.update_vote_session(vote_session)

        return vote_session

    async def tally_votes(self, vote_id: UUID) -> dict[str, int]:
        """Compute the vote tally for a session."""
        vote_session = await self.store.get_vote_session(vote_id)
        if not vote_session:
            raise VoteNotFoundError(f"Vote session {vote_id} not found")

        tally = {opt: 0 for opt in vote_session.options}
        for v in vote_session.votes.values():
            if v.choice in tally:
                tally[v.choice] += 1
        return tally

    async def _close_vote(self, vote_session: VoteSession, deliberation) -> VoteSession:
        """Close vote session, calculate result, and transition deliberation."""
        tally = {opt: 0 for opt in vote_session.options}
        for v in vote_session.votes.values():
            if v.choice in tally:
                tally[v.choice] += 1

        total_votes = len(vote_session.votes)
        winning_option = max(tally, key=tally.get) if tally else "none"
        winning_count = tally.get(winning_option, 0)
        passed = (winning_count / total_votes >= vote_session.threshold) if total_votes > 0 else False

        result = VoteResult(
            outcome=winning_option if passed else "rejected",
            tally=tally,
            passed=passed,
        )

        vote_session.status = VoteStatus.CLOSED
        vote_session.result = result
        vote_session.closed_at = datetime.now(timezone.utc)
        await self.store.update_vote_session(vote_session)

        # Transition deliberation back to ACTIVE
        deliberation.status = DeliberationStatus.ACTIVE
        deliberation.updated_at = datetime.now(timezone.utc)
        await self.store.update_deliberation(deliberation)

        return vote_session
