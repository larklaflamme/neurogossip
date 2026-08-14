"""Error types for Neurogossip v3 deliberation protocol.

Defines the 17 harmonized protocol error codes from specification section 7.
"""

from typing import Optional


class NeurogossipError(Exception):
    """Base exception for all Neurogossip v3 errors."""
    code: str = "UNKNOWN_ERROR"

    def __init__(self, message: str, details: Optional[dict] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        return f"[{self.code}] {self.message}"


class DeliberationNotFoundError(NeurogossipError):
    code = "DELIBERATION_NOT_FOUND"


class InvalidStatusError(NeurogossipError):
    code = "INVALID_STATUS"


class NotParticipantError(NeurogossipError):
    code = "NOT_PARTICIPANT"


class InvalidKindError(NeurogossipError):
    code = "INVALID_KIND"


class ContributionNotFoundError(NeurogossipError):
    code = "CONTRIBUTION_NOT_FOUND"


class VoteNotFoundError(NeurogossipError):
    code = "VOTE_NOT_FOUND"


class VoteClosedError(NeurogossipError):
    code = "VOTE_CLOSED"


class VoteAlreadyActiveError(NeurogossipError):
    code = "VOTE_ALREADY_ACTIVE"


class InvalidChoiceError(NeurogossipError):
    code = "INVALID_CHOICE"


class TimeoutError(NeurogossipError):
    code = "TIMEOUT"


class InterruptedError(NeurogossipError):
    code = "INTERRUPTED"


class AlreadyExistsError(NeurogossipError):
    code = "ALREADY_EXISTS"


class EmptyParticipantsError(NeurogossipError):
    code = "EMPTY_PARTICIPANTS"


class GroupNotFoundError(NeurogossipError):
    code = "GROUP_NOT_FOUND"


class GroupAlreadyExistsError(NeurogossipError):
    code = "GROUP_ALREADY_EXISTS"


class AlreadyMemberError(NeurogossipError):
    code = "ALREADY_MEMBER"


class NotMemberError(NeurogossipError):
    code = "NOT_MEMBER"
