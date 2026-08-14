"""TurnBasedAdapter and SuspendTurn exception for Skye's turn-based runtime."""

from typing import Any, Optional, Set
from uuid import UUID


class SuspendTurn(Exception):
    """Raised by wait_for_responses() in TURN_BASED mode when waiting for replies."""

    def __init__(
        self,
        deliberation_id: UUID,
        expected_responders: Set[str],
        timeout_s: float,
        context: Optional[dict[str, Any]] = None,
    ):
        self.deliberation_id = deliberation_id
        self.expected_responders = expected_responders
        self.timeout_s = timeout_s
        self.context = context or {
            "deliberation_id": str(deliberation_id),
            "expected_responders": sorted(list(expected_responders)),
            "timeout_s": timeout_s,
        }
        super().__init__(
            f"Turn suspended for deliberation {deliberation_id}, waiting for {expected_responders}"
        )


class TurnBasedAdapter:
    """Wraps DeliberationManager for turn-based agents (e.g. Skye CLI runtime)."""

    def __init__(self, manager: Any):
        self.manager = manager
        self._suspended_contexts: dict[UUID, dict] = {}

    def serialize_context(self, suspend_turn: SuspendTurn) -> dict[str, Any]:
        """Serialize SuspendTurn context for cross-turn persistence."""
        return suspend_turn.context

    @staticmethod
    def deserialize_context(data: dict[str, Any]) -> SuspendTurn:
        """Reconstruct SuspendTurn from serialized context dictionary."""
        del_id = UUID(data["deliberation_id"])
        responders = set(data["expected_responders"])
        timeout_s = float(data["timeout_s"])
        return SuspendTurn(
            deliberation_id=del_id,
            expected_responders=responders,
            timeout_s=timeout_s,
            context=data,
        )

    async def handle_turn(self, incoming_frame: dict) -> Optional[dict]:
        """Process one incoming message turn. Returns result or raises SuspendTurn."""
        # Loopback or process through manager
        del_id_str = incoming_frame.get("conversation_id") or incoming_frame.get("deliberation_id")
        if not del_id_str:
            return None

        del_id = UUID(del_id_str)
        missed = await self.manager.catch_up(del_id)
        return {"deliberation_id": str(del_id), "new_contributions": [m.model_dump(mode="json") for m in missed]}
