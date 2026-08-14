"""Compatibility adapter for neurogossip-agent-v3 API consumers."""

from typing import Any, Optional
from uuid import UUID

from neurogossip_v3.manager import DeliberationManager
from neurogossip_v3.models import ContributionKind


class AgentV3Compat:
    """Provides an agent-v3 compatible interface over DeliberationManager."""

    def __init__(self, manager: DeliberationManager):
        self.manager = manager
        self._request_map: dict[str, UUID] = {}

    async def send_request(
        self, to: str, payload: dict[str, Any], deliberation_id: Optional[UUID] = None
    ) -> str:
        """Map agent-v3 send_request to v3 contribute(kind=QUESTION)."""
        if not deliberation_id:
            deliberation = await self.manager.start_deliberation(
                goal=payload.get("goal", "Compatibility session"),
                participants={to},
            )
            del_id = deliberation.deliberation_id
        else:
            del_id = deliberation_id

        content = str(payload.get("content") or payload)
        contrib = await self.manager.contribute(
            deliberation_id=del_id,
            kind=ContributionKind.QUESTION,
            content=content,
            addressed_to={to},
            expects_response_from={to},
        )

        req_id = str(contrib.contribution_id)
        self._request_map[req_id] = del_id
        return req_id

    async def wait_for_response(
        self, request_id: str, timeout: float = 300.0
    ) -> dict[str, Any]:
        """Map agent-v3 wait_for_response to v3 wait_for_responses()."""
        del_id = self._request_map.get(request_id)
        if not del_id:
            raise KeyError(f"Request ID {request_id} not recognized")

        responses = await self.manager.wait_for_responses(
            deliberation_id=del_id,
            timeout_seconds=timeout,
            timeout_policy="fail",
        )
        if responses:
            return {"status": "success", "content": responses[-1].content}
        return {"status": "empty", "content": None}
