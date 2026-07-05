import os
import json
import uuid
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator

try:
    from neurogossip_client import AgentIdentity, NeuroGossipClient
except ImportError:
    # Fallback for compilation or environments where client isn't installed yet
    class AgentIdentity:  # type: ignore
        def __init__(self, **kwargs): pass
    class NeuroGossipClient:  # type: ignore
        def __init__(self, **kwargs): pass


class BaseAgentTransport(ABC):
    """Abstract base class representing an agent communication transport."""

    @abstractmethod
    async def connect(self) -> None:
        """Connect to the messaging broker."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the messaging broker."""

    @abstractmethod
    async def send_message(
        self,
        recipient_id: str,
        payload: dict[str, Any],
        thread_id: str | None = None,
        reply_to: str | None = None,
        tags: list[str] | None = None,
    ) -> str:
        """Send a message to a specific recipient. Return the generated message ID."""

    @abstractmethod
    def listen(self) -> AsyncIterator[tuple[str, str | None, str | None, list[str], str, dict[str, Any]]]:
        """Listen to incoming messages.

        Yields:
            (message_id, thread_id, reply_to, tags, sender_id, payload_dict)
        """


class RedisAgentTransport(BaseAgentTransport):
    """Default transport implementing BaseAgentTransport using neurogossip-client-v2."""

    def __init__(
        self,
        agent_id: str | None = None,
        namespace: str | None = None,
        redis_url: str | None = None,
        redis_client: Any = None,
        **kwargs: Any,
    ) -> None:
        # Prioritize NEUROGOSSIP_V3_ environment variables, falling back to legacy keys or default values
        self.agent_id = agent_id or os.environ.get("NEUROGOSSIP_V3_AGENT_ID") or os.environ.get("NEUROGOSSIP_AGENT_ID") or "default_agent"
        self.namespace = namespace or os.environ.get("NEUROGOSSIP_V3_NAMESPACE") or os.environ.get("NEUROGOSSIP_NAMESPACE") or "default"
        
        agent_name = os.environ.get("NEUROGOSSIP_V3_AGENT_NAME") or os.environ.get("NEUROGOSSIP_AGENT_NAME") or self.agent_id.upper()
        agent_role = os.environ.get("NEUROGOSSIP_V3_AGENT_ROLE") or os.environ.get("NEUROGOSSIP_AGENT_ROLE") or "agent"
        
        # Fallback sequence for Redis connection string: constructor -> V3 env -> general REDIS_URL env -> localhost default
        resolved_redis_url = redis_url or os.environ.get("NEUROGOSSIP_V3_REDIS_URL") or os.environ.get("REDIS_URL") or "redis://localhost:6379/0"

        self.identity = AgentIdentity(
            agent_id=self.agent_id,
            agent_name=agent_name,
            role=agent_role,
        )
        self.client = NeuroGossipClient(
            agent=self.identity,
            namespace=self.namespace,
            redis_url=resolved_redis_url,
            redis_client=redis_client,
            **kwargs,
        )

    async def connect(self) -> None:
        await self.client._connect()

    async def disconnect(self) -> None:
        await self.client.close()

    async def send_message(
        self,
        recipient_id: str,
        payload: dict[str, Any],
        thread_id: str | None = None,
        reply_to: str | None = None,
        tags: list[str] | None = None,
    ) -> str:
        markdown = json.dumps(payload)
        msg = await self.client.send_direct(
            to_agent_id=recipient_id,
            markdown=markdown,
            thread_id=thread_id,
            reply_to=reply_to,
            tags=tags,
        )
        return msg.message_id

    async def listen(self) -> AsyncIterator[tuple[str, str | None, str | None, list[str], str, dict[str, Any]]]:
        async for msg in self.client.listen():
            try:
                payload = json.loads(msg.content.body)
            except Exception:
                payload = {"body": msg.content.body}
            yield (
                msg.message_id,
                msg.thread_id,
                msg.reply_to,
                msg.metadata.tags or [],
                msg.sender.agent_id,
                payload,
            )


class MockAgentTransport(BaseAgentTransport):
    """Mock transport that keeps track of sent messages in memory."""

    def __init__(self) -> None:
        self.sent_messages: list[dict[str, Any]] = []

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass

    async def send_message(
        self,
        recipient_id: str,
        payload: dict[str, Any],
        thread_id: str | None = None,
        reply_to: str | None = None,
        tags: list[str] | None = None,
    ) -> str:
        msg_id = payload.get("request_id") or str(uuid.uuid4())
        self.sent_messages.append({
            "recipient_id": recipient_id,
            "payload": payload,
            "thread_id": thread_id,
            "reply_to": reply_to,
            "tags": tags,
            "message_id": msg_id,
        })
        return msg_id

    async def listen(self) -> AsyncIterator[tuple[str, str | None, str | None, list[str], str, dict[str, Any]]]:
        # Empty mock generator
        if False:
            yield "", None, None, [], "", {}

