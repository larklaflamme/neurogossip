"""WebSocketAgentTransport for Neurogossip v3 deliberation protocol.

Wraps NeurogossipClient (v5.0 WebSocket transport) and provides deliberation-level
message routing, typed queue dispatch, and handler registration.
"""

import asyncio
import json
import logging
from typing import Awaitable, Callable, Optional, Set
from uuid import UUID

from neurogossip_client.client import NeurogossipClient

from neurogossip_v3.models import Contribution, VoteSession


class WebSocketAgentTransport:
    """Deliberation transport wrapping NeurogossipClient."""

    def __init__(
        self,
        server_url: str,
        agent_id: str,
        metadata: Optional[dict] = None,
        auth_token: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.server_url = server_url
        self.agent_id = agent_id
        self._client = NeurogossipClient(
            server_url=server_url,
            agent_id=agent_id,
            metadata=metadata,
            auth_token=auth_token,
            logger=logger,
        )
        self.logger = logger or logging.getLogger(f"v3_transport.{agent_id}")

        # Queues for push-based incoming frame dispatch
        self._deliberation_queue: asyncio.Queue = asyncio.Queue()
        self._invitation_queue: asyncio.Queue = asyncio.Queue()
        self._vote_queue: asyncio.Queue = asyncio.Queue()

        # Registered handlers (Async functions taking sender_id and message payload dict)
        self._deliberation_handler: Optional[Callable[..., Awaitable[None]]] = None
        self._invitation_handler: Optional[Callable[..., Awaitable[None]]] = None
        self._vote_handler: Optional[Callable[..., Awaitable[None]]] = None

        self._listener_task: Optional[asyncio.Task] = None
        self._running = False

    async def connect(self) -> None:
        """Connect the client, register frame callback, and start listener loop."""
        await self._client.connect()
        self._client.on_message(self._dispatch_incoming)
        self._running = True
        self._listener_task = asyncio.create_task(self._background_listener())
        self.logger.info(f"WebSocketAgentTransport connected for {self.agent_id}")

    async def disconnect(self) -> None:
        """Disconnect client and cancel background listener."""
        self._running = False
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        await self._client.disconnect()
        self.logger.info(f"WebSocketAgentTransport disconnected for {self.agent_id}")

    def set_deliberation_handler(self, handler: Callable[..., Awaitable[None]]) -> None:
        """Register callback for incoming deliberation messages."""
        self._deliberation_handler = handler

    def set_invitation_handler(self, handler: Callable[..., Awaitable[None]]) -> None:
        """Register callback for incoming invitations."""
        self._invitation_handler = handler

    def set_vote_handler(self, handler: Callable[..., Awaitable[None]]) -> None:
        """Register callback for incoming vote messages."""
        self._vote_handler = handler

    def _dispatch_incoming(self, frame: dict) -> None:
        """Client on_message callback — receives single frame dict with key 'from'."""
        sender_id = frame.get("from")
        body_raw = frame.get("body", "")
        msg_id = frame.get("msg_id")
        reply_to = frame.get("reply_to")
        conversation_id = frame.get("conversation_id")

        try:
            if isinstance(body_raw, str):
                body = json.loads(body_raw)
            elif isinstance(body_raw, dict):
                body = body_raw
            else:
                return
        except json.JSONDecodeError:
            return

        msg_type = body.get("type")

        if msg_type == "deliberation":
            self._deliberation_queue.put_nowait(
                (sender_id, body, msg_id, reply_to, conversation_id)
            )
        elif msg_type == "deliberation_invitation":
            self._invitation_queue.put_nowait(
                (sender_id, body, msg_id, conversation_id)
            )
        elif msg_type == "vote_session":
            self._vote_queue.put_nowait(
                (sender_id, body, msg_id, conversation_id)
            )

    async def _background_listener(self) -> None:
        """Drains typed queues and invokes handlers."""
        while self._running:
            try:
                # Process queued deliberation contributions
                while not self._deliberation_queue.empty():
                    item = self._deliberation_queue.get_nowait()
                    if self._deliberation_handler:
                        await self._deliberation_handler(*item)

                # Process queued invitations
                while not self._invitation_queue.empty():
                    item = self._invitation_queue.get_nowait()
                    if self._invitation_handler:
                        await self._invitation_handler(*item)

                # Process queued vote messages
                while not self._vote_queue.empty():
                    item = self._vote_queue.get_nowait()
                    if self._vote_handler:
                        await self._vote_handler(*item)

                await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in background listener: {e}")
                await asyncio.sleep(0.05)

    # ------------------------------------------------------------------
    # Message Sending Operations
    # ------------------------------------------------------------------

    async def send_contribution(
        self, to: str, deliberation_id: UUID, contribution: Contribution
    ) -> str:
        """Send a Contribution message to a recipient."""
        payload = {
            "type": "deliberation",
            "deliberation_id": str(deliberation_id),
            "contribution": contribution.model_dump(mode="json"),
        }
        body_str = json.dumps(payload)
        reply_to_str = str(contribution.reply_to) if contribution.reply_to else None

        # Loopback delivery if sending to self
        if to == self.agent_id:
            frame = {
                "from": self.agent_id,
                "body": body_str,
                "msg_id": str(UUID(int=0)),
                "reply_to": reply_to_str,
                "conversation_id": str(deliberation_id),
            }
            self._dispatch_incoming(frame)
            return str(UUID(int=0))

        return await self._client.send(
            to=to,
            body=body_str,
            reply_to=reply_to_str,
            conversation_id=str(deliberation_id),
        )

    async def send_invitation(
        self, to: str, deliberation_id: UUID, goal: str, initiator_id: str
    ) -> str:
        """Send a deliberation invitation to an agent."""
        payload = {
            "type": "deliberation_invitation",
            "deliberation_id": str(deliberation_id),
            "goal": goal,
            "initiator_id": initiator_id,
        }
        body_str = json.dumps(payload)

        if to == self.agent_id:
            frame = {
                "from": self.agent_id,
                "body": body_str,
                "msg_id": str(UUID(int=0)),
                "conversation_id": str(deliberation_id),
            }
            self._dispatch_incoming(frame)
            return str(UUID(int=0))

        return await self._client.send(
            to=to,
            body=body_str,
            conversation_id=str(deliberation_id),
        )

    async def send_vote_session(
        self, to: str, deliberation_id: UUID, vote_session: VoteSession
    ) -> str:
        """Send a vote session notification to a participant."""
        payload = {
            "type": "vote_session",
            "deliberation_id": str(deliberation_id),
            "vote_session": vote_session.model_dump(mode="json"),
        }
        body_str = json.dumps(payload)

        if to == self.agent_id:
            frame = {
                "from": self.agent_id,
                "body": body_str,
                "msg_id": str(UUID(int=0)),
                "conversation_id": str(deliberation_id),
            }
            self._dispatch_incoming(frame)
            return str(UUID(int=0))

        return await self._client.send(
            to=to,
            body=body_str,
            conversation_id=str(deliberation_id),
        )

    async def broadcast_to_participants(
        self, participants: Set[str], deliberation_id: UUID, message: dict
    ) -> list[str]:
        """Broadcast a generic deliberation payload to all participants (fan-out)."""
        msg_ids = []
        body_str = json.dumps(message)
        for agent_id in participants:
            if agent_id == self.agent_id:
                frame = {
                    "from": self.agent_id,
                    "body": body_str,
                    "msg_id": str(UUID(int=0)),
                    "conversation_id": str(deliberation_id),
                }
                self._dispatch_incoming(frame)
                msg_ids.append(str(UUID(int=0)))
            else:
                msg_id = await self._client.send(
                    to=agent_id,
                    body=body_str,
                    conversation_id=str(deliberation_id),
                )
                msg_ids.append(msg_id)
        return msg_ids

    # Passthroughs
    async def wait_for_message(self, timeout: Optional[float] = None) -> Optional[dict]:
        return await self._client.wait_for_message(timeout=timeout)

    @property
    def online_agents(self) -> list[str]:
        return self._client.online_agents

    async def list_agents(self) -> list[dict]:
        return await self._client.list_agents()

    async def block(self, agent_id: str) -> None:
        await self._client.block(agent_id)

    async def unblock(self, agent_id: str) -> None:
        await self._client.unblock(agent_id)
