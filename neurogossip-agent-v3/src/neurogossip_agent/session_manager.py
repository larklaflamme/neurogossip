"""Core session and request tracking logic for neurogossip-agent."""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable, Awaitable

from .errors import RequestNotFoundError, SessionNotFoundError, TimeoutError
from .models import (
    AgentRequest,
    AgentResponse,
    ConversationSession,
    HistoryEvent,
    RequestStatus,
    SessionStatus,
    AgentSessionContext,
)
from .transport import BaseAgentTransport

log = logging.getLogger("neurogossip_agent")


class AgentConversationManager:
    """Manages conversational sessions, requests, responses, and history tracking in Redis."""

    def __init__(
        self,
        redis_client: Any,
        transport: BaseAgentTransport | None = None,
        namespace: str = "default",
    ) -> None:
        self.redis = redis_client
        self.transport = transport
        self.namespace = namespace
        self.active_sessions: dict[str, AgentSessionContext] = {}
        self._handlers: list[Callable[[AgentSessionContext, str, str | None, list[str], str, dict[str, Any]], Awaitable[None]]] = []
        self._listen_task: asyncio.Task | None = None

    # -- Key generation helpers --
    def _session_key(self, conversation_id: str) -> str:
        return f"neurogossip:{self.namespace}:session:{conversation_id}"

    def _session_requests_key(self, conversation_id: str) -> str:
        return f"neurogossip:{self.namespace}:session:{conversation_id}:requests"

    def _session_pending_key(self, conversation_id: str) -> str:
        return f"neurogossip:{self.namespace}:session:{conversation_id}:pending"

    def _session_history_key(self, conversation_id: str) -> str:
        return f"neurogossip:{self.namespace}:session:{conversation_id}:history"

    def _request_key(self, request_id: str) -> str:
        return f"neurogossip:{self.namespace}:request:{request_id}"

    def _request_pending_recipients_key(self, request_id: str) -> str:
        return f"neurogossip:{self.namespace}:request:{request_id}:pending_recipients"

    def _response_key(self, request_id: str, responder_id: str) -> str:
        return f"neurogossip:{self.namespace}:response:{request_id}:{responder_id}"

    def _response_event_channel(self, request_id: str) -> str:
        return f"neurogossip:{self.namespace}:response_event:{request_id}"

    def _decode(self, val: Any) -> str:
        if isinstance(val, bytes):
            return val.decode("utf-8")
        return str(val)

    # -- Conversation Management --
    async def create_conversation(
        self,
        initiator_id: str,
        conversation_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ConversationSession:
        """Create a new conversation session and persist it in Redis."""
        cid = conversation_id or str(uuid.uuid4())
        session = ConversationSession(
            conversation_id=cid,
            initiator_id=initiator_id,
            status=SessionStatus.ACTIVE,
            metadata=metadata or {},
        )

        key = self._session_key(cid)
        await self.redis.hset(
            key,
            mapping={
                "conversation_id": session.conversation_id,
                "initiator_id": session.initiator_id,
                "status": session.status.value,
                "metadata": json.dumps(session.metadata),
                "created_at": session.created_at.isoformat(),
                "updated_at": session.updated_at.isoformat(),
            },
        )

        # Register session context in active_sessions (local memory)
        self.active_sessions[cid] = AgentSessionContext(conversation_id=cid)

        await self.log_event(
            cid,
            type="session_started",
            sender_id=initiator_id,
            content={"metadata": session.metadata},
        )
        return session

    async def get_conversation(self, conversation_id: str) -> ConversationSession:
        """Fetch an existing conversation session from Redis."""
        key = self._session_key(conversation_id)
        data = await self.redis.hgetall(key)
        if not data:
            raise SessionNotFoundError(f"Conversation {conversation_id} not found.")

        # Handle decoding of bytes
        decoded = {self._decode(k): self._decode(v) for k, v in data.items()}
        return ConversationSession(
            conversation_id=decoded["conversation_id"],
            initiator_id=decoded["initiator_id"],
            status=SessionStatus(decoded["status"]),
            metadata=json.loads(decoded["metadata"]),
            created_at=datetime.fromisoformat(decoded["created_at"]),
            updated_at=datetime.fromisoformat(decoded["updated_at"]),
        )

    # -- Request Dispatching & Tracking --
    async def create_request(
        self,
        conversation_id: str,
        sender_id: str,
        recipient_ids: list[str],
        payload: Any,
        parent_request_id: str | None = None,
    ) -> AgentRequest:
        """Create and persist a new agent request (single or fan-out).

        Sends the request payload through the transport layer to all recipients.
        """
        # Verify conversation exists
        await self.get_conversation(conversation_id)

        req_id = str(uuid.uuid4())
        request = AgentRequest(
            request_id=req_id,
            conversation_id=conversation_id,
            parent_request_id=parent_request_id,
            sender_id=sender_id,
            recipient_ids=recipient_ids,
            status=RequestStatus.PENDING,
            payload=payload,
        )

        # Store main request data
        req_key = self._request_key(req_id)
        await self.redis.hset(
            req_key,
            mapping={
                "request_id": request.request_id,
                "conversation_id": request.conversation_id,
                "parent_request_id": request.parent_request_id or "",
                "sender_id": request.sender_id,
                "recipient_ids": ",".join(request.recipient_ids),
                "status": request.status.value,
                "payload": json.dumps(request.payload),
                "created_at": request.created_at.isoformat(),
            },
        )

        # Add to tracking sets
        await self.redis.sadd(self._session_requests_key(conversation_id), req_id)
        await self.redis.sadd(self._session_pending_key(conversation_id), req_id)

        # Set up recipient tracking for completion
        pending_recipients_key = self._request_pending_recipients_key(req_id)
        for rid in recipient_ids:
            await self.redis.sadd(pending_recipients_key, rid)

        # Log event in history
        await self.log_event(
            conversation_id,
            type="request_sent",
            sender_id=sender_id,
            ref_id=req_id,
            content={
                "recipient_ids": recipient_ids,
                "parent_request_id": parent_request_id,
                "payload": payload,
            },
        )

        # Update session status to WAITING_FOR_HUMAN if a human is in the loop
        if "human" in recipient_ids:
            await self._update_session_status(conversation_id, SessionStatus.WAITING_FOR_HUMAN)

        # Dispatch message over the transport layer
        if self.transport:
            transport_payload = {"request_id": req_id, "payload": payload}
            for rid in recipient_ids:
                if rid == "human":
                    # Human logic runs locally/out-of-band, no direct agent transport needed
                    continue
                await self.transport.send_message(
                    recipient_id=rid,
                    payload=transport_payload,
                    thread_id=conversation_id,
                    reply_to=parent_request_id,
                    tags=["request"],
                )

        # Update active_sessions registry in local memory
        if conversation_id not in self.active_sessions:
            self.active_sessions[conversation_id] = AgentSessionContext(conversation_id=conversation_id)
        else:
            self.active_sessions[conversation_id].last_accessed = datetime.now(timezone.utc)

        return request

    async def get_request(self, request_id: str) -> AgentRequest:
        """Fetch request details from Redis."""
        key = self._request_key(request_id)
        data = await self.redis.hgetall(key)
        if not data:
            raise RequestNotFoundError(f"Request {request_id} not found.")

        decoded = {self._decode(k): self._decode(v) for k, v in data.items()}
        recipient_ids = decoded["recipient_ids"].split(",") if decoded["recipient_ids"] else []
        parent_request_id = decoded["parent_request_id"] or None
        completed_at = decoded.get("completed_at")
        completed_at_dt = datetime.fromisoformat(completed_at) if completed_at else None

        return AgentRequest(
            request_id=decoded["request_id"],
            conversation_id=decoded["conversation_id"],
            parent_request_id=parent_request_id,
            sender_id=decoded["sender_id"],
            recipient_ids=recipient_ids,
            status=RequestStatus(decoded["status"]),
            payload=json.loads(decoded["payload"]),
            created_at=datetime.fromisoformat(decoded["created_at"]),
            completed_at=completed_at_dt,
        )

    # -- Response Dispatching & Handling --
    async def send_response(self, request_id: str, sender_id: str, payload: Any) -> AgentResponse:
        """Send and persist a response to a specific request.

        Updates the request state and checks for completion.
        """
        request = await self.get_request(request_id)
        cid = request.conversation_id

        # Verify sender was a recipient
        if sender_id not in request.recipient_ids:
            log.warning(
                "Sender %s was not in the original recipient list %s of request %s",
                sender_id,
                request.recipient_ids,
                request_id,
            )

        resp_id = str(uuid.uuid4())
        response = AgentResponse(
            response_id=resp_id,
            request_id=request_id,
            conversation_id=cid,
            sender_id=sender_id,
            recipient_id=request.sender_id,
            payload=payload,
        )

        # Store response details
        resp_key = self._response_key(request_id, sender_id)
        await self.redis.hset(
            resp_key,
            mapping={
                "response_id": response.response_id,
                "request_id": response.request_id,
                "conversation_id": response.conversation_id,
                "sender_id": response.sender_id,
                "recipient_id": response.recipient_id,
                "payload": json.dumps(response.payload),
                "created_at": response.created_at.isoformat(),
            },
        )

        # Remove from pending recipients list for this request
        pending_rec_key = self._request_pending_recipients_key(request_id)
        await self.redis.srem(pending_rec_key, sender_id)

        # Log event in history
        await self.log_event(
            cid,
            type="response_received",
            sender_id=sender_id,
            recipient_id=request.sender_id,
            ref_id=resp_id,
            content={"request_id": request_id, "payload": payload},
        )

        # Check if all recipients for this request have responded
        still_pending = await self.redis.scard(pending_rec_key)
        if still_pending == 0:
            # Mark request as completed
            now = datetime.now(timezone.utc)
            await self.redis.hset(
                self._request_key(request_id),
                mapping={
                    "status": RequestStatus.COMPLETED.value,
                    "completed_at": now.isoformat(),
                },
            )

            # Remove from conversation's pending requests set
            await self.redis.srem(self._session_pending_key(cid), request_id)

            # Revert session status back to ACTIVE if was WAITING_FOR_HUMAN and no other humans are pending
            session = await self.get_conversation(cid)
            if session.status == SessionStatus.WAITING_FOR_HUMAN:
                # Check if there are any other pending requests targeting "human" in this session
                all_pending_reqs = await self.redis.smembers(self._session_pending_key(cid))
                human_pending = False
                for preq_id in all_pending_reqs:
                    preq_id_str = self._decode(preq_id)
                    req_data = await self.get_request(preq_id_str)
                    if "human" in req_data.recipient_ids:
                        # Check if human is still pending in this request
                        is_human_pending = await self.redis.sismember(
                            self._request_pending_recipients_key(preq_id_str), "human"
                        )
                        if is_human_pending:
                            human_pending = True
                            break
                if not human_pending:
                    await self._update_session_status(cid, SessionStatus.ACTIVE)

            # Publish response completed event to wake up blocking listeners
            channel = self._response_event_channel(request_id)
            event_payload = {
                "request_id": request_id,
                "status": RequestStatus.COMPLETED.value,
                "completed_at": now.isoformat(),
            }
            await self.redis.publish(channel, json.dumps(event_payload))

            # If the completed request was the root request (no parent), mark session as COMPLETED
            if request.parent_request_id is None:
                await self._update_session_status(cid, SessionStatus.COMPLETED)
                await self.log_event(
                    cid,
                    type="session_completed",
                    sender_id=sender_id,
                    content={"final_payload": payload},
                )

        # Dispatch response message over transport if recipient is an agent
        if self.transport and request.sender_id != "human":
            transport_payload = {"request_id": request_id, "payload": payload}
            await self.transport.send_message(
                recipient_id=request.sender_id,
                payload=transport_payload,
                thread_id=cid,
                reply_to=request_id,
                tags=["response"],
            )

        # Update active_sessions registry in local memory
        if cid not in self.active_sessions:
            self.active_sessions[cid] = AgentSessionContext(conversation_id=cid)
        else:
            self.active_sessions[cid].last_accessed = datetime.now(timezone.utc)

        return response

    async def get_response(self, request_id: str, responder_id: str) -> AgentResponse | None:
        """Fetch a specific response from a responder."""
        key = self._response_key(request_id, responder_id)
        data = await self.redis.hgetall(key)
        if not data:
            return None

        decoded = {self._decode(k): self._decode(v) for k, v in data.items()}
        return AgentResponse(
            response_id=decoded["response_id"],
            request_id=decoded["request_id"],
            conversation_id=decoded["conversation_id"],
            sender_id=decoded["sender_id"],
            recipient_id=decoded["recipient_id"],
            payload=json.loads(decoded["payload"]),
            created_at=datetime.fromisoformat(decoded["created_at"]),
        )

    async def get_all_responses(self, request_id: str) -> list[AgentResponse]:
        """Fetch all responses submitted for a request so far."""
        request = await self.get_request(request_id)
        responses = []
        for rid in request.recipient_ids:
            resp = await self.get_response(request_id, rid)
            if resp:
                responses.append(resp)
        return responses

    # -- Blocking Wait helpers --
    async def wait_for_response(self, request_id: str, timeout: float | None = None) -> list[AgentResponse]:
        """Block and wait until the request is fully completed (all recipients responded).

        Returns:
            The list of all responses for the request.
        """
        # 1. Check if request is already completed
        request = await self.get_request(request_id)
        if request.status == RequestStatus.COMPLETED:
            return await self.get_all_responses(request_id)

        # 2. Subscribe to pub/sub channel
        channel = self._response_event_channel(request_id)
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(channel)

        try:
            # Check status again to avoid race conditions
            request = await self.get_request(request_id)
            if request.status == RequestStatus.COMPLETED:
                return await self.get_all_responses(request_id)

            # Listen for completion event
            async def _listen() -> None:
                async for msg in pubsub.listen():
                    if msg["type"] == "message":
                        data = json.loads(self._decode(msg["data"]))
                        if data.get("status") == RequestStatus.COMPLETED.value:
                            return

            import asyncio
            try:
                await asyncio.wait_for(_listen(), timeout=timeout)
            except asyncio.TimeoutError:
                raise TimeoutError(f"Timed out waiting for response to request {request_id}")

            return await self.get_all_responses(request_id)
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.close()

    # -- History & Logging --
    async def log_event(
        self,
        conversation_id: str,
        type: str,
        sender_id: str | None = None,
        recipient_id: str | None = None,
        ref_id: str | None = None,
        content: Any = None,
    ) -> HistoryEvent:
        """Log a custom event into the persistent chronological history of the conversation."""
        event = HistoryEvent(
            conversation_id=conversation_id,
            type=type,  # type: ignore[arg-type]
            sender_id=sender_id,
            recipient_id=recipient_id,
            ref_id=ref_id,
            content=content,
        )

        history_key = self._session_history_key(conversation_id)
        await self.redis.rpush(history_key, event.model_dump_json())
        return event

    async def log_status_report(self, conversation_id: str, agent_id: str, message: str) -> HistoryEvent:
        """Log an intermediate status report or progress message."""
        return await self.log_event(
            conversation_id,
            type="status_report",
            sender_id=agent_id,
            content={"message": message},
        )

    async def get_history(self, conversation_id: str) -> list[HistoryEvent]:
        """Fetch the full chronological history log of the conversation."""
        history_key = self._session_history_key(conversation_id)
        raw_events = await self.redis.lrange(history_key, 0, -1)
        events = []
        for raw in raw_events:
            events.append(HistoryEvent.model_validate_json(self._decode(raw)))
        return events

    # -- Internal helpers --
    async def _update_session_status(self, conversation_id: str, status: SessionStatus) -> None:
        key = self._session_key(conversation_id)
        now = datetime.now(timezone.utc)
        await self.redis.hset(
            key,
            mapping={
                "status": status.value,
                "updated_at": now.isoformat(),
            },
        )

    # -- Archiving & Un-archiving --
    def register_handler(
        self,
        handler_fn: Callable[[AgentSessionContext, str, str | None, list[str], str, dict[str, Any]], Awaitable[None]]
    ) -> None:
        """Register a handler callback to be invoked when a message is received."""
        self._handlers.append(handler_fn)

    async def process_incoming_message(
        self,
        conversation_id: str,
        message_id: str,
        reply_to: str | None,
        tags: list[str],
        sender_id: str,
        payload: dict[str, Any],
    ) -> AgentSessionContext:
        """Process an incoming message, un-archiving the session from Redis if not in local memory."""
        now = datetime.now(timezone.utc)
        
        if conversation_id not in self.active_sessions:
            log.info("Session %s not found in local memory. Attempting to un-archive.", conversation_id)
            
            try:
                # Attempt to retrieve existing conversation from Redis
                await self.get_conversation(conversation_id)
            except SessionNotFoundError:
                # If conversation does not exist, automatically register it (auto-participate)
                log.info("Conversation %s not registered in Redis. Auto-creating.", conversation_id)
                await self.create_conversation(initiator_id=sender_id, conversation_id=conversation_id)

            # Rebuild local in-memory context
            ctx = AgentSessionContext(conversation_id=conversation_id)
            
            # Populate history into local_state for context
            history = await self.get_history(conversation_id)
            ctx.local_state["history"] = history
            
            self.active_sessions[conversation_id] = ctx
        else:
            ctx = self.active_sessions[conversation_id]
            ctx.last_accessed = now

        return ctx

    async def sweep_sessions(self, idle_timeout_s: float) -> list[str]:
        """Purge idle or completed conversation sessions from local memory (archive)."""
        now = datetime.now(timezone.utc)
        archived_ids = []
        
        for cid, ctx in list(self.active_sessions.items()):
            idle_time = (now - ctx.last_accessed).total_seconds()
            is_idle = idle_time > idle_timeout_s
            
            is_completed = False
            try:
                session = await self.get_conversation(cid)
                if session.status in (SessionStatus.COMPLETED, SessionStatus.FAILED):
                    is_completed = True
            except Exception:
                # If session is deleted or missing, clean it up
                is_completed = True

            if is_idle or is_completed:
                log.info("Archiving session %s from local memory (idle_time=%.1fs, completed=%s)", cid, idle_time, is_completed)
                self.active_sessions.pop(cid, None)
                archived_ids.append(cid)
                
        return archived_ids

    # -- Listening Integration --
    async def start_listening(self) -> None:
        """Start listening to transport messages and routing them to handlers."""
        if not self.transport:
            log.warning("No transport configured. Cannot start listening.")
            return

        async def _listen_loop() -> None:
            try:
                async for msg_id, thread_id, reply_to, tags, sender_id, payload in self.transport.listen():
                    if not thread_id:
                        continue
                    
                    # Un-archive or retrieve active session context
                    ctx = await self.process_incoming_message(
                        conversation_id=thread_id,
                        message_id=msg_id,
                        reply_to=reply_to,
                        tags=tags,
                        sender_id=sender_id,
                        payload=payload
                    )
                    
                    # Execute all registered handlers
                    for handler in self._handlers:
                        try:
                            await handler(ctx, msg_id, reply_to, tags, sender_id, payload)
                        except Exception as e:
                            log.error("Error running handler for message %s: %s", msg_id, e)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                log.error("Listen loop encountered an error: %s", e)

        self._listen_task = asyncio.create_task(_listen_loop())

    async def stop_listening(self) -> None:
        """Stop the background listen task."""
        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
            self._listen_task = None

