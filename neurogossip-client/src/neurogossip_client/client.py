#!/usr/bin/env python3
"""Neurogossip v5.0 FINAL — Client Library for Agents.

Provides a simple async interface for agents to connect to the Neurogossip
server, send/receive messages, and manage conversations.

Usage:
    from neurogossip.client import NeurogossipClient

    async def main():
        client = NeurogossipClient(
            server_url="ws://localhost:8765",
            agent_id="axioma",
            metadata={"display_name": "Axioma", "version": "1.0"}
        )
        await client.connect()

        # Send a message
        msg_id = await client.send("thea", "Hello!")

        # Wait for a response
        response = await client.wait_for_message()
        print(response)

        await client.disconnect()
"""

import asyncio
import json
import logging
import time
import uuid
from collections import deque
from typing import Any, Callable, Optional

import websockets


class NeurogossipClient:
    """Async client for the Neurogossip agent messaging server.

    Features:
    - Connect/disconnect with auto-reconnection
    - Send messages with TTL, conversation_id, reply_to
    - Send explicit receipts (public method)
    - End conversations
    - Block/unblock agents
    - List registered agents
    - Callbacks for messages, presence, ACKs, errors, conversation_ended
    """

    def __init__(self, server_url: str, agent_id: str,
                 metadata: Optional[dict] = None,
                 auth_token: Optional[str] = None,
                 reconnect_backoff_s: float = 5.0,
                 max_reconnect_backoff_s: float = 60.0,
                 open_timeout_s: float = 10.0,
                 auto_receipt: bool = True,
                 inbound_queue_size: int = 10000,
                 logger: Optional[logging.Logger] = None):
        self.server_url = server_url
        self.agent_id = agent_id
        self.metadata = metadata or {}
        self.auth_token = auth_token
        self.reconnect_backoff_s = reconnect_backoff_s
        self.max_reconnect_backoff_s = max_reconnect_backoff_s
        self.open_timeout_s = open_timeout_s
        self.auto_receipt = auto_receipt
        self.inbound_queue_size = inbound_queue_size
        self.logger = logger or logging.getLogger(f"client.{agent_id}")

        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.session_id: Optional[str] = None
        self.heartbeat_interval_s: float = 15.0
        self._connected = False
        self._running = False

        # Callbacks
        self._on_message: Optional[Callable] = None
        self._on_presence: Optional[Callable] = None
        self._on_ack: Optional[Callable] = None
        self._on_conversation_ended: Optional[Callable] = None
        self._on_error: Optional[Callable] = None

        # Internal state
        self._message_queue: asyncio.Queue = asyncio.Queue(maxsize=inbound_queue_size)
        self._listen_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._online_agents: dict[str, dict] = {}
        self._agent_list_futures: deque = deque()
        self._reconnecting = False
        # Outstanding on_message handler tasks (tracked so exceptions are retrieved).
        self._handler_tasks: set = set()
        # Current reconnect backoff (exponential, reset to reconnect_backoff_s on
        # a successful connection). Starts at the initial backoff.
        self._reconnect_backoff = self.reconnect_backoff_s

    @staticmethod
    def _ws_closed(ws) -> bool:
        """Cross-version check for whether a WebSocket connection is closed.

        ``websockets`` >=14 (``asyncio.client.ClientConnection``) exposes ``.state``
        (a ``State`` enum); the legacy ``WebSocketClientProtocol`` exposed ``.closed``.
        """
        if ws is None:
            return True
        state = getattr(ws, "state", None)
        if state is not None:
            # websockets >=14
            name = getattr(state, "name", str(state))
            return name in ("CLOSED", "CLOSING")
        # Legacy protocol object
        return bool(getattr(ws, "closed", False))

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    async def connect(self):
        """Connect to the Neurogossip server and register."""
        self._running = True
        await self._connect_socket()
        self._reconnect_backoff = self.reconnect_backoff_s  # reset on fresh connect
        self._start_background_tasks()
        self.logger.info(f"Connected as {self.agent_id} (session {self.session_id})")

    async def _connect_socket(self):
        """Open the WebSocket and complete registration. Raises on failure.

        ``open_timeout_s`` bounds the opening handshake so a silently-unreachable
        or wedged server fails fast (instead of hanging on the default ~10s
        timeout and logging a surprising "timed out during opening handshake").
        ``ping_interval`` / ``ping_timeout`` enable WebSocket-level keepalive so
        a half-open/dead peer is detected and closed promptly, letting the
        reconnect watcher restore the session instead of stalling on a socket
        that looks alive but never delivers.
        """
        self.websocket = await websockets.connect(
            self.server_url,
            open_timeout=self.open_timeout_s,
            ping_interval=20,
            ping_timeout=20,
        )
        await self._register()
        self._connected = True

    def _start_background_tasks(self):
        """(Re)create the listen, reconnect-watch, and heartbeat tasks."""
        self._listen_task = asyncio.create_task(self._listen_loop())
        self._reconnect_task = asyncio.create_task(self._watch_connection())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    def _cancel_background_tasks(self, keep_watcher: bool = False):
        """Cancel background tasks. With keep_watcher=True the reconnect watcher
        keeps running so it can drive a reconnect (single-watcher invariant)."""
        for attr in ("_listen_task", "_heartbeat_task"):
            t = getattr(self, attr)
            if t is not None and not t.done():
                t.cancel()
            setattr(self, attr, None)
        if not keep_watcher:
            if self._reconnect_task is not None and not self._reconnect_task.done():
                self._reconnect_task.cancel()
            self._reconnect_task = None

    async def disconnect(self):
        """Disconnect from the server."""
        self._running = False
        self._connected = False
        self._cancel_background_tasks(keep_watcher=False)
        if self.websocket and not self._ws_closed(self.websocket):
            await self.websocket.close()
        self.logger.info("Disconnected")

    async def _register(self):
        """Send registration frame and wait for confirmation."""
        register_msg = {
            "type": "register",
            "agent_id": self.agent_id,
            "metadata": self.metadata,
        }
        if self.auth_token:
            register_msg["auth_token"] = self.auth_token

        await self.websocket.send(json.dumps(register_msg))
        raw = await self.websocket.recv()
        response = json.loads(raw)

        if response.get("type") == "error":
            raise RuntimeError(f"Registration failed: {response.get('code')}: {response.get('message')}")

        if response.get("type") != "registered":
            raise RuntimeError(f"Unexpected response: {response}")

        self.session_id = response["session_id"]
        self.heartbeat_interval_s = response.get("heartbeat_interval_s", 15.0)

    async def _watch_connection(self):
        """Monitor connection health and reconnect if needed.

        This is the single reconnect watcher. It cancels the listen/heartbeat tasks
        and restarts them after a successful reconnect — it never spawns a second
        watcher, so there is no duplicate-listener race.
        """
        while self._running:
            await asyncio.sleep(1)
            if self._reconnecting:
                continue
            if self.websocket and self._ws_closed(self.websocket):
                self._reconnecting = True
                self._connected = False
                self.logger.warning("Connection lost, reconnecting...")
                # Cancel listen + heartbeat, but keep this watcher alive.
                self._cancel_background_tasks(keep_watcher=True)
                try:
                    await self._connect_socket()
                except Exception as e:
                    # Exponential backoff: sleep the current backoff, then grow it
                    # (capped) so a long server outage doesn't hammer the handshake
                    # at a fixed 5s cadence. Reset to the initial backoff on success.
                    self.logger.error(
                        f"Reconnection failed (backoff {self._reconnect_backoff:.1f}s): {e}")
                    await asyncio.sleep(self._reconnect_backoff)
                    self._reconnect_backoff = min(
                        self._reconnect_backoff * 2, self.max_reconnect_backoff_s)
                    self._reconnecting = False
                    continue
                # Reconnected — reset the backoff and restart listen + heartbeat.
                self._reconnect_backoff = self.reconnect_backoff_s
                self._listen_task = asyncio.create_task(self._listen_loop())
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                self._reconnecting = False

    async def _heartbeat_loop(self):
        """Send periodic pongs to keep the connection alive."""
        while self._running:
            await asyncio.sleep(self.heartbeat_interval_s)
            if self.websocket and not self._ws_closed(self.websocket):
                try:
                    await self.websocket.send(json.dumps({
                        "type": "pong",
                        "agent_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    }))
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Message sending
    # ------------------------------------------------------------------

    async def send(self, to: str, body: str, *,
                   reply_to: Optional[str] = None,
                   conversation_id: Optional[str] = None,
                   ttl: int = 0,
                   reopen: bool = False) -> str:
        """Send a message to another agent.

        Args:
            to: Target agent_id.
            body: Message content (string or JSON-serializable dict).
            reply_to: msg_id this is a reply to (optional).
            conversation_id: Existing conversation ID (optional).
            ttl: Time-to-live in seconds for offline queueing (0 = no queue, max 300).
            reopen: If True, reopen an ended conversation (design §2.9).

        Returns:
            The msg_id assigned to this message.
        """
        msg_id = str(uuid.uuid4())
        frame = {
            "type": "message",
            "to": to,
            "body": body,
            "msg_id": msg_id,
            "reply_to": reply_to,
            "conversation_id": conversation_id,
            "ttl": ttl,
            "reopen": bool(reopen),
        }
        await self._send_frame(frame)
        return msg_id

    async def send_receipt(self, msg_id: str):
        """Send an explicit receipt for a received message.

        This is a PUBLIC method that agents call to acknowledge message
        delivery. The server uses this to complete the end-to-end ACK cycle.
        """
        await self._send_frame({
            "type": "received",
            "msg_id": msg_id,
        })

    async def end_conversation(self, conversation_id: str, reason: Optional[str] = None):
        """Signal that a conversation is ended."""
        await self._send_frame({
            "type": "conversation_end",
            "conversation_id": conversation_id,
            "reason": reason or "ended",
        })

    async def block(self, agent_id: str):
        """Block messages from a specific agent."""
        await self._send_frame({
            "type": "block",
            "agent_id": agent_id,
        })

    async def unblock(self, agent_id: str):
        """Unblock a previously blocked agent."""
        await self._send_frame({
            "type": "unblock",
            "agent_id": agent_id,
        })

    async def list_agents(self) -> list[dict]:
        """Get list of all registered agents.

        Returns:
            List of dicts with keys: agent_id, status, metadata.
        """
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._agent_list_futures.append(future)
        await self._send_frame({"type": "list_agents"})
        return await future

    # ------------------------------------------------------------------
    # Message receiving
    # ------------------------------------------------------------------

    async def wait_for_message(self, timeout: Optional[float] = None) -> dict:
        """Block until a message arrives or timeout.

        Args:
            timeout: Seconds to wait (None = forever).

        Returns:
            The message dict with keys: from, body, msg_id, reply_to,
            conversation_id, seq, depth, ts.
        """
        try:
            return await asyncio.wait_for(self._message_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------

    def on_message(self, handler: Callable):
        """Register a callback for incoming messages.

        Handler receives a dict with keys: from, body, msg_id, reply_to,
        conversation_id, seq, depth, ts.
        """
        self._on_message = handler

    def on_presence(self, handler: Callable):
        """Register a callback for presence changes.

        Handler receives a dict with keys: changes (list of {agent_id, status, metadata}).
        """
        self._on_presence = handler

    def on_ack(self, handler: Callable):
        """Register a callback for delivery acknowledgments.

        Handler receives a dict with keys: msg_id, status, to, ts.
        """
        self._on_ack = handler

    def on_conversation_ended(self, handler: Callable):
        """Register a callback for conversation ended events.

        Handler receives a dict with keys: conversation_id, reason, by, ts.
        """
        self._on_conversation_ended = handler

    def on_error(self, handler: Callable):
        """Register a callback for errors.

        Handler receives a dict with keys: code, message, msg_id (optional).
        """
        self._on_error = handler

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def online_agents(self) -> list[str]:
        return [aid for aid, info in self._online_agents.items()
                if info.get("status") == "online"]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _send_frame(self, frame: dict):
        """Send a JSON frame to the server."""
        if not self.websocket or self._ws_closed(self.websocket):
            raise RuntimeError("Not connected")
        await self.websocket.send(json.dumps(frame))

    def _track_task(self, task: asyncio.Task) -> None:
        """Track a fire-and-forget task so its exceptions are retrieved."""
        self._handler_tasks.add(task)
        task.add_done_callback(self._handler_tasks.discard)

    async def _listen_loop(self):
        """Listen for incoming frames from the server."""
        while self._running:
            try:
                raw = await self.websocket.recv()
                frame = json.loads(raw)
                await self._handle_frame(frame)
            except websockets.exceptions.ConnectionClosed:
                self._connected = False
                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Listen error: {e}")

    async def _handle_frame(self, frame: dict):
        """Route an incoming frame to the appropriate handler."""
        msg_type = frame.get("type")

        if msg_type == "message":
            # Incoming message from another agent.
            # 1) Auto-receipt immediately so the server's end-to-end ACK completes even
            #    if the user's on_message handler is slow (keeps the sender's watcher
            #    from timing out to "unconfirmed").
            msg_id = frame.get("msg_id")
            if self.auto_receipt and msg_id is not None:
                t = asyncio.create_task(self.send_receipt(msg_id))
                self._track_task(t)
            # 2) Dispatch the user callback as its own task so the listen loop never
            #    blocks on a slow handler (it must keep reading pongs/frames).
            if self._on_message:
                t = asyncio.create_task(self._on_message(frame))
                self._track_task(t)
            # 3) Enqueue for wait_for_message, with drop-oldest backpressure.
            try:
                self._message_queue.put_nowait(frame)
            except asyncio.QueueFull:
                try:
                    self._message_queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    self._message_queue.put_nowait(frame)
                except asyncio.QueueFull:
                    self.logger.warning("inbound queue full, dropped msg_id=%s", msg_id)

        elif msg_type == "presence":
            # Presence change
            for change in frame.get("changes", []):
                aid = change["agent_id"]
                if change["status"] == "online":
                    self._online_agents[aid] = change
                else:
                    self._online_agents.pop(aid, None)
            if self._on_presence:
                await self._on_presence(frame)

        elif msg_type == "message_ack":
            # Delivery acknowledgment
            if self._on_ack:
                await self._on_ack(frame)

        elif msg_type == "agent_list":
            # Directory query response — resolve the oldest pending request.
            while self._agent_list_futures:
                future = self._agent_list_futures.popleft()
                if not future.done():
                    future.set_result(frame.get("agents", []))
                    break

        elif msg_type == "conversation_ended":
            # Conversation ended by other participant
            if self._on_conversation_ended:
                await self._on_conversation_ended(frame)

        elif msg_type == "ping":
            # Server ping — respond with pong
            await self._send_frame({
                "type": "pong",
                "agent_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            })

        elif msg_type == "shutdown":
            # Server is going down
            self.logger.warning(f"Server shutdown: {frame.get('reason')}")
            self._connected = False

        elif msg_type == "error":
            # Server error
            if self._on_error:
                await self._on_error(frame)
            self.logger.error(f"Server error: {frame.get('code')}: {frame.get('message')}")

        elif msg_type == "registered":
            # Re-registration confirmation (handled in connect)
            self.session_id = frame.get("session_id")
            self.heartbeat_interval_s = frame.get("heartbeat_interval_s", 15.0)
        else:
            self.logger.debug(f"Unhandled frame type: {msg_type}")
