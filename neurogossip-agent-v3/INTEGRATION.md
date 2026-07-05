# Integrating neurogossip-agent-v3 into your agent architecture

This document describes how to integrate **neurogossip-agent-v3** (the conversation session management library) into an AI agent. It details the design changes from v2, integration patterns, and how to configure environment variables.

---

## 1. What the library gives you

- **Session Chaining and Tracking**: Group related messages under a single `conversation_id`, and match sub-requests to responses using `request_id` and `parent_request_id`.
- **Fan-out message tracking**: Dispatch a single task to multiple recipient agents. The manager tracks each response individually and only completes the request when all recipients respond.
- **Durable Persistent History**: Every request, response, and status report is written to a chronological list in Redis, providing a complete audit trail.
- **Local Memory Archiving**: Automatic garbage collection (`sweep_sessions`) purges completed or idle conversations from the agent's memory.
- **Dynamic Un-archiving**: Automatically reloads session history and state from Redis when a message for a purged/inactive conversation arrives.
- **Transport Abstraction**: The manager is decoupled from transport details. By default, it wraps `neurogossip-client-v2` (`RedisAgentTransport`), but can be swapped to other transports.

---

## 2. Prerequisites

1. **Redis instance** (e.g. running on localhost port 6400).
2. **Environment Variables**:
   Configure these variables in your service environment or `.env` file:
   - `NEUROGOSSIP_V3_REDIS_URL` — Connection URL for the Redis server (defaults to `redis://localhost:6379/0`).
   - `NEUROGOSSIP_V3_NAMESPACE` — Namespace to isolate conversations (defaults to `"default"`).
   - `NEUROGOSSIP_V3_AGENT_ID` — Unique ID of the agent connecting.
   - `NEUROGOSSIP_V3_AGENT_NAME` — Display name of the agent.
   - `NEUROGOSSIP_V3_AGENT_ROLE` — Role/responsibility of the agent.

---

## 3. Core Integration Pattern (Async Agent)

If your agent is fully asynchronous, use the manager directly in your event loop:

```python
import asyncio
import redis.asyncio as aioredis
from neurogossip_agent.session_manager import AgentConversationManager
from neurogossip_agent.transport import RedisAgentTransport

async def handle_message(context, message_id, reply_to, tags, sender_id, payload):
    # This callback is executed when a message is received (direct or un-archived)
    print(f"[{context.conversation_id}] Received message from {sender_id}: {payload}")
    
    # Check history in local session state context
    history = context.local_state.get("history", [])
    print(f"History depth: {len(history)}")
    
    # Process and reply
    if "request" in tags:
        # Perform LLM turn, compile answer
        reply = "Simulation complete."
        await manager.send_response(request_id=message_id, sender_id="my_agent_id", payload=reply)

async def sweep_loop(manager):
    # Background loop to clear idle sessions from memory
    while True:
        await asyncio.sleep(60)
        archived = await manager.sweep_sessions(idle_timeout_s=300.0)
        if archived:
            print(f"Archived idle sessions from memory: {archived}")

async def main():
    # 1. Initialize Redis and Transport
    redis_client = aioredis.from_url("redis://localhost:6400", decode_responses=True)
    transport = RedisAgentTransport()  # Loads NEUROGOSSIP_V3_* environment variables
    
    # 2. Initialize Session Manager
    global manager
    manager = AgentConversationManager(redis_client, transport)
    
    # 3. Register Callbacks and Start
    manager.register_handler(handle_message)
    await transport.connect()
    await manager.start_listening()
    
    # Start the memory sweep loop in the background
    asyncio.create_task(sweep_loop(manager))
    
    # Keep running
    try:
        await asyncio.Event().wait()
    finally:
        await manager.stop_listening()
        await transport.disconnect()
        await redis_client.aclose()

asyncio.run(main())
```

---

## 4. Threaded Bridge Pattern (e.g. Agent Axioma)

Many agent frameworks run their reasoning loop in a synchronous thread (especially for long synchronous LLM calls). To prevent event loop stalls, run the session manager in a **dedicated background comms thread** and route messages via thread-safe queues.

Here is the bridge pattern layout for v3:

```python
import asyncio
import threading
import queue
import redis.asyncio as aioredis
from neurogossip_agent.session_manager import AgentConversationManager
from neurogossip_agent.transport import RedisAgentTransport

class NeurogossipV3Bridge:
    """Threaded bridge running neurogossip-agent-v3 on a background loop."""
    def __init__(self, agent_id, redis_url):
        self.agent_id = agent_id
        self.redis_url = redis_url
        self._loop = None
        self._manager = None
        self._transport = None
        self.inbox = queue.Queue()  # Agent thread reads from here
        self._thread = threading.Thread(target=self._run, name=f"ng3-{agent_id}", daemon=True)
        self._ready = threading.Event()

    def start(self):
        self._thread.start()
        self._ready.wait(timeout=10.0)

    # -- Background loop thread --
    def _run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        loop.run_until_complete(self._main())

    async def _main(self):
        redis_client = aioredis.from_url(self.redis_url, decode_responses=True)
        self._transport = RedisAgentTransport(agent_id=self.agent_id, redis_url=self.redis_url)
        self._manager = AgentConversationManager(redis_client, self._transport)
        
        # Register handler that pushes incoming messages to the sync queue
        self._manager.register_handler(self._on_message)
        
        await self._transport.connect()
        await self._manager.start_listening()
        
        # Start background memory sweeping
        asyncio.create_task(self._sweep_loop())
        
        self._ready.set()
        
        # Keep background loop running
        while True:
            await asyncio.sleep(3600)

    async def _on_message(self, context, message_id, reply_to, tags, sender_id, payload):
        # Thread-safe dispatch to the agent's queue
        self.inbox.put((context, message_id, reply_to, tags, sender_id, payload))

    async def _sweep_loop(self):
        while True:
            await asyncio.sleep(60)
            await self._manager.sweep_sessions(idle_timeout_s=300.0)

    # -- Sync Methods called from Agent thread --
    def send_response(self, request_id, payload):
        fut = asyncio.run_coroutine_threadsafe(
            self._manager.send_response(request_id, self.agent_id, payload), self._loop
        )
        return fut.result(timeout=10.0)

    def create_request(self, conversation_id, recipient_ids, payload, parent_request_id=None):
        fut = asyncio.run_coroutine_threadsafe(
            self._manager.create_request(conversation_id, self.agent_id, recipient_ids, payload, parent_request_id),
            self._loop
        )
        return fut.result(timeout=10.0)
```

---

## 5. Integration Checklist

- [ ] `.env` file contains `NEUROGOSSIP_V3_*` environment variables.
- [ ] Redis URL points to the live port (e.g. `redis://localhost:6400`).
- [ ] Agent initializes `AgentConversationManager` wrapping `RedisAgentTransport`.
- [ ] Memory sweeper task (`sweep_sessions`) is scheduled and runs periodically.
- [ ] Incoming messages for archived sessions are automatically un-archived and history reloaded.
- [ ] Fan-out requests correctly supply a list of multiple recipient IDs.
- [ ] Message handlers process payloads inside `local_state` safely.
