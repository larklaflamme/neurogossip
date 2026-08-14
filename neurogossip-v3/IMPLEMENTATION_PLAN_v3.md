# Neurogossip v3 — Implementation Plan v3

**Version:** 3.0.0-final
**Date:** 2026-08-06
**Supersedes:** IMPLEMENTATION_PLAN_v2.md (v2.0.0-draft)
**Depends on:** design/design.md, spec/specification.md, design/research.md
**Reviews incorporated:**
- v1 reviews: review.md, axioma_review_neurogossip_v3.md, thea_review.md, theoria_review.md
- v2 reviews: axioma_review_IMPL_PLAN.md, implementation_plan_signoff.md, thea_review_IMPL.md, theoria_review_impl.md
- v3 reviews: axioma_review_IMPL_PLAN_v2.md, implementation_plan_v2_signoff.md, thea_review_IMPL_v2.md, theoria_review_impl_v2.md

---

## 0. Revision Notes (v2 → v3)

This revision addresses all findings from the four v3 reviews. The changes are:

| Finding | Source | Severity | Resolution |
|---------|--------|----------|------------|
| B.1: Design/Spec still reference wrong client API — no concrete fix task | Axioma | BLOCKING | §4.0 — new task 0.0 added to Phase 0: update design.md §8.1 and spec.md §6.1 to describe actual v5.0 WebSocket client API |
| B.2: `on_message` callback signature mismatch (single dict, not positional args) | Axioma | BLOCKING | §2.1, §2.3, §4.2 task 2.2 — all three locations corrected: callback receives a single `frame: dict`, key is `"from"` not `"sender_id"` |
| B.3: `block_agent`/`unblock_agent` vs actual `block`/`unblock` | Axioma | BLOCKING | §2.1 table — corrected to `block(agent_id)` / `unblock(agent_id)` |
| H.1: No concrete task to update design/spec documents | Axioma | HIGH | §4.0 — task 0.0 (same as B.1) |
| H.2: Phase 2 Task 2.2 wording still references DeliberationManager | Axioma | HIGH | §4.2 task 2.2 — wording corrected: dispatches to typed `asyncio.Queue` instances, not directly to DeliberationManager |
| M-NEW: Error codes don't match the spec (plan: 18, spec: 17) | Theoria | MODERATE | §4.0 task 0.3 — harmonized to spec's 17 error codes. Removed 8 plan-only codes (BOUNDS_EXCEEDED, TRANSPORT_ERROR, CONNECTION_LOST, MESSAGE_TOO_LARGE, RATE_LIMITED, AUTH_FAILED, SERIALIZATION_ERROR, UNKNOWN_ERROR). Added 7 spec codes (CONTRIBUTION_NOT_FOUND, INVALID_CHOICE, DELIBERATION_CLOSED, INSUFFICIENT_PRIVILEGES, QUORUM_NOT_MET, CONSENSUS_NOT_REACHED, STALEMATE). Renamed VOTE_NOT_OPEN → VOTE_CLOSED, ALREADY_VOTED → VOTE_ALREADY_ACTIVE. |
| — | `formation_timeout_seconds` defaults to 30.0 in plan but 60s in design §6.1 | Theoria | MINOR | §4.3 task 3.2 — aligned to 60s |
| H.3 | Parallelization claim — Phase 6 integrates with Phase 3's contribute() flow | Theoria | MINOR | §3 dependency graph — Phase 6 now correctly shown as depending on Phase 3, not parallelizable with it |
| H.4 | No real-server smoke test until Phase 8 | Theoria | MINOR | §4.2 task 2.3 — new task: smoke test with real neurogossip-server after transport adapter is built |
| M.2 | `catch_up()` mechanism still unspecified for v5.0 WebSocket client | Theoria | MINOR | §4.3 task 3.8 — specified: catch_up() uses `wait_for_message(timeout=0.1)` in a tight loop to drain the inbound queue, then queries the state store for the contribution log |
| — | Phase numbering in §1.3 uses different semantics from implementation phases | Thea | OBSERVATION | §1.3 — clarified: "Phase 1" and "Phase 2" refer to release milestones, not implementation phases. Implementation phases are numbered 0–10. |

---

## 1. Overview

This plan describes how to build the neurogossip-v3 deliberation protocol on top of the existing neurogossip-client (v5.0, WebSockets) and neurogossip-server (v5.0). The implementation is a new Python package `neurogossip_v3` that sits above the transport layer and provides the Deliberation Manager — the stateful, multi-turn, multi-agent conversation engine described in the design and specification.

### 1.1 What Exists (Do Not Rebuild)

| Component | Location | Role |
|-----------|----------|------|
| **neurogossip-client v5.0** | `neurogossip-client/` | WebSocket transport: connect, send, receive, receipts, presence, reconnection |
| **neurogossip-server v5.0** | `neurogossip-server/` | Message routing, agent registry, presence, offline queue, rate limiting, loop prevention |
| **neurogossip-agent-v3** | `neurogossip-agent-v3/` | Session management (Redis-backed): conversations, requests, responses, history, fan-out, HITL. **Will be replaced by v3 deliberation model.** |

### 1.2 What We Build

A new package `neurogossip_v3` at `/home/ubuntu/neurogossip/neurogossip-v3/src/neurogossip_v3/` containing:

1. **Data models** — Deliberation, Contribution, Resolution, Group, VoteSession, DissentingOpinion (Pydantic v2)
2. **State store** — In-memory (Phase 1) + optional Redis (Phase 2) for deliberation state, contribution log, vote tallies, agent cursors
3. **Deliberation Manager** — the core engine: start, contribute, wait, resolve, interrupt, vote, decline, summarize, catch_up
4. **Group Manager** — create, join, leave, list groups
5. **Bounds Enforcer** — max turns, max duration, circularity detection (v3.0); semantic drift (v3.1)
6. **WebSocket Agent Transport** — wraps neurogossip-client v5.0 for the deliberation layer
7. **Turn-based adapter** — SuspendTurn exception for Skye's turn-based runtime
8. **Tests** — unit, integration, and scenario tests

### 1.3 What We Do NOT Build (Deferred)

| Item | Target Version | Reason |
|------|---------------|--------|
| Semantic drift detection | v3.1 | Requires calibration experiment (embedding model, threshold) |
| Redis persistence backend | Release milestone 2 | In-memory store sufficient for milestone 1; Redis adds durability |
| A2A-compatible HTTP gateway | Release milestone 3 | External agent participation; not needed for closed family |
| Example deliberation transcript | Implementation phase | Thea F11 — will add during integration testing |

> **Note on "Phase" vs "Release Milestone":** Implementation phases are numbered 0–10. "Release milestone 1" = initial release (in-memory, single-process), "Release milestone 2" = Redis persistence, "Release milestone 3" = A2A gateway. These are product milestones, not implementation phases.

---

## 2. Transport Grounding — The v5.0 WebSocket Client API

> **This section documents the actual v5.0 client API.** It addresses Axioma's B.1–B.3 findings: the design and spec describe the v2 Redis client API, but the implementation plan correctly targets the v5.0 WebSocket client. This section documents the actual v5.0 client API and the exact mapping the transport adapter will use. **Task 0.0 (Phase 0) will update the design and spec documents to match.**

### 2.1 Actual v5.0 Client API (from source code audit)

The `NeurogossipClient` class in `neurogossip-client/src/neurogossip_client/client.py` provides:

| Method / Property | Signature | Purpose |
|-------------------|-----------|---------|
| `connect()` | `async` | Connect to the WebSocket server, register agent |
| `disconnect()` | `async` | Graceful disconnect |
| `send(to, body, *, reply_to, conversation_id, ttl)` | `async → msg_id` | Send a message to an agent. `body` is a string. Returns the message ID. |
| `wait_for_message(timeout)` | `async → dict` | Block until a message arrives or timeout. Returns the raw message dict. |
| `on_message(callback)` | `sync` | Register a push callback. The callback receives a **single dict argument** (`frame`) with keys: `from`, `body`, `msg_id`, `reply_to`, `conversation_id`, `seq`, `depth`, `ts`. |
| `list_agents()` | `async → list[dict]` | List all registered agents with their metadata and online status. |
| `online_agents` | `property → list[str]` | List of agent_ids currently online. |
| `end_conversation(conversation_id)` | `async` | Signal conversation end to the server. |
| `block(agent_id)` / `unblock(agent_id)` | `async` | Block/unblock an agent from messaging this agent. |

**Key differences from the v2 Redis client API described in the design/spec:**

| Design/Spec §8.1/§6.1 says | Actual v5.0 WebSocket client |
|---|---|
| `send_direct(to_agent_id, message)` | `send(to, body, *, reply_to, conversation_id)` |
| `publish(room_id, message)` | No native publish — fan-out of `send()` calls |
| `listen()` (async iterator) | `wait_for_message(timeout)` or `on_message()` callback |
| `GossipMessage` envelope | Plain dicts (JSON over WebSocket) |
| `presence.list_online_agents()` | `client.list_agents()` or `client.online_agents` |

### 2.2 Transport Adapter Mapping

The `WebSocketAgentTransport` (Phase 2) wraps `NeuroGossipClient` and provides these deliberation-level methods. The mapping is:

| Transport Adapter Method | Maps To | Implementation |
|--------------------------|---------|----------------|
| `send_contribution(deliberation_id, contribution: Contribution)` | `client.send(to, body, reply_to=..., conversation_id=deliberation_id)` | Serialize contribution to JSON, wrap in `{"type": "deliberation", "deliberation_id": ..., "contribution": ...}` |
| `broadcast_to_group(group_id, message)` | Loop of `client.send()` to each group member | Fetch member list from GroupManager, fan-out sends |
| `send_invitation(deliberation_id, to_agent)` | `client.send(to, body, conversation_id=deliberation_id)` | Wrap in `{"type": "deliberation_invitation", ...}` |
| `send_vote_session(deliberation_id, vote_session)` | `client.send()` to each participant | Wrap in `{"type": "vote_session", ...}` |
| `wait_for_message(timeout)` | `client.wait_for_message(timeout)` | Direct passthrough |
| `get_online_agents()` | `client.online_agents` | Direct passthrough |
| `list_all_agents()` | `client.list_agents()` | Direct passthrough |

### 2.3 Background Listener Architecture

The transport adapter uses a push-based listener pattern:

```
client.on_message(callback=_dispatch_incoming)
                                    │
                                    ▼
def _dispatch_incoming(frame: dict):
    sender_id = frame["from"]        # NOTE: key is "from", not "sender_id"
    body = frame["body"]
    msg_id = frame["msg_id"]
    reply_to = frame.get("reply_to")
    conversation_id = frame.get("conversation_id")

    # Route by message type
    msg = json.loads(body)
    msg_type = msg.get("type")

    if msg_type == "deliberation":
        deliberation_queue.put_nowait((sender_id, msg, msg_id, reply_to, conversation_id))
    elif msg_type == "deliberation_invitation":
        invitation_queue.put_nowait((sender_id, msg, msg_id, conversation_id))
    elif msg_type == "vote_session":
        vote_queue.put_nowait((sender_id, msg, msg_id, conversation_id))
    # ... other types
```

The `_background_listener()` task (started at `connect()`) reads from these `asyncio.Queue` instances and dispatches to the appropriate manager method. This decouples the WebSocket callback (which must return quickly) from the deliberation logic (which may involve state transitions and waiting).

---

## 3. Implementation Phases — Dependency Graph

```
Phase 0: Scaffolding & Models
    │
    ▼
Phase 1: State Store
    │
    ▼
Phase 2: Transport Adapter ──► Phase 2.3: Smoke test with real server
    │
    ▼
Phase 3: Manager Core ◄────────────────────────────┐
    │                                                │
    ├──────────────┬──────────────┬─────────────────┤
    ▼              ▼              ▼                  │
Phase 4:       Phase 5:       Phase 7:              │
Voting         Groups         Turn Adapter          │
    │              │                                 │
    └──────┬───────┘                                 │
           ▼                                         │
     Phase 6: Bounds ───────────────────────────────┘
     (depends on Phase 3's contribute() flow)
           │
           ▼
     Phase 8: Integration Tests
           │
           ▼
     Phase 9: Migration
           │
           ▼
     Phase 10: Documentation
```

**Parallelization:** Phases 4, 5, and 7 can run in parallel (they touch different subsystems). Phase 6 depends on Phase 3 (it integrates with `contribute()`) and should run after Phase 3 is complete — it is NOT parallelizable with Phase 3. Phase 6 can run in parallel with Phases 4, 5, and 7.

---

## 4. Phase-by-Phase Task Breakdown

### Phase 0: Scaffolding & Data Models (Days 1–2)

**Goal:** Package structure, all Pydantic v2 data models, error types, test infrastructure.

#### Task 0.0: Update Design/Spec Transport Bindings (NEW in v3)

Update the design and spec documents to describe the actual v5.0 WebSocket client API instead of the v2 Redis client API. This is a mechanical fix:

- **design.md §8.1:** Replace `send_direct()` → `send()`, `publish()` → fan-out `send()`, `GossipMessage` → dict envelope, `listen()` → `on_message()` callback, `presence.list_online_agents()` → `client.list_agents()` / `client.online_agents`
- **spec.md §6.1:** Same replacements
- Add a note that the v2 Redis client API is documented for historical reference in an appendix

#### Task 0.1: Package Scaffolding

```
neurogossip-v3/
├── src/
│   └── neurogossip_v3/
│       ├── __init__.py
│       ├── models.py          # Pydantic v2 data models
│       ├── errors.py          # Error types
│       ├── state_store.py     # Abstract + InMemory
│       ├── transport.py       # WebSocketAgentTransport
│       ├── manager.py         # DeliberationManager
│       ├── group_manager.py   # GroupManager
│       ├── bounds.py          # BoundsEnforcer
│       ├── voting.py          # Voting subsystem
│       └── turn_adapter.py    # TurnBasedAdapter
├── tests/
│   ├── __init__.py
│   ├── test_models.py
│   ├── test_state_store.py
│   ├── test_transport.py
│   ├── test_manager.py
│   ├── test_voting.py
│   ├── test_groups.py
│   ├── test_bounds.py
│   └── test_integration.py
├── examples/
│   ├── simple_deliberation.py
│   ├── group_voting.py
│   └── turn_based_agent.py
├── pyproject.toml
└── README.md
```

#### Task 0.2: Data Models (Pydantic v2)

Implement all data models from spec §3:

- `Deliberation`, `DeliberationStatus`, `DeliberationBounds`
- `Contribution`, `ContributionKind`
- `Resolution`, `ResolutionKind`, `DissentingOpinion`
- `Group`
- `VoteSession`, `VoteStatus`, `Vote`
- `ExecutionMode` enum: `ASYNC`, `TURN_BASED`

All models use Pydantic v2 with `model_config = ConfigDict(extra="forbid")`. UUIDs are `uuid.UUID`. Datetimes are `datetime.datetime` with UTC timezone.

#### Task 0.3: Error Types

Implement the 17 error codes from spec §7:

| Code | Meaning |
|------|---------|
| `DELIBERATION_NOT_FOUND` | deliberation_id does not exist |
| `INVALID_STATUS` | operation not valid in current status |
| `NOT_PARTICIPANT` | agent is not a participant |
| `INVALID_KIND` | contribution kind not valid in current status |
| `CONTRIBUTION_NOT_FOUND` | reply_to or contribution_id does not exist |
| `VOTE_NOT_FOUND` | vote_id does not exist |
| `VOTE_CLOSED` | vote is no longer OPEN |
| `VOTE_ALREADY_ACTIVE` | a vote is already in progress |
| `INVALID_CHOICE` | vote choice not in options |
| `TIMEOUT` | operation timed out |
| `INTERRUPTED` | deliberation was interrupted |
| `ALREADY_EXISTS` | deliberation/group with this ID already exists |
| `EMPTY_PARTICIPANTS` | participants set is empty |
| `GROUP_NOT_FOUND` | group_id does not exist |
| `GROUP_ALREADY_EXISTS` | group with this name already exists |
| `ALREADY_MEMBER` | agent is already a member |
| `NOT_MEMBER` | agent is not a member |

Each error is a subclass of `NeurogossipError` with a `code` class attribute and a human-readable `message`.

#### Task 0.4: Test Infrastructure

- `pytest` with `pytest-asyncio`
- `conftest.py` with fixtures: sample deliberation, sample contribution, sample group, sample vote session
- CI configuration (`.github/workflows/test.yml` or local equivalent)

---

### Phase 1: State Store (Days 2–3)

**Goal:** Abstract state store interface + InMemoryStateStore implementation.

#### Task 1.1: Abstract Interface

```python
class StateStore(ABC):
    # Deliberation CRUD
    @abstractmethod
    async def create_deliberation(self, deliberation: Deliberation) -> None: ...
    @abstractmethod
    async def get_deliberation(self, deliberation_id: UUID) -> Deliberation | None: ...
    @abstractmethod
    async def update_deliberation(self, deliberation: Deliberation) -> None: ...
    @abstractmethod
    async def delete_deliberation(self, deliberation_id: UUID) -> None: ...

    # Contribution log
    @abstractmethod
    async def append_contribution(self, contribution: Contribution) -> None: ...
    @abstractmethod
    async def get_contributions(
        self, deliberation_id: UUID,
        limit: int = 100,
        before_contribution_id: UUID | None = None
    ) -> list[Contribution]: ...

    # Vote sessions
    @abstractmethod
    async def create_vote_session(self, vote: VoteSession) -> None: ...
    @abstractmethod
    async def get_vote_session(self, vote_id: UUID) -> VoteSession | None: ...
    @abstractmethod
    async def update_vote_session(self, vote: VoteSession) -> None: ...

    # Group CRUD
    @abstractmethod
    async def create_group(self, group: Group) -> None: ...
    @abstractmethod
    async def get_group(self, group_id: UUID) -> Group | None: ...
    @abstractmethod
    async def update_group(self, group: Group) -> None: ...
    @abstractmethod
    async def delete_group(self, group_id: UUID) -> None: ...
    @abstractmethod
    async def list_groups(self) -> list[Group]: ...

    # Agent cursor tracking (for catch_up)
    @abstractmethod
    async def set_agent_cursor(
        self, agent_id: str, deliberation_id: UUID, last_seen_contribution_id: UUID
    ) -> None: ...
    @abstractmethod
    async def get_agent_cursor(
        self, agent_id: str, deliberation_id: UUID
    ) -> UUID | None: ...
```

#### Task 1.2: InMemoryStateStore

Thread-safe implementation using `asyncio.Lock`:

- `dict[UUID, Deliberation]` for deliberations
- `dict[UUID, list[Contribution]]` for contribution logs (append-only)
- `dict[UUID, VoteSession]` for vote sessions
- `dict[UUID, Group]` for groups
- `dict[tuple[str, UUID], UUID]` for agent cursors

Key format: `ng3:{namespace}:{entity_type}:{entity_id}` for future Redis migration.

All state-mutating operations are broadcast-ready: after mutation, the store returns the updated object so the caller can broadcast to other agents.

#### Task 1.3: State Store Tests

- CRUD operations for all entity types
- Concurrent access (multiple coroutines appending contributions)
- Cursor tracking and catch_up pagination
- Broadcast return values

---

### Phase 2: WebSocket Transport Adapter (Days 3–4)

**Goal:** `WebSocketAgentTransport` wrapping `NeuroGossipClient` v5.0.

#### Task 2.1: Transport Adapter Class

```python
class WebSocketAgentTransport:
    def __init__(self, server_url: str, agent_id: str, metadata: dict | None = None):
        self._client = NeurogossipClient(server_url, agent_id, metadata)
        self._deliberation_queue: asyncio.Queue = asyncio.Queue()
        self._invitation_queue: asyncio.Queue = asyncio.Queue()
        self._vote_queue: asyncio.Queue = asyncio.Queue()
        self._listener_task: asyncio.Task | None = None

    async def connect(self):
        await self._client.connect()
        self._client.on_message(self._dispatch_incoming)
        self._listener_task = asyncio.create_task(self._background_listener())

    async def disconnect(self):
        if self._listener_task:
            self._listener_task.cancel()
        await self._client.disconnect()

    def _dispatch_incoming(self, frame: dict):
        """Called by client.on_message() — receives a single dict."""
        sender_id = frame["from"]        # NOTE: key is "from"
        body = frame["body"]
        msg_id = frame["msg_id"]
        reply_to = frame.get("reply_to")
        conversation_id = frame.get("conversation_id")

        msg = json.loads(body)
        msg_type = msg.get("type")

        if msg_type == "deliberation":
            self._deliberation_queue.put_nowait(
                (sender_id, msg, msg_id, reply_to, conversation_id)
            )
        elif msg_type == "deliberation_invitation":
            self._invitation_queue.put_nowait(
                (sender_id, msg, msg_id, conversation_id)
            )
        elif msg_type == "vote_session":
            self._vote_queue.put_nowait(
                (sender_id, msg, msg_id, conversation_id)
            )

    async def _background_listener(self):
        """Read from typed queues and dispatch to appropriate handlers."""
        while True:
            # Check all queues with short timeouts
            try:
                item = self._deliberation_queue.get_nowait()
                await self._handle_deliberation_message(*item)
            except asyncio.QueueEmpty:
                pass
            try:
                item = self._invitation_queue.get_nowait()
                await self._handle_invitation(*item)
            except asyncio.QueueEmpty:
                pass
            try:
                item = self._vote_queue.get_nowait()
                await self._handle_vote_message(*item)
            except asyncio.QueueEmpty:
                pass
            await asyncio.sleep(0.01)  # prevent tight loop

    # Public API — maps to client methods
    async def send_contribution(
        self, to: str, deliberation_id: UUID, contribution: Contribution
    ) -> str:
        body = json.dumps({
            "type": "deliberation",
            "deliberation_id": str(deliberation_id),
            "contribution": contribution.model_dump(mode="json")
        })
        return await self._client.send(
            to, body,
            reply_to=str(contribution.reply_to) if contribution.reply_to else None,
            conversation_id=str(deliberation_id)
        )

    async def send_invitation(
        self, to: str, deliberation_id: UUID, goal: str, initiator_id: str
    ) -> str:
        body = json.dumps({
            "type": "deliberation_invitation",
            "deliberation_id": str(deliberation_id),
            "goal": goal,
            "initiator_id": initiator_id
        })
        return await self._client.send(to, body, conversation_id=str(deliberation_id))

    async def send_vote_session(
        self, to: str, deliberation_id: UUID, vote_session: VoteSession
    ) -> str:
        body = json.dumps({
            "type": "vote_session",
            "deliberation_id": str(deliberation_id),
            "vote_session": vote_session.model_dump(mode="json")
        })
        return await self._client.send(to, body, conversation_id=str(deliberation_id))

    async def broadcast_to_participants(
        self, participants: set[str], deliberation_id: UUID, message: dict
    ) -> list[str]:
        """Fan-out send to all participants. Returns list of message IDs."""
        msg_ids = []
        body = json.dumps(message)
        for agent_id in participants:
            if agent_id != self._client.agent_id:  # skip self
                msg_id = await self._client.send(
                    agent_id, body, conversation_id=str(deliberation_id)
                )
                msg_ids.append(msg_id)
        return msg_ids

    async def wait_for_message(self, timeout: float | None = None) -> dict:
        return await self._client.wait_for_message(timeout)

    @property
    def online_agents(self) -> list[str]:
        return self._client.online_agents

    async def list_agents(self) -> list[dict]:
        return await self._client.list_agents()

    async def block(self, agent_id: str) -> None:
        return await self._client.block(agent_id)

    async def unblock(self, agent_id: str) -> None:
        return await self._client.unblock(agent_id)
```

#### Task 2.2: Background Listener Integration

The `_background_listener()` dispatches to handler methods that the DeliberationManager registers:

```python
class WebSocketAgentTransport:
    def set_deliberation_handler(self, handler: Callable):
        """Register handler for incoming deliberation messages."""
        self._deliberation_handler = handler

    def set_invitation_handler(self, handler: Callable):
        """Register handler for incoming invitations."""
        self._invitation_handler = handler

    def set_vote_handler(self, handler: Callable):
        """Register handler for incoming vote messages."""
        self._vote_handler = handler

    async def _handle_deliberation_message(self, sender_id, msg, msg_id, reply_to, conv_id):
        if self._deliberation_handler:
            await self._deliberation_handler(sender_id, msg, msg_id, reply_to, conv_id)
```

The DeliberationManager registers its handlers at initialization time. The transport does NOT reference the DeliberationManager directly — it uses the handler callbacks, keeping the layers decoupled.

#### Task 2.3: Smoke Test with Real Server (NEW in v3)

Before proceeding to Phase 3, verify the transport adapter works with a real neurogossip-server:

1. Start neurogossip-server on localhost:8765
2. Create two `WebSocketAgentTransport` instances (agent_a, agent_b)
3. Connect both
4. agent_a sends a deliberation message to agent_b
5. agent_b's background listener receives it via the queue
6. Verify message round-trip: type, deliberation_id, contribution content
7. Disconnect both

This catches transport integration issues early, rather than at Phase 8.

#### Task 2.4: Transport Tests (Mocked)

- Mock `NeuroGossipClient` for unit tests
- Test `_dispatch_incoming` routing by message type
- Test `broadcast_to_participants` fan-out
- Test handler registration and invocation
- Test connect/disconnect lifecycle

---

### Phase 3: Deliberation Manager Core (Days 4–7)

**Goal:** The core deliberation engine — start, contribute, wait, resolve, interrupt, decline, catch_up, summarize.

#### Task 3.1: DeliberationManager Constructor

```python
class DeliberationManager:
    def __init__(
        self,
        agent_id: str,
        state_store: StateStore,
        transport: WebSocketAgentTransport,
        execution_mode: ExecutionMode = ExecutionMode.ASYNC,
    ):
        self.agent_id = agent_id
        self._store = state_store
        self._transport = transport
        self._execution_mode = execution_mode

        # Register transport handlers
        self._transport.set_deliberation_handler(self._handle_incoming_contribution)
        self._transport.set_invitation_handler(self._handle_invitation)
        self._transport.set_vote_handler(self._handle_incoming_vote)

        # Active waiting: dict[deliberation_id, asyncio.Event]
        self._wait_events: dict[UUID, asyncio.Event] = {}
        self._wait_results: dict[UUID, list[Contribution]] = {}
```

#### Task 3.2: start_deliberation()

```
start_deliberation(
    goal: str,
    participants: set[str],
    bounds: DeliberationBounds | None = None,
    parent_deliberation_id: UUID | None = None,
    async_child: bool = False,
    formation_timeout_seconds: float = 60.0,  # aligned with design §6.1
) → Deliberation
```

State machine:
1. Create Deliberation with status=FORMING
2. Store in state store
3. Send invitations to all participants (except self)
4. Wait for acceptances (up to `formation_timeout_seconds`)
5. On all accepted OR formation_timeout: transition to ACTIVE with current participants
6. Broadcast status change to all participants
7. Return the Deliberation

If `parent_deliberation_id` is set and `async_child=False`, the parent deliberation transitions to WAITING until the child resolves. If `async_child=True`, the parent continues in parallel.

#### Task 3.3: contribute()

```
contribute(
    deliberation_id: UUID,
    kind: ContributionKind,
    content: str,
    reply_to: UUID | None = None,
    addressed_to: set[str] | None = None,
    expects_response_from: set[str] | None = None,
    confidence: float | None = None,
    references: list[Reference] | None = None,
) → Contribution
```

Validations:
- Deliberation exists and status is ACTIVE (or WAITING if this agent is an expected responder)
- Agent is a participant
- If `kind=VOTE`, deliberation status MUST be VOTING (returns INVALID_KIND otherwise)
- If `reply_to` is set, the referenced contribution exists

State transitions:
- If `expects_response_from` is non-empty: ACTIVE → WAITING
- Otherwise: stays ACTIVE

After storing the contribution, broadcast to all participants.

#### Task 3.4: wait_for_responses()

```
wait_for_responses(
    deliberation_id: UUID,
    timeout_seconds: float = 300.0,
    timeout_policy: str = "fail",  # "fail" | "return_partial" | "extend"
) → list[Contribution]
```

Behavior:
1. Look up the most recent contribution from this agent in this deliberation
2. Extract `expects_response_from`
3. If empty, return immediately
4. If `execution_mode == TURN_BASED`, raise `SuspendTurn` with the wait context
5. If `execution_mode == ASYNC`, block on an `asyncio.Event` until all expected responders have replied or timeout
6. On timeout: apply `timeout_policy`:
   - `"fail"`: raise TIMEOUT error
   - `"return_partial"`: return whatever responses have arrived
   - `"extend"`: double the timeout and wait again (once)

#### Task 3.5: resolve()

```
resolve(
    deliberation_id: UUID,
    kind: ResolutionKind,
    summary: str,
    conclusion: str | None = None,
) → Resolution
```

Validations:
- Deliberation exists and status is ACTIVE, WAITING, or VOTING
- Agent is a participant

State transition: current → RESOLVING → terminal (RESOLVED, DEADLOCKED, etc.)

Broadcast resolution to all participants.

#### Task 3.6: interrupt()

```
interrupt(
    deliberation_id: UUID,
    reason: str,
) → Resolution
```

Validations:
- Deliberation exists and is not already in a terminal state
- Agent is a participant

State transition: current → INTERRUPTED (terminal).

Broadcast interruption to all participants. Any waiting agents are unblocked with an INTERRUPTED error.

#### Task 3.7: decline_invitation()

```
decline_invitation(
    deliberation_id: UUID,
    reason: str | None = None,
) → None
```

Removes the agent from the FORMING deliberation's participant list. If the initiator declines, the deliberation is ABANDONED.

#### Task 3.8: catch_up()

```
catch_up(
    deliberation_id: UUID,
    limit: int = 100,
    before_contribution_id: UUID | None = None,
) → list[Contribution]
```

Implementation for v5.0 WebSocket client:
1. Query the state store for the agent's cursor (last seen contribution_id)
2. Fetch all contributions after that cursor from the contribution log
3. If `before_contribution_id` is set, paginate: return up to `limit` contributions before that ID
4. Update the agent's cursor to the latest contribution_id
5. Return the missed contributions

For the initial implementation (in-memory state store), this is a simple list slice. For Redis (milestone 2), this becomes a ZRANGEBYSCORE on the contribution log sorted set.

The transport layer's `wait_for_message(timeout=0.1)` is used in a tight loop to drain any queued messages before querying the state store, ensuring the agent doesn't miss contributions that arrived between the last cursor update and the catch_up call.

#### Task 3.9: summarize()

```
summarize(
    deliberation_id: UUID,
    max_length: int = 500,
) → str
```

Returns a natural-language summary of the deliberation so far: goal, key contributions, current status, outstanding questions. Used by agents reconnecting mid-deliberation to quickly understand context.

#### Task 3.10: Manager Core Tests

- Full lifecycle: FORMING → ACTIVE → RESOLVED
- Response waiting: agent A sends with `expects_response_from=[B]`, agent B contributes, agent A unblocks
- Response waiting timeout: `timeout_policy="return_partial"`
- Interruption mid-wait: agent C interrupts, waiting agents unblock with INTERRUPTED
- Sub-deliberation: parent blocks while child runs (`async_child=False`)
- Sub-deliberation: parent continues in parallel (`async_child=True`)
- Decline invitation: agent declines, deliberation proceeds without them
- Catch-up: agent reconnects, catches up on missed contributions
- Summarize: returns coherent summary of deliberation state
- VOTE kind restriction: `kind=VOTE` in ACTIVE status → INVALID_KIND

---

### Phase 4: Voting Subsystem (Days 7–8)

**Goal:** propose_vote, cast_vote, tally, timeout policies.

#### Task 4.1: propose_vote()

```
propose_vote(
    deliberation_id: UUID,
    proposal: str,
    options: list[str],
    threshold: float = 0.5,
    timeout_s: float = 300.0,
    on_timeout: str = "fail",  # "fail" | "pass" | "extend"
) → VoteSession
```

Validations:
- Deliberation exists and status is ACTIVE or WAITING
- Agent is a participant
- No vote already active (returns VOTE_ALREADY_ACTIVE)

State transition: current → VOTING.

Returns the created `VoteSession` (not a `VoteResult` — the vote is just starting).

#### Task 4.2: cast_vote()

```
cast_vote(
    vote_id: UUID,
    choice: str,
) → Vote
```

Validations:
- Vote session exists and status is OPEN
- Choice is in options (returns INVALID_CHOICE)
- Agent hasn't already voted

After casting, check if all participants have voted or threshold is mathematically decided. If so, close the vote and transition deliberation back to ACTIVE.

#### Task 4.3: Vote Timeout Handling

When a vote times out:
- `"fail"`: vote fails, deliberation transitions to ACTIVE
- `"pass"`: vote passes with current tallies, deliberation transitions to ACTIVE
- `"extend"`: timeout doubled once, then `"fail"` on second timeout

#### Task 4.4: Voting Tests

- Propose vote, all cast, tally correct
- Vote timeout with each policy
- VOTE_ALREADY_ACTIVE when proposing while vote is open
- INVALID_CHOICE for choice not in options
- Threshold met early (e.g., 3/5 votes cast, 2 agree → threshold 0.5 met)

---

### Phase 5: Group Manager (Days 8–9)

**Goal:** Group CRUD, member management, group deliberation start.

#### Task 5.1: GroupManager Class

```python
class GroupManager:
    def __init__(self, state_store: StateStore, transport: WebSocketAgentTransport):
        ...

    async def create_group(self, name: str, members: set[str]) -> Group: ...
    async def join_group(self, group_id: UUID) -> Group: ...
    async def leave_group(self, group_id: UUID) -> Group: ...
    async def list_groups(self) -> list[Group]: ...
    async def get_group(self, group_id: UUID) -> Group: ...
    async def delete_group(self, group_id: UUID) -> None: ...
```

#### Task 5.2: Group Deliberation Start

```
start_group_deliberation(
    group_id: UUID,
    goal: str,
    bounds: DeliberationBounds | None = None,
) → Deliberation
```

Fetches group members, creates a deliberation with all of them as participants, sends invitations. Same formation timeout semantics as `start_deliberation()`.

#### Task 5.3: Group Tests

- Create group, join, leave, delete
- Start group deliberation, all members invited
- Member leaves group mid-deliberation (remains in existing deliberations, excluded from new ones)

---

### Phase 6: Bounds Enforcement (Days 9–10)

**Goal:** max_turns, max_duration, circularity detection.

> **Dependency note:** Phase 6 integrates with Phase 3's `contribute()` flow. It must run after Phase 3 is complete. It can run in parallel with Phases 4, 5, and 7.

#### Task 6.1: BoundsEnforcer Class

```python
class BoundsEnforcer:
    def __init__(self, state_store: StateStore):
        ...

    async def check_bounds(self, deliberation_id: UUID) -> list[str]:
        """Check all bounds. Returns list of violated bound names (empty = all clear)."""
        ...
```

#### Task 6.2: Turn Limit Enforcement

After each `contribute()`, check if `contribution_count >= max_turns`. If so, auto-interrupt with reason "max_turns exceeded".

#### Task 6.3: Duration Enforcement

Background task checks active deliberations every 30 seconds. If `now - created_at >= max_duration_s`, auto-timeout with TIMED_OUT resolution.

#### Task 6.4: Circularity Detection

After each `contribute()`, check for circularity:

1. Only check contributions with `len(content) >= min_contribution_length_for_circularity_check` (default 20 characters) — prevents false positives on short votes/acks
2. Compute cosine similarity between the new contribution's text embedding and the last N contributions (default N=5)
3. Use `nomic-embed-text` via local Ollama for embeddings
4. If similarity > 0.95 for any pair, auto-interrupt with reason "circularity detected"

This aligns with spec §5.3 (cosine similarity on embeddings).

#### Task 6.5: Bounds Tests

- Turn limit exceeded → auto-interrupt
- Duration exceeded → auto-timeout
- Circularity detected → auto-interrupt
- Short contributions (< 20 chars) excluded from circularity check
- BoundsEnforcer returns empty list when all bounds pass

---

### Phase 7: Turn-Based Adapter (Days 10–11)

**Goal:** SuspendTurn exception and TurnBasedAdapter for Skye's runtime.

#### Task 7.1: SuspendTurn Exception

```python
class SuspendTurn(Exception):
    """Raised by wait_for_responses() in TURN_BASED mode."""
    def __init__(self, deliberation_id: UUID, expected_responders: set[str], timeout_s: float):
        self.deliberation_id = deliberation_id
        self.expected_responders = expected_responders
        self.timeout_s = timeout_s
        self.context = {
            "deliberation_id": str(deliberation_id),
            "expected_responders": list(expected_responders),
            "timeout_s": timeout_s,
        }
```

#### Task 7.2: TurnBasedAdapter

```python
class TurnBasedAdapter:
    """Wraps DeliberationManager for turn-based agents like Skye."""

    def __init__(self, manager: DeliberationManager):
        self._manager = manager
        self._pending_waits: dict[UUID, asyncio.Event] = {}

    async def handle_turn(self, incoming_message: dict) -> dict | None:
        """Process one turn. Returns a SuspendTurn context if waiting, or a result."""
        ...

    def serialize_context(self, suspend_turn: SuspendTurn) -> dict:
        """Serialize the wait context for cross-session persistence."""
        return suspend_turn.context

    @staticmethod
    def deserialize_context(data: dict) -> SuspendTurn:
        """Reconstruct a SuspendTurn from serialized context."""
        ...
```

#### Task 7.3: Turn Adapter Tests

- `wait_for_responses()` raises `SuspendTurn` in TURN_BASED mode
- `SuspendTurn.context` is JSON-serializable
- `TurnBasedAdapter.deserialize_context()` reconstructs correctly
- ASYNC mode does NOT raise SuspendTurn

---

### Phase 8: Integration & Scenario Tests (Days 11–13)

**Goal:** 10 scenario tests covering full lifecycle edge cases.

#### Task 8.1: Scenario Tests

| # | Scenario | What It Tests |
|---|----------|---------------|
| 1 | Simple deliberation | Two agents, 3-turn exchange, RESOLVED |
| 2 | Response waiting | Agent A waits for B, B responds, A unblocks |
| 3 | Group deliberation | 3 agents in a group, all contribute, vote, resolve |
| 4 | Interruption | Agent C interrupts a running deliberation |
| 5 | Bounds: max turns | Deliberation auto-interrupts at turn limit |
| 6 | Bounds: circularity | Loop detected, auto-interrupt |
| 7 | Catch-up | Agent disconnects, misses 3 contributions, reconnects, catches up |
| 8 | Timeout | wait_for_responses() times out with return_partial |
| 9 | Voting deadlock | Vote ties, timeout policy applied |
| 10 | Decline invitation | Agent declines, deliberation proceeds without them |

#### Task 8.2: Integration Test Harness

- `TestHarness` class that spins up multiple `DeliberationManager` instances sharing an `InMemoryStateStore`
- Each instance uses a mocked transport (no real WebSocket server needed for integration tests)
- The mocked transport routes messages between instances directly via the state store

---

### Phase 9: Migration & Compatibility (Days 13–14)

**Goal:** Migration guide from agent-v3 to v3, compatibility layer, Skye integration plan.

#### Task 9.1: MIGRATION.md

Document:
- Conceptual mapping: agent-v3 "conversation session" → v3 "deliberation"
- API mapping: `send_request()` → `contribute(kind=QUESTION)`, `wait_for_response()` → `wait_for_responses()`
- Data migration: conversation history → contribution log
- Breaking changes and rationale

#### Task 9.2: Compatibility Layer

```python
# compat.py — adapter for agent-v3 API consumers
class AgentV3Compat:
    """Wraps DeliberationManager to expose an agent-v3-like API."""

    def __init__(self, manager: DeliberationManager):
        self._manager = manager

    async def send_request(self, to: str, payload: dict) -> str:
        """Maps to contribute(kind=QUESTION, addressed_to={to})."""
        ...

    async def wait_for_response(self, request_id: str, timeout: float) -> dict:
        """Maps to wait_for_responses()."""
        ...
```

#### Task 9.3: Skye Integration Roadmap

- Where neurogossip_v3 fits in Skye's architecture
- How the TurnBasedAdapter integrates with Skye's turn loop
- Configuration: `NEUROGOSSIP_V3_ENABLED`, `NEUROGOSSIP_V3_SERVER_URL`, `NEUROGOSSIP_V3_EXECUTION_MODE`
- Rollout plan: shadow mode → single deliberation → full integration

---

### Phase 10: Documentation & Examples (Days 14–15)

**Goal:** README, runnable examples, final polish.

#### Task 10.1: README.md

- Overview and architecture diagram
- Quick start: install, configure, run
- API reference (auto-generated from docstrings)
- Link to design, spec, and research documents

#### Task 10.2: Runnable Examples

- `simple_deliberation.py` — two agents resolve a question
- `group_voting.py` — three agents deliberate and vote
- `turn_based_agent.py` — Skye-style turn-based agent using TurnBasedAdapter

#### Task 10.3: Final Polish

- Type hints complete (mypy strict)
- Docstrings on all public methods
- pyproject.toml with dependencies and entry points
- LICENSE file

---

## 5. Timeline

| Phase | Days | Parallel? | Cumulative |
|-------|------|-----------|------------|
| 0: Scaffolding & Models | 1–2 | — | 2 |
| 1: State Store | 2–3 | — | 5 |
| 2: Transport Adapter | 3–4 | — | 9 |
| 3: Manager Core | 4–7 | — | 16 |
| 4: Voting | 7–8 | ∥ 5, 7 | 17 |
| 5: Groups | 8–9 | ∥ 4, 7 | 18 |
| 6: Bounds | 9–10 | ∥ 4, 5, 7 (after Phase 3) | 19 |
| 7: Turn Adapter | 10–11 | ∥ 4, 5 | 20 |
| 8: Integration Tests | 11–13 | — | 23 |
| 9: Migration | 13–14 | — | 24 |
| 10: Documentation | 14–15 | — | 25 |

**Best case:** 12–15 days (with parallelization of Phases 4, 5, 7, and 6-after-3).
**Realistic range:** 3–6 weeks (accounting for debugging, iteration, and integration surprises).

---

## 6. Review Traceability Matrix

Every finding from all review rounds is mapped to its resolution:

| Finding ID | Source | Review Round | Section | Resolution |
|------------|--------|-------------|---------|------------|
| B.1 (v1) | Axioma, Theoria | v2 | §2 | Transport Grounding section |
| B.2 (v1) | Axioma, Theoria | v2 | §4.2 task 2.1 | Explicit API mapping table |
| B.3 (v1) | Axioma, Theoria | v2 | §4.2 task 2.2 | Background listener architecture |
| F1/M1 (v1) | Thea, Theoria | v2 | §4.3 task 3.2 | Sub-deliberation parameters |
| F2/M2 (v1) | Thea, Theoria | v2 | §4.6 task 6.4 | Cosine similarity on embeddings |
| M3/F3 (v1) | Thea, Theoria | v2 | §4.3 task 3.2 | Formation timeout → ACTIVE |
| M4/F4 (v1) | Thea, Theoria | v2 | §4.3 task 3.3 | VOTE kind validation |
| Cond 1 (v1) | Signoff | v2 | §4.1, §4.2 | Broadcast requirement |
| Cond 2 (v1) | Signoff | v2 | §4.3 task 3.1 | execution_mode parameter |
| Cond 3 (v1) | Signoff | v2 | §4.6 task 6.4 | 20-char guard rail |
| F5 (v1) | Thea | v2 | §5 | Realistic timeline range |
| B.1 (v2) | Axioma | v3 | §4.0 task 0.0 | Update design/spec transport bindings |
| B.2 (v2) | Axioma | v3 | §2.1, §2.3, §4.2 | on_message callback signature fix |
| B.3 (v2) | Axioma | v3 | §2.1 table | block/unblock naming fix |
| H.1 (v2) | Axioma | v3 | §4.0 task 0.0 | Same as B.1 |
| H.2 (v2) | Axioma | v3 | §4.2 task 2.2 | Wording fix |
| M-NEW (v2) | Theoria | v3 | §4.0 task 0.3 | Error codes harmonized to spec |
| — (v2) | Theoria | v3 | §4.3 task 3.2 | formation_timeout 30s → 60s |
| H.3 (v2) | Theoria | v3 | §3, §6 | Phase 6 dependency corrected |
| H.4 (v2) | Theoria | v3 | §4.2 task 2.3 | Early smoke test added |
| M.2 (v2) | Theoria | v3 | §4.3 task 3.8 | catch_up mechanism specified |
| — (v2) | Thea | v3 | §1.3 | Phase numbering clarified |

---

## 7. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| v5.0 client API changes during development | Low | High | Pin client version; §2 documents exact API at time of writing |
| InMemoryStateStore doesn't scale to many deliberations | Low | Medium | Redis backend planned for milestone 2; in-memory is sufficient for family of 4 agents |
| WebSocket connection drops during deliberation | Medium | Medium | v5.0 client has auto-reconnection; catch_up() replays missed contributions |
| Turn-based adapter doesn't fit Skye's runtime | Medium | Medium | Phase 7 is isolated; can be redesigned without touching core |
| Circularity detection false positives | Medium | Low | 20-char guard rail; threshold tunable; semantic drift deferred to v3.1 |
| Design/spec documents still reference wrong API after task 0.0 | Low | Medium | Task 0.0 is explicit and mechanical; verification step included |
| Error code mismatch between plan and spec | Low | Low | Resolved in v3; both now have 17 harmonized codes |

---

*End of IMPLEMENTATION_PLAN_v3.md*
