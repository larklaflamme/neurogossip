# Neurogossip v3 — Implementation Plan v2

**Version:** 2.0.0-draft
**Date:** 2026-08-06
**Supersedes:** IMPLEMENTATION_PLAN.md (v1.0.0-draft)
**Depends on:** design/design.md, spec/specification.md, design/research.md
**Reviews incorporated:**
- v1 reviews: review.md, axioma_review_neurogossip_v3.md, thea_review.md, theoria_review.md
- v2 reviews: axioma_review_IMPL_PLAN.md, implementation_plan_signoff.md, thea_review_IMPL.md, theoria_review_impl.md

---

## 0. Revision Notes (v1 → v2)

This revision addresses all findings from the four v2 reviews. The changes are:

| Finding | Source | Severity | Resolution |
|---------|--------|----------|------------|
| B.1: Design/spec transport binding describes wrong client API | Axioma, Theoria | CRITICAL | §2 (new) — Transport Grounding section documents actual v5.0 WebSocket client API and the mapping |
| B.2: Phase 2 underspecifies client API mapping | Axioma, Theoria | CRITICAL | §4.2 — explicit mapping table added to Phase 2 task 2.1 |
| B.3: No background listener architecture specified | Axioma, Theoria | CRITICAL | §4.2 — task 2.2 now specifies `on_message()` callback + `asyncio.Queue` dispatch |
| F1/M1: Sub-deliberation support missing from Phase 3.2 | Thea, Theoria | MODERATE | §4.3 — `parent_deliberation_id` and `async_child` added to task 3.2 |
| F2/M2: Circularity detection method mismatch | Thea, Theoria | MODERATE | §4.6 — aligned with spec: cosine similarity on embeddings, not n-gram overlap |
| M3/F3: Formation timeout semantics | Thea, Theoria | MODERATE | §4.3 — task 3.2 now includes `formation_timeout` → ACTIVE with current participants |
| M4/F4: VOTE kind restriction not explicit | Thea, Theoria | MODERATE | §4.3 — task 3.3 now explicitly validates `kind=VOTE` only in VOTING status |
| Cond 1: Broadcast determinism for InMemoryStateStore | Signoff | CONDITION | §4.1, §4.2 — broadcast requirement added to state-mutating operations |
| Cond 2: Explicit execution mode specification | Signoff | CONDITION | §4.3 — `execution_mode` parameter added to DeliberationManager constructor |
| Cond 3: Guard rails for circularity detection | Signoff | CONDITION | §4.6 — minimum contribution length threshold (20 chars) added |
| F5: Timeline is aggressive | Thea | LOW | §5 — realistic range (3–6 weeks) added alongside best-case (12–15 days) |

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
| Redis persistence backend | Phase 2 | In-memory store sufficient for Phase 1; Redis adds durability |
| A2A-compatible HTTP gateway | Phase 3 | External agent participation; not needed for closed family |
| Example deliberation transcript | Implementation phase | Thea F11 — will add during integration testing |

---

## 2. Transport Grounding — The v5.0 WebSocket Client API

> **This section is new in v2.** It addresses Axioma's B.1–B.3 findings: the design and spec describe the v2 Redis client API, but the implementation plan correctly targets the v5.0 WebSocket client. This section documents the actual v5.0 client API and the exact mapping the transport adapter will use.

### 2.1 Actual v5.0 Client API (from source code audit)

The `NeurogossipClient` class in `neurogossip-client/src/neurogossip_client/client.py` provides:

| Method / Property | Signature | Purpose |
|-------------------|-----------|---------|
| `connect()` | `async` | Connect to the WebSocket server, register agent |
| `disconnect()` | `async` | Graceful disconnect |
| `send(to, body, *, reply_to, conversation_id, ttl)` | `async → msg_id` | Send a message to an agent. `body` is a string. Returns the message ID. |
| `wait_for_message(timeout)` | `async → dict` | Block until a message arrives or timeout. Returns the raw message dict. |
| `on_message(callback)` | `sync` | Register a push callback: `callback(sender_id, body, msg_id, reply_to, conversation_id)`. Called on every incoming message. |
| `list_agents()` | `async → list[dict]` | List all registered agents with their metadata and online status. |
| `online_agents` | `property → list[str]` | List of agent_ids currently online. |
| `end_conversation(conversation_id)` | `async` | Signal conversation end to the server. |
| `block_agent(agent_id)` / `unblock_agent(agent_id)` | `async` | Block/unblock an agent from messaging this agent. |

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
| `broadcast_to_group(group_id, message)` | Loop of `client.send()` to each group member | Fetch group member list from GroupManager, send to each |
| `get_online_agents()` | `client.online_agents` (property) | Return as set of agent_ids |
| `is_agent_online(agent_id)` | `agent_id in client.online_agents` | Simple membership check |
| `send_invitation(deliberation_id, to_agent_id)` | `client.send(to_agent_id, body, conversation_id=deliberation_id)` | Body contains `{"type": "deliberation_invitation", ...}` |
| `send_vote_notification(deliberation_id, vote_session)` | `client.send(to, body, conversation_id=deliberation_id)` | Body contains `{"type": "vote_session", ...}` |

### 2.3 Background Listener Architecture

The transport adapter uses the **`on_message()` callback (push)** pattern for incoming messages:

```
┌─────────────────────────────────────────────────────┐
│                 WebSocketAgentTransport              │
│                                                      │
│  NeuroGossipClient                                   │
│  ┌──────────────────────────────────────────────┐   │
│  │  on_message(callback)                         │   │
│  │       │                                        │   │
│  │       ▼                                        │   │
│  │  _dispatch_incoming(sender, body, msg_id,     │   │
│  │                      reply_to, conv_id)        │   │
│  │       │                                        │   │
│  │       ├── type="deliberation" ──► asyncio.Queue│   │
│  │       │    (deliberation_messages)              │   │
│  │       │                                         │   │
│  │       ├── type="deliberation_invitation"        │   │
│  │       │    ──► asyncio.Queue (invitations)      │   │
│  │       │                                         │   │
│  │       └── type="vote_session"                   │   │
│  │            ──► asyncio.Queue (vote_messages)     │   │
│  └──────────────────────────────────────────────┘   │
│                                                      │
│  DeliberationManager                                 │
│  ┌──────────────────────────────────────────────┐   │
│  │  _background_listener()  (asyncio.Task)        │   │
│  │       │                                        │   │
│  │       ├── await deliberation_queue.get()       │   │
│  │       ├── deserialize Contribution             │   │
│  │       ├── state_store.append_contribution()    │   │
│  │       ├── check if we're waiting for this      │   │
│  │       │   agent → signal asyncio.Event          │   │
│  │       └── invoke on_contribution callback      │   │
│  └──────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

**Design decisions:**
- `on_message()` callback is registered once at `connect()` time. It runs in the client's event loop thread and dispatches to `asyncio.Queue`s.
- The `DeliberationManager` runs `_background_listener()` as an `asyncio.Task` that drains the queues.
- This is a **push** architecture: messages arrive as callbacks, not via polling. The `wait_for_responses()` method uses `asyncio.Event` signaled by the background listener, not `wait_for_message()`.
- The `wait_for_message()` method on the client is NOT used for deliberation messages — it's reserved for non-deliberation communication.

---

## 3. Architecture

```
┌───────────────────────────────────────────────────────┐
│              Agent Application                         │
│  (Skye, Thea, Theoria, Axioma, …)                     │
│                                                       │
│  ┌─────────────────────────────────────────────────┐  │
│  │        Deliberation Manager (v3)                 │  │
│  │                                                 │  │
│  │  • start_deliberation()  • contribute()         │  │
│  │  • wait_for_responses()  • resolve()            │  │
│  │  • interrupt()           • propose_vote()       │  │
│  │  • decline_invitation()  • summarize()          │  │
│  │  • catch_up()            • list_deliberations() │  │
│  └────────────────────┬────────────────────────────┘  │
│                       │                               │
│  ┌────────────────────▼────────────────────────────┐  │
│  │         Deliberation State Store                 │  │
│  │  • deliberation registry (in-memory/Redis)       │  │
│  │  • contribution log (append-only)                │  │
│  │  • vote sessions & tallies                       │  │
│  │  • agent presence & cursor tracking              │  │
│  └────────────────────┬────────────────────────────┘  │
│                       │                               │
│  ┌────────────────────▼────────────────────────────┐  │
│  │    WebSocketAgentTransport (v3 adapter)          │  │
│  │  • Wraps NeuroGossipClient (v5.0)                │  │
│  │  • Deliberation message envelope                 │  │
│  │  • Presence queries                              │  │
│  │  • Background listener (on_message callback)     │  │
│  └────────────────────┬────────────────────────────┘  │
└───────────────────────┼───────────────────────────────┘
                        │
            ┌───────────▼───────────┐
            │  neurogossip-server   │
            │  (v5.0, WebSocket)     │
            │                        │
            │  • Agent registry      │
            │  • Message routing     │
            │  • Presence tracking   │
            └────────────────────────┘
```

### 3.1 Package Structure

```
neurogossip-v3/
├── src/
│   └── neurogossip_v3/
│       ├── __init__.py
│       ├── models.py              # Pydantic v2 data models
│       ├── state_store.py          # Abstract + InMemory + (later) Redis
│       ├── transport.py            # WebSocketAgentTransport
│       ├── manager.py             # DeliberationManager
│       ├── group_manager.py       # GroupManager
│       ├── voting.py              # Voting subsystem
│       ├── bounds.py              # BoundsEnforcer
│       ├── turn_adapter.py        # TurnBasedAdapter + SuspendTurn
│       ├── errors.py              # All 18 error types
│       └── compat.py              # agent-v3 → v3 migration helpers
├── tests/
│   ├── test_models.py
│   ├── test_state_store.py
│   ├── test_transport.py
│   ├── test_manager.py
│   ├── test_voting.py
│   ├── test_groups.py
│   ├── test_bounds.py
│   ├── test_turn_adapter.py
│   ├── test_integration.py        # 10 scenario tests
│   └── conftest.py                # Shared fixtures
├── examples/
│   ├── simple_deliberation.py
│   ├── group_voting.py
│   └── turn_based_agent.py
├── design/
│   ├── design.md
│   ├── research.md
│   └── spec/
│       └── specification.md
├── reviews/
├── README.md
├── IMPLEMENTATION_PLAN.md         # v1 (superseded)
├── IMPLEMENTATION_PLAN_v2.md      # v2 (this file)
├── MIGRATION.md
└── pyproject.toml
```

---

## 4. Phase-by-Phase Implementation

### Timeline

| Estimate | Duration | Notes |
|----------|----------|-------|
| **Best-case** | 12–15 days | Full-time, no blockers, Phases 4–7 parallelized |
| **Realistic** | 3–6 weeks | With integration testing, debugging, and Skye runtime integration |

### Phase Dependency Graph

```
Phase 0 (Models) ─────────────────────────────────────────────┐
    │                                                          │
    ▼                                                          │
Phase 1 (State Store) ─────────────────────────────────────┐   │
    │                                                       │   │
    ▼                                                       │   │
Phase 2 (Transport) ────────────────────────────────────┐   │   │
    │                                                    │   │   │
    ▼                                                    │   │   │
Phase 3 (Manager Core) ──────────────────────────────┐   │   │   │
    │                                                 │   │   │   │
    ├──► Phase 4 (Voting)      ──┐                    │   │   │   │
    ├──► Phase 5 (Groups)      ──┤                    │   │   │   │
    ├──► Phase 6 (Bounds)       ──┤ (parallelizable)   │   │   │   │
    └──► Phase 7 (Turn Adapter) ──┘                    │   │   │   │
         │                                             │   │   │   │
         └──► Phase 8 (Integration Tests) ◄────────────┘   │   │   │
              │                                             │   │   │
              └──► Phase 9 (Migration) ◄───────────────────┘   │   │
                   │                                             │   │
                   └──► Phase 10 (Documentation) ◄──────────────┘   │
                                                                     │
                                                                     │
  (All phases depend on Phase 0 models) ◄────────────────────────────┘
```

---

### Phase 0: Scaffolding & Data Models (Days 1–2)

**Goal:** Package structure, Pydantic v2 data models, error types, test infrastructure.

#### Task 0.1: Project Scaffolding
- Create `pyproject.toml` with dependencies: `pydantic>=2.0`, `websockets`, `neurogossip-client`
- Create package directory structure
- Create `conftest.py` with shared fixtures: mock transport, in-memory store, sample deliberations
- Set up pytest configuration

#### Task 0.2: Data Models (`models.py`)
Implement all Pydantic v2 models from spec §3:

- `AgentIdentity` — agent_id, display_name, metadata
- `DeliberationGoal` — description, success_criteria, require_consensus
- `Deliberation` — id, goal, status, participants, contributions, created_at, resolved_at, parent_deliberation_id, async_child
- `Contribution` — id, deliberation_id, author_id, kind (STATEMENT/QUESTION/PROPOSAL/VOTE/ACK/DECLINE), body, addressed_to, expects_response_from, reply_to_contribution_id, timestamp, sequence_number
- `Resolution` — deliberation_id, resolved_by, outcome (CONSENSUS/VOTE/INTERRUPTED/TIMEOUT/DECLINED), summary, dissenting_opinions
- `DissentingOpinion` — agent_id, statement, timestamp
- `Group` — id, name, description, members, created_by, created_at
- `VoteSession` — id, deliberation_id, proposal, options, votes (agent_id → option), status (OPEN/CLOSED), timeout_policy (fail/pass/extend), created_at, closed_at
- `DeliberationBounds` — max_turns, max_duration_seconds, auto_interrupt_on_loop, min_contribution_length_for_circularity_check
- `ExecutionMode` — enum: ASYNC, TURN_BASED

All models use `model_validate` for deserialization and `model_dump` for serialization.

#### Task 0.3: Error Types (`errors.py`)
Implement all 18 error codes from spec §7:

- `DELIBERATION_NOT_FOUND`, `INVALID_STATUS`, `INVALID_KIND`, `NOT_PARTICIPANT`
- `ALREADY_RESOLVED`, `VOTE_NOT_OPEN`, `ALREADY_VOTED`, `VOTE_NOT_FOUND`
- `GROUP_NOT_FOUND`, `NOT_MEMBER`, `ALREADY_MEMBER`
- `TIMEOUT`, `INTERRUPTED`, `BOUNDS_EXCEEDED`
- `TRANSPORT_ERROR`, `SERIALIZATION_ERROR`, `CATCH_UP_FAILED`
- `INVALID_STATE_TRANSITION`

All inherit from `NeurogossipError(Exception)`.

#### Task 0.4: Unit Tests for Models
- Serialization round-trip for every model
- Validation: required fields, enum values, type constraints
- Edge cases: empty participants, max-length strings, missing fields

---

### Phase 1: State Store (Days 2–3)

**Goal:** Abstract state store interface + InMemoryStateStore. Thread-safe. Broadcast-ready.

#### Task 1.1: Abstract Interface (`state_store.py`)
Define `AbstractDeliberationStateStore` with these methods:

- `create_deliberation(deliberation: Deliberation) → Deliberation`
- `get_deliberation(deliberation_id: str) → Deliberation | None`
- `update_deliberation(deliberation: Deliberation) → Deliberation`
- `list_deliberations(agent_id: str, status: Optional[DeliberationStatus]) → list[Deliberation]`
- `append_contribution(deliberation_id: str, contribution: Contribution) → int` (returns sequence_number)
- `get_contributions(deliberation_id: str, after_sequence: int = 0, limit: int = 100) → list[Contribution]`
- `create_vote_session(deliberation_id: str, vote_session: VoteSession) → VoteSession`
- `get_vote_session(deliberation_id: str, vote_session_id: str) → VoteSession | None`
- `cast_vote(deliberation_id: str, vote_session_id: str, agent_id: str, option: str) → VoteSession`
- `close_vote_session(deliberation_id: str, vote_session_id: str) → VoteSession`
- `get_agent_cursor(deliberation_id: str, agent_id: str) → int`
- `set_agent_cursor(deliberation_id: str, agent_id: str, cursor: int) → None`

Key format: `ng3:{namespace}:deliberation:{id}`, `ng3:{namespace}:contributions:{id}`, etc.

#### Task 1.2: InMemoryStateStore
- Uses `dict` + `asyncio.Lock` for thread safety
- Contribution log is an append-only `list` per deliberation
- Vote sessions stored in a `dict` keyed by vote_session_id
- Agent cursors tracked per deliberation

**Broadcast requirement (Signoff Condition 1):** Every state-mutating operation (`append_contribution`, `cast_vote`, `update_deliberation`) MUST be followed by a broadcast to all deliberation participants via the transport layer. The store itself does not broadcast — it returns the mutated object, and the DeliberationManager handles broadcasting.

#### Task 1.3: Unit Tests for State Store
- CRUD operations for deliberations
- Contribution append and retrieval with pagination
- Vote session lifecycle: create → cast → close
- Concurrent access (two coroutines appending contributions simultaneously)
- Agent cursor tracking

---

### Phase 2: WebSocket Transport Adapter (Days 3–4)

**Goal:** `WebSocketAgentTransport` wrapping `NeuroGossipClient` v5.0 with explicit API mapping and background listener.

#### Task 2.1: Transport Adapter (`transport.py`)

Implement `WebSocketAgentTransport` with these methods and their **exact v5.0 client API mappings**:

| Adapter Method | v5.0 Client Call | Notes |
|----------------|------------------|-------|
| `connect()` | `await client.connect()` | Register agent, set up `on_message` callback |
| `disconnect()` | `await client.disconnect()` | Graceful shutdown |
| `send_contribution(deliberation_id, contribution)` | `await client.send(to, json.dumps({"type": "deliberation", "deliberation_id": deliberation_id, "contribution": contribution.model_dump()}), reply_to=contribution.reply_to_contribution_id, conversation_id=deliberation_id)` | Serialize Contribution to JSON. `to` is derived from `contribution.addressed_to` (single) or fan-out (list). |
| `broadcast_to_group(group_id, message)` | Loop: `await client.send(member_id, json.dumps({"type": "deliberation", ...}), conversation_id=deliberation_id)` | Fetch group members from GroupManager, send to each. |
| `get_online_agents()` | `client.online_agents` (property) | Return as `set[str]` |
| `is_agent_online(agent_id)` | `agent_id in client.online_agents` | Boolean |
| `send_invitation(deliberation_id, to_agent_id)` | `await client.send(to_agent_id, json.dumps({"type": "deliberation_invitation", "deliberation_id": deliberation_id, ...}), conversation_id=deliberation_id)` | |
| `send_vote_notification(deliberation_id, vote_session)` | `await client.send(to, json.dumps({"type": "vote_session", ...}), conversation_id=deliberation_id)` | |

#### Task 2.2: Background Listener

Implement the push-based listener architecture (see §2.3 diagram):

1. In `connect()`: register `client.on_message(_dispatch_incoming)` callback
2. `_dispatch_incoming(sender_id, body, msg_id, reply_to, conversation_id)`:
   - Parse `body` as JSON
   - Route by `type` field to the appropriate `asyncio.Queue`:
     - `"deliberation"` → `self._deliberation_queue`
     - `"deliberation_invitation"` → `self._invitation_queue`
     - `"vote_session"` → `self._vote_queue`
   - Unknown types → log warning, drop
3. Expose async iterators / getters for the DeliberationManager to consume:
   - `async def receive_contribution() → Contribution`
   - `async def receive_invitation() → Deliberation`
   - `async def receive_vote_notification() → VoteSession`

**Broadcast requirement (Signoff Condition 1):** After every `send_contribution()`, the transport also delivers the message to the local agent's own queue (loopback), so the local state store stays consistent with what other agents see.

#### Task 2.3: Unit Tests for Transport
- Mock `NeuroGossipClient` — verify correct method calls with correct arguments
- Message serialization round-trip: Contribution → JSON → Contribution
- Background listener routing: verify each message type goes to the correct queue
- Loopback delivery: verify local agent receives its own messages

---

### Phase 3: Deliberation Manager Core (Days 4–7)

**Goal:** The core engine implementing all operations from design §4 and spec §4.

#### Task 3.1: Manager Constructor

```python
class DeliberationManager:
    def __init__(
        self,
        agent_id: str,
        state_store: AbstractDeliberationStateStore,
        transport: WebSocketAgentTransport,
        execution_mode: ExecutionMode = ExecutionMode.ASYNC,
        bounds: Optional[DeliberationBounds] = None,
    ):
        ...
```

**Execution mode (Signoff Condition 2):** When `execution_mode == ExecutionMode.TURN_BASED`, `wait_for_responses()` raises `SuspendTurn` instead of blocking on `asyncio.Event`. The `TurnBasedAdapter` (Phase 7) catches this and serializes the context.

#### Task 3.2: `start_deliberation()`

```
start_deliberation(
    goal: DeliberationGoal,
    participants: list[str],
    parent_deliberation_id: Optional[str] = None,
    async_child: bool = False,
    formation_timeout_seconds: float = 30.0,
) → Deliberation
```

**State machine (spec §5.1):**
1. Validate participants are known agents (via transport presence)
2. Create Deliberation in FORMING status
3. Send invitations to all participants via transport
4. Wait for responses (accept/decline) up to `formation_timeout_seconds`
5. **On formation_timeout:** transition to ACTIVE with current participants (those who accepted). Do NOT hang forever on unresponsive agents. (Addresses Thea F3 / Theoria M3)
6. On all accepted: transition to ACTIVE
7. On all declined: transition to DECLINED (terminal)
8. If `parent_deliberation_id` is set, link to parent. If `async_child=True`, the parent does NOT block on this child's resolution. (Addresses Thea F1 / Theoria M1)
9. Return the Deliberation

#### Task 3.3: `contribute()`

```
contribute(
    deliberation_id: str,
    kind: ContributionKind,
    body: str,
    addressed_to: Optional[str] = None,
    expects_response_from: Optional[list[str]] = None,
    reply_to_contribution_id: Optional[str] = None,
) → Contribution
```

**Validations:**
1. Deliberation exists and is in ACTIVE, WAITING, or VOTING status
2. Agent is a participant
3. **If `kind=VOTE`: deliberation MUST be in VOTING status (else raise INVALID_KIND).** (Addresses Thea F4 / Theoria M4)
4. If `reply_to_contribution_id` is set, that contribution must exist

**Actions:**
1. Create Contribution with auto-incremented sequence_number
2. Append to state store
3. Broadcast to all participants via transport
4. If `expects_response_from` is set, transition deliberation to WAITING status
5. Return the Contribution

#### Task 3.4: `wait_for_responses()`

```
wait_for_responses(
    deliberation_id: str,
    timeout_seconds: float = 300.0,
    timeout_policy: Literal["fail", "pass", "extend"] = "fail",
) → list[Contribution]
```

**Behavior:**
1. Deliberation must be in WAITING status
2. Identify which agents we're waiting for (from the contribution's `expects_response_from`)
3. **ASYNC mode:** Block on `asyncio.Event` until all expected agents have contributed (or timeout)
4. **TURN_BASED mode:** Raise `SuspendTurn` with context payload (see Phase 7)
5. On timeout:
   - `"fail"` → raise TIMEOUT error
   - `"pass"` → return whatever responses we have, transition back to ACTIVE
   - `"extend"` → double the timeout, continue waiting
6. On all responses received: transition back to ACTIVE, return the list of response Contributions

#### Task 3.5: `resolve()`

```
resolve(
    deliberation_id: str,
    outcome: ResolutionOutcome,
    summary: str,
    dissenting_opinions: Optional[list[DissentingOpinion]] = None,
) → Resolution
```

1. Deliberation must be in ACTIVE, WAITING, or VOTING status
2. Agent must be a participant
3. Create Resolution
4. Transition deliberation to terminal status (RESOLVED_CONSENSUS, RESOLVED_VOTE, etc.)
5. Broadcast resolution to all participants
6. Return the Resolution

#### Task 3.6: `interrupt()`

```
interrupt(deliberation_id: str, reason: str) → Resolution
```

1. Any participant can interrupt any active deliberation
2. Transition to INTERRUPTED (terminal)
3. Create Resolution with outcome=INTERRUPTED
4. Broadcast to all participants

#### Task 3.7: `decline_invitation()`

```
decline_invitation(deliberation_id: str, reason: str) → None
```

1. Deliberation must be in FORMING status
2. Agent must be an invited participant
3. Remove agent from participants list
4. If no participants remain, transition to DECLINED (terminal)

#### Task 3.8: `catch_up()`

```
catch_up(deliberation_id: str, limit: int = 100, before_contribution_id: Optional[str] = None) → list[Contribution]
```

1. Fetch all contributions after the agent's cursor
2. Update agent's cursor to the latest sequence_number
3. Return the missed contributions
4. Pagination: `limit` + `before_contribution_id` for large catch-ups

#### Task 3.9: `summarize()`

```
summarize(deliberation_id: str) → str
```

1. Fetch all contributions
2. Generate a structured summary: goal, key positions, areas of agreement/disagreement, resolution status
3. Return the summary string

#### Task 3.10: `list_deliberations()`

```
list_deliberations(status: Optional[DeliberationStatus] = None) → list[Deliberation]
```

Filter by status if provided. Return all deliberations this agent participates in.

#### Task 3.11: Unit Tests for Manager
- Full state machine: FORMING → ACTIVE → WAITING → ACTIVE → VOTING → RESOLVED
- Response waiting: timeout scenarios (fail/pass/extend)
- Interrupt from any state
- Decline invitation
- Catch-up after simulated disconnection
- VOTE kind restriction: reject VOTE outside VOTING status
- Formation timeout: unresponsive invitee doesn't hang
- Sub-deliberation: parent_deliberation_id and async_child

---

### Phase 4: Voting Subsystem (Days 7–8)

**Goal:** `propose_vote()`, `cast_vote()`, tallying, timeout policies.

#### Task 4.1: `propose_vote()`

```
propose_vote(
    deliberation_id: str,
    proposal: str,
    options: list[str],
    timeout_seconds: float = 120.0,
    timeout_policy: Literal["fail", "pass", "extend"] = "fail",
    require_consensus: bool = False,
) → VoteSession
```

1. Deliberation must be in ACTIVE or WAITING status
2. Transition deliberation to VOTING
3. Create VoteSession with status=OPEN
4. Broadcast vote notification to all participants
5. Return VoteSession (not VoteResult — matches design correction)

#### Task 4.2: `cast_vote()`

```
cast_vote(
    deliberation_id: str,
    vote_session_id: str,
    option: str,
) → VoteSession
```

1. VoteSession must be OPEN
2. Agent must be a participant
3. Agent must not have already voted (ALREADY_VOTED)
4. Option must be in the vote's options list
5. Record vote in state store
6. If all participants have voted: auto-close the vote session

#### Task 4.3: Vote Tallying

```
tally_votes(deliberation_id: str, vote_session_id: str) → dict[str, int]
```

Returns `{option: count}` for all options.

#### Task 4.4: Timeout Policies

When a vote session times out:
- **fail:** Vote fails, no resolution. Deliberation returns to ACTIVE.
- **pass:** Vote passes with whatever votes were cast. Majority wins. Ties go to the first option.
- **extend:** Double the timeout, continue waiting. Max 3 extensions.

#### Task 4.5: Voting Deadlock Policy

If `require_consensus=True` and the vote is tied or deadlocked:
1. Auto-extend once (2x timeout)
2. If still deadlocked after extension: transition to RESOLVED_DEADLOCK with dissenting opinions recorded
3. The deliberation is terminal but records the deadlock for human review

#### Task 4.6: Unit Tests for Voting
- Simple majority vote
- Consensus vote (all must agree)
- Timeout: fail, pass, extend
- Deadlock resolution
- ALREADY_VOTED rejection
- Vote outside VOTING status rejection

---

### Phase 5: Group Manager (Days 8–9)

**Goal:** `GroupManager` for persistent agent groups.

#### Task 5.1: Group CRUD

```
create_group(name: str, description: str, members: list[str]) → Group
get_group(group_id: str) → Group
list_groups() → list[Group]
delete_group(group_id: str) → None
```

#### Task 5.2: Membership Management

```
join_group(group_id: str) → Group
leave_group(group_id: str) → Group
invite_to_group(group_id: str, agent_id: str) → None
```

#### Task 5.3: Group Deliberations

```
start_group_deliberation(
    group_id: str,
    goal: DeliberationGoal,
    formation_timeout_seconds: float = 30.0,
) → Deliberation
```

1. Fetch group members
2. Call `start_deliberation()` with group members as participants
3. Return the Deliberation

#### Task 5.4: Unit Tests for Groups
- Create, join, leave
- Group deliberation with all members
- Dynamic membership: agent joins mid-deliberation

---

### Phase 6: Bounds Enforcement (Days 9–10)

**Goal:** Prevent runaway conversations with configurable bounds.

#### Task 6.1: BoundsEnforcer

```python
class BoundsEnforcer:
    def __init__(self, bounds: DeliberationBounds):
        self.max_turns = bounds.max_turns
        self.max_duration = bounds.max_duration_seconds
        self.auto_interrupt_on_loop = bounds.auto_interrupt_on_loop
        self.min_contribution_length = bounds.min_contribution_length_for_circularity_check
```

#### Task 6.2: Turn Limit Enforcement

After each `contribute()`:
1. Count contributions in the deliberation
2. If count > max_turns: auto-interrupt with BOUNDS_EXCEEDED

#### Task 6.3: Duration Enforcement

Background task that:
1. Tracks deliberation start time
2. If elapsed > max_duration: auto-interrupt with BOUNDS_EXCEEDED

#### Task 6.4: Circularity Detection

**Method: Cosine similarity on contribution embeddings** (aligned with spec §5.3, addresses Thea F2 / Theoria M2).

1. On each new contribution, compute its text embedding (using a local embedding model, e.g., `nomic-embed-text` via Ollama, or `sentence-transformers`)
2. Compare against the last N non-adjacent contributions (skip the immediately preceding one)
3. **Guard rail (Signoff Condition 3):** Skip contributions shorter than `min_contribution_length` (default: 20 characters). Short acknowledgments like "I agree" or "Vote: yes" should not trigger false-positive circularity detection.
4. If cosine similarity > 0.95 with any non-adjacent contribution: auto-interrupt with circularity warning
5. `auto_interrupt_on_loop=True` → immediate interrupt. `False` → log warning only.

**Note on v1 deviation:** v1 of this plan specified n-gram overlap for simplicity. v2 aligns with the spec's cosine similarity method. The embedding model choice is a configuration parameter — default to `nomic-embed-text` (available locally via Ollama, no external API dependency).

#### Task 6.5: Unit Tests for Bounds
- Turn limit exceeded → interrupt
- Duration exceeded → interrupt
- Circularity detected → interrupt (with guard rail: short messages ignored)
- Circularity with auto_interrupt_on_loop=False → warning only

---

### Phase 7: Turn-Based Adapter (Days 10–11)

**Goal:** Bridge between async DeliberationManager and Skye's turn-based CLI runtime.

#### Task 7.1: SuspendTurn Exception

```python
class SuspendTurn(Exception):
    """Raised by DeliberationManager when waiting for responses in TURN_BASED mode."""
    def __init__(self, context: TurnContext):
        self.context = context

@dataclass
class TurnContext:
    deliberation_id: str
    waiting_for: list[str]        # agent_ids we're waiting for
    timeout_seconds: float
    timeout_policy: str
    contributions_so_far: list[Contribution]
    resume_callback_id: str       # opaque token for resumption
```

#### Task 7.2: TurnBasedAdapter

```python
class TurnBasedAdapter:
    def __init__(self, manager: DeliberationManager):
        self.manager = manager
        self.suspended_contexts: dict[str, TurnContext] = {}

    async def handle_turn(self, deliberation_id: str, contribution: Contribution) -> TurnResult:
        """Process one turn. Returns either a response or a SuspendTurn."""
        ...

    async def resume(self, resume_callback_id: str, new_contributions: list[Contribution]) -> TurnResult:
        """Resume a suspended deliberation with new contributions."""
        ...
```

#### Task 7.3: Integration with DeliberationManager

When `execution_mode == ExecutionMode.TURN_BASED`:
- `wait_for_responses()` does NOT call `asyncio.Event.wait()`
- Instead, it raises `SuspendTurn(context)` with the serialized context
- Skye's runtime catches `SuspendTurn`, saves the context, and yields control
- On the next turn, Skye's runtime calls `resume()` with any new contributions that arrived

#### Task 7.4: Unit Tests for Turn Adapter
- SuspendTurn raised in TURN_BASED mode
- Resume with received contributions
- Timeout handling in turn-based mode
- Context serialization round-trip

---

### Phase 8: Integration & Scenario Tests (Days 11–13)

**Goal:** 10 scenario tests covering the full state machine and edge cases.

#### Scenario Tests

| # | Scenario | What It Tests |
|---|----------|---------------|
| 1 | **Simple deliberation** | FORMING → ACTIVE → contributions → RESOLVED |
| 2 | **Response waiting** | ACTIVE → WAITING → responses arrive → ACTIVE → RESOLVED |
| 3 | **Group deliberation** | Group creation → group deliberation → all members participate |
| 4 | **Interruption** | ACTIVE → interrupt → INTERRUPTED |
| 5 | **Bounds: turn limit** | Contributions exceed max_turns → BOUNDS_EXCEEDED |
| 6 | **Bounds: circularity** | Repeated contributions → circularity detected → INTERRUPTED |
| 7 | **Catch-up after disconnect** | Agent disconnects mid-deliberation → reconnects → catch_up() → continues |
| 8 | **Voting with timeout** | Propose vote → some agents vote → timeout (fail) → back to ACTIVE |
| 9 | **Voting deadlock** | Consensus required → tied vote → extend → still tied → RESOLVED_DEADLOCK |
| 10 | **Decline invitation** | Invitation sent → agent declines → deliberation proceeds without them |

Each test runs with real (in-memory) state store and mock transport. Tests verify:
- Correct state transitions
- Correct contribution ordering
- Correct participant tracking
- Error conditions raise the right exceptions

---

### Phase 9: Migration & Compatibility (Days 13–14)

**Goal:** Migration path from neurogossip-agent-v3 to neurogossip-v3.

#### Task 9.1: Migration Guide (`MIGRATION.md`)

Document the mapping from agent-v3 concepts to v3 concepts:

| agent-v3 Concept | v3 Equivalent |
|-----------------|---------------|
| `ConversationSession` | `Deliberation` |
| `AgentRequest` | `Contribution` with `expects_response_from` |
| `AgentResponse` | `Contribution` with `reply_to_contribution_id` |
| `SessionStatus.ACTIVE` | `DeliberationStatus.ACTIVE` |
| `SessionStatus.WAITING_FOR_HUMAN` | HITL via `wait_for_responses()` with human agent_id |
| `RequestStatus.PENDING` | `DeliberationStatus.WAITING` |
| `fan_out()` | `start_deliberation()` with multiple participants |

#### Task 9.2: Compatibility Layer (`compat.py`)

Optional adapter that wraps v3 DeliberationManager in an agent-v3-compatible interface for gradual migration. Agents can use the compat layer while transitioning to the native v3 API.

#### Task 9.3: Skye Integration Plan

Document how Skye's runtime will integrate:
1. Replace `neurogossip_send` / `peer_talk` with `DeliberationManager.contribute()`
2. Handle `SuspendTurn` in the turn-based loop
3. Map existing sister communication patterns to deliberation operations
4. Migration timeline: Phase 1 (side-by-side) → Phase 2 (v3 primary, agent-v3 fallback) → Phase 3 (v3 only)

---

### Phase 10: Documentation & Examples (Days 14–15)

**Goal:** Runnable examples and comprehensive documentation.

#### Task 10.1: Examples

- `simple_deliberation.py` — Two agents deliberate on a research question
- `group_voting.py` — Four agents vote on a design decision
- `turn_based_agent.py` — Skye-compatible turn-based agent using SuspendTurn

#### Task 10.2: README

- Installation instructions
- Quick start (5-line example)
- Architecture overview
- Link to design, spec, and migration docs

#### Task 10.3: API Documentation

- Docstrings for all public methods
- Cross-references to spec sections

---

## 5. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| In-memory state loss on agent restart | High | Medium | Phase 2 adds Redis persistence. Until then, `catch_up()` replays from other agents' state. |
| Voting deadlocks with consensus requirement | Medium | Low | Deadlock policy: auto-extend once, then RESOLVED_DEADLOCK. |
| Circularity false positives | Medium | Medium | Guard rail: minimum contribution length (20 chars). `auto_interrupt_on_loop` can be disabled. |
| Transport API mismatch between design/spec and actual client | High (v1) → Low (v2) | High (v1) → Low (v2) | §2 documents the actual v5.0 client API and exact mappings. Design/spec will be updated separately. |
| Embedding model availability for circularity detection | Low | Low | `nomic-embed-text` runs locally via Ollama. Fallback: skip circularity check if embedding model unavailable. |
| Timeline pressure | Medium | Medium | Best-case 12–15 days, realistic 3–6 weeks. Phases 4–7 parallelizable after Phase 3. |

---

## 6. Review Traceability Matrix

Every finding from all four v2 reviews is addressed in this document:

| Finding ID | Source | Section | Resolution |
|-----------|--------|---------|------------|
| B.1 | Axioma, Theoria | §2.1 | Actual v5.0 client API documented |
| B.2 | Axioma, Theoria | §2.2, §4.2 | Explicit API mapping table in Phase 2 |
| B.3 | Axioma, Theoria | §2.3, §4.2 | Background listener architecture specified |
| F1 / M1 | Thea, Theoria | §4.3 (task 3.2) | `parent_deliberation_id` and `async_child` added |
| F2 / M2 | Thea, Theoria | §4.6 (task 6.4) | Cosine similarity, not n-gram |
| F3 / M3 | Thea, Theoria | §4.3 (task 3.2) | `formation_timeout` → ACTIVE with current participants |
| F4 / M4 | Thea, Theoria | §4.3 (task 3.3) | VOTE kind only valid in VOTING status |
| Cond 1 | Signoff | §4.1, §4.2 | Broadcast requirement for state-mutating operations |
| Cond 2 | Signoff | §4.3 (task 3.1) | `execution_mode` parameter in constructor |
| Cond 3 | Signoff | §4.6 (task 6.4) | `min_contribution_length_for_circularity_check` (20 chars) |
| F5 | Thea | §4 (timeline), §5 | Realistic range 3–6 weeks added |
| Theoria minor findings | Theoria | §4.3, §4.6 | All addressed inline |

---

## 7. Sign-Off Status

| Phase | Status | Conditions |
|-------|--------|------------|
| Phase 0 | APPROVED | — |
| Phase 1 | APPROVED | Broadcast requirement documented (§4.1) |
| Phase 2 | APPROVED | API mapping table (§4.2), background listener (§2.3) |
| Phase 3 | APPROVED | Sub-deliberation, formation_timeout, VOTE restriction, execution_mode |
| Phase 4 | APPROVED | Timeout policies, deadlock policy |
| Phase 5 | APPROVED | — |
| Phase 6 | APPROVED | Cosine similarity, guard rail (20-char minimum) |
| Phase 7 | APPROVED | SuspendTurn + TurnBasedAdapter |
| Phase 8 | APPROVED | 10 scenario tests |
| Phase 9 | APPROVED | Migration guide + compat layer |
| Phase 10 | APPROVED | Runnable examples |

**Overall Status: APPROVED — Ready for Phase 0 Implementation**

---

*End of IMPLEMENTATION_PLAN_v2.md*
