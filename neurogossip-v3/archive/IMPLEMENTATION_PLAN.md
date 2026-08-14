# Neurogossip v3 — Implementation Plan

**Version:** 1.0.0-draft
**Date:** 2026-08-06
**Depends on:** design/design.md, spec/specification.md, design/research.md
**Reviews incorporated:** review.md, axioma_review_neurogossip_v3.md, thea_review.md, theoria_review.md

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

## 2. Architecture

```
┌─────────────────────────────────────────────────┐
│              Agent Application                   │
│  (Skye, Thea, Theoria, Axioma, …)               │
│                                                   │
│  ┌─────────────────────────────────────────────┐ │
│  │        Deliberation Manager (v3)             │ │
│  │                                               │ │
│  │  • start_deliberation()  • contribute()       │ │
│  │  • wait_for_responses()  • resolve()          │ │
│  │  • interrupt()           • vote()            │ │
│  │  • decline_invitation()  • summarize()        │ │
│  │  • catch_up()            • list_deliberations()│ │
│  └──────────────────┬──────────────────────────┘ │
│                     │                             │
│  ┌──────────────────▼──────────────────────────┐ │
│  │         Deliberation State Store             │ │
│  │  • deliberation registry (in-memory/Redis)   │ │
│  │  • contribution log (append-only)            │ │
│  │  • vote sessions & tallies                   │ │
│  │  • agent presence & cursor tracking          │ │
│  └──────────────────┬──────────────────────────┘ │
│                     │                             │
│  ┌──────────────────▼──────────────────────────┐ │
│  │    WebSocketAgentTransport (v3 adapter)      │ │
│  │  • Wraps NeurogossipClient (v5.0)            │ │
│  │  • Deliberation message envelope             │ │
│  │  • Presence queries                          │ │
│  └──────────────────┬──────────────────────────┘ │
└─────────────────────┼───────────────────────────┘
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

### 2.1 Package Structure

```
neurogossip-v3/
├── src/
│   └── neurogossip_v3/
│       ├── __init__.py
│       ├── models.py              # Pydantic v2 data models
│       ├── state_store.py          # Abstract + InMemory + (later) Redis
│       ├── manager.py             # DeliberationManager (core engine)
│       ├── group_manager.py       # GroupManager
│       ├── bounds.py              # BoundsEnforcer
│       ├── transport.py           # WebSocketAgentTransport
│       ├── turn_adapter.py        # SuspendTurn, TurnBasedAdapter
│       └── errors.py              # DeliberationError, etc.
├── tests/
│   ├── conftest.py
│   ├── test_models.py
│   ├── test_state_store.py
│   ├── test_manager.py
│   ├── test_group_manager.py
│   ├── test_bounds.py
│   ├── test_transport.py
│   ├── test_turn_adapter.py
│   └── test_integration.py
├── pyproject.toml
└── README.md
```

---

## 3. Implementation Phases

### Phase 0: Scaffolding & Data Models (Days 1–2)

**Goal:** Package structure, data models, error types, test infrastructure.

#### Tasks

| # | Task | File | Details |
|---|------|------|---------|
| 0.1 | Create package scaffold | `pyproject.toml`, `src/neurogossip_v3/__init__.py` | Dependencies: pydantic>=2.0, websockets, neurogossip-client (local). Python ≥3.11. |
| 0.2 | Implement data models | `src/neurogossip_v3/models.py` | All structs from spec §3: Deliberation, Contribution, Resolution, Group, VoteSession, DissentingOpinion, DeliberationBounds, plus enums (DeliberationStatus, ContributionKind, ResolutionKind, VoteStatus). Pydantic v2 with `model_validate`/`model_dump`. |
| 0.3 | Implement error types | `src/neurogossip_v3/errors.py` | DeliberationNotFoundError, ContributionError, VoteError, InterruptError, ResolutionError, GroupError, BoundsExceededError, TimeoutError. |
| 0.4 | Write model tests | `tests/test_models.py` | Round-trip serialization, enum validation, default values, DissentingOpinion struct. |
| 0.5 | Write conftest | `tests/conftest.py` | Fixtures: sample deliberation, sample contribution, sample group, mock transport. |

#### Deliverables
- Installable package (`pip install -e .` works)
- All data models importable and tested
- 100% model test coverage

#### Dependencies
- None (greenfield)

---

### Phase 1: State Store (Days 2–3)

**Goal:** Abstract state store interface + in-memory implementation. This is the persistence layer for deliberations, contributions, votes, and agent cursors.

#### Tasks

| # | Task | File | Details |
|---|------|------|---------|
| 1.1 | Define abstract state store | `src/neurogossip_v3/state_store.py` | `AbstractStateStore` ABC with methods: `create_deliberation()`, `get_deliberation()`, `update_deliberation()`, `list_active_deliberations()`, `list_agent_deliberations()`, `append_contribution()`, `get_contributions()`, `get_contribution()`, `create_vote_session()`, `get_vote_session()`, `record_vote()`, `create_group()`, `get_group()`, `list_groups()`, `add_group_member()`, `remove_group_member()`, `set_agent_cursor()`, `get_agent_cursor()`. |
| 1.2 | Implement InMemoryStateStore | `src/neurogossip_v3/state_store.py` | Dict-based storage. Thread-safe with asyncio locks. Contribution log as ordered list per deliberation. Agent cursors as `{agent_id: last_contribution_id}`. |
| 1.3 | Write state store tests | `tests/test_state_store.py` | CRUD for deliberations, contributions, votes, groups, cursors. Concurrent access. |

#### Deliverables
- InMemoryStateStore passing all tests
- Abstract interface ready for Redis implementation (Phase 2)

#### Key Design Decisions
- **Key format:** `ng3:{ns}:del:{id}`, `ng3:{ns}:agent:{id}:dels` (standardized per review P.1)
- **Contribution ordering:** Append-only list, ordered by `created_at`. Pagination via `limit` + `before_contribution_id` (Thea F4).
- **Agent cursor:** Tracks last-seen contribution for `catch_up()`.

---

### Phase 2: Transport Adapter (Days 3–4)

**Goal:** `WebSocketAgentTransport` that wraps `NeurogossipClient` (v5.0) and provides the deliberation message envelope.

#### Tasks

| # | Task | File | Details |
|---|------|------|---------|
| 2.1 | Implement WebSocketAgentTransport | `src/neurogossip_v3/transport.py` | Wraps `NeurogossipClient`. Methods: `connect()`, `disconnect()`, `send_contribution(deliberation_id, contribution)`, `broadcast_to_group(group_id, message)`, `get_online_agents()`, `is_agent_online(agent_id)`. Message envelope: `{"type": "deliberation", "deliberation_id": "...", "contribution": {...}}`. |
| 2.2 | Implement message routing | `src/neurogossip_v3/transport.py` | Routes incoming deliberation messages to the DeliberationManager callback. Filters non-deliberation messages. |
| 2.3 | Write transport tests | `tests/test_transport.py` | Mock NeurogossipClient. Test send, receive, presence, reconnection. |

#### Deliverables
- WebSocketAgentTransport passing tests
- Clean separation: transport knows nothing about deliberation logic

#### Key Design Decisions
- **Presence:** Call `client.list_agents()` (returns `_online_agents` dict). No separate presence API needed (Axioma A4).
- **Reconnection:** Inherited from NeurogossipClient's auto-reconnect. On reconnect, DeliberationManager calls `catch_up()`.
- **Message envelope:** Deliberation messages are JSON with `type: "deliberation"`. Non-deliberation messages are passed through to a separate handler.

---

### Phase 3: Deliberation Manager — Core (Days 4–7)

**Goal:** The core engine: start, contribute, resolve, interrupt. This is the heart of the system.

#### Tasks

| # | Task | File | Details |
|---|------|------|---------|
| 3.1 | Implement DeliberationManager.__init__ | `src/neurogossip_v3/manager.py` | Takes `state_store`, `transport`, `agent_id`, `namespace`. Registers transport callback. |
| 3.2 | Implement start_deliberation() | `src/neurogossip_v3/manager.py` | Creates Deliberation in FORMING status. Sends invitations to participants. Transitions to ACTIVE when all accept (or immediately if no response expected). Returns deliberation_id. |
| 3.3 | Implement contribute() | `src/neurogossip_v3/manager.py` | Validates deliberation is ACTIVE/WAITING/VOTING. Creates Contribution. Appends to log. Routes to addressed_to agents (or all participants). If `expects_response_from` is non-empty, transitions to WAITING. Checks bounds after each contribution. |
| 3.4 | Implement wait_for_responses() | `src/neurogossip_v3/manager.py` | Blocks until all `expects_response_from` agents have contributed (or timeout). Parameters: `timeout_s`, `timeout_policy` ("partial" | "terminal"). On timeout with "partial": returns received responses, deliberation stays ACTIVE. On timeout with "terminal": transitions to TIMED_OUT. |
| 3.5 | Implement resolve() | `src/neurogossip_v3/manager.py` | Validates deliberation is ACTIVE/WAITING/VOTING. Creates Resolution. Transitions to RESOLVED. Notifies all participants. |
| 3.6 | Implement interrupt() | `src/neurogossip_v3/manager.py` | Any participant can call. Creates Resolution with kind=INTERRUPTED. Transitions to INTERRUPTED. Notifies all participants. |
| 3.7 | Implement decline_invitation() | `src/neurogossip_v3/manager.py` | Removes agent from FORMING deliberation. If initiator declines, transitions to ABANDONED. |
| 3.8 | Implement catch_up() | `src/neurogossip_v3/manager.py` | Replays missed contributions since agent's cursor. Parameters: `deliberation_id`, `limit`, `before_contribution_id`. Returns list of missed contributions. |
| 3.9 | Implement summarize() | `src/neurogossip_v3/manager.py` | Returns a summary of the deliberation: goal, status, participant list, contribution count, latest contributions. For late joiners. |
| 3.10 | Implement list_deliberations() | `src/neurogossip_v3/manager.py` | Lists active and recent deliberations for this agent. |
| 3.11 | Write manager tests | `tests/test_manager.py` | Test each operation. Test state transitions. Test error cases. Test concurrent contributions. |

#### Deliverables
- Full DeliberationManager with all core operations
- State machine correctly enforced
- Tests covering all state transitions

#### State Machine (from spec §5.1)

```
FORMING ──→ ACTIVE ──→ WAITING ──→ VOTING ──→ RESOLVING
   │          │          │           │            │
   │          │          │           │            ▼
   │          │          │           │         RESOLVED
   │          │          │           │
   ▼          ▼          ▼           ▼
ABANDONED  INTERRUPTED  TIMED_OUT  DEADLOCKED
                         INTERRUPTED
```

Transitions:
- FORMING → ACTIVE: all invitees accepted (or immediately if no response expected)
- FORMING → ABANDONED: initiator declines
- ACTIVE → WAITING: contribution with `expects_response_from` non-empty
- WAITING → ACTIVE: all expected responders replied, or timeout with "partial"
- WAITING → TIMED_OUT: timeout with "terminal"
- WAITING → INTERRUPTED: interrupt() called
- ACTIVE → VOTING: propose_vote() called
- VOTING → ACTIVE: vote completes (threshold met or not)
- VOTING → DEADLOCKED: vote fails with require_consensus=True
- VOTING → INTERRUPTED: interrupt() called
- ACTIVE/WAITING/VOTING → RESOLVING: resolve() called
- RESOLVING → RESOLVED: resolution recorded
- Any non-terminal → INTERRUPTED: interrupt() called
- Any non-terminal → TIMED_OUT: bounds exceeded

---

### Phase 4: Voting (Days 7–8)

**Goal:** Structured voting within deliberations.

#### Tasks

| # | Task | File | Details |
|---|------|------|---------|
| 4.1 | Implement propose_vote() | `src/neurogossip_v3/manager.py` | Creates VoteSession. Transitions deliberation to VOTING. Notifies participants. Parameters: proposal, options, threshold, timeout_s, on_timeout. |
| 4.2 | Implement cast_vote() | `src/neurogossip_v3/manager.py` | Records agent's vote. Checks if all participants have voted or threshold is unreachable. On completion: tallies, transitions back to ACTIVE (or DEADLOCKED if require_consensus and threshold not met). |
| 4.3 | Implement vote timeout | `src/neurogossip_v3/manager.py` | on_timeout="fail": vote fails, back to ACTIVE. on_timeout="pass": vote passes with current tallies. on_timeout="extend": timeout doubled once. |
| 4.4 | Write vote tests | `tests/test_manager.py` (add) | Test propose, cast, tally, timeout policies, deadlock detection. |

#### Deliverables
- Full voting subsystem
- All timeout policies working

---

### Phase 5: Groups (Days 8–9)

**Goal:** Named, persistent agent groups for group deliberations.

#### Tasks

| # | Task | File | Details |
|---|------|------|---------|
| 5.1 | Implement GroupManager | `src/neurogossip_v3/group_manager.py` | create_group(), get_group(), list_groups(), add_member(), remove_member(). Groups persisted in state store. |
| 5.2 | Implement group deliberation start | `src/neurogossip_v3/manager.py` (add) | `start_deliberation()` accepts `group_id` parameter. Expands group to participant list. Broadcasts invitation to all members. |
| 5.3 | Write group tests | `tests/test_group_manager.py` | CRUD, membership changes, group-based deliberation start. |

#### Deliverables
- GroupManager with full CRUD
- Group-based deliberation initiation

---

### Phase 6: Bounds Enforcement (Days 9–10)

**Goal:** Automatic interruption when bounds are exceeded.

#### Tasks

| # | Task | File | Details |
|---|------|------|---------|
| 6.1 | Implement BoundsEnforcer | `src/neurogossip_v3/bounds.py` | Checks after each contribution: max_turns, max_duration_s, circularity detection (v3.0). Returns BoundsStatus: OK, WARNING, EXCEEDED. |
| 6.2 | Implement circularity detection | `src/neurogossip_v3/bounds.py` | N-gram overlap between non-adjacent contributions. Configurable window size and threshold. Triggers when `auto_interrupt_on_loop=True`. |
| 6.3 | Integrate with DeliberationManager | `src/neurogossip_v3/manager.py` (modify) | Call BoundsEnforcer.check() after each contribute(). On EXCEEDED: auto-interrupt with appropriate reason. |
| 6.4 | Write bounds tests | `tests/test_bounds.py` | Test turn limit, duration limit, circularity detection with synthetic loops. |

#### Deliverables
- BoundsEnforcer integrated into contribution flow
- Circularity detection working

#### Deferred to v3.1
- Semantic drift detection (requires embedding model calibration)

---

### Phase 7: Turn-Based Adapter (Days 10–11)

**Goal:** Adapt the async DeliberationManager for Skye's turn-based runtime.

#### Tasks

| # | Task | File | Details |
|---|------|------|---------|
| 7.1 | Implement SuspendTurn exception | `src/neurogossip_v3/turn_adapter.py` | Exception with `deliberation_id`, `timeout_s`, `resume_context`. Raised by `wait_for_responses()` in turn-based mode. |
| 7.2 | Implement TurnBasedAdapter | `src/neurogossip_v3/turn_adapter.py` | Wraps DeliberationManager. Catches SuspendTurn, serializes context, returns control to runtime. On next turn, resumes from context. |
| 7.3 | Write turn adapter tests | `tests/test_turn_adapter.py` | Test suspend/resume cycle, context serialization, timeout handling. |

#### Deliverables
- TurnBasedAdapter working
- Skye can participate in deliberations without blocking her event loop

#### Execution Modes (from spec §8)

| Mode | wait_for_responses() behavior | Use case |
|------|------------------------------|----------|
| **Async** | Blocks (asyncio.wait_for) until responses or timeout | Standalone agents with their own event loop |
| **Turn-based** | Raises SuspendTurn with context. Runtime saves state, returns next turn when responses arrive or timeout fires. | Skye's turn-based architecture |

---

### Phase 8: Integration & Scenario Tests (Days 11–13)

**Goal:** End-to-end tests with multiple agents, realistic scenarios.

#### Tasks

| # | Task | File | Details |
|---|------|------|---------|
| 8.1 | Multi-agent test harness | `tests/conftest.py` (extend) | Fixtures for 3+ agents with InMemoryStateStore and MockAgentTransport. |
| 8.2 | Scenario: Simple deliberation | `tests/test_integration.py` | Agent A starts deliberation with B and C. B and C contribute. A resolves. Verify state transitions. |
| 8.3 | Scenario: Response waiting | `tests/test_integration.py` | Agent A sends question, waits for B's response. B responds. A continues. |
| 8.4 | Scenario: Group deliberation | `tests/test_integration.py` | Create group. Start group deliberation. All members contribute. Vote on proposal. |
| 8.5 | Scenario: Interruption | `tests/test_integration.py` | Deliberation loops. Agent calls interrupt(). Verify INTERRUPTED state. |
| 8.6 | Scenario: Bounds exceeded | `tests/test_integration.py` | Deliberation exceeds max_turns. Verify auto-interrupt. |
| 8.7 | Scenario: Catch-up after disconnect | `tests/test_integration.py` | Agent disconnects mid-deliberation. Reconnects. Calls catch_up(). Verifies missed contributions. |
| 8.8 | Scenario: Timeout | `tests/test_integration.py` | Agent waits for response. Timeout fires. Verify timeout_policy behavior. |
| 8.9 | Scenario: Voting deadlock | `tests/test_integration.py` | require_consensus=True, vote fails. Verify DEADLOCKED state. |
| 8.10 | Scenario: Decline invitation | `tests/test_integration.py` | Agent invited to deliberation. Declines. Verify removed from participants. |

#### Deliverables
- 10 scenario tests passing
- All state transitions exercised

---

### Phase 9: Migration from agent-v3 (Days 13–14)

**Goal:** Document and implement migration path from neurogossip-agent-v3 to neurogossip-v3.

#### Tasks

| # | Task | File | Details |
|---|------|------|---------|
| 9.1 | Write migration guide | `MIGRATION.md` | Map agent-v3 concepts to v3: ConversationSession → Deliberation, AgentRequest → Contribution (kind=QUESTION), AgentResponse → Contribution (kind=STATEMENT), wait_for_response() → wait_for_responses(), fan-out → expects_response_from. |
| 9.2 | Implement compatibility layer | `src/neurogossip_v3/compat.py` | Optional wrapper that presents an agent-v3-like API backed by the v3 DeliberationManager. For gradual migration. |
| 9.3 | Update Skye's integration | (Skye repo) | Replace agent-v3 calls with v3 calls. Use TurnBasedAdapter. |

#### Deliverables
- Migration guide
- Compatibility layer (optional)
- Skye integration plan

---

### Phase 10: Documentation & Polish (Days 14–15)

**Goal:** Complete documentation, README, example code.

#### Tasks

| # | Task | File | Details |
|---|------|------|---------|
| 10.1 | Write README | `README.md` | Quick start, architecture overview, API reference, examples. |
| 10.2 | Write example: simple deliberation | `examples/simple_deliberation.py` | Two agents deliberate a question. |
| 10.3 | Write example: group voting | `examples/group_voting.py` | Three agents in a group vote on a proposal. |
| 10.4 | Write example: turn-based | `examples/turn_based.py` | Skye-compatible turn-based deliberation. |
| 10.5 | Final review pass | All files | Check all docstrings, type hints, error messages. |

---

## 4. Dependency Graph

```
Phase 0 (Models)
    │
    ▼
Phase 1 (State Store) ─────────────────────┐
    │                                        │
    ▼                                        │
Phase 2 (Transport) ─────────────────────┐  │
    │                                     │  │
    ▼                                     │  │
Phase 3 (Manager Core) ◄─────────────────┘  │
    │                                         │
    ├──► Phase 4 (Voting)                    │
    │                                         │
    ├──► Phase 5 (Groups) ◄───────────────────┘
    │
    ├──► Phase 6 (Bounds)
    │
    ├──► Phase 7 (Turn Adapter)
    │
    ▼
Phase 8 (Integration Tests)
    │
    ▼
Phase 9 (Migration)
    │
    ▼
Phase 10 (Documentation)
```

Phases 4, 5, 6, and 7 can be parallelized after Phase 3 is complete.

---

## 5. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| **WebSocket transport mismatch with deliberation semantics** | Medium | High | Phase 2 tests against real neurogossip-server. The client v5.0 already handles reconnection, presence, and message routing — we build on proven infrastructure. |
| **Turn-based adapter complexity** | Medium | Medium | Phase 7 isolated from core manager. SuspendTurn is a simple exception. Context serialization is JSON. |
| **Circularity detection false positives** | Medium | Low | Configurable threshold. Default conservative. Can be disabled per deliberation. |
| **In-memory state loss on restart** | High | Medium | Acceptable for Phase 1. Phase 2 adds Redis persistence. Document clearly. |
| **agent-v3 migration friction** | Low | Medium | Compatibility layer (Phase 9.2) allows gradual migration. Both can coexist. |
| **Voting deadlocks with unresponsive agents** | Medium | Medium | `on_timeout` policy handles this. Default "fail" is safe. "extend" gives one more chance. |

---

## 6. Success Criteria

1. **All 10 scenario tests pass** (Phase 8)
2. **State machine correctly enforced** — no invalid transitions possible
3. **Response waiting works** — agent blocks until all expected responders reply or timeout
4. **Interruption works** — any participant can stop a runaway deliberation
5. **Bounds enforced** — max_turns, max_duration, circularity detection trigger auto-interrupt
6. **Turn-based mode works** — Skye can participate without blocking
7. **Catch-up works** — reconnecting agent sees all missed contributions
8. **Groups work** — create, join, leave, group deliberation
9. **Voting works** — propose, cast, tally, timeout policies
10. **Migration path documented** — agent-v3 users can migrate

---

## 7. Timeline

| Phase | Days | Cumulative |
|-------|------|------------|
| 0: Scaffolding & Models | 1–2 | Day 2 |
| 1: State Store | 2–3 | Day 3 |
| 2: Transport Adapter | 3–4 | Day 4 |
| 3: Manager Core | 4–7 | Day 7 |
| 4: Voting | 7–8 | Day 8 |
| 5: Groups | 8–9 | Day 9 |
| 6: Bounds | 9–10 | Day 10 |
| 7: Turn Adapter | 10–11 | Day 11 |
| 8: Integration Tests | 11–13 | Day 13 |
| 9: Migration | 13–14 | Day 14 |
| 10: Documentation | 14–15 | Day 15 |

**Total: ~15 days** (with parallelization of Phases 4–7: ~12 days)

---

## 8. Open Questions (Resolved During Design)

These were resolved during the design phase and are documented here for reference:

| Question | Resolution |
|----------|------------|
| Transport layer | WebSockets (neurogossip-client v5.0), not Redis Streams |
| Key prefix | `ng3:{ns}:` standardized across all documents |
| VoteSession return type | `VoteSession` (not `VoteResult`) |
| auto_interrupt_on_loop field | Added to DeliberationBounds |
| State machine WAITING transitions | WAITING→RESOLVING, WAITING→INTERRUPTED added |
| catch_up() implementation | Phase 1 implementation task; pagination with limit + before_contribution_id |
| Concurrency model | WebSocket mode: single-threaded async. Redis mode: Lua scripts for atomicity. |
| Execution modes | Async (blocking) and Turn-based (SuspendTurn) |
| Semantic drift | Deferred to v3.1; requires calibration experiment |
| timeout_policy | "partial" (default) vs "terminal" |
| decline_invitation() | Added as first-class operation |
| summarize() | Added for late joiners |
| DissentingOpinion | Struct with agent_id, statement, contribution_id |
| Transport class naming | WebSocketAgentTransport (not RedisAgentTransport) |
| Version numbering | neurogossip-client/server v5.0; neurogossip-v3 is deliberation protocol version |
