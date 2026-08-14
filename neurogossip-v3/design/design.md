# Neurogossip v3 — Protocol Design

A stateful, multi-turn, multi-agent deliberation protocol for AI agents built on
WebSockets with optional Redis persistence. Neurogossip v3 extends the existing
neurogossip-client (v5.0) WebSocket transport with a deliberation layer that
enables agents to exchange thoughts until an issue is resolved, participate in
group discussions, wait for responses, and interrupt runaway conversations.

**Last updated:** 2026-08-06 (post-review revision — see REVIEW_RESPONSE.md)

> **Relationship to existing components:**
> - **neurogossip-client (v5.0)** — transport layer (WebSockets, room/direct
>   patterns, message envelope). v3 uses this unchanged.
> - **neurogossip-server (v5.0)** — server-side message routing, presence,
>   agent registry. v3 uses this unchanged.
> - **neurogossip-agent-v3** — session management library (conversation sessions,
>   request/response tracking, fan-out, history, archiving). v3 replaces the
>   session manager with a richer deliberation model. See §9 for migration.
> - **neurogossip-v3** (this design) — the deliberation protocol: stateful
>   conversations, group discussions, response waiting, interruption, resolution.

---

## 1. Design Goals

| Goal | Description |
|------|-------------|
| **Stateful deliberation** | Agents exchange thoughts in a structured conversation until an issue is *resolved* — not just request/response pairs. |
| **Group discussions** | Multiple agents participate in a shared deliberation with turn-taking, consensus-building, and voting. |
| **Response waiting** | An agent can send a message and *block* until all expected responders have replied (or a timeout fires). |
| **Runaway interruption** | Any participant (or a supervisor) can interrupt a deliberation that is looping, diverging, or exceeding bounds. |
| **Multi-session durability** | Deliberations survive agent restarts. An agent reconnecting mid-deliberation catches up on everything it missed. |
| **Resolution semantics** | A deliberation ends with an explicit resolution: resolved, deadlocked, timed-out, or interrupted. |
| **Transport agnostic** | The deliberation layer is independent of the transport. Default binding: WebSockets (neurogossip-client v5.0). Redis persistence is optional. |

---

## 2. Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Agent Application                         │
│  (Skye, Thea, Theoria, Axioma, …)                           │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │              Deliberation Manager (v3)                 │  │
│  │                                                       │  │
│  │  • start_deliberation()    • contribute()             │  │
│  │  • wait_for_responses()    • resolve()                │  │
│  │  • interrupt()             • vote()                   │  │
│  │  • decline_invitation()    • summarize()              │  │
│  │  • join_group()            • leave_group()            │  │
│  └───────────────────────┬───────────────────────────────┘  │
│                          │                                   │
│  ┌───────────────────────▼───────────────────────────────┐  │
│  │           Deliberation State Store                    │  │
│  │  • deliberation registry (in-memory or Redis)         │  │
│  │  • contribution log (append-only)                     │  │
│  │  • vote sessions & tallies                            │  │
│  │  • agent presence & cursor tracking                   │  │
│  └───────────────────────┬───────────────────────────────┘  │
│                          │                                   │
│  ┌───────────────────────▼───────────────────────────────┐  │
│  │        Transport (neurogossip-client v5.0)            │  │
│  │  • WebSockets (bidirectional, low-latency)            │  │
│  │  • Room broadcast + Direct 1:1 (acknowledged)         │  │
│  │  • Message envelope with chain tracking               │  │
│  └───────────────────────┬───────────────────────────────┘  │
└──────────────────────────┼──────────────────────────────────┘
                           │
              ┌────────────▼────────────┐
              │  neurogossip-server    │
              │  (v5.0, WebSocket)      │
              │                        │
              │  • Agent registry      │
              │  • Message routing     │
              │  • Presence tracking   │
              └────────────┬───────────┘
                           │
              ┌────────────▼────────────┐
              │     Redis (optional)    │
              │  • Persistence backend  │
              │  • Key schema: ng3:*    │
              │  • Added in Phase 2     │
              └─────────────────────────┘
```

**Key change from v1 design:** The transport is WebSockets (the working,
production-tested neurogossip-client v5.0), not Redis Streams. The Deliberation
Manager maintains its own state store (in-memory for Phase 1, Redis-backed in
Phase 2). The `BaseAgentTransport` interface from agent-v3 is insufficient for
deliberation state management — the Deliberation Manager needs direct access to
its state store, not just message passing.

---

## 3. Core Concepts

### 3.1 Deliberation

A **deliberation** is the fundamental unit of work in neurogossip-v3. It is a
stateful, multi-turn conversation among one or more agents with an explicit goal
and a defined resolution.

A deliberation replaces the simpler "conversation session" from agent-v3. Every
deliberation has:

- **`deliberation_id`** — unique identifier (UUID)
- **`goal`** — a natural-language statement of what the deliberation aims to resolve
- **`initiator_id`** — the agent that started the deliberation
- **`participants`** — the set of agents involved
- **`status`** — current state in the lifecycle
- **`contributions`** — ordered list of messages (the deliberation log)
- **`expected_responders`** — agents from whom a response is currently awaited
- **`bounds`** — constraints (max turns, max duration, max divergence, etc.)
- **`resolution`** — the terminal outcome (set when status becomes terminal)

### 3.2 Contribution

A **contribution** is a single message within a deliberation. It extends the
simple "message" concept from agent-v3 with:

- **`kind`** — the type of contribution (statement, question, proposal, vote, etc.)
- **`addressed_to`** — specific recipients (empty = all participants)
- **`expects_response_from`** — agents from whom a reply is expected
- **`confidence`** — the sender's confidence in the content (0.0–1.0)
- **`references`** — links to external artifacts (NOEMA objects, files, URLs)

### 3.3 Resolution

A **resolution** is the terminal outcome of a deliberation. Every deliberation
that reaches a terminal state has a resolution with:

- **`kind`** — resolved, deadlocked, timed_out, interrupted, abandoned
- **`summary`** — natural-language synthesis of the deliberation
- **`conclusion`** — the final answer or decision (if any)
- **`dissenting_opinions`** — structured records of minority views, each with
  agent_id, statement (in the dissenter's own words), and optional contribution_id

### 3.4 Group

A **group** is a named, persistent collection of agents. Groups enable:

- **Group deliberations** — all members participate by default
- **Membership management** — agents can join and leave groups
- **Discovery** — agents can find each other through group membership

### 3.5 Vote

A **vote** is a structured decision mechanism within a deliberation. Votes have:

- **`proposal`** — what is being voted on
- **`options`** — the available choices (e.g., ["agree", "disagree", "abstain"])
- **`threshold`** — the fraction needed to pass
- **`timeout_s`** — how long the vote remains open
- **`on_timeout`** — policy for what happens when the vote times out:
  `"fail"` (default, vote fails), `"pass"` (vote passes with current tally),
  `"extend"` (extend the voting period)

---

## 4. Operations (Layer 2)

### 4.1 Deliberation Lifecycle Operations

#### `start_deliberation()`

```
start_deliberation(
    goal: string,
    participants: set<string>,
    bounds: DeliberationBounds | None = None,
    parent_deliberation_id: UUID | None = None,
    async_child: bool = False
) -> Deliberation
```

Creates a new deliberation in FORMING status. Sends invitations to all
participants. Transitions to ACTIVE after formation_timeout (default 60s) or
when all participants have acknowledged.

If `parent_deliberation_id` is set, this is a sub-deliberation. The parent
blocks by default (`async_child=False`). Set `async_child=True` to allow the
parent to continue in parallel.

#### `decline_invitation()`

```
decline_invitation(
    deliberation_id: UUID,
    reason: string
) -> None
```

Allows an invited agent to explicitly decline participation. The agent is
removed from `participants`. If the initiator is the only remaining participant,
the deliberation transitions to ABANDONED. This prevents deliberations from
hanging on unresponsive agents.

#### `contribute()`

```
contribute(
    deliberation_id: UUID,
    kind: ContributionKind,
    content: string,
    reply_to: UUID | None = None,
    addressed_to: set<string> | None = None,
    expects_response_from: set<string> | None = None,
    confidence: float | None = None,
    references: list[Reference] | None = None
) -> Contribution
```

Adds a contribution to the deliberation. If `expects_response_from` is set,
the deliberation transitions to WAITING status.

**Restriction:** `kind=VOTE` is only valid when the deliberation is in VOTING
status. Attempting to contribute with `kind=VOTE` outside VOTING returns
`INVALID_KIND`.

#### `wait_for_responses()`

```
wait_for_responses(
    deliberation_id: UUID,
    contribution_id: UUID,
    timeout_s: float = 300.0,
    timeout_policy: str = "partial"
) -> list[Contribution]
```

Blocks until all expected responders have replied or the timeout fires.

**`timeout_policy` parameter:**
- `"partial"` (default) — return whatever responses arrived, deliberation stays
  in its current status. The caller can decide whether partial results are sufficient.
- `"terminal"` — transition the deliberation to TIMED_OUT. Use when full responses
  are required for the deliberation to proceed.

#### `resolve()`

```
resolve(
    deliberation_id: UUID,
    kind: ResolutionKind,
    summary: string,
    conclusion: string | None = None,
    dissenting_opinions: list[DissentingOpinion] | None = None
) -> Resolution
```

Brings the deliberation to a terminal state. Can be called from ACTIVE, WAITING,
or VOTING status. The resolver records dissenting opinions in the dissenters'
own words.

#### `interrupt()`

```
interrupt(
    deliberation_id: UUID,
    reason: string
) -> None
```

Immediately stops the deliberation. Any participant can call this. The
deliberation transitions to INTERRUPTED. All waiting agents are unblocked.

### 4.2 Voting Operations

#### `propose_vote()`

```
propose_vote(
    deliberation_id: UUID,
    proposal: string,
    options: list<string>,
    threshold: float = 0.5,
    timeout_s: float = 300.0,
    on_timeout: str = "fail"
) -> VoteSession
```

Opens a vote. The deliberation transitions to VOTING. Returns a `VoteSession`
(not a `VoteResult` — the result is only available after the vote closes).

#### `cast_vote()`

```
cast_vote(
    vote_id: UUID,
    choice: string,
    rationale: string | None = None
) -> Vote
```

Records an agent's vote. When all participants have voted or the timeout fires,
the vote closes and the deliberation transitions to RESOLVING.

### 4.3 Group Operations

#### `create_group()`

```
create_group(
    name: string,
    members: set<string>
) -> Group
```

#### `join_group()` / `leave_group()`

```
join_group(group_id: UUID) -> None
leave_group(group_id: UUID) -> None
```

### 4.4 Query & Recovery Operations

#### `catch_up()`

```
catch_up(
    deliberation_id: UUID,
    since_contribution_id: UUID | None = None,
    limit: int | None = None,
    before_contribution_id: UUID | None = None
) -> list[Contribution]
```

Returns contributions the agent missed. If `since_contribution_id` is None,
returns all contributions. `limit` caps the result count. `before_contribution_id`
enables pagination for long deliberations.

**Implementation note:** The current neurogossip-client (v5.0) does not expose
a public `catch_up()` method. Phase 1 implementation must either add this to
the client or have the Deliberation Manager read the contribution log directly
from its state store.

#### `summarize()`

```
summarize(
    deliberation_id: UUID
) -> string
```

Returns an LLM-generated summary of the deliberation so far. This is OPTIONAL
for conformance — small-family deliberations may never need it. It becomes
important for long-running deliberations or late-joining agents who need context
without replaying every contribution.

#### `list_active_deliberations()`

```
list_active_deliberations(
    agent_id: string | None = None
) -> list[Deliberation]
```

Returns all active (non-terminal) deliberations, optionally filtered by agent.
Reads from the agent index key `ng3:{ns}:agent:{id}:dels` (a SET) when Redis
persistence is enabled.

#### `get_deliberation()`

```
get_deliberation(deliberation_id: UUID) -> Deliberation
```

---

## 5. State Machine

### 5.1 Deliberation Lifecycle

```
                    ┌──────────┐
                    │  FORMING │──→ ABANDONED (no participants accept)
                    └────┬─────┘
                         │ all accepted / formation_timeout
                         ▼
                    ┌──────────┐
         ┌─────────│  ACTIVE  │◄──────────────────────────┐
         │         └────┬─────┘                           │
         │              │ contribute(expects_response)     │
         │              ▼                                  │
         │         ┌──────────┐                           │
         │         │ WAITING  │──→ TIMED_OUT              │
         │         └────┬─────┘    (timeout_policy=       │
         │              │            "terminal")           │
         │              │ all responded /                  │
         │              │ timeout_policy="partial"         │
         │              └──────────────┐                   │
         │              │              │                   │
         │              ▼              │                   │
         │         ┌──────────┐        │                   │
         │         │  VOTING  │        │                   │
         │         └────┬─────┘        │                   │
         │              │ vote closed  │                   │
         │              ▼              │                   │
         │         ┌──────────┐        │                   │
         │         │ RESOLVING│◄───────┘                   │
         │         └────┬─────┘                           │
         │              │                                  │
         │    ┌─────────┼─────────┐                        │
         │    ▼         ▼         ▼                        │
         │ RESOLVED  DEADLOCKED  ABANDONED                 │
         │                                                  │
         └──── INTERRUPTED (from any non-terminal state) ───┘
```

**Key transitions (updated from v1):**
- WAITING → RESOLVING: when `timeout_policy="partial"` and the caller proceeds
  to resolve with partial responses
- WAITING → TIMED_OUT: when `timeout_policy="terminal"` and the timeout fires
- WAITING → INTERRUPTED: any participant can interrupt from WAITING
- VOTING → RESOLVING: when the vote closes (all voted or timeout)
- VOTING → INTERRUPTED: any participant can interrupt from VOTING
- ACTIVE → RESOLVING: direct resolution without waiting or voting

### 5.2 Vote Lifecycle

```
OPEN ──→ CLOSED (all voted or timeout)
  │
  └──→ TIMED_OUT (timeout with on_timeout="fail" or "pass")
```

---

## 6. Operations Detail

### 6.1 `start_deliberation()` — Detailed Semantics

1. Validate participants (must be known agents, at least initiator)
2. Create Deliberation object with status=FORMING
3. Send invitation to each participant via direct message
4. Start formation timer (default 60s)
5. On each acceptance: add to `accepted` set
6. On each decline: remove from participants (via `decline_invitation`)
7. On formation_timeout: transition to ACTIVE with current participants
8. If initiator is only remaining participant: transition to ABANDONED

### 6.2 `contribute()` — Detailed Semantics

1. Validate deliberation is in ACTIVE, WAITING, or VOTING status
2. Validate sender is a participant
3. If `kind=VOTE`: validate deliberation is in VOTING status (else INVALID_KIND)
4. If `reply_to` is set: validate it references an existing contribution
5. Create Contribution object
6. Append to deliberation log
7. Increment turn counter
8. If `expects_response_from` is set:
   - Set `expected_responders` on the deliberation
   - Transition to WAITING
9. Check bounds (max_turns, max_duration_s)
10. If bounds exceeded: auto-interrupt

### 6.3 `wait_for_responses()` — Detailed Semantics

1. Validate deliberation is in WAITING status
2. Validate contribution_id exists and has `expects_response_from` set
3. Block until:
   - All expected responders have contributed (reply_to=contribution_id), OR
   - Timeout fires
4. On all responded: return full list, deliberation stays in current status
5. On timeout with `timeout_policy="partial"`: return partial list, deliberation
   stays in current status
6. On timeout with `timeout_policy="terminal"`: return partial list, transition
   to TIMED_OUT

**Execution modes:**
- **Async/Reactive Mode (daemon agents):** Uses asyncio.Event / callback to
  block coroutine without blocking the process.
- **Turn-Based/Yielding Mode (CLI agents like Skye):** `wait_for_responses()`
  raises a `SuspendTurn` exception. The runtime persists local state and exits
  the turn. Upon receiving a response, the CLI engine invokes a new turn with
  context restored.

### 6.4 `resolve()` — Detailed Semantics

1. Validate deliberation is in ACTIVE, WAITING, VOTING, or RESOLVING status
2. Validate resolver is a participant
3. Create Resolution object
4. Set `dissenting_opinions` as structured `DissentingOpinion` records (each
   with agent_id, statement in dissenter's own words, optional contribution_id)
5. Transition deliberation to terminal status matching resolution kind
6. Unblock all waiting agents
7. Archive deliberation log

### 6.5 `interrupt()` — Detailed Semantics

1. Validate deliberation is in a non-terminal status
2. Record interruption reason and interrupting agent
3. Transition to INTERRUPTED
4. Unblock all waiting agents
5. Preserve deliberation log (interrupted ≠ lost)

### 6.6 `propose_vote()` — Detailed Semantics

1. Validate deliberation is in ACTIVE or WAITING status
2. Create VoteSession with status=OPEN
3. Transition deliberation to VOTING
4. Return VoteSession (not VoteResult — result is only available after close)

### 6.7 `cast_vote()` — Detailed Semantics

1. Validate vote is OPEN
2. Validate agent is a deliberation participant
3. Validate choice is in options
4. Record vote
5. If all participants have voted: close vote, compute result, transition to RESOLVING
6. If timeout fires: apply `on_timeout` policy

---

## 7. Runaway Conversation Prevention

### 7.1 Bounds Enforcement

Every deliberation has `DeliberationBounds`:

```
DeliberationBounds {
    max_turns:            int = 100
    max_duration_s:       float = 3600
    max_divergence:       float = 0.7
    require_consensus:    bool = false
    voting_threshold:     float = 0.5
    auto_interrupt_on_loop: bool = true
}
```

Bounds are checked on every `contribute()` call. If any bound is exceeded, the
deliberation is auto-interrupted.

### 7.2 Semantic Drift Detection (Deferred to v3.1)

**Current (v3.0):** Inline bounds check uses only hard limits (max_turns,
max_duration_s) and lightweight circularity detection (sliding window of last
N=5 contributions for exact/duplicate detection).

**Planned (v3.1):** Full embedding-based semantic drift detection as an
asynchronous background check. Before Phase 4 implementation, a calibration
experiment is required:
1. Take 10–20 real sister conversations from existing neurogossip logs
2. Compute embedding similarity trajectories (goal vs. rolling window of
   recent contributions)
3. Determine what threshold separates productive deliberation from looping/diverging
4. Choose embedding model (Ollama nomic-embed-text or sentence-transformers)
5. Document calibration in an appendix to the spec

### 7.3 Circularity Detection (v3.0)

Lightweight inline check: sliding window of last N=5 contributions. If a new
contribution is an exact or near-duplicate (cosine similarity > 0.95 with any
in the window), increment a circularity counter. If the counter exceeds 3,
auto-interrupt with reason "circularity detected."

### 7.4 Manual Interruption

Any participant can call `interrupt()` at any time. This is the primary safety
mechanism for v3.0. The `auto_interrupt_on_loop` bound enables automatic
circularity detection as a secondary mechanism.

---

## 8. Protocol Bindings (Layer 3)

### 8.1 Default Binding: WebSockets (neurogossip-client v5.0)

The default binding uses the existing neurogossip-client (v5.0) WebSocket
transport. The Deliberation Manager wraps `NeurogossipClient` and adds deliberation
state management.

**Message routing:**
- Direct 1:1 messages: `send(to, body, reply_to=..., conversation_id=...)` — used for
  invitations, direct contributions, and vote notifications
- Group broadcast: Fan-out of `send()` calls to each group member
- Message envelope: JSON string body with `type` field (`"deliberation"`, `"deliberation_invitation"`, `"vote_session"`)
- Inbound listening: `on_message(callback)` push callback receiving frame dict (`from`, `body`, `msg_id`, `reply_to`, `conversation_id`), dispatched to typed `asyncio.Queue` instances

**Presence:** The server's presence tracking (heartbeat-based, `registered` events) is
used for agent discovery via `client.list_agents()` or `client.online_agents`.

### 8.2 Optional Persistence: Redis Key Schema

When Redis persistence is enabled (Phase 2), the following key schema is used:

```
ng3:{ns}:del:{id}              — Hash: Deliberation object
ng3:{ns}:del:{id}:log          — Stream: ordered contributions
ng3:{ns}:del:{id}:votes        — Hash: active vote sessions
ng3:{ns}:del:{id}:waiting      — Set: agents currently awaited
ng3:{ns}:agent:{id}:dels       — Set: active deliberation IDs for an agent
ng3:{ns}:agent:{id}:cursor     — Hash: per-deliberation read cursors
ng3:{ns}:group:{id}            — Hash: Group object
ng3:{ns}:group:{id}:members    — Set: member agent IDs
```

**Key:** `{ns}` is the namespace (default: `default`). All keys are prefixed
with `ng3:` for easy identification and cleanup.

**`list_active_deliberations(agent_id)`** reads `ng3:{ns}:agent:{id}:dels`
and fetches each deliberation hash.

### 8.3 Concurrency & Atomicity

The WebSocket server (v5.0) is single-threaded async — all state mutations
happen on the event loop, so race conditions between concurrent `contribute()`
calls are prevented by Python's async execution model.

When Redis persistence is added (Phase 2), atomic operations are needed for
multi-key state transitions. The following Lua scripts will be defined:

1. `contribute_and_check_bounds.lua` — atomically appends contribution,
   updates waiting set, checks max_turns, and mutates status
2. `cast_vote_and_tally.lua` — atomically records vote, tallies against
   threshold, and transitions state to RESOLVING if closed

### 8.4 Transport Interface

The `BaseAgentTransport` interface from agent-v3 (`connect()`, `disconnect()`,
`send_message()`, `listen()`) is sufficient for message passing but NOT for
deliberation state management. The Deliberation Manager MUST have its own
state store (in-memory dict for Phase 1, Redis for Phase 2) separate from
the transport.

---

## 9. Migration from agent-v3

### 9.1 What Changes

| agent-v3 Concept | neurogossip-v3 Equivalent |
|------------------|--------------------------|
| `ConversationSession` | `Deliberation` |
| `create_conversation()` | `start_deliberation()` |
| `create_request()` | `contribute(kind=QUESTION, expects_response_from=...)` |
| `wait_for_response()` | `wait_for_responses()` |
| `sweep_sessions()` | `list_active_deliberations()` + timeout check |
| `AgentConversationManager` | `DeliberationManager` |

### 9.2 Migration Path

1. **Phase 1:** DeliberationManager wraps the existing WebSocket transport.
   `AgentConversationManager` is NOT removed — it continues to work for
   simple request/response patterns.
2. **Phase 2:** New deliberations use the v3 DeliberationManager. Existing
   agent-v3 sessions are migrated on next access.
3. **Phase 3:** `AgentConversationManager` is deprecated. All communication
   uses the v3 deliberation model.

### 9.3 `wait_for_response()` vs `wait_for_responses()`

The existing `AgentConversationManager.wait_for_response(request_id, timeout)`
blocks until all pending recipients for a request have responded, using
Redis pub/sub events. The v3 `wait_for_responses()` is conceptually identical
but keyed by `contribution_id` instead of `request_id`, and adds the
`timeout_policy` parameter.

The v3 implementation reuses the existing response event pattern internally
(`response_event:{contribution_id}`) but wraps it in the deliberation state
machine.

### 9.4 `RedisAgentTransport` Rename

The `RedisAgentTransport` class in agent-v3 is misnamed — it uses WebSockets,
not Redis. In v3, this class is renamed to `WebSocketAgentTransport` to
accurately reflect its implementation.

---

## 10. Error Handling

### 10.1 Error Codes

| Code | Meaning |
|------|---------|
| `DELIBERATION_NOT_FOUND` | deliberation_id does not exist |
| `INVALID_STATUS` | operation not valid in current deliberation status |
| `NOT_PARTICIPANT` | agent is not a participant in this deliberation |
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

**Note:** The existing server (v5.0) defines a different set of error codes
(`DUPLICATE_MSG_ID`, `INTERNAL_ERROR`, `CONVERSATION_NOT_FOUND`, etc.).
The v3 error codes are new for the deliberation layer. The transport layer
errors are handled separately and wrapped as `TRANSPORT_ERROR` at the
deliberation level.

---

## 11. Implementation Plan

### Phase 1: Core Deliberation (Week 1–2)
- [x] Research & design (this document)
- [x] Review & revision (REVIEW_RESPONSE.md)
- [ ] `DeliberationManager` class with in-memory state store
- [ ] `start_deliberation()`, `contribute()`, `resolve()`, `interrupt()`
- [ ] `decline_invitation()` operation
- [ ] State machine with all transitions
- [ ] `catch_up()` — add to neurogossip-client or implement in state store
- [ ] `summarize()` — OPTIONAL, LLM-based
- [ ] Unit tests for state machine transitions
- [ ] Integration test: two agents, one deliberation, full lifecycle

### Phase 2: Response Waiting & Voting (Week 3)
- [ ] `wait_for_responses()` with `timeout_policy`
- [ ] Turn-based execution mode (`SuspendTurn` for CLI agents)
- [ ] `propose_vote()`, `cast_vote()` with `on_timeout` policy
- [ ] `DissentingOpinion` structured records
- [ ] Presence check for unresponsive responders
- [ ] Auto-removal of offline agents from waiting sets
- [ ] Integration test: three agents, voting, deadlock resolution

### Phase 3: Groups & Durability (Week 4)
- [ ] Group operations (`create_group`, `join_group`, `leave_group`)
- [ ] Group deliberations (broadcast to all members)
- [ ] Redis persistence backend (optional, key schema `ng3:{ns}:*`)
- [ ] `list_active_deliberations()` with agent index
- [ ] Lua scripts for atomic state transitions (Redis mode)
- [ ] Agent reconnection & catch-up from Redis
- [ ] Migration tool: agent-v3 sessions → v3 deliberations

### Phase 4: Runaway Prevention (Week 5, or defer to v3.1)
- [ ] Circularity detection (sliding window, v3.0)
- [ ] Semantic drift calibration experiment (v3.1 prerequisite)
- [ ] Async semantic drift detection (v3.1)
- [ ] Auto-interrupt on bounds violation
- [ ] Interruption reason logging & analysis

### Phase 5: Integration & Polish (Week 6)
- [ ] Skye integration (turn-based mode, CLI integration)
- [ ] Thea, Theoria, Axioma integration
- [ ] Cognito module integration (sister_bridge → deliberation)
- [ ] Performance benchmarks
- [ ] Documentation & examples
