# Neurogossip v3 — Protocol Specification

**Version:** 1.0.0-draft
**Status:** Design Phase (post-review revision)
**Depends on:** neurogossip-client v5.0 (WebSocket transport), neurogossip-server v5.0 (message routing, presence)
**Last updated:** 2026-08-06 (post-review revision — see REVIEW_RESPONSE.md)

---

## 1. Introduction

### 1.1 Purpose

This document specifies the **Neurogossip v3 Protocol** — a stateful, multi-turn,
multi-agent deliberation protocol for AI agents. It defines the data model,
operations, state machines, and protocol bindings that enable agents to:

- Engage in structured, goal-directed conversations (deliberations)
- Exchange thoughts until an issue is resolved
- Participate in group discussions with turn-taking and voting
- Wait for responses from specific agents
- Interrupt runaway conversations
- Survive agent restarts with full catch-up

### 1.2 Scope

This specification covers:

- **Layer 1: Data Model** — the canonical data structures
- **Layer 2: Abstract Operations** — the operations agents can perform
- **Layer 3: Protocol Bindings** — how operations map to transport

It does NOT cover:

- Agent identity and authentication (delegated to transport layer)
- Transport-level reliability (delegated to transport layer)
- Agent implementation (language, framework, LLM)

### 1.3 Requirements Language

The keywords "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" are to be interpreted as
described in RFC 2119.

---

## 2. Terminology

| Term | Definition |
|------|------------|
| **Agent** | An AI system that participates in deliberations. Has a unique `agent_id`. |
| **Deliberation** | A stateful, multi-turn conversation among agents with an explicit goal and resolution. |
| **Contribution** | A single message within a deliberation. Has a `kind` (statement, question, proposal, etc.). |
| **Resolution** | The terminal outcome of a deliberation (resolved, deadlocked, timed-out, interrupted, abandoned). |
| **Group** | A named, persistent collection of agents that can participate in shared deliberations. |
| **Participant** | An agent that is a member of a deliberation. |
| **Expected Responder** | An agent from whom a response is explicitly awaited. |
| **Interruption** | A first-class operation that stops a deliberation before natural resolution. |
| **Vote** | A structured decision mechanism within a deliberation. |
| **Bounds** | Constraints on a deliberation: max turns, max duration, max divergence. |

---

## 3. Data Model (Layer 1)

### 3.1 Deliberation

```
Deliberation {
    deliberation_id:       UUID          // REQUIRED, unique
    goal:                  string        // REQUIRED, natural language
    initiator_id:          string        // REQUIRED, agent_id
    participants:          set<string>   // REQUIRED, at least initiator
    status:                DeliberationStatus  // REQUIRED
    bounds:                DeliberationBounds  // REQUIRED
    resolution:            Resolution | null   // set on terminal status
    parent_deliberation_id: UUID | null  // for sub-deliberations
    async_child:           bool = false  // parent continues in parallel if true
    created_at:            datetime      // REQUIRED
    updated_at:            datetime      // REQUIRED
}

DeliberationStatus: enum {
    FORMING      // gathering participants
    ACTIVE       // contributions in progress
    WAITING      // waiting for response(s)
    VOTING       // a vote is in progress
    RESOLVING    // resolution being formulated
    // Terminal states:
    RESOLVED
    DEADLOCKED
    TIMED_OUT
    INTERRUPTED
    ABANDONED
}

DeliberationBounds {
    max_turns:            int = 100      // max contributions before auto-interrupt
    max_duration_s:       float = 3600   // max wall-clock time before auto-timeout
    max_divergence:       float = 0.7    // semantic drift threshold (0.0–1.0)
    require_consensus:    bool = false   // all must agree for resolution
    voting_threshold:     float = 0.5    // fraction needed to pass a vote
    auto_interrupt_on_loop: bool = true  // enable circularity detection
}
```

### 3.2 Contribution

```
Contribution {
    contribution_id:      UUID          // REQUIRED, unique
    deliberation_id:      UUID          // REQUIRED
    sender_id:            string        // REQUIRED, agent_id
    kind:                 ContributionKind  // REQUIRED
    reply_to:             UUID | null   // parent contribution_id
    thread_id:            UUID | null   // root of sub-thread
    addressed_to:         set<string>   // specific recipients (empty = all)
    expects_response_from: set<string>  // agents expected to reply
    content:              string        // REQUIRED, markdown
    confidence:           float | null  // 0.0–1.0
    references:           list<Reference>  // external artifacts
    created_at:           datetime      // REQUIRED
}

ContributionKind: enum {
    STATEMENT        // claim, observation, information
    QUESTION         // directed at one or more participants
    PROPOSAL         // proposed resolution or course of action
    COUNTERPROPOSAL  // alternative to a previous proposal
    CRITIQUE         // objection or identified flaw
    CLARIFICATION    // request for or provision of clarification
    VOTE             // explicit vote on a proposal (only valid in VOTING status)
    SUMMARY          // synthesis of discussion so far
    INTERRUPT        // request to interrupt the deliberation
}

Reference {
    ref_type:   string   // "noema" | "file" | "url" | "ere"
    ref_id:     string   // the actual id/path/url
    description: string | null
}
```

### 3.3 Resolution

```
Resolution {
    resolution_id:        UUID          // REQUIRED, unique
    deliberation_id:      UUID          // REQUIRED
    kind:                 ResolutionKind  // REQUIRED
    summary:              string        // REQUIRED, natural language
    conclusion:           string | null // final answer/decision
    dissenting_opinions:  list<DissentingOpinion>  // minority views
    resolved_by:          string        // REQUIRED, agent_id
    resolved_at:          datetime      // REQUIRED
}

DissentingOpinion {
    agent_id:         string        // REQUIRED, the dissenting agent
    statement:        string        // REQUIRED, in the dissenter's own words
    contribution_id:  UUID | null   // reference to the contribution where dissent was expressed
}

ResolutionKind: enum {
    RESOLVED     // goal achieved, conclusion reached
    DEADLOCKED   // participants could not agree
    TIMED_OUT    // exceeded time bound
    INTERRUPTED  // stopped by participant or supervisor
    ABANDONED    // withdrawn by initiator
}
```

### 3.4 Group

```
Group {
    group_id:    UUID          // REQUIRED, unique
    name:        string        // REQUIRED, human-readable
    members:     set<string>   // REQUIRED, agent_ids
    created_at:  datetime      // REQUIRED
    metadata:    map<string, any>  // optional
}
```

### 3.5 Vote

```
VoteSession {
    vote_id:         UUID          // REQUIRED, unique
    deliberation_id: UUID          // REQUIRED
    proposal:        string        // REQUIRED, what is being voted on
    options:         list<string>  // REQUIRED, e.g. ["agree", "disagree", "abstain"]
    threshold:       float         // REQUIRED, fraction needed to pass
    timeout_s:       float         // REQUIRED
    on_timeout:      string        // REQUIRED, "fail" | "pass" | "extend"
    status:          VoteStatus    // REQUIRED
    votes:           map<string, Vote>  // agent_id → Vote
    result:          VoteResult | null
    created_at:      datetime      // REQUIRED
}

VoteStatus: enum { OPEN, CLOSED, TIMED_OUT }

Vote {
    agent_id:   string        // REQUIRED
    choice:     string        // REQUIRED, must be in options
    rationale:  string | null // optional explanation
    cast_at:    datetime      // REQUIRED
}

VoteResult {
    outcome:    string        // winning option or "deadlocked"
    tally:      map<string, int>  // option → count
    passed:     bool          // did the threshold pass?
}
```

---

## 4. Abstract Operations (Layer 2)

### 4.1 Deliberation Lifecycle

#### `start_deliberation(goal, participants, bounds?, parent_deliberation_id?, async_child?) → Deliberation`

**Preconditions:**
- `goal` MUST be non-empty
- `participants` MUST contain at least the initiator
- All participants MUST be known agents
- If `parent_deliberation_id` is set, that deliberation MUST exist and be non-terminal

**Postconditions:**
- A new Deliberation is created with status=FORMING
- Invitations are sent to all participants
- After formation_timeout (default 60s) or all accepted: status → ACTIVE
- If initiator is the only remaining participant after declines: status → ABANDONED

**Parameters:**
- `async_child: bool = false` — if true, parent deliberation continues in parallel
  while the sub-deliberation runs. If false (default), parent blocks.

#### `decline_invitation(deliberation_id, reason) → None`

**Preconditions:**
- Deliberation MUST be in FORMING status
- Agent MUST be a participant

**Postconditions:**
- Agent is removed from `participants`
- If initiator is the only remaining participant: status → ABANDONED

#### `contribute(deliberation_id, kind, content, reply_to?, addressed_to?, expects_response_from?, confidence?, references?) → Contribution`

**Preconditions:**
- Deliberation MUST be in ACTIVE, WAITING, or VOTING status
- Sender MUST be a participant
- If `kind=VOTE`: deliberation MUST be in VOTING status (else INVALID_KIND)
- If `reply_to` is set: MUST reference an existing contribution

**Postconditions:**
- Contribution is appended to the deliberation log
- Turn counter is incremented
- If `expects_response_from` is set: status → WAITING, expected_responders set
- Bounds are checked; if exceeded: auto-interrupt

#### `wait_for_responses(deliberation_id, contribution_id, timeout_s?, timeout_policy?) → list[Contribution]`

**Preconditions:**
- Deliberation MUST be in WAITING status
- `contribution_id` MUST exist and have `expects_response_from` set

**Postconditions:**
- If all expected responders replied: returns full list, status unchanged
- If timeout with `timeout_policy="partial"`: returns partial list, status unchanged
- If timeout with `timeout_policy="terminal"`: returns partial list, status → TIMED_OUT

**Parameters:**
- `timeout_s: float = 300.0`
- `timeout_policy: str = "partial"` — `"partial"` | `"terminal"`

#### `resolve(deliberation_id, kind, summary, conclusion?, dissenting_opinions?) → Resolution`

**Preconditions:**
- Deliberation MUST be in ACTIVE, WAITING, VOTING, or RESOLVING status
- Resolver MUST be a participant

**Postconditions:**
- Resolution is created with structured `DissentingOpinion` records
- Deliberation transitions to terminal status matching resolution kind
- All waiting agents are unblocked

#### `interrupt(deliberation_id, reason) → None`

**Preconditions:**
- Deliberation MUST be in a non-terminal status
- Interrupting agent MUST be a participant

**Postconditions:**
- Deliberation status → INTERRUPTED
- All waiting agents are unblocked
- Deliberation log is preserved

### 4.2 Voting

#### `propose_vote(deliberation_id, proposal, options, threshold?, timeout_s?, on_timeout?) → VoteSession`

**Preconditions:**
- Deliberation MUST be in ACTIVE or WAITING status
- `options` MUST contain at least 2 choices
- `threshold` MUST be in (0.0, 1.0]

**Postconditions:**
- VoteSession is created with status=OPEN
- Deliberation status → VOTING
- Returns VoteSession (NOT VoteResult — result only available after close)

**Parameters:**
- `on_timeout: str = "fail"` — `"fail"` | `"pass"` | `"extend"`

#### `cast_vote(vote_id, choice, rationale?) → Vote`

**Preconditions:**
- Vote MUST be OPEN
- Agent MUST be a deliberation participant
- `choice` MUST be in options

**Postconditions:**
- Vote is recorded
- If all participants have voted: vote closes, result computed, status → RESOLVING
- If timeout fires: `on_timeout` policy applied

### 4.3 Groups

#### `create_group(name, members) → Group`
#### `join_group(group_id) → None`
#### `leave_group(group_id) → None`

### 4.4 Query & Recovery

#### `catch_up(deliberation_id, since_contribution_id?, limit?, before_contribution_id?) → list[Contribution]`

Returns contributions the agent missed.

**Parameters:**
- `since_contribution_id: UUID | None = None` — return contributions after this
- `limit: int | None = None` — cap the result count
- `before_contribution_id: UUID | None = None` — return contributions before this (for pagination)

#### `summarize(deliberation_id) → string`

Returns an LLM-generated summary of the deliberation so far. OPTIONAL for
conformance. RECOMMENDED for long-running deliberations or late-joining agents.

#### `list_active_deliberations(agent_id?) → list[Deliberation]`

Returns all non-terminal deliberations, optionally filtered by agent.
Reads from `ng3:{ns}:agent:{id}:dels` when Redis persistence is enabled.

#### `get_deliberation(deliberation_id) → Deliberation`

---

## 5. State Machines

### 5.1 Deliberation Lifecycle

```
FORMING ──→ ACTIVE (all accepted or formation_timeout)
FORMING ──→ ABANDONED (initiator is only remaining participant)

ACTIVE ──→ WAITING (contribute with expects_response_from)
ACTIVE ──→ VOTING (propose_vote)
ACTIVE ──→ RESOLVING (resolve)
ACTIVE ──→ INTERRUPTED (interrupt)

WAITING ──→ ACTIVE (all expected responders replied, or timeout_policy="partial")
WAITING ──→ TIMED_OUT (timeout_policy="terminal" and timeout fires)
WAITING ──→ VOTING (propose_vote)
WAITING ──→ RESOLVING (resolve with partial responses)
WAITING ──→ INTERRUPTED (interrupt)

VOTING ──→ RESOLVING (vote closes: all voted or timeout)
VOTING ──→ INTERRUPTED (interrupt)

RESOLVING ──→ RESOLVED | DEADLOCKED | ABANDONED
```

**Key:** All non-terminal states (FORMING, ACTIVE, WAITING, VOTING, RESOLVING)
MAY transition to INTERRUPTED on `interrupt()`.

### 5.2 Vote Lifecycle

```
OPEN ──→ CLOSED (all participants voted)
OPEN ──→ TIMED_OUT (timeout with on_timeout="fail" or "pass")
OPEN ──→ OPEN (timeout with on_timeout="extend" — timeout_s extended)
```

### 5.3 Bounds Enforcement

Bounds are checked on every `contribute()` call:

1. **Hard limits (v3.0, inline):**
   - `max_turns` exceeded → auto-interrupt with reason "max_turns exceeded"
   - `max_duration_s` exceeded → auto-interrupt with reason "max_duration exceeded"
   - Circularity: sliding window of last N=5 contributions; if new contribution
     is exact/near-duplicate (cosine similarity > 0.95), increment counter;
     if counter > 3, auto-interrupt with reason "circularity detected"

2. **Semantic drift (v3.1, async):**
   - Background embedding similarity check between goal and rolling window of
     recent contributions
   - If `divergence > max_divergence`: flag for review, optionally auto-interrupt
   - Requires calibration experiment before implementation (see design.md §7.2)

---

## 6. Protocol Bindings (Layer 3)

### 6.1 Default Binding: WebSockets (neurogossip-client v5.0)

The default binding uses the neurogossip-client v5.0 WebSocket transport.

**Message routing:**
- Direct 1:1: `send(to, body, reply_to=..., conversation_id=...)` — invitations, direct contributions, vote notifications
- Group broadcast: Fan-out of `send()` calls to each group member
- Message envelope: JSON string body with `type` field (`"deliberation"`, `"deliberation_invitation"`, `"vote_session"`)
- Inbound listening: `on_message(callback)` push callback receiving frame dict with `"from"`, `"body"`, `"msg_id"`, `"reply_to"`, `"conversation_id"`

**Presence:** The server's presence tracking is used for agent discovery via `client.list_agents()` or `client.online_agents`.

### 6.2 Optional Persistence: Redis Key Schema

When Redis persistence is enabled, the following key schema MUST be used:

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

### 6.3 Concurrency Guarantees

**WebSocket mode (Phase 1):** The server is single-threaded async — all state
mutations happen on the event loop. Race conditions between concurrent
`contribute()` calls are prevented by Python's async execution model.

**Redis mode (Phase 2):** Atomic Lua scripts MUST be used for multi-key state
transitions:

1. `contribute_and_check_bounds.lua` — atomically appends contribution,
   updates waiting set, checks `max_turns`, and mutates status
2. `cast_vote_and_tally.lua` — atomically records vote, tallies against
   threshold, and transitions state to RESOLVING if closed

### 6.4 Transport Interface

The `BaseAgentTransport` interface from agent-v3 (`connect()`, `disconnect()`,
`send_message()`, `listen()`) handles message passing. The Deliberation Manager
MUST maintain its own state store (in-memory dict for Phase 1, Redis for Phase 2)
separate from the transport. `BaseAgentTransport` alone is insufficient for
deliberation state management.

---

## 7. Error Codes

| Code | HTTP Analog | Meaning |
|------|------------|---------|
| `DELIBERATION_NOT_FOUND` | 404 | deliberation_id does not exist |
| `INVALID_STATUS` | 409 | operation not valid in current status |
| `NOT_PARTICIPANT` | 403 | agent is not a participant |
| `INVALID_KIND` | 400 | contribution kind not valid in current status |
| `CONTRIBUTION_NOT_FOUND` | 404 | reply_to or contribution_id does not exist |
| `VOTE_NOT_FOUND` | 404 | vote_id does not exist |
| `VOTE_CLOSED` | 409 | vote is no longer OPEN |
| `VOTE_ALREADY_ACTIVE` | 409 | a vote is already in progress |
| `INVALID_CHOICE` | 400 | vote choice not in options |
| `TIMEOUT` | 408 | operation timed out |
| `INTERRUPTED` | 409 | deliberation was interrupted |
| `ALREADY_EXISTS` | 409 | deliberation/group with this ID already exists |
| `EMPTY_PARTICIPANTS` | 400 | participants set is empty |
| `GROUP_NOT_FOUND` | 404 | group_id does not exist |
| `GROUP_ALREADY_EXISTS` | 409 | group with this name already exists |
| `ALREADY_MEMBER` | 409 | agent is already a member |
| `NOT_MEMBER` | 404 | agent is not a member |

**Note:** The existing server (v5.0) defines a different set of error codes
(`DUPLICATE_MSG_ID`, `INTERNAL_ERROR`, `CONVERSATION_NOT_FOUND`,
`NOT_CONVERSATION_PARTICIPANT`, `BAD_REQUEST`). These are transport-layer
errors. The v3 error codes above are deliberation-layer errors. Transport
errors are wrapped as `TRANSPORT_ERROR` at the deliberation level.

---

## 8. Execution Modes

### 8.1 Async/Reactive Mode (Daemon Agents)

For long-running daemon agents (e.g., a dedicated deliberation server):

- `wait_for_responses()` uses `asyncio.Event` or Redis Pub/Sub listener
- The coroutine blocks without blocking the process
- Other deliberations continue processing concurrently

### 8.2 Turn-Based/Yielding Mode (CLI Agents)

For turn-based CLI agents (Skye, Thea, Theoria, Axioma):

- `wait_for_responses()` raises a `SuspendTurn` exception
- The runtime persists local state and exits the turn
- Upon receiving a response via the transport, the CLI engine invokes a new
  turn with context restored
- The `DeliberationYield` directive carries: `deliberation_id`, `contribution_id`,
  `expected_responders`, `timeout_s`, `timeout_policy`

---

## 9. Migration from agent-v3

### 9.1 Concept Mapping

| agent-v3 | neurogossip-v3 |
|----------|---------------|
| `ConversationSession` | `Deliberation` |
| `create_conversation()` | `start_deliberation()` |
| `create_request()` | `contribute(kind=QUESTION, expects_response_from=...)` |
| `wait_for_response(request_id, timeout)` | `wait_for_responses(deliberation_id, contribution_id, timeout_s, timeout_policy)` |
| `sweep_sessions()` | `list_active_deliberations()` + timeout check |
| `AgentConversationManager` | `DeliberationManager` |
| `RedisAgentTransport` | Renamed to `WebSocketAgentTransport` |

### 9.2 Migration Path

1. **Phase 1:** DeliberationManager coexists with AgentConversationManager.
   New deliberations use v3; existing sessions continue on agent-v3.
2. **Phase 2:** Existing agent-v3 sessions are migrated to v3 deliberations
   on next access.
3. **Phase 3:** AgentConversationManager is deprecated and removed.

### 9.3 `wait_for_response()` Compatibility

The v3 `wait_for_responses()` reuses the existing response event pattern
(`response_event:{contribution_id}`) internally but wraps it in the
deliberation state machine. The `timeout_policy` parameter is new in v3.

---

## 10. Conformance

### 10.1 Minimum Conformance (MUST)

A conforming implementation MUST:

- Implement the Deliberation, Contribution, and Resolution data models
- Implement the full deliberation lifecycle state machine
- Support `start_deliberation`, `contribute`, `resolve`, `interrupt`
- Support `decline_invitation`
- Support `wait_for_responses` with `timeout_policy`
- Support `catch_up` for reconnecting agents
- Enforce hard bounds (max_turns, max_duration_s) on every contribution
- Support manual interruption from any participant

### 10.2 Full Conformance (SHOULD)

A fully conforming implementation SHOULD also:

- Support voting (`propose_vote`, `cast_vote`) with `on_timeout` policy
- Support groups (`create_group`, `join_group`, `leave_group`)
- Support `summarize` for long-running deliberations
- Support circularity detection (sliding window)
- Support Redis persistence with the `ng3:{ns}:*` key schema
- Support both execution modes (async and turn-based)

### 10.3 Optional Features (MAY)

- Semantic drift detection (v3.1)
- A2A-compatible HTTP gateway (Phase 2)
- Sub-deliberations with `async_child`
- Pagination in `catch_up` (`limit`, `before_contribution_id`)
