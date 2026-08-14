# Neurogossip v3 — Protocol Review & Design Assessment

**Review Date:** August 6, 2026  
**Target Documents:**
- [`research.md`](file:///home/ubuntu/neurogossip/neurogossip-v3/design/research.md)
- [`design.md`](file:///home/ubuntu/neurogossip/neurogossip-v3/design/design.md)
- [`specification.md`](file:///home/ubuntu/neurogossip/neurogossip-v3/spec/specification.md)

---

## 1. Executive Summary

Neurogossip v3 is a well-conceived, domain-tailored deliberation protocol designed for a closed family of collaborating AI agents (Skye, Thea, Theoria, Axioma). Moving away from traditional 1:1 request/response or generic task-execution paradigms, it introduces **Deliberations** as stateful, goal-directed units of multi-agent work with explicit lifecycle management, response waiting, voting, and first-class runaway interruption.

Overall, the design is solid and directly addresses key failure modes of LLM multi-agent conversations (looping, semantic drift, unhandled waiting). However, before proceeding to implementation, several **cross-document inconsistencies**, **concurrency race conditions in Redis state management**, and **execution model ambiguities for turn-based runtimes** need to be remediated.

---

## 2. Key Strengths & Architectural Highlights

1. **Appropriate Conceptual Abstraction ("Deliberation" vs "Task")**
   - The choice of "Deliberation" as the core primitive over "Task" accurately reflects collaborative decision-making among equal sister agents rather than hierarchical task delegation.
2. **Clean Layered Architecture**
   - Decoupling Layer 1 (Data Model), Layer 2 (Abstract Operations), and Layer 3 (Protocol Bindings) ensures the deliberation protocol remains independent of the underlying Redis Streams transport layer (`neurogossip-client-v2`).
3. **First-Class Interruption & Bounds Enforcement**
   - Addressing runaway conversations with explicit bounds (`max_turns`, `max_duration_s`, `max_divergence`) and semantic/circularity detection directly addresses common LLM divergence patterns.
4. **Explicit Resolution Semantics**
   - Enforcing terminal states (`RESOLVED`, `DEADLOCKED`, `TIMED_OUT`, `INTERRUPTED`, `ABANDONED`) and capturing dissenting opinions prevents ambiguous or unclosed multi-agent discussions.
5. **Durable State & Reconnection Catch-Up**
   - Externalizing all state to Redis Streams guarantees that agent restarts do not destroy ongoing deliberations.

---

## 3. Cross-Document Inconsistencies & Discrepancies

During review, several discrepancies between `design.md` and `specification.md` were identified:

| Item | `design.md` | `specification.md` | Resolution / Recommendation |
|------|-------------|--------------------|-----------------------------|
| **Redis Key Schema Prefix** | `neurogossip:{namespace}:deliberation:{id}` (§8.1) | `ng3:{ns}:del:{id}` (§6.1) | **Standardize on `ng3:{ns}:del:{id}`** for brevity and consistency across Redis keys. |
| **`propose_vote()` Return Type** | Returns `VoteResult` (§6.6) | Returns `VoteSession` (§4.2) | **Standardize on `VoteSession`**. `VoteResult` is only computed when the vote closes. |
| **`DeliberationBounds` Fields** | Fields: `max_turns`, `max_duration_s`, `max_divergence`, `require_consensus`, `voting_threshold` (§4.1) | Adds `auto_interrupt_on_loop: bool = true` (§3.1) | **Update `design.md`** to include `auto_interrupt_on_loop`. |
| **State Machine Transitions** | §5.1 diagram shows `RESOLVING` reachable only from `ACTIVE` (via `resolve`/`vote`) | §4.1 `resolve()` preconditions allow invocation from `WAITING` or `VOTING` | **Update §5.1 state diagram** in both docs to explicitly depict transitions from `WAITING` and `VOTING` directly to `RESOLVING` or `INTERRUPTED`. |

---

## 4. Technical Analysis & Edge Case Evaluation

### 4.1 Concurrency & Race Conditions in Redis State

**Issue:**  
The specification outlines individual Redis commands (e.g., `XADD` to stream, updates to hash keys, set operations for waiting lists). In a multi-agent environment where agents contribute concurrently, non-atomic multi-key updates present race conditions:

- **Response Waiting Completion:** If two expected responders (e.g., Thea and Theoria) submit contributions simultaneously, both check `ng3:{ns}:del:{id}:waiting`. Both might read the non-empty set, perform `SREM`, and miss setting the unblock event if atomic checks are absent.
- **Turn Limits & Auto-Interruption:** If turn 99 and 100 arrive simultaneously, both calls to `contribute()` may pass the `max_turns` check before either updates the deliberation status to `INTERRUPTED`.
- **Vote Tallying:** Concurrent calls to `cast_vote()` may simultaneously evaluate `threshold` checks and attempt to trigger state transitions to `RESOLVING`.

**Recommendation:**  
Define explicit **Redis Lua scripts** or `MULTI/EXEC` transactions for atomic state transitions:
1. `contribute_and_check_bounds.lua`: Atomically appends contribution, updates waiting set, checks `max_turns`, and mutates status (`ACTIVE` vs `WAITING` vs `INTERRUPTED`).
2. `cast_vote_and_tally.lua`: Atomically records vote, tallies votes against threshold, and transitions state to `RESOLVING` if closed.

---

### 4.2 Skye Turn-Based Runtime Integration vs. Blocking `wait_for_responses()`

**Issue:**  
Section 6.3 of `specification.md` defines `wait_for_responses()` as a blocking async call:
```python
wait_for_responses(deliberation_id: UUID, contribution_id: UUID, timeout_s: float = 300.0) -> list[Contribution]
```
However, Skye is a turn-based agent executed via CLI/runtime loop (`runtime/cli.py`). A blocking 300-second Python wait holding an event loop or process thread conflicts with turn-based execution and resource utilization.

**Recommendation:**  
Explicitly formalize the two execution modes in `specification.md`:
1. **Async / Reactive Mode (Long-Running Daemon Agents):** Uses `asyncio.Event` / Redis Pub/Sub listener to block coroutine execution without blocking the process.
2. **Turn-Based / Yielding Mode (CLI Agents like Skye):** `wait_for_responses()` raises a `SuspendTurn` exception (or returns a `DeliberationYield` directive to the CLI engine). The runtime persists local state and exits the turn. Upon receiving a response via Redis stream, the CLI engine invokes a new turn with the context restored.

---

### 4.3 Semantic Drift & Circularity Detection Operationalization

**Issue:**  
`specification.md` §5.2 states that semantic drift detection is a **MUST** for bounds enforcement (`divergence(goal, recent_contributions) > bounds.max_divergence`). However, `research.md` §4.1 lists embedding models as an **Open Question**.
Computing vector embeddings or heavy n-gram comparisons on every single `contribute()` call introduces:
- Significant latency overhead on contribution publishing.
- External model dependency (e.g., local Ollama vs sentence-transformers).

**Recommendation:**  
- Downgrade semantic drift in core `contribute()` to an asynchronous background check or an optional middleware guard.
- For synchronous inline bounds checking, specify a lightweight default algorithm:
  - **Level 1 (Inline/Fast):** Turn count limit + exact/fuzzy duplicate contribution detector (sliding window of last $N=5$ contributions).
  - **Level 2 (Async/Optional):** Background embedding similarity evaluator using `sentence-transformers/all-MiniLM-L6-v2` or Ollama embeddings.

---

### 4.4 Unresponsive Responders & Group Membership Changes

**Issue:**  
- **Crashing/Offline Agents:** If Agent B crashes after being designated in `expects_response_from`, Agent A will block for `timeout_s` (default 5 minutes). In interactive CLI sessions, 5 minutes is excessively long.
- **Dynamic Group Departure:** If an agent leaves a group (`leave_group()`) while being an expected responder in an active group deliberation, `specification.md` does not specify whether `deliberation.participants` or `expects_response_from` is automatically updated.

**Recommendation:**  
1. **Presence & Heartbeat Check:** Integrate with `neurogossip-client-v2` presence keys (Redis TTL). If an expected responder's presence key expires, `wait_for_responses()` should fail fast with an `AGENT_OFFLINE` error rather than waiting for full timeout.
2. **Responder Cancellation on Group Departure:** When an agent leaves a group, the deliberation manager MUST automatically remove the agent from `expects_response_from` sets across active group deliberations and notify waiting initiators.

---

### 4.5 Voting & Consensus Deadlocks

**Issue:**  
When `require_consensus = True`, a single offline or abstaining agent can permanently block resolution until `max_duration_s` expires.

**Recommendation:**  
Add an explicit policy for non-voting or offline participants:
- `on_timeout` policy in `VoteSession`: `DEADLOCK` (default) vs `PROCEED_WITH_QUORUM`.
- Explicit support for `ABSTAIN` votes which count toward quorum without blocking consensus.

---

## 5. Implementation Readiness & Action Plan

### Recommended Remediation Steps Before Phase 1 Implementation

1. **Align Documentation Schemas:** Update `design.md` and `specification.md` to use identical Redis key prefixes (`ng3:{ns}:...`) and function signatures.
2. **Draft Lua Scripts Schema:** Define atomic Lua script specifications in `specification.md` §6.1 for `contribute`, `wait_for_responses`, and `cast_vote`.
3. **Formalize Turn-Based Yield Protocol:** Clarify the `SuspendTurn` / return-signal mechanism for Skye's CLI runtime in `design.md` §9.2.
4. **Refine Drift Bounds:** Adjust §5.2 of `specification.md` so that complex embedding-based drift checks are SHOULD/OPTIONAL while exact sliding-window repetition checks are MUST.

---

### Final Verdict

**Status:** **Approved with Modifications Required**  
The Neurogossip v3 design is architecturally sound and represents a significant advancement for multi-agent deliberation. Resolving the identified schema inconsistencies, Redis concurrency guards, and execution model details will ensure a smooth implementation phase.
