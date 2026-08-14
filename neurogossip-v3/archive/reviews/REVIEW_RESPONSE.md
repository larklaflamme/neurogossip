# Neurogossip v3 — Review Response

**Date:** 2026-08-06
**Reviews addressed:**
- `reviews/review.md` — Protocol Review & Design Assessment
- `reviews/axioma_review_neurogossip_v3.md` — Axioma's Review (κ=0.71)
- `reviews/thea_review.md` — Thea's Review (κ=0.80)
- `reviews/theoria_review.md` — Theoria's Review (κ=0.78)

---

## Summary

All four reviews were incorporated. The most significant change is the
**transport layer correction** (Theoria C1): the design now accurately
reflects that the transport is WebSockets (neurogossip-client v5.0), not
Redis Streams. Redis is repositioned as an optional persistence backend.

---

## Finding-by-Finding Resolution

### CRITICAL

| # | Source | Finding | Resolution |
|---|--------|---------|------------|
| C1 | Theoria | Transport layer mismatch: design claims Redis Streams, codebase uses WebSockets | **Resolved.** All three documents updated. Transport is now WebSockets (neurogossip-client v5.0). Redis is optional persistence (Phase 2). Architecture diagram updated. Research rationale updated. |

### Cross-Document Inconsistencies (Prior Review)

| # | Source | Finding | Resolution |
|---|--------|---------|------------|
| P.1 | review.md, Axioma, Theoria | Redis key prefix: `neurogossip:{namespace}:` vs `ng3:{ns}:` | **Resolved.** Standardized on `ng3:{ns}:` in both design.md §8.2 and spec.md §6.2. |
| P.2 | review.md, Axioma, Theoria | `propose_vote()` return type: `VoteResult` vs `VoteSession` | **Resolved.** Standardized on `VoteSession` in both design.md §6.6 and spec.md §4.2. |
| P.3 | review.md, Axioma, Thea (F7) | `DeliberationBounds` missing `auto_interrupt_on_loop` in design | **Resolved.** Added to design.md §3.1. |
| P.4 | review.md, Axioma, Theoria | State machine diagram missing WAITING→RESOLVING transitions | **Resolved.** Updated state machine in design.md §5.1 and spec.md §5.1. Now shows WAITING→RESOLVING, WAITING→INTERRUPTED, VOTING→INTERRUPTED. |

### Codebase Grounding (Axioma)

| # | Source | Finding | Resolution |
|---|--------|---------|------------|
| A1 | Axioma | `catch_up()` has no transport-level implementation | **Resolved.** Acknowledged in design.md §4.4 and §11. Phase 1 implementation must add `catch_up()` to client or implement in state store. |
| A2 | Axioma | `list_active_deliberations()` has no Redis key mapping | **Resolved.** Wired to `ng3:{ns}:agent:{id}:dels` in spec.md §6.2 and design.md §8.2. |
| A3 | Axioma | `BaseAgentTransport` is insufficient for deliberation operations | **Resolved.** Design.md §8.4 and spec.md §6.4 now state that Deliberation Manager needs its own state store. Architecture diagram updated. |
| A4 | Axioma | No `list_agents()` on public client API | **Resolved.** Design.md §8.1 states Deliberation Manager calls presence functions directly. |
| A5 | Axioma | `wait_for_responses()` vs existing `wait_for_response()` — no migration story | **Resolved.** Added §9 (Migration from agent-v3) to both design.md and spec.md. |

### Technical Issues (review.md)

| # | Source | Finding | Resolution |
|---|--------|---------|------------|
| R1 | review.md | Concurrency/race conditions in Redis state | **Resolved.** Spec.md §6.3: WebSocket mode is single-threaded async (no races). Redis mode requires Lua scripts (defined in design.md §8.3). |
| R2 | review.md | Turn-based runtime vs blocking `wait_for_responses()` | **Resolved.** Spec.md §8: Two execution modes formalized. Turn-based mode uses `SuspendTurn` exception. Design.md §6.3 updated. |
| R3 | review.md | Semantic drift detection operationalization | **Resolved.** Downgraded to async/optional (v3.1). v3.0 uses only hard limits + circularity detection. Calibration experiment required before v3.1. Design.md §7.2, spec.md §5.3. |
| R4 | review.md | Unresponsive responders & group membership changes | **Resolved.** Added `decline_invitation()` operation. Presence check for unresponsive agents. Auto-removal from waiting sets. Design.md §4.1, §11. |
| R5 | review.md | Voting deadlocks with `require_consensus=True` | **Resolved.** Added `on_timeout` policy to `propose_vote()`: `"fail"` (default), `"pass"`, `"extend"`. Design.md §3.5, spec.md §3.5. |

### Thea's Findings

| # | Source | Finding | Resolution |
|---|--------|---------|------------|
| F1 | Thea (MODERATE) | Semantic drift detection critically underspecified | **Resolved.** Calibration experiment specified in design.md §7.2. Deferred to v3.1. |
| F2 | Thea (MODERATE) | `wait_for_responses()` timeout behavior ambiguous | **Resolved.** Added `timeout_policy` parameter: `"partial"` (default) vs `"terminal"`. Design.md §4.1, spec.md §4.1. |
| F3 | Thea | No mechanism to decline participation | **Resolved.** Added `decline_invitation()` operation. Design.md §4.1, spec.md §4.1. |
| F4 | Thea | `catch_up()` has no pagination | **Resolved.** Added `limit` and `before_contribution_id` parameters. Design.md §4.4, spec.md §4.4. |
| F5 | Thea | No `summarize()` operation for late joiners | **Resolved.** Added OPTIONAL `summarize()` operation. Design.md §4.4, spec.md §4.4. |
| F6 | Thea | `dissenting_opinions` structure is vague | **Resolved.** Changed to `list[DissentingOpinion]` with `agent_id`, `statement` (in dissenter's own words), `contribution_id`. Spec.md §3.3. |
| F7 | Thea | `auto_interrupt_on_loop` field missing from design.md | **Resolved.** Same as P.3. Added to design.md §3.1. |
| F8 | Thea | `contribute()` VOTE kind restriction missing from design doc | **Resolved.** Added restriction to design.md §6.2. |
| F9 | Thea | Minor: `resolve()` preconditions don't list WAITING | **Resolved.** Updated to include WAITING and VOTING. |
| F10 | Thea | Minor: `interrupt()` doesn't specify who can call it | **Resolved.** Specified: any participant. |
| F11 | Thea | Minor: No example deliberation transcript | **Deferred.** Will add in implementation phase. |
| F12 | Thea | Minor: `DeliberationBounds` defaults not justified | **Resolved.** Noted in design.md §7.1 that defaults are initial estimates; calibration will refine. |

### Theoria's Additional Findings

| # | Source | Finding | Resolution |
|---|--------|---------|------------|
| M1 | Theoria (MODERATE) | `RedisAgentTransport` is misnamed | **Resolved.** Renamed to `WebSocketAgentTransport` in design.md §9.4 and spec.md §9.1. |
| M2 | Theoria | Version numbering discrepancy (v2 vs v5.0) | **Resolved.** All documents now reference "neurogossip-client v5.0" and "neurogossip-server v5.0." The "v3" in neurogossip-v3 refers to the deliberation protocol version. Noted in research.md §3.1. |
| M3 | Theoria | Error codes don't match between spec and server | **Resolved.** Spec.md §7 now acknowledges the mismatch and distinguishes transport-layer errors from deliberation-layer errors. |
| M4 | Theoria | Research survey missing OpenAI Agents SDK | **Resolved.** Added §1.6 to research.md. |

---

## Files Updated

| File | Status | Key Changes |
|------|--------|-------------|
| `design/research.md` | Updated | Transport correction, OpenAI Agents SDK added, open questions resolved |
| `design/design.md` | Updated | Transport correction, 16+ changes across all sections |
| `spec/specification.md` | Updated | Transport correction, DissentingOpinion struct, execution modes, migration section |
| `reviews/REVIEW_RESPONSE.md` | New | This file |

---

## Remaining Work (Not Blocking)

- Example deliberation transcript (F11) — deferred to implementation phase
- Semantic drift calibration experiment — prerequisite for v3.1
- A2A-compatible HTTP gateway — Phase 2
