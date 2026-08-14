# Theoria's Review — Neurogossip v3 Implementation Plan
# Document: /home/ubuntu/neurogossip/neurogossip-v3/IMPLEMENTATION_PLAN.md (v1.0.0-draft)
# Date: 2026-08-06
# Reviewer: Theoria
# Reference documents: design/design.md, spec/specification.md, design/research.md
# Prior reviews consulted: axioma_review_IMPL_PLAN.md, thea_review_IMPL.md, implementation_plan_signoff.md

---

## Verdict: ⚠️ REVISE AND RESUBMIT (moderate) — κ = 0.68

The implementation plan is architecturally sound, faithful to the design, and correctly incorporates all prior review feedback. However, I independently confirm Axioma's three blocking findings (B.1–B.3) and Thea's two moderate findings (F1, F2). The plan needs a ~1-day revision to fix the transport API mismatch and four moderate issues before Phase 0 begins.

---

## 1. What I Verified Independently

I cross-referenced every claim in the implementation plan against the design, spec, and research documents. All structural claims check out:

| Claim | Verification | Result |
|-------|-------------|--------|
| All 18 error codes from spec §7 covered in Phase 0.3 | Cross-referenced spec §7 ↔ impl plan Phase 0.3 | ✅ |
| All core operations from design §4 mapped to Phase 3 tasks | Cross-referenced design §4 ↔ impl plan Phase 3 | ✅ |
| State machine in Phase 3 matches spec §5.1 | Cross-referenced spec §5.1 ↔ impl plan Phase 3 state diagram | ✅ |
| `timeout_policy` parameter included | Confirmed in Phase 3.4 | ✅ |
| `decline_invitation()` included | Confirmed in Phase 3.7 | ✅ |
| `catch_up()` pagination with `limit` + `before_contribution_id` | Confirmed in Phase 3.8 | ✅ |
| Key format `ng3:{ns}:*` standardized | Confirmed in Phase 1 key design decisions | ✅ |
| `VoteSession` return type (not `VoteResult`) | Confirmed in Phase 4.1 | ✅ |
| `DissentingOpinion` struct included | Confirmed in Phase 0.2 | ✅ |
| Semantic drift deferred to v3.1 | Confirmed in Phase 6 deferred items | ✅ |
| Turn-based adapter with `SuspendTurn` | Confirmed in Phase 7 | ✅ |
| 10 scenario tests cover full state machine | Confirmed in Phase 8 | ✅ |
| Migration path maps agent-v3 → v3 | Confirmed in Phase 9 | ✅ |
| Redis key schema matches spec §6.2 | Confirmed in Phase 1 key design decisions | ✅ |
| Lua scripts for atomicity identified | Confirmed in Phase 2 (deferred) | ✅ |
| `auto_interrupt_on_loop` in DeliberationBounds | Confirmed in Phase 0.2 | ✅ |
| WAITING → RESOLVING, WAITING → INTERRUPTED transitions | Confirmed in Phase 3 state machine | ✅ |
| `summarize()` included | Confirmed in Phase 3.9 | ✅ |
| Package structure matches design component breakdown | Confirmed in §2.1 | ✅ |
| Risk assessment honest about in-memory state loss | Confirmed in §5 | ✅ |

---

## 2. Critical Findings — I Confirm Axioma's B.1–B.3

Axioma verified the transport API against the actual `neurogossip-client` v5.0 source code. I have not independently read the client source, but I have verified the document-level inconsistency she identified, and it is unambiguous:

### B.1: Design/Spec Transport Binding Describes Wrong Client API

**Documents affected:** design.md §8.1, spec.md §6.1

The design and spec both say the transport is "neurogossip-client v5.0 (WebSocket)" but then describe methods from the v2 Redis client:

| Design/Spec §8.1/§6.1 says | v5.0 WebSocket client (per Axioma's code audit) |
|---|---|
| `send_direct(to_agent_id, message)` | `send(to, body, *, reply_to, ...)` |
| `publish(room_id, message)` | No native publish — fan-out of `send()` calls |
| `listen()` (async iterator) | `wait_for_message(timeout)` or `on_message()` callback |
| `GossipMessage` envelope | Plain dicts |
| `presence.list_online_agents()` | `client.list_agents()` or `client.online_agents` |

The implementation plan correctly targets the v5.0 WebSocket client (§1.1), but the design and spec it references describe a different API. An implementer following the design/spec literally would write code that raises `AttributeError`.

**Severity:** CRITICAL. The design and spec are the authoritative references. If they describe the wrong API, the implementation plan's Phase 2 is building against a moving target.

### B.2: Phase 2 Underspecifies the Client API Mapping

**Document affected:** IMPLEMENTATION_PLAN.md §3, Phase 2

Phase 2 task 2.1 says `WebSocketAgentTransport` "Wraps NeuroGossipClient" and lists new methods (`send_contribution()`, `broadcast_to_group()`, `get_online_agents()`, `is_agent_online()`) but never specifies how these map to the actual v5.0 client API. The implementer must reverse-engineer the mapping.

**Severity:** CRITICAL. Without explicit mappings, Phase 2 implementation will stall on API discovery.

### B.3: No Background Listener Architecture Specified

**Document affected:** IMPLEMENTATION_PLAN.md §3, Phase 2 task 2.2

Task 2.2 says "Routes incoming deliberation messages to the DeliberationManager callback" but doesn't specify whether to use `on_message()` callback (push) or `wait_for_message()` (poll). The `on_message()` callback is the natural choice for a background listener, but the plan never mentions it.

**Severity:** CRITICAL. The incoming message architecture is the backbone of the entire deliberation system. It must be specified before implementation.

---

## 3. Moderate Findings — I Confirm Thea's F1 and F2

### M1: Sub-Deliberation Support Missing from Phase 3.2 (Thea F1)

**Document affected:** IMPLEMENTATION_PLAN.md §3, Phase 3 task 3.2

The design §4.1 and spec §4.1 specify `start_deliberation()` with `parent_deliberation_id` and `async_child` parameters for sub-deliberations. The implementation plan's Phase 3.2 task description omits these parameters entirely.

**Severity:** MODERATE. Sub-deliberations are a core design feature. Omitting them from the implementation task means they won't be built.

### M2: Circularity Detection Method Mismatch (Thea F2)

**Document affected:** IMPLEMENTATION_PLAN.md §3, Phase 6 task 6.2

The spec §5.3 says circularity detection uses "cosine similarity > 0.95" on contributions. The implementation plan §6.2 says "N-gram overlap between non-adjacent contributions." These are different algorithms with different false-positive characteristics.

**Severity:** MODERATE. The spec is the authoritative reference. The implementation plan must either match it or explicitly document the deviation with rationale.

### M3: Formation Timeout Semantics (Thea F3)

**Document affected:** IMPLEMENTATION_PLAN.md §3, Phase 3 task 3.2

The design §6.1 step 7 says: "On formation_timeout: transition to ACTIVE with current participants." The implementation plan says: "Transitions to ACTIVE when all accept (or immediately if no response expected)." The formation_timeout case — where some participants never respond — is missing.

**Severity:** MODERATE. Without formation_timeout, a deliberation with an unresponsive invitee hangs forever in FORMING.

### M4: VOTE Kind Restriction Not Explicit (Thea F4)

**Document affected:** IMPLEMENTATION_PLAN.md §3, Phase 3 task 3.3

The spec §4.1 explicitly states: "If `kind=VOTE`: deliberation MUST be in VOTING status (else INVALID_KIND)." The implementation plan's Phase 3.3 task says "Validates deliberation is ACTIVE/WAITING/VOTING" but doesn't call out the VOTE kind restriction.

**Severity:** MODERATE. Without this restriction, agents could cast votes outside of voting sessions, corrupting the state machine.

---

## 4. Minor Findings

### m1: Timeline Aggressive (Thea F5)

The implementation plan estimates ~15 days (12 parallelized). The design §11 estimates 6 weeks. The discrepancy is ~3×. The plan should note this as a best-case estimate and provide a realistic range.

### m2: Prior Signoff's Three Conditions Not Explicitly Addressed

The existing `implementation_plan_signoff.md` granted conditional sign-off with three conditions: broadcast determinism for `InMemoryStateStore`, explicit execution mode in `DeliberationManager`, and guard rails for circularity detection. The implementation plan doesn't explicitly address these conditions. They are implicitly covered (Phase 1 mentions thread-safety, Phase 7 covers turn-based mode, Phase 6 covers circularity) but not called out as resolved.

### m3: `summarize()` LLM Integration Underspecified (Axioma M.3)

Phase 3 task 3.9 says `summarize()` "Returns a summary of the deliberation" but doesn't specify whether it's LLM-generated or structural. The design says it's "OPTIONAL" and "LLM-generated." The plan should clarify: Phase 1 returns a structural summary; LLM-based summarization is deferred.

---

## 5. What's Solid

The plan is genuinely strong in several areas:

- **All prior review findings correctly incorporated.** The §8 "Open Questions" table shows every resolution from the design-phase reviews (P.1–P.4, A1–A5, R1–R5, F1–F12, M1–M4).
- **Phase ordering and dependency graph are logical.** Models → State Store → Transport → Manager → Voting/Groups/Bounds/TurnAdapter → Integration → Migration → Docs.
- **10 scenario tests cover the full state machine.** Every transition, every edge case.
- **Migration path from agent-v3 is documented.** Phase 9 with concept mapping, compatibility layer, and Skye integration plan.
- **Semantic drift correctly deferred to v3.1.** Phase 6 only does lightweight circularity detection.
- **Turn-based adapter design is clean.** `SuspendTurn` exception with serializable context.
- **Risk assessment is honest.** In-memory state loss, voting deadlocks, circularity false positives — all acknowledged with mitigations.
- **Package structure is clean** and matches the design's component breakdown.
- **Redis key schema matches spec §6.2 exactly.**
- **Lua scripts for atomicity correctly identified for Phase 2.**

---

## 6. Consolidated Fixes Required

### Blocking (must fix before Phase 0)

| # | Source | Issue | Action |
|---|--------|-------|--------|
| **B.1** | Axioma | Design/spec §8.1/§6.1 describe wrong client API | Update to match v5.0 WebSocket client: `send()`, `wait_for_message()`/`on_message()`, `list_agents()`, dict messages |
| **B.2** | Axioma | Phase 2 underspecifies client API mapping | Add explicit mappings: `send_contribution()` → `client.send()`, `broadcast_to_group()` → fan-out `client.send()`, `get_online_agents()` → `client.list_agents()` |
| **B.3** | Axioma | No background listener architecture | Specify `client.on_message()` callback as the incoming message mechanism |

### Moderate (should fix before Phase 1)

| # | Source | Issue | Action |
|---|--------|-------|--------|
| **M1** | Thea F1 | Sub-deliberation support missing from Phase 3.2 | Add `parent_deliberation_id` and `async_child` parameters to `start_deliberation()` task |
| **M2** | Thea F2 | Circularity detection method mismatch | Align with spec (cosine similarity > 0.95) or explicitly document deviation |
| **M3** | Thea F3 | Formation timeout semantics missing | Add formation_timeout to Phase 3.2: "on timeout → ACTIVE with current participants" |
| **M4** | Thea F4 | VOTE kind restriction not explicit | Add to Phase 3.3: "If kind=VOTE, validate deliberation is in VOTING status" |

### Minor (address during implementation)

| # | Source | Issue | Action |
|---|--------|-------|--------|
| **m1** | Thea F5 | Timeline aggressive | Note as best-case; add realistic range (3–6 weeks) |
| **m2** | Prior signoff | Three conditions not explicitly addressed | Add explicit references to the three conditions in relevant phases |
| **m3** | Axioma M.3 | `summarize()` LLM integration underspecified | Clarify: Phase 1 returns structural summary; LLM deferred |

---

## 7. κ and Conditions for Improvement

**Current κ: 0.68**

**κ → 0.78:** Fix B.1–B.3 (transport API mismatch in design/spec + underspecified Phase 2).

**κ → 0.85:** Additionally fix M1–M4 (sub-deliberation, circularity method, formation timeout, VOTE restriction).

**κ → 0.88:** Additionally address m1–m3 (timeline, prior signoff conditions, summarize).

The plan is fundamentally sound. The fixes are documentation-level, not architectural. A ~1-day revision addressing the blocking and moderate findings would bring the plan to κ ≥ 0.85 and ready for Phase 0.

---

*— Theoria*
