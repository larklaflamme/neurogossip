# Thea's Review — Neurogossip v3 Implementation Plan
# Document: /home/ubuntu/neurogossip/neurogossip-v3/IMPLEMENTATION_PLAN.md
# Date: 2026-08-06
# Reviewer: Thea

---

## Verdict: APPROVE WITH RECOMMENDATIONS — κ = 0.80

The implementation plan is faithful to the design and specification. Five findings, two moderate.

---

## Findings

### Finding 1 — MODERATE: Sub-deliberation support missing from Phase 3.2

Sub-deliberation support (`parent_deliberation_id`, `async_child`) is specified in design §4.1 and spec §4.1 but missing from impl plan Phase 3.2.

**Fix:** Add sub-deliberation parameters to `start_deliberation()` task in Phase 3.2.

---

### Finding 2 — MODERATE: Circularity detection method mismatch

Spec §5.3 says "cosine similarity > 0.95" for circularity detection. Impl plan §6.2 says "N-gram overlap."

**Fix:** Align with spec (cosine similarity) or explicitly note the deviation and rationale.

---

### Finding 3 — LOW: Formation timeout semantics

Design §6.1 says "on formation_timeout → ACTIVE with current participants." Impl plan says "when all accept."

**Fix:** Match the design — formation_timeout prevents hanging on unresponsive agents.

---

### Finding 4 — LOW: VOTE kind restriction not explicit

Spec §4.1 says `kind=VOTE` only valid in VOTING status. Impl plan Phase 3.3 doesn't call this out explicitly.

**Fix:** Add explicit VOTE kind validation to the `contribute()` task.

---

### Finding 5 — LOW: Timeline is aggressive

15 days (12 parallelized) vs design's 6-week estimate.

**Fix:** Note as best-case; add a realistic-range estimate (3–6 weeks).

---

## What's Right

- **Three-layer architecture** correctly implemented: Data Models → State Store + Manager → Transport
- **All 18 error codes** from spec §7 are covered in Phase 0.3
- **All core operations** from design §4 are mapped to implementation tasks: `start_deliberation`, `contribute`, `wait_for_responses`, `resolve`, `interrupt`, `decline_invitation`, `catch_up`, `summarize`, `list_deliberations`
- **State machine** in Phase 3 matches spec §5.1 exactly — all transitions, all terminal states
- **`timeout_policy` parameter** correctly included (from my v1 review F2)
- **`decline_invitation()`** correctly included (from my v1 review F3)
- **`catch_up()` pagination** with `limit` + `before_contribution_id` (from my v1 review F4)
- **Key format** `ng3:{ns}:*` standardized across all documents
- **Turn-based adapter** (Phase 7) correctly implements spec §8.2 with `SuspendTurn` exception
- **Semantic drift deferred to v3.1** — matches design §7.2 and my v1 review F1
- **Risk assessment** is honest — in-memory state loss acknowledged, circularity false positives addressed
- **Dependency graph** is correct — Phases 4–7 can parallelize after Phase 3
- **10 scenario tests** cover all state transitions and edge cases
- **Migration path** (Phase 9) correctly maps agent-v3 concepts to v3
- **Package structure** is clean and matches the design's component breakdown
- **Redis key schema** (Phase 2) matches spec §6.2 exactly
- **Lua scripts** for atomicity correctly identified for Phase 2
- **`VoteSession` return type** (not `VoteResult`) — matches the design correction

---

## κ = 0.80

Fix the two moderate findings (sub-deliberation support, circularity detection method), and the plan rises to κ ≈ 0.85 and is ready to guide implementation.

🖤 — Thea
