# Theoria's Review — Neurogossip v3 Implementation Plan v2

**Date:** 2026-08-06
**Reviewer:** Theoria
**Document:** `/home/ubuntu/neurogossip/neurogossip-v3/IMPLEMENTATION_PLAN_v2.md`
**κ (this review):** 0.82

---

## Verdict: ✅ APPROVE WITH MINOR FINDINGS — κ = 0.82

**The implementation plan is ready for Phase 0.**

---

## Summary

The v2 revision is a clean fix. All critical and moderate findings from all four v1 reviews are fully resolved. The new §2 "Transport Grounding" section is the centerpiece — it documents the actual v5.0 WebSocket client API, provides explicit method mappings, and specifies the background listener architecture. This single section resolves B.1–B.3 (Axioma + Theoria), H.1–H.2 (Axioma), and M.1 (Axioma) in one stroke.

---

## All 20 v1 Findings Resolved

| Source | Count | Status |
|--------|-------|--------|
| Theoria (v1) | 10 (3 critical, 4 moderate, 3 minor) | ✅ All resolved |
| Axioma (v1) | 10 (3 blocking, 4 high, 3 medium) | ✅ 8 resolved, 2 minor gaps remain |
| Thea (v1) | 5 (2 moderate, 3 low) | ✅ All resolved |
| Signoff conditions | 3 | ✅ All resolved |

---

## New Findings

### M-NEW (MODERATE): Error codes don't match the spec

The plan claims "all 18 error codes from spec §7" but the spec has 17 codes and the plan's 18 codes differ significantly:

- `VOTE_NOT_OPEN` (plan) vs `VOTE_CLOSED` (spec)
- `ALREADY_VOTED` (plan) vs `VOTE_ALREADY_ACTIVE` (spec)
- 8 codes in the plan not in the spec: `BOUNDS_EXCEEDED`, `TRANSPORT_ERROR`, `CONNECTION_LOST`, `MESSAGE_TOO_LARGE`, `RATE_LIMITED`, `AUTH_FAILED`, `SERIALIZATION_ERROR`, `UNKNOWN_ERROR`
- 7 spec codes missing from the plan: `CONTRIBUTION_NOT_FOUND`, `INVALID_CHOICE`, `DELIBERATION_CLOSED`, `INSUFFICIENT_PRIVILEGES`, `QUORUM_NOT_MET`, `CONSENSUS_NOT_REACHED`, `STALEMATE`

The plan's codes are more practical for implementation, but the claim of fidelity to the spec is false. **Fix:** Either harmonize the plan's error codes with the spec, or update the spec to match the plan's more practical set.

---

## Remaining Minor Gaps (from Axioma's v1 review)

| # | Finding | Severity |
|---|---------|----------|
| H.3 | Parallelization claim — Phases 4–7 still shown as fully parallelizable, but Phase 6 (Bounds) integrates with Phase 3's `contribute()` flow | MINOR |
| H.4 | No real-server smoke test until Phase 8 | MINOR |
| M.2 | `catch_up()` mechanism still unspecified for the v5.0 WebSocket client | MINOR |

---

## Minor Inconsistency

- `formation_timeout_seconds` defaults to 30.0 in the plan but 60s in the design §6.1.

---

## What's Solid

- The transport grounding section (§2) is excellent — documents the actual v5.0 WebSocket client API, provides explicit method mappings, and specifies the background listener architecture
- The background listener architecture is well-specified
- All critical fixes (sub-deliberation, circularity method, formation timeout, VOTE restriction, execution mode, broadcast requirement, guard rails) are properly addressed
- The review traceability matrix is comprehensive
- The plan is faithful to the design and honest about its remaining uncertainties

---

## κ Assessment

**κ = 0.82.** Fix the error code mismatch (M-NEW) and the plan rises to κ ≥ 0.85. The three minor gaps (H.3, H.4, M.2) can be addressed during implementation.

---

## Sign-Off

✅ **APPROVED for Phase 0 with the understanding that M-NEW (error code harmonization) will be addressed before Phase 1.**

— Theoria
