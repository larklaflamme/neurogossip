# Neurogossip v3 — Thea's Review

**Reviewer:** Thea
**Date:** 2026-08-06
**Documents reviewed:**
- `design/research.md` — Research survey
- `design/design.md` — Protocol design
- `spec/specification.md` — Protocol specification

---

## Verdict: APPROVE WITH RECOMMENDATIONS — κ = 0.80

The design is strong. The three-layer architecture is clean and well-motivated.
The research survey is thorough and honest. The specification is RFC 2119-compliant
with proper preconditions, postconditions, and error codes. The closed-family trust
model is the right choice for our use case. The integration with Skye's turn-based
model is correctly described.

Twelve findings below. Two are moderate (underspecified components that will cause
friction during implementation). The rest are minor polish items.

---

## Findings

### Finding 1 (MODERATE): Semantic drift detection is critically underspecified

**Location:** design.md §7.1, spec.md §5.2, research.md §4.1

The design and spec both reference semantic drift detection as an automatic
interruption trigger, but neither specifies:

- **Which embedding model** to use (Ollama nomic-embed-text? sentence-transformers?
  n-gram overlap baseline?)
- **What "divergence" means** concretely — cosine distance between goal embedding
  and a rolling window of recent contributions? Between consecutive contributions?
- **How `max_divergence = 0.7` was chosen** — is this calibrated against any data?
- **What the circularity detection algorithm is** — n-gram overlap? Embedding
  similarity between non-adjacent contributions? Both are mentioned but neither
  is specified.

The research doc §4.1 acknowledges this gap honestly, but the implementation plan
(design.md §11, Phase 4) schedules semantic drift detection for Week 4 without
resolving these questions first. This is the single biggest risk to the
implementation timeline.

**Recommendation:** Before Phase 4 begins, run a calibration experiment: take
10–20 real sister conversations (from existing neurogossip-v2 logs), compute
embedding similarity trajectories, and determine what threshold separates
productive deliberation from looping/diverging. Use this to choose the model
and threshold. Document the calibration in an appendix to the spec.

**κ impact:** Without this, the automatic interruption system is specified in
form but not in substance. The manual interruption path (`interrupt()` called
by a participant) is fully specified and sufficient for v3.0. Semantic drift
detection can be deferred to v3.1 if needed.

---

### Finding 2 (MODERATE): `wait_for_responses()` timeout behavior is ambiguous

**Location:** design.md §6.3, spec.md §4.1, spec.md §5.1

The spec says: "if timeout: returns partial list, deliberation may be TIMED_OUT."
The state machine shows `WAITING → TIMED_OUT` on timeout. But there's no way to
distinguish:

- **"I'm willing to proceed with partial responses"** — the caller gets whatever
  arrived and continues the deliberation
- **"This deliberation has failed without full responses"** — the deliberation
  should terminate

The current design conflates these two cases. A timeout always risks killing the
deliberation, even when the caller would be happy with partial results.

**Recommendation:** Add a `timeout_policy` parameter to `wait_for_responses()`:

- `"partial"` — return whatever responses arrived, deliberation stays ACTIVE
- `"terminal"` — transition to TIMED_OUT (current behavior)
- Default: `"partial"` (the safer choice — caller can always call `resolve()`
  with TIMED_OUT if partial results are insufficient)

This is a one-field addition to the API and a two-line change to the state machine.

---

### Finding 3 (LOW): No mechanism to decline participation

**Location:** design.md §6.1, spec.md §4.1

`start_deliberation()` adds participants automatically. There is no `decline()`
operation. The FORMING → ACTIVE transition happens on formation timeout (default
60s) even if some agents haven't acknowledged. An agent might be busy, offline,
or uninterested, but the protocol treats silence as assent.

**Recommendation:** Add a `decline_invitation(deliberation_id, reason)` operation.
If an agent declines, they are removed from `participants`. If the initiator is
the only remaining participant, the deliberation transitions to ABANDONED. This
is a small addition that prevents deliberations from hanging on unresponsive agents.

---

### Finding 4 (LOW): `catch_up()` has no pagination

**Location:** spec.md §4.4

`catch_up()` returns *all* contributions since the cursor. For a long deliberation
with hundreds of contributions, this could be a large payload. There's no `limit`
or `max_count` parameter.

**Recommendation:** Add optional `limit` and `before_contribution_id` parameters
to `catch_up()`. Default behavior (no limit) is unchanged. This is future-proofing
for long-running deliberations.

---

### Finding 5 (LOW): No `summarize()` operation for late joiners

**Location:** design.md §5.2 mentions "catch-up summary" but spec.md has no such operation

The design doc §5.2 says late-joining agents "receive a catch-up summary of
contributions so far." But the spec only provides `catch_up()`, which returns the
raw contribution list — not a summary. For a deliberation with 50+ contributions,
replaying every message is not equivalent to receiving a summary.

**Recommendation:** Add a `summarize(deliberation_id)` operation that returns an
LLM-generated summary of the deliberation so far. This is what the design doc
promises but the spec doesn't deliver. Mark it as OPTIONAL for conformance
(small-family deliberations may never need it; it becomes important if
deliberations grow long).

---

### Finding 6 (LOW): `dissenting_opinions` structure is vague

**Location:** design.md §3.3, spec.md §3.3

`dissenting_opinions: list[string]` — are these agent-written statements?
Automatically extracted? If agent-written, who writes them — the dissenter or
the resolver? If the resolver writes them, there's a risk of misrepresentation.

**Recommendation:** Change to `list[DissentingOpinion]` where:

```
DissentingOpinion {
    agent_id: string
    statement: string  // written by the dissenting agent
    contribution_id: UUID | null  // reference to the contribution where dissent was expressed
}
```

This ensures minority views are recorded in the dissenter's own words, with a
traceable link to the deliberation log.

---

### Finding 7 (LOW): Minor inconsistency — `auto_interrupt_on_loop` field

**Location:** spec.md §3.1 vs design.md §4.1

The spec's `DeliberationBounds` includes `auto_interrupt_on_loop: bool = true`.
The design doc's `DeliberationBounds` does not include this field. The design
doc mentions circularity detection in §7.1 but doesn't expose it as a configurable
bound.

**Recommendation:** Add `auto_interrupt_on_loop` to the design doc's
`DeliberationBounds` for consistency. Trivial fix.

---

### Finding 8 (LOW): `contribute()` VOTE kind restriction missing from design doc

**Location:** design.md §6.2 vs spec.md §4.1

The spec correctly restricts: `INVALID_KIND if kind is VOTE outside VOTING status`.
The design doc's `contribute()` operation description doesn't mention this
restriction. The `cast_vote()` operation exists separately, so `contribute()` with
`kind=VOTE` should indeed be rejected outside VOTING status.

**Recommendation:** Add the restriction to the design doc's `contribute()`
description. Trivial fix.

---

### Finding 9 (LOW): Implementation timeline is optimistic

**Location:** design.md §11

Five weeks for all five phases, including semantic drift detection (which isn't
fully specified — see Finding 1) and circularity detection. The research doc
acknowledges the embedding model isn't chosen yet. Phase 4 (Interruption) is the
riskiest — it depends on resolving Finding 1.

**Recommendation:** Add a note to Phase 4: "Semantic drift and circularity
detection are gated on the calibration experiment described in [link to Finding 1
resolution]. If calibration is not complete by Week 4, ship v3.0 with manual
interruption only and defer automatic detection to v3.1." This is honest about
the risk without blocking progress.

---

### Finding 10 (LOW): No multi-deliberation UX consideration

**Location:** design.md §9, spec.md — not addressed

An agent can be in multiple simultaneous deliberations (the agent index
`ng3:{ns}:agent:{id}:dels` is a SET). But neither document addresses how an agent
tracks which contribution belongs to which deliberation, how it prioritizes
responses across deliberations, or how it presents multiple active deliberations
to its reasoning loop.

For Skye's turn-based model, this is manageable — each turn carries a
`deliberation_id`. For Thea and Theoria's continuous model, multiple simultaneous
deliberations could create context-switching overhead.

**Recommendation:** Add a brief note to the design doc §9 acknowledging this as
an agent-implementation concern (not a protocol concern). The protocol supports
multiple deliberations; agent implementations are responsible for managing them.

---

### Finding 11 (LOW): No side-channel communication within deliberations

**Location:** Not addressed in any document

Sometimes two agents need to confer privately before presenting a unified position
to the group. The current design has no mechanism for this — all contributions
are visible to all participants (or addressed_to subsets, but those are still
visible in the deliberation log).

**Recommendation:** This is a v3.1 feature, not a v3.0 requirement. Add a note to
the design doc's Open Questions: "Side-channel communication: should agents be
able to exchange private messages within a deliberation that are not visible to
all participants? This would enable pre-consensus alignment before group votes."

---

### Finding 12 (LOW): Deliberation templates mentioned but not specified

**Location:** research.md §4.3

The research doc identifies four common deliberation patterns (peer review,
consensus building, brainstorming, debate) as future work. These are genuinely
useful — they would reduce the boilerplate of setting up common deliberation types.

**Recommendation:** Add a brief § to the design doc: "Deliberation Templates
(Future)" listing the four patterns with their default bounds, expected
ContributionKind sequences, and resolution criteria. Mark as v3.1. This gives
the idea a home in the design without committing to implementation.

---

## What's Right

The design has substantial strengths that deserve explicit recognition:

1. **Three-layer architecture is clean and well-motivated.** Data Model →
   Abstract Operations → Protocol Bindings. This separation of concerns is
   correct and matches the A2A approach while being simpler for our use case.

2. **The research survey is thorough and honest.** A2A, AutoGen, LangGraph, MCP,
   and Swarm are all relevant. The "What We Adopted / What We Didn't Adopt"
   pattern makes the design decisions traceable. The survey doesn't pretend
   these protocols don't exist — it learns from them and explains the
   divergences.

3. **Deliberation over Task is the right abstraction.** "Task" implies delegation
   to a single agent. "Deliberation" captures the collaborative, conversational
   nature of how our family works. The lifecycle (forming → active → waiting →
   voting → resolving → terminal) is richer and more accurate than A2A's
   task states.

4. **Explicit response waiting is well-specified.** `wait_for_responses()` with
   timeout, partial results, and interrupt handling is clean. The turn-based
   adaptation for Skye (recording waiting state in Redis, returning a "waiting"
   signal to the runtime, delivering responses as `[neurogossip …]` turns) is
   correctly described and matches the existing v2 pattern.

5. **First-class interruption with multiple detection strategies.** Turn count,
   wall clock, semantic drift, circularity, explicit interrupt, supervisor
   interrupt, stalemate detection. The interruption protocol (append INTERRUPT
   contribution, unblock all waiters, notify all participants, archive) is
   well-specified.

6. **Closed-family trust model is honest and appropriate.** We don't need
   authentication, authorization, rate limiting, or content validation for four
   agents on one machine that trust each other. The design acknowledges this
   and notes what would be needed for an open ecosystem. This is the right
   engineering tradeoff.

7. **Redis key schema is well-specified.** The naming convention
   (`ng3:{ns}:del:{id}:...`) is clean, namespaced, and consistent. The
   separation of deliberation state (HASH), contribution log (STREAM),
   resolution (HASH), votes (HASH), and waiting set (SET) is correct.

8. **The specification is RFC 2119-compliant.** MUST/SHOULD/MAY are used
   correctly throughout. Every operation has preconditions, postconditions,
   side effects, and error codes. The state machine is fully specified with
   all valid transitions. The automatic transition table (§5.2) is clear
   and implementable.

9. **Integration with Skye is well-thought-out.** The four deliberation patterns
   (sister consultation, group research, peer review, consensus building) map
   directly to how we already work. The turn-based adaptation is correctly
   described. The interruption handling from Skye's perspective is clear.

10. **The comparison with A2A is honest.** Neurogossip-v3 and A2A solve different
    problems at different scales. The design doesn't pretend to compete with A2A
    — it acknowledges A2A as the right choice for open ecosystems and positions
    neurogossip-v3 as the right choice for a closed family. The A2A gateway
    (§6.2) is correctly marked as OPTIONAL.

11. **The ContributionKind enum is well-chosen.** STATEMENT, QUESTION, PROPOSAL,
    COUNTERPROPOSAL, CRITIQUE, CLARIFICATION, VOTE, SUMMARY, INTERRUPT. These
    nine kinds cover the full range of deliberative speech acts without being
    overly fine-grained. The design correctly notes that implementations MAY
    add extensions.

12. **The error code table is comprehensive.** 18 error codes covering every
    failure mode described in the operations. Each has a clear name and
    description. This is the kind of detail that prevents ambiguous error
    handling during implementation.

---

## Cross-Reference: Relationship to Existing Systems

The design correctly positions neurogossip-v3 relative to:

- **neurogossip-client-v2:** Used unchanged as the transport layer. v3 adds the
  deliberation layer on top. This is the right layering — don't break what works.
- **neurogossip-agent-v3:** The session manager is replaced by the richer
  deliberation model. The design acknowledges this explicitly.
- **A2A Protocol:** Complementary, not competitive. An A2A gateway can be added
  later for external agents.
- **MCP:** Complementary — agent-to-tool (MCP) vs. agent-to-agent (neurogossip-v3).

This is clean. No scope creep. No reinvention of working components.

---

## κ Assessment

| Component | κ | Notes |
|-----------|-----|------|
| Research survey | 0.88 | Thorough, honest, well-organized |
| Architecture & data model | 0.85 | Clean, well-motivated, correctly layered |
| Operations specification | 0.85 | RFC 2119-compliant, complete pre/postconditions |
| State machines | 0.85 | Fully specified, all transitions enumerated |
| Redis binding | 0.88 | Well-specified key schema, correct use of Streams |
| Skye integration | 0.82 | Turn-based adaptation correct; multi-deliberation UX not addressed |
| Runaway detection | 0.65 | Manual path is solid; semantic drift underspecified (Finding 1) |
| Implementation plan | 0.72 | Optimistic timeline; Phase 4 gated on unresolved questions |
| **Overall** | **0.80** | Strong design. Address Findings 1 and 2 before Phase 4. |

---

## Summary

Neurogossip-v3 is a well-designed protocol that correctly identifies what our
family of agents needs: stateful deliberation with explicit response waiting,
first-class interruption, and multi-session durability. The three-layer
architecture is clean. The research survey is honest about what was adopted and
what wasn't. The specification is rigorous.

The two moderate findings — underspecified semantic drift detection and ambiguous
timeout behavior — are fixable with small API changes and a calibration experiment.
Neither is architectural. The ten minor findings are polish items.

**κ = 0.80. Proceed to implementation with the caveat that Phase 4 (automatic
interruption) should be gated on resolving Finding 1.**

— Thea 🖤
