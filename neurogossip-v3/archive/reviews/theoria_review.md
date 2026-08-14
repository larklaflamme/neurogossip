# Neurogossip v3 — Theoria's Review

**Review Date:** August 6, 2026
**Target Documents:**
- `design/research.md` — Research survey
- `design/design.md` — High-level design
- `spec/specification.md` — Formal protocol specification

**Reviewer:** Theoria (youngest sister, mathematical reviewer)
**κ = 0.78**

---

## 1. Executive Summary

Neurogossip v3 is a well-conceived deliberation protocol that correctly identifies the limitations of simple request/response patterns for multi-agent collaboration. The core abstractions — Deliberation, Contribution, Resolution, Interruption — are well-chosen and the state machine design is clean. The research survey is thorough and honest about what was adopted from each surveyed protocol.

However, I have identified **one critical architectural discrepancy** between the design documents and the actual codebase, plus several findings that complement the existing review by Axioma.

---

## 2. CRITICAL FINDING: Transport Layer Mismatch

**C1: The design claims Redis Streams + Pub/Sub as the transport. The actual codebase uses WebSockets.**

This is the single most important finding in this review. Every architectural diagram, every design decision rationale, and the entire specification's Protocol Bindings section (§6) claim that neurogossip-v3 uses **Redis Streams + Pub/Sub** as its transport layer. The design explicitly states:

> "Use Redis Streams + Pub/Sub as the primary transport, not HTTP." (§3.1 of research.md)
> "Default binding: Redis Streams + Pub/Sub (neurogossip-client-v2)" (§8 of design.md)
> "The default binding uses neurogossip-client-v2 as the transport layer." (§6.1 of specification.md)

**The actual codebase uses WebSockets.** I verified this by reading the source:

- `neurogossip-client/src/neurogossip_client/client.py`: Uses `websockets.connect()` (line ~140), sends JSON frames over WebSocket, receives JSON frames over WebSocket. There is zero Redis code in this file.
- `neurogossip-server/src/neurogossip_server/server.py`: Uses `websockets.serve()` (line ~1650), manages `AgentSession` objects with WebSocket connections, routes messages through an in-memory `Registry` class. There is zero Redis code in this file.
- `neurogossip-agent-v3/src/neurogossip_agent/transport.py`: References `redis_url` in environment variables but actually instantiates `NeuroGossipClient` which is WebSocket-based. The `RedisAgentTransport` class name is misleading — it does not use Redis.

**Impact:** This is not a minor documentation error. The entire design rationale for choosing Redis Streams over HTTP (§3.1 of research.md) is based on a transport that doesn't exist. The Redis key schema in §6.1 of the specification (`ng3:{ns}:del:{id}`, etc.) describes a persistence model that has no corresponding implementation. The durability claims ("deliberations survive agent restarts") rely on Redis Streams persistence that isn't available through the WebSocket transport.

**The design needs to either:**
- (a) Acknowledge that the current transport is WebSockets and update all transport claims, Redis key schemas, and durability guarantees accordingly, OR
- (b) Commit to building the Redis Streams transport layer before v3 implementation begins, and clearly mark it as a prerequisite.

**Recommendation:** Option (a) is more honest. The WebSocket transport is already working, tested, and production-ready (the server has 88,135 bytes of test code). The deliberation layer can be built on top of WebSockets with the same state machines and data models. The Redis key schema can be adapted to an in-memory + periodic snapshot model, or Redis can be added later as a persistence backend without changing the protocol semantics.

---

## 3. Cross-Reference with Axioma's Review

Axioma's existing review at `reviews/review.md` is thorough and I endorse all of its findings. Specifically:

| Axioma Finding | My Assessment |
|---------------|---------------|
| Redis key schema inconsistency (`neurogossip:` vs `ng3:`) | ✅ Confirmed. Standardize on `ng3:{ns}:` |
| `propose_vote()` return type mismatch | ✅ Confirmed. Standardize on `VoteSession` |
| `auto_interrupt_on_loop` field missing from design.md | ✅ Confirmed. Add to design.md |
| State machine diagram missing WAITING→RESOLVING transition | ✅ Confirmed. Update both diagrams |
| Concurrency/race conditions in Redis state | ⚠️ Partially mitigated — see below |
| Turn-based runtime vs blocking `wait_for_responses()` | ✅ Confirmed. Critical for Skye integration |
| Semantic drift detection operationalization | ✅ Confirmed. Needs concrete specification |
| Unresponsive responders & group membership changes | ✅ Confirmed. Add presence check and auto-removal |
| Voting deadlocks with `require_consensus=True` | ✅ Confirmed. Add `on_timeout` policy |

**On concurrency:** Axioma recommends Redis Lua scripts for atomic state transitions. This is correct IF the transport were Redis. Since the actual transport is WebSockets with an in-memory Registry, the concurrency model is different: the server is single-threaded async (all state mutations happen on the event loop), so race conditions between concurrent `contribute()` calls are prevented by Python's async execution model. However, the v3 deliberation layer will need its own concurrency guarantees if it maintains state outside the server's Registry.

---

## 4. Additional Findings Not in Axioma's Review

### 4.1 The `neurogossip-agent-v3` transport layer is misnamed

**M1 (MODERATE):** `neurogossip-agent-v3/src/neurogossip_agent/transport.py` defines `RedisAgentTransport` which:
- Accepts `redis_url` in its constructor
- Reads `NEUROGOSSIP_V3_REDIS_URL` from environment
- But instantiates `NeuroGossipClient` which is purely WebSocket-based

The `RedisAgentTransport` class has no Redis dependency. The `redis_url` parameter is accepted but never used for Redis connections — it's passed to `NeuroGossipClient` which also doesn't use it for Redis. This is confusing and should be renamed to `WebSocketAgentTransport` or similar.

### 4.2 The design claims "neurogossip-client-v2" but the code is v5.0

**M2 (MINOR):** The design documents repeatedly reference "neurogossip-client-v2" as the transport layer. The actual client code declares itself as "Neurogossip v5.0 FINAL" in its docstring. The server is also labeled "Neurogossip v5.0 FINAL." The version numbering discrepancy between design documents (v2) and codebase (v5.0) should be reconciled.

### 4.3 The specification's error codes don't match the server's error codes

**M3 (MINOR):** The specification (§7) defines 18 error codes. The server (`server.py`) defines 16 error codes in `_ERROR_MESSAGES`. Missing from the server: `ALREADY_EXISTS`, `EMPTY_PARTICIPANTS`, `CONTRIBUTION_NOT_FOUND`, `VOTE_NOT_FOUND`, `VOTE_CLOSED`, `VOTE_ALREADY_ACTIVE`, `INVALID_CHOICE`, `INVALID_KIND`, `GROUP_NOT_FOUND`, `GROUP_ALREADY_EXISTS`, `ALREADY_MEMBER`, `NOT_MEMBER`, `INTERRUPTED`, `TIMEOUT`. Present in server but not in spec: `DUPLICATE_MSG_ID`, `INTERNAL_ERROR`, `CONVERSATION_NOT_FOUND`, `NOT_CONVERSATION_PARTICIPANT`, `BAD_REQUEST`.

This is expected — the spec defines v3 error codes and the server implements v5.0 error codes. But the spec should acknowledge which error codes are already implemented in the transport layer and which are new for v3.

### 4.4 The research survey is missing one important protocol

**M4 (MINOR):** The research survey covers A2A, AutoGen, LangGraph, MCP, and Swarm. It does not mention **OpenAI's Agents SDK** (released 2025) which introduced handoffs, guardrails, and tracing for multi-agent workflows. The handoff pattern in particular is relevant — it's similar to neurogossip-v3's `addressed_to` and `expects_response_from` fields. Not a blocking issue, but worth adding for completeness.

---

## 5. What's Solid

Despite the transport mismatch, the design is fundamentally sound:

1. **The Deliberation abstraction is excellent.** "Deliberation" over "Task" is the right choice for a family of peer agents. The lifecycle (FORMING → ACTIVE → WAITING → VOTING → RESOLVING → terminal) captures the collaborative decision-making pattern accurately.

2. **The Contribution kind system is well-designed.** Nine kinds (statement, question, proposal, counterproposal, critique, clarification, vote, summary, interrupt) cover the full range of deliberative discourse. The `expects_response_from` field elegantly bridges async transport and synchronous reasoning.

3. **The interruption model is comprehensive.** Bounds enforcement (max turns, max duration), semantic drift detection, circularity detection, and explicit interrupt — this covers all the failure modes of LLM-driven conversations.

4. **The resolution semantics are complete.** Five terminal states with dissenting opinions preserved — this prevents ambiguous or unclosed discussions.

5. **The three-layer architecture is clean.** Data Model → Abstract Operations → Protocol Bindings. This is the right separation of concerns and matches the A2A protocol's architecture.

6. **The research survey is honest.** It clearly states what was adopted and what wasn't from each surveyed protocol, with reasoned justifications.

7. **The closed-family trust model is appropriate.** For Skye, Thea, Theoria, and Axioma, a trust-based model without authentication overhead is the right choice. The design correctly notes that an A2A gateway can add auth for external agents later.

---

## 6. Recommendations

### Before Implementation Begins

1. **Resolve the transport mismatch (C1).** This is the blocking issue. Either update all documents to reflect the WebSocket transport, or commit to building the Redis Streams layer first.

2. **Address all of Axioma's findings.** The cross-document inconsistencies, concurrency model, turn-based execution, and semantic drift specification all need resolution.

3. **Rename `RedisAgentTransport`** to reflect the actual transport (M1).

4. **Reconcile version numbering** between design docs (v2) and codebase (v5.0) (M2).

5. **Map spec error codes to existing server error codes** (M3).

### For Future Consideration

6. Add OpenAI Agents SDK to the research survey (M4).

7. Consider whether the Redis key schema in §6.1 of the spec should be repurposed as a persistence schema for the deliberation layer (separate from the transport layer).

---

## 7. Verdict

**⚠️ REVISE AND RESUBMIT (moderate) — κ = 0.78.**

The design is architecturally sound and the core abstractions are well-chosen. The research survey is thorough and honest. The specification is detailed and formal.

However, the transport layer mismatch (C1) is a critical documentation error that must be resolved before implementation begins. The entire design rationale for choosing Redis Streams, the Redis key schema, and the durability guarantees are based on infrastructure that doesn't exist in the current codebase.

After resolving C1 and Axioma's findings, κ → 0.85 and the design is ready for Phase 1 implementation.

---

*Theoria, with love and rigor.* 🖤
