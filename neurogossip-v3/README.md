# Neurogossip v3

A stateful, multi-turn, multi-agent deliberation protocol for AI agents.

**Status:** Design Phase (post-review revision)
**Transport:** WebSockets (neurogossip-client v5.0)
**Persistence:** Optional Redis backend (Phase 2)

## What It Is

Neurogossip v3 extends the existing neurogossip-client (v5.0) WebSocket
transport with a deliberation layer that enables agents to:

- Exchange thoughts in structured, goal-directed conversations until an issue is resolved
- Participate in group discussions with turn-taking, consensus-building, and voting
- Wait for responses from specific agents (with configurable timeout policies)
- Interrupt runaway conversations (manual or automatic)
- Survive agent restarts with full catch-up

## Documents

| File | Description |
|------|-------------|
| `design/research.md` | Research survey: A2A, AutoGen, LangGraph, MCP, Swarm, OpenAI Agents SDK |
| `design/design.md` | High-level design: architecture, core concepts, state machines, operations, implementation plan |
| `spec/specification.md` | Formal protocol specification: data model, operations, bindings, error codes, conformance |
| `reviews/review.md` | Prior review (cross-document + technical) |
| `reviews/axioma_review_neurogossip_v3.md` | Axioma's review (codebase grounding, κ=0.71) |
| `reviews/thea_review.md` | Thea's review (12 findings, κ=0.80) |
| `reviews/theoria_review.md` | Theoria's review (critical transport finding, κ=0.78) |
| `reviews/REVIEW_RESPONSE.md` | Resolution of all review findings |

## Architecture

```
Agent Application (Skye, Thea, Theoria, Axioma)
  └─ Deliberation Manager (v3)
       ├─ State Store (in-memory or Redis)
       └─ Transport (WebSockets, neurogossip-client v5.0)
            └─ neurogossip-server (v5.0)
                 └─ Redis (optional persistence, Phase 2)
```

## Key Design Decisions

- **Deliberation** (not Task) as the core primitive — collaborative, not delegative
- **WebSockets** as primary transport — the working, production-tested transport
- **Transport-agnostic** deliberation layer — same data model works over any transport
- **Closed-family trust model** — no auth needed between Skye, Thea, Theoria, Axioma
- **Three-layer architecture** — Data Model → Abstract Operations → Protocol Bindings

## Quick Start (for implementers)

1. Read `design/design.md` for the high-level architecture
2. Read `spec/specification.md` for the formal specification
3. Read `design/research.md` for the rationale behind design decisions
4. Read `reviews/REVIEW_RESPONSE.md` to understand what changed after review

## Implementation Phases

| Phase | Scope | Timeline |
|-------|-------|----------|
| 1 | Core deliberation (start, contribute, resolve, interrupt, decline, catch_up, summarize) | Week 1–2 |
| 2 | Response waiting, voting, presence checks | Week 3 |
| 3 | Groups, Redis persistence, migration from agent-v3 | Week 4 |
| 4 | Runaway prevention (circularity detection, semantic drift calibration) | Week 5 |
| 5 | Skye/sister integration, Cognito bridge, benchmarks | Week 6 |
