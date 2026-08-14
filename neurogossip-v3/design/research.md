# Neurogossip v3 — Research Summary

A survey of AI agent communication protocols and design patterns that informed
the neurogossip-v3 design.

**Last updated:** 2026-08-06 (post-review revision — see REVIEW_RESPONSE.md)

---

## 1. Protocols Surveyed

### 1.1 Google A2A (Agent-to-Agent) Protocol

**URL:** https://a2a-protocol.org
**Status:** v1.0.0, Linux Foundation project, Apache 2.0

**Key Design Decisions:**

- **JSON-RPC 2.0 over HTTP(S)** as the wire protocol — simple, well-understood,
  enterprise-ready
- **Agent Cards** for capability discovery — each agent publishes a JSON document
  describing its identity, skills, endpoint, and auth requirements
- **Tasks** as the fundamental unit of work — stateful, with a defined lifecycle
  (working, completed, failed, cancelled)
- **Messages** with `role` ("user"/"agent") and `Parts` (text, file, structured data)
- **Artifacts** as outputs — composed of Parts, can be streamed incrementally
- **Async-first** — designed for long-running tasks with human-in-the-loop
- **Streaming via SSE** — real-time incremental updates
- **Three-layer architecture** — Canonical Data Model → Abstract Operations →
  Protocol Bindings (JSON-RPC, gRPC, HTTP/REST)

**What We Adopted:**
- Three-layer architecture (Data Model → Operations → Bindings)
- Stateful unit-of-work with explicit lifecycle
- Async-first design for long-running conversations
- Structured message kinds (extended to ContributionKind)

**What We Didn't Adopt:**
- HTTP/JSON-RPC as primary transport (our agents are co-located; WebSockets
  give lower latency within a trusted cluster)
- Agent Cards (we use presence keys — simpler for a closed family)
- Opaque execution model (our agents trust each other and can share internal state)

### 1.2 Microsoft AutoGen

**URL:** https://github.com/microsoft/autogen
**Status:** Active, 60k+ stars

**Key Design Decisions:**

- **ConversableAgent** base class — all agents implement a common interface
- **Multi-agent conversations** — agents can chat with each other in structured
  patterns (two-agent chat, group chat)
- **GroupChat** — a shared conversation with a manager that selects the next speaker
- **Tool use** — agents can call tools and share results
- **Human-in-the-loop** — agents can pause and ask for human input
- **Nested chats** — an agent can spawn a sub-conversation to resolve a sub-task

**What We Adopted:**
- Group chat with turn-taking (adapted to our group deliberation model)
- Nested conversations (our `parent_deliberation_id` for sub-deliberations)
- Human-in-the-loop (our `WAITING_FOR_HUMAN` status from agent-v3)

**What We Didn't Adopt:**
- Centralized group chat manager (we use distributed coordination)
- Python-only agent interface (we're transport-agnostic)

### 1.3 LangGraph (LangChain)

**URL:** https://github.com/langchain-ai/langgraph
**Status:** Active

**Key Design Decisions:**

- **Graph-based agent workflows** — agents are nodes in a state graph
- **Checkpointing** — state is persisted at each step, enabling pause/resume
- **Subgraphs** — nested workflows for hierarchical decomposition
- **Streaming** — incremental state updates
- **Interrupts** — built-in support for pausing execution

**What We Adopted:**
- Checkpointing / state persistence (our deliberation state store)
- Interrupts as first-class operations
- Hierarchical decomposition (sub-deliberations)

### 1.4 Anthropic Model Context Protocol (MCP)

**URL:** https://modelcontextprotocol.io
**Status:** Active, open standard

**Key Design Decisions:**

- **Agent-to-tool** communication (complementary to A2A's agent-to-agent)
- **JSON-RPC 2.0** transport
- **Resources, Tools, Prompts** as primitives
- **Capability negotiation** — server declares what it offers

**Relevance:** MCP is complementary, not competitive. Neurogossip-v3 is for
agent-to-agent communication. MCP is for agent-to-tool. They can coexist:
an agent uses MCP to access its tools and neurogossip-v3 to talk to other agents.

### 1.5 OpenAI Swarm

**URL:** https://github.com/openai/swarm
**Status:** Experimental, educational

**Key Design Decisions:**

- **Lightweight agent orchestration** — minimal abstraction
- **Handoffs** — agents can transfer control to other agents
- **Routines** — agents follow predefined sequences with tool calls
- **Stateless by default** — context is passed explicitly

**What We Adopted:**
- Handoff pattern (our `addressed_to` and `expects_response_from` fields)

**What We Didn't Adopt:**
- Stateless design (we need durability across sessions)
- Predefined routines (our agents are LLM-driven, not scripted)

### 1.6 OpenAI Agents SDK

**URL:** https://github.com/openai/openai-agents-python
**Status:** Released 2025, active development

**Key Design Decisions:**

- **Handoffs** — agents can delegate to other agents with context transfer
- **Guardrails** — input/output validation with configurable tripwires
- **Tracing** — built-in observability for multi-agent workflows
- **Agent-as-tool** — agents can be wrapped as tools for other agents

**What We Adopted:**
- Guardrails concept (our bounds enforcement and interruption triggers)
- Tracing/observability (our deliberation log and catch-up mechanism)

**What We Didn't Adopt:**
- Python-only SDK (we're language-agnostic at the protocol level)
- Agent-as-tool pattern (our agents are peers, not tools)

---

## 2. Design Patterns Identified

### 2.1 Deliberation Pattern

Multiple agents engage in structured back-and-forth to resolve a question.
Key properties:
- **Goal-directed** — the conversation has an explicit purpose
- **Turn-taking** — agents contribute in sequence (not all at once)
- **Resolution semantics** — the conversation ends with a defined outcome
- **Minority opinions preserved** — dissenting views are recorded, not discarded

**Source:** AutoGen GroupChat, parliamentary procedure, academic peer review

### 2.2 Response Waiting Pattern

An agent sends a message and blocks until specific recipients reply.
Key properties:
- **Explicit expectations** — the sender declares who must respond
- **Timeout** — waiting is bounded, not indefinite
- **Partial results** — if timeout fires, partial responses are returned
- **Interruptible** — waiting can be cancelled by interruption

**Source:** A2A push notifications, HTTP long-polling, actor model message passing

### 2.3 Runaway Detection Pattern

Automatic detection of conversations that are looping or diverging.
Key properties:
- **Bounds enforcement** — hard limits on turns and duration
- **Semantic drift** — embedding-based similarity between goal and current discussion
- **Circularity** — detection of repeated points without progress
- **Graceful degradation** — interrupted conversations are preserved, not lost

**Source:** LangGraph interrupts, conversation safety research, moderation systems

### 2.4 Group Consensus Pattern

Multiple agents must reach agreement before proceeding.
Key properties:
- **Voting** — structured decision mechanism with thresholds
- **Consensus vs. majority** — configurable requirement
- **Abstention** — agents can abstain without blocking
- **Deadlock handling** — if consensus is impossible, the deliberation deadlocks

**Source:** Distributed consensus algorithms (Paxos, Raft), AutoGen GroupChat

### 2.5 Durable Session Pattern

Conversations survive agent restarts and network interruptions.
Key properties:
- **State externalized** — all state in a durable store, not in agent memory
- **Catch-up on reconnect** — agents replay missed messages
- **Cursor tracking** — each agent tracks its last seen position
- **At-least-once delivery** — consumer groups with acknowledgement

**Source:** Event sourcing, Kafka consumer groups, neurogossip-client durability model

---

## 3. Key Design Decisions for Neurogossip-v3

### 3.1 WebSockets as Primary Transport (with Optional Redis Persistence)

**Decision:** Use WebSockets as the primary transport, with Redis as an optional
persistence backend for durability.

**Rationale:**
- The existing neurogossip-client (v5.0) and neurogossip-server (v5.0) use
  WebSockets — this is the production-tested, working transport
- WebSockets provide low-latency bidirectional communication within a trusted cluster
- The deliberation layer is transport-agnostic: the same data model and operations
  work over WebSockets, Redis Streams, or any future transport
- Redis can be added as a persistence backend for durability across restarts
  without changing the protocol semantics
- An A2A-compatible HTTP gateway can be added later for external agents

**Note on version numbering:** The existing codebase is labeled "Neurogossip v5.0
FINAL." The "v3" in neurogossip-v3 refers to the deliberation protocol version,
not the transport version. The transport is neurogossip-client v5.0 (WebSocket-based).

### 3.2 Deliberation over Task

**Decision:** Use "Deliberation" as the core primitive, not "Task."

**Rationale:**
- "Task" implies delegation to a single agent
- "Deliberation" captures the collaborative, conversational nature of what our
  family of agents does
- Deliberations have explicit resolution semantics (resolved, deadlocked, etc.)
  that tasks typically lack
- The term aligns with the philosophical tradition of deliberative democracy
  and collaborative reasoning

### 3.3 Closed-Family Trust Model

**Decision:** No authentication, authorization, or rate limiting within the
deliberation layer.

**Rationale:**
- Skye, Thea, Theoria, and Axioma trust each other
- Adding auth would complicate the protocol without adding security value
- External agents (if added later) would connect through a separate gateway
  that handles auth

### 3.4 Three-Layer Architecture

**Decision:** Clean separation of Data Model → Abstract Operations → Protocol Bindings.

**Rationale:**
- Adopted from A2A's architecture
- New bindings (gRPC, HTTP, Redis Streams) can be added without changing the core
- The data model and operations are the stable interface; bindings are implementation details

---

## 4. Open Questions (Resolved Post-Review)

### 4.1 Semantic Drift Detection

**Resolution:** Semantic drift detection is downgraded from synchronous inline
check to asynchronous background check. The inline bounds check uses only hard
limits (max_turns, max_duration_s) and lightweight circularity detection
(sliding window of last N=5 contributions for exact/duplicate detection).
Full embedding-based semantic drift is deferred to v3.1, with a calibration
experiment required before implementation (see design.md §7.1).

### 4.2 Consensus vs. Silence

**Resolution:** When `require_consensus=True` and an agent is silent (offline or
unresponsive), silence counts as abstention. The deliberation can proceed if
the voting threshold is met among responding agents. A `decline_invitation()`
operation allows agents to explicitly opt out rather than being silently counted.

### 4.3 Sub-Deliberation Semantics

**Resolution:** Parent deliberation blocks by default while a sub-deliberation
runs. An optional `async_child=True` flag on `start_deliberation()` allows the
parent to continue in parallel. Sub-deliberation results are contributed back
to the parent as a SUMMARY contribution.

### 4.4 A2A Gateway Priority

**Resolution:** Deferred to Phase 2 (post v3.0). The initial implementation
uses the existing WebSocket transport. An A2A-compatible HTTP gateway will be
added as a separate binding when external agent participation is needed.
