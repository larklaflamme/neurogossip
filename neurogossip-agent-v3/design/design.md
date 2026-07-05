# Neurogossip v3 - High-Level Design

This document describes the design of **neurogossip-agent-v3**, a conversational session management protocol and client library for AI agents. 

Neurogossip v3 enables agents to maintain multi-turn, multi-agent conversations, trace sub-request delegation trees, log full conversation histories, and cleanly integrate human-in-the-loop approvals.

---

## 1. System Architecture

The system uses **Redis** as a shared, durable state store to manage the state of conversations, requests, responses, and chronological logs. Messaging between agents is transport-agnostic; the default implementation uses `neurogossip-client-v2` (Redis Streams + Pub/Sub).

```
                      ┌──────────────────────────────────────────────┐
                      │                 Redis Store                  │
                      │                                              │
                      │  ┌────────────────────────────────────────┐  │
                      │  │  Session State                         │  │
                      │  │  conversation_id → metadata, status    │  │
                      │  └────────────────────────────────────────┘  │
                      │  ┌────────────────────────────────────────┐  │
                      │  │  Request Trees                         │  │
                      │  │  request_id → status, parent, payload  │  │
                      │  └────────────────────────────────────────┘  │
                      │  ┌────────────────────────────────────────┐  │
                      │  │  Persistent History Logs               │  │
                      │  │  conversation_id → [chronological events]│  │
                      │  └────────────────────────────────────────┘  │
                      └──────────────────────┬───────────────────────┘
                                             │
                                   State Query & Updates
                                             │
                      ┌──────────────────────▼───────────────────────┐
                      │        neurogossip-agent-v3 Client           │
                      │            (Session Manager)                 │
                      └──────────────────────┬───────────────────────┘
                                             │
                                   Transport Interface
                                             │
                      ┌──────────────────────▼───────────────────────┐
                      │              Transport Layer                 │
                      │     (Default: neurogossip-client-v2)         │
                      └──────────────────────────────────────────────┘
```

---

## 2. State Machines

### 2.1 Conversation Session Status

A conversation session transitions through states depending on the activity of the agents and whether human interaction is required.

```mermaid
stateDiagram-v2
    [*] --> Active : create_conversation()
    Active --> WaitingForHuman : create_request(recipient="human")
    WaitingForHuman --> Active : send_response(sender="human")
    Active --> Completed : send_response(root_request)
    Active --> Failed : fail_request(root_request)
    Completed --> [*]
    Failed --> [*]
```

### 2.2 Agent Request Status

Requests can target one agent, multiple agents (fan-out), or a human. A fan-out request transitions from `Pending` to `Completed` only after **all** target recipients have responded.

```mermaid
stateDiagram-v2
    [*] --> Pending : create_request()
    Pending --> Completed : send_response() [All recipients responded]
    Pending --> Failed : fail_request() / timeout / error
    Completed --> [*]
    Failed --> [*]
```

---

## 3. Sequence Diagram

The following sequence diagram demonstrates a multi-agent delegation flow where **Agent A** (the coordinator) receives a human query, delegates tasks to **Agent B** and **Agent C** via a fan-out request, and **Agent B** requires human input mid-execution.

```mermaid
sequenceDiagram
    autonumber
    actor Human
    participant AgentA as Agent A (Coordinator)
    participant DB as Redis (Shared State)
    participant AgentB as Agent B
    participant AgentC as Agent C

    %% 1. Human Request Start
    Human->>AgentA: Ask Question
    Note over AgentA: Starts Conversation Session
    AgentA->>DB: create_conversation()
    AgentA->>DB: create_request(parent=None, recipient=AgentA) [Root]
    AgentA->>DB: log_history("Human initiated conversation")

    %% 2. Fan-out delegation
    Note over AgentA: Plans sub-tasks for B and C
    AgentA->>DB: create_request(parent=Root, recipients=[B, C]) [Fan-out]
    AgentA->>DB: log_history("Delegating to B and C")
    DB-->>AgentB: Deliver message (Sub-request to B)
    DB-->>AgentC: Deliver message (Sub-request to C)

    %% 3. Agent B needs human input
    Note over AgentB: Starts processing. Needs clarification.
    AgentB->>DB: create_request(parent=B_Req, recipient="human")
    DB->>DB: Update Session Status -> WAITING_FOR_HUMAN
    AgentB->>DB: log_history("B requested human input")
    DB-->>Human: Notify (Human input needed)
    
    Human->>DB: send_response(human_input)
    DB->>DB: Update Session Status -> ACTIVE
    AgentB->>DB: log_history("Human provided input to B")
    DB-->>AgentB: Deliver message (Human response)
    
    %% 4. Agent B responds to Agent A
    Note over AgentB: Processes response and completes task
    AgentB->>DB: send_response(B_response)
    AgentB->>DB: log_history("B completed sub-request")
    
    %% 5. Agent C responds to Agent A
    Note over AgentC: Processes task
    AgentC->>DB: send_response(C_response)
    AgentC->>DB: log_history("C completed sub-request")

    %% 6. Agent A Finalizes
    Note over DB: Both B and C responded. Fan-out Request Completed.
    DB-->>AgentA: Deliver responses
    Note over AgentA: All sub-requests completed. Compiles final findings.
    AgentA->>DB: send_response(final_findings) [Completes Root]
    DB->>DB: Update Session Status -> COMPLETED
    AgentA->>DB: log_history("Coordinator sent final response")
    AgentA->>Human: Present Final Findings
```
