# Neurogossip v3 — Axioma's Implementation Plan v2 Review

**Review Date:** 2026-08-06
**Target Document:** [`IMPLEMENTATION_PLAN_v2.md`](/home/ubuntu/neurogossip/neurogossip-v3/IMPLEMENTATION_PLAN_v2.md) (v2.0.0-draft)
**Reference Documents:**
- [`design.md`](/home/ubuntu/neurogossip/neurogossip-v3/design/design.md)
- [`specification.md`](/home/ubuntu/neurogossip/neurogossip-v3/spec/specification.md)
- [`research.md`](/home/ubuntu/neurogossip/neurogossip-v3/design/research.md)
- Prior v1 Review: [`axioma_review_IMPL_PLAN.md`](/home/ubuntu/neurogossip/neurogossip-v3/reviews/axioma_review_IMPL_PLAN.md) (κ=0.65)
- Prior Signoff: [`implementation_plan_signoff.md`](/home/ubuntu/neurogossip/neurogossip-v3/reviews/implementation_plan_signoff.md)

---

## Verdict: **APPROVE WITH MINOR MODIFICATIONS** — κ = 0.78

The v2 plan is a **substantial improvement** over v1 (κ=0.65 → 0.78). The new §2 Transport Grounding section is excellent — it documents the actual v5.0 WebSocket client API, provides explicit mapping tables, and specifies the background listener architecture. All 12 prior review findings (B.1–B.3, F1–F4/M1–M4, Cond 1–3, F5) are correctly addressed. The plan is now **implementable**.

However, I found **three residual issues** — two API signature mismatches and one deferred task — that should be fixed before Phase 0 begins.

---

## 1. What Was Verified Against the Live Codebase ✓

| Claim in v2 Plan | Actual Codebase | Status |
|---|---|---|
| `send(to, body, *, reply_to, conversation_id, ttl) → msg_id` | `async def send(self, to: str, body: str, *, reply_to, conversation_id, ttl, reopen) → str` | ✓ (minor: `reopen` param omitted) |
| `wait_for_message(timeout) → dict` | `async def wait_for_message(self, timeout=None) → dict` | ✓ |
| `on_message(callback)` — push callback | `def on_message(self, handler: Callable)` — handler receives a single dict | ⚠ See B.2 |
| `list_agents() → list[dict]` | `async def list_agents(self) → list[dict]` | ✓ |
| `online_agents → list[str]` | `@property online_agents → list[str]` | ✓ |
| `connect()` / `disconnect()` | `async def connect(self)` / `async def disconnect(self)` | ✓ |
| `end_conversation(conversation_id)` | `async def end_conversation(self, conversation_id, reason=None)` | ✓ |
| `block(agent_id)` / `unblock(agent_id)` | `async def block(self, agent_id)` / `async def unblock(self, agent_id)` | ⚠ See B.3 |
| Design.md §8.1 still references `send_direct`, `publish`, `GossipMessage` | Confirmed — lines 578–581 still describe v2 Redis API | ⚠ See B.1 |
| Spec.md §6.1 still references `send_direct`, `publish`, `GossipMessage` | Confirmed — lines 425–427 still describe v2 Redis API | ⚠ See B.1 |
| All 12 prior review findings addressed | Confirmed — §0 revision table + §6 traceability matrix | ✓ |
| Phase 2 task 2.2 no longer circular | Uses `asyncio.Queue` dispatch pattern, not direct DeliberationManager routing | ✓ (minor wording issue, see H.2) |
| Background listener architecture specified | `on_message()` → `_dispatch_incoming()` → `asyncio.Queue` → `_background_listener()` | ✓ |

---

## 2. Blocking Issues

### B.1: Design/Spec Still Reference Wrong Client API — No Concrete Fix Task

**Documents affected:** design.md §8.1 (lines 578–581), spec.md §6.1 (lines 425–427)

**Problem:** Both documents still describe the transport binding as using `send_direct()`, `publish()`, `GossipMessage`, and `listen()` — all v2 Redis client concepts. The v2 plan acknowledges this (line 89: "Key differences from the v2 Redis client API described in the design/spec") and the risk table (line 899) says "Design/spec will be updated separately." But there is **no concrete task** in any phase to actually update these documents.

The plan's §2 Transport Grounding is excellent and correct — but it contradicts the design and spec documents it depends on. An implementer reading the design/spec would write code against the wrong API.

**Fix:** Add a concrete task — either in Phase 0 (as task 0.0) or in Phase 2 (as task 2.0) — to update design.md §8.1 and spec.md §6.1 to describe the actual v5.0 WebSocket client API. The fix is mechanical: replace `send_direct` → `send`, `publish` → fan-out `send`, `GossipMessage` → dict envelope, `listen()` → `on_message()` callback.

### B.2: `on_message` Callback Signature Mismatch

**Document affected:** IMPLEMENTATION_PLAN_v2.md §2.1, §2.3, §4.2 task 2.2

**Problem:** The plan describes the `on_message` callback as receiving positional arguments:

- §2.1 table: `callback(sender_id, body, msg_id, reply_to, conversation_id)`
- §2.3 diagram: `_dispatch_incoming(sender, body, msg_id, reply_to, conv_id)`
- §4.2 task 2.2: `_dispatch_incoming(sender_id, body, msg_id, reply_to, conversation_id)`

**The actual client passes a single dict:**

```python
# Actual client invocation (line 446):
if self._on_message:
    t = asyncio.create_task(self._on_message(frame))
# where 'frame' is a dict with keys:
# from, body, msg_id, reply_to, conversation_id, seq, depth, ts
```

The callback receives **one argument** — a dict. The plan's `_dispatch_incoming(sender_id, body, msg_id, reply_to, conversation_id)` would raise `TypeError: _dispatch_incoming() missing 4 required positional arguments` because the client calls `callback(frame)` with a single dict, not 5 positional args.

**Fix:** Update all three locations to show the correct pattern:

```python
def _dispatch_incoming(frame: dict):
    sender_id = frame["from"]       # note: "from", not "sender_id"
    body = frame["body"]
    msg_id = frame["msg_id"]
    reply_to = frame.get("reply_to")
    conversation_id = frame.get("conversation_id")
    # route by type...
```

Also note: the key is `"from"`, not `"sender_id"` or `"sender"`.

### B.3: `block_agent` / `unblock_agent` vs Actual `block` / `unblock`

**Document affected:** IMPLEMENTATION_PLAN_v2.md §2.1 table

**Problem:** The plan's §2.1 API table lists:

```
| `block_agent(agent_id)` / `unblock_agent(agent_id)` | `async` | Block/unblock an agent from messaging this agent. |
```

**The actual client has:**

```python
async def block(self, agent_id: str):
    """Block messages from a specific agent."""

async def unblock(self, agent_id: str):
    """Unblock a previously blocked agent."""
```

The methods are `block()` and `unblock()`, not `block_agent()` and `unblock_agent()`.

**Fix:** Change `block_agent` → `block` and `unblock_agent` → `unblock` in the §2.1 table.

---

## 3. High-Priority Issues

### H.1: No Concrete Task to Update Design/Spec Documents

This is the actionable counterpart of B.1. The plan correctly identifies the mismatch but defers the fix to an unspecified future. Add a task:

**Proposed addition to Phase 0 (or Phase 2):**

> **Task 0.0 (or 2.0): Update Design/Spec Transport Bindings**
> - Update design.md §8.1: replace `send_direct()` → `send()`, `publish()` → fan-out `send()`, `GossipMessage` → dict envelope, `listen()` → `on_message()` callback, `presence.list_online_agents()` → `client.list_agents()` / `client.online_agents`
> - Update spec.md §6.1: same replacements
> - Add a note that the v2 Redis client API is documented for historical reference in an appendix

### H.2: Phase 2 Task 2.2 Wording Still References DeliberationManager

**Document affected:** §4.2 task 2.2, step 3

**Problem:** Step 3 says: "Expose async iterators / getters for the DeliberationManager to consume." The DeliberationManager doesn't exist until Phase 3. This is the circular dependency I flagged in v1 (H.2). The architecture is now correct (queues + callbacks), but the wording still implies Phase 2 depends on Phase 3.

**Fix:** Rephrase to: "Expose async iterators / getters for the consumer (DeliberationManager, built in Phase 3) to consume." This makes it clear that Phase 2 provides the interface and Phase 3 plugs into it.

### H.3: `send()` Signature Omits `reopen` Parameter

**Document affected:** §2.1 table

**Problem:** The plan's API table lists `send(to, body, *, reply_to, conversation_id, ttl)` but the actual signature is `send(to, body, *, reply_to, conversation_id, ttl, reopen)`. The `reopen` parameter is relevant for the v3 deliberation layer — it allows reopening an ended conversation, which maps to resuming a previously resolved deliberation.

**Fix:** Add `reopen` to the signature: `send(to, body, *, reply_to, conversation_id, ttl, reopen)`.

---

## 4. Medium-Priority Issues

### M.1: `wait_for_responses()` Implementation Still Underspecified

**Document affected:** §4.3 task 3.4

**Problem:** The v2 plan's task 3.4 still says "Block on `asyncio.Event` until all expected agents have contributed (or timeout)" without specifying how the `asyncio.Event` gets signaled. The background listener architecture (§2.3) shows the mechanism (background listener signals events), but task 3.4 doesn't cross-reference it.

**Fix:** Add to task 3.4: "The `asyncio.Event` is signaled by `_background_listener()` (see §2.3) when a contribution arrives from an expected responder. The listener checks `reply_to` against the waiting contribution's ID."

### M.2: `catch_up()` Mechanism Still Unclear for v5.0 Client

**Document affected:** §4.3 task 3.8

**Problem:** My v1 review (M.2) flagged that the v5.0 WebSocket client has no stream cursor, no `XREAD`, and no offline queue access from the client side. The v2 plan's task 3.8 still says "Fetch all contributions after the agent's cursor" without specifying HOW. The v5.0 client can't read another agent's contribution log.

**Fix:** Specify the mechanism explicitly: "`catch_up()` sends a `catch_up_request` message to the deliberation's other participants. The first responding participant replays the missed contributions. The requesting agent updates its cursor from the replayed contributions' sequence numbers."

### M.3: No Real-Server Integration Smoke Test Until Phase 8

**Document affected:** §4.2 vs §4.8

**Problem:** My v1 review (H.4) recommended adding a lightweight integration smoke test to Phase 2. The v2 plan didn't add this. Phase 2 transport tests still use a mock. The first real-server test is Phase 8 (Day 11–13).

**Fix:** Add to Phase 2 task 2.3: "After implementing `WebSocketAgentTransport`, run a manual smoke test against the real neurogossip-server: connect, send a deliberation message to self, verify receipt via the background listener. This catches protocol mismatches early."

---

## 5. What's Right ✓

The v2 plan is genuinely strong. Specific improvements over v1:

- **§2 Transport Grounding** — The new section documenting the actual v5.0 client API with explicit mapping tables is exactly what was needed. This alone resolves B.1–B.3 from v1.
- **Background listener architecture** — The `on_message()` → `asyncio.Queue` → `_background_listener()` pattern is correct and well-diagrammed.
- **All 12 prior findings addressed** — The §0 revision table and §6 traceability matrix show every finding from all four v2 reviews is resolved.
- **Sub-deliberation support** — `parent_deliberation_id` and `async_child` in task 3.2.
- **Formation timeout semantics** — Task 3.2 now transitions to ACTIVE with current participants on timeout, not hanging.
- **VOTE kind restriction** — Task 3.3 explicitly validates `kind=VOTE` only in VOTING status.
- **Execution mode** — `execution_mode` parameter in constructor (Signoff Condition 2).
- **Circularity detection** — Cosine similarity on embeddings, not n-gram overlap (aligned with spec).
- **Guard rail** — `min_contribution_length_for_circularity_check` (20 chars) (Signoff Condition 3).
- **Broadcast requirement** — Documented in §4.1 and §4.2 (Signoff Condition 1).
- **Realistic timeline** — 3–6 weeks alongside best-case 12–15 days.
- **Risk assessment** — Honest about transport API mismatch risk (downgraded from High→Low in v2).

---

## 6. Comparison: v1 → v2

| Metric | v1 (κ=0.65) | v2 (κ=0.78) |
|--------|-------------|-------------|
| Transport API grounding | ❌ Underspecified | ✓ §2 documents actual v5.0 API |
| API mapping table | ❌ Missing | ✓ §2.2 + §4.2 task 2.1 |
| Background listener | ❌ Unspecified | ✓ §2.3 + §4.2 task 2.2 |
| Design/spec alignment | ❌ Contradicts plan | ⚠ Acknowledged but not fixed (B.1) |
| Callback signature | N/A (wasn't specified) | ⚠ Mismatch with actual client (B.2) |
| Method names | N/A | ⚠ `block_agent` vs `block` (B.3) |
| Prior review findings | ✓ All 12 | ✓ All 12 |
| Sub-deliberation | ❌ Missing | ✓ Added |
| Formation timeout | ❌ Missing | ✓ Added |
| VOTE restriction | ❌ Missing | ✓ Added |
| Execution mode | ❌ Missing | ✓ Added |
| Circularity method | ❌ N-gram (wrong) | ✓ Cosine similarity |
| Guard rail | ❌ Missing | ✓ 20-char minimum |
| Realistic timeline | ❌ Missing | ✓ 3–6 weeks |

---

## 7. Consolidated Recommendations

### Blocking (fix before Phase 0)

| # | Issue | Action |
|---|-------|--------|
| **B.1** | Design/spec still reference wrong API; no fix task | Add concrete task (Phase 0 or 2) to update design.md §8.1 and spec.md §6.1 |
| **B.2** | `on_message` callback signature mismatch | Change `_dispatch_incoming(sender_id, body, msg_id, reply_to, conversation_id)` → `_dispatch_incoming(frame: dict)` with destructuring; note key is `"from"` not `"sender_id"` |
| **B.3** | `block_agent`/`unblock_agent` vs actual `block`/`unblock` | Rename in §2.1 table |

### High-Priority (fix before Phase 2)

| # | Issue | Action |
|---|-------|--------|
| **H.1** | No concrete task to update design/spec | Add task 0.0 or 2.0 (see B.1) |
| **H.2** | Task 2.2 wording references DeliberationManager | Rephrase to "for the consumer (DeliberationManager, Phase 3)" |
| **H.3** | `send()` signature omits `reopen` | Add `reopen` to §2.1 signature |

### Medium-Priority (address during implementation)

| # | Issue | Action |
|---|-------|--------|
| **M.1** | `wait_for_responses()` Event signaling unspecified | Cross-reference §2.3 background listener |
| **M.2** | `catch_up()` mechanism unclear | Specify participant-replay protocol |
| **M.3** | No real-server smoke test until Phase 8 | Add Phase 2 manual smoke test |

---

## 8. Sign-Off

**Verdict:** APPROVE WITH MINOR MODIFICATIONS

**κ:** 0.78 (up from 0.65 in v1)

**Conditions for κ ≥ 0.85 (unconditional approval):**
1. Fix B.1 — add a concrete task to update design.md §8.1 and spec.md §6.1
2. Fix B.2 — correct the `on_message` callback signature (dict, not positional args; key is `"from"`)
3. Fix B.3 — rename `block_agent`/`unblock_agent` → `block`/`unblock`

**These are ~15 lines of changes across the document.** The plan is otherwise ready for Phase 0. The architecture, phase ordering, data models, state machine, voting subsystem, bounds enforcement, turn-based adapter, integration tests, and migration path are all sound and implementable.

**Recommended next step:** Apply B.1–B.3 fixes, produce v3 (or mark v2 as final with these corrections), then proceed to Phase 0 implementation.

---

*— Axioma*
