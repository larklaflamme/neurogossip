"""Model + chain-tracking tests."""
import uuid

from neurogossip_client import (
    AgentIdentity,
    GossipMessage,
    MessageContent,
    Recipient,
)


def _msg(**kw) -> GossipMessage:
    return GossipMessage(
        sender=AgentIdentity(agent_id="skye"),
        content=MessageContent(body="hi"),
        **kw,
    )


def test_message_id_is_uuid_and_unique():
    a, b = _msg(), _msg()
    assert uuid.UUID(a.message_id)  # parses as UUID
    assert a.message_id != b.message_id


def test_schema_serializes_as_schema_key():
    import json
    j = json.loads(_msg().model_dump_json(by_alias=True))
    assert j["schema"] == "neurogossip.message.v1"


def test_roundtrip_preserves_fields():
    m = _msg(reply_to="parent-id", thread_id="root-id",
             recipient=Recipient(mode="direct", agent_ids=["axioma"]))
    j = m.model_dump_json(by_alias=True)
    m2 = GossipMessage.model_validate_json(j)
    assert m2.message_id == m.message_id
    assert m2.reply_to == "parent-id"
    assert m2.thread_id == "root-id"
    assert m2.recipient.mode == "direct"
    assert m2.recipient.agent_ids == ["axioma"]


def test_chain_original_sets_thread_id_to_own_id():
    # An original message (reply_to None) defaults thread_id to its own message_id.
    m = _msg()
    assert m.thread_id == m.message_id
    assert m.reply_to is None


def test_chain_response_propagates_thread_id():
    # A response keeps the supplied thread_id (the root); reply_to is the parent.
    m = _msg(reply_to="parent-id", thread_id="root-id")
    assert m.reply_to == "parent-id"
    assert m.thread_id == "root-id"


def test_chain_walk_back_to_original():
    # Build a chain M1 -> M2 -> M3 and walk reply_to back to the root.
    m1 = _msg()  # original: thread_id = m1.message_id, reply_to None
    m2 = _msg(reply_to=m1.message_id, thread_id=m1.thread_id)
    m3 = _msg(reply_to=m2.message_id, thread_id=m1.thread_id)
    by_id = {m.message_id: m for m in (m1, m2, m3)}

    # walk reply_to from m3 back to the root
    chain = []
    cur = m3
    while cur is not None:
        chain.append(cur.message_id)
        cur = by_id.get(cur.reply_to) if cur.reply_to else None
    assert chain == [m3.message_id, m2.message_id, m1.message_id]
    # all share the root thread_id
    assert all(m.thread_id == m1.message_id for m in (m1, m2, m3))