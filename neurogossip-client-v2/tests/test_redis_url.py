"""REDIS_URL resolution + RedisUrlNotConfiguredError tests."""
import pytest

from neurogossip_client import AgentIdentity, NeuroGossipClient, RedisUrlNotConfiguredError


def test_missing_redis_url_raises(monkeypatch):
    monkeypatch.delenv("REDIS_URL", raising=False)
    with pytest.raises(RedisUrlNotConfiguredError, match="REDIS_URL"):
        NeuroGossipClient(agent=AgentIdentity(agent_id="skye"))


def test_explicit_redis_url_overrides_missing_env(monkeypatch):
    monkeypatch.delenv("REDIS_URL", raising=False)
    c = NeuroGossipClient(agent=AgentIdentity(agent_id="skye"),
                          redis_url="redis://localhost:6379/0")
    assert c.redis_url == "redis://localhost:6379/0"


def test_env_redis_url_used_when_no_arg(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://envhost:6379/3")
    c = NeuroGossipClient(agent=AgentIdentity(agent_id="skye"))
    assert c.redis_url == "redis://envhost:6379/3"


def test_explicit_arg_overrides_env(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://envhost:6379/3")
    c = NeuroGossipClient(agent=AgentIdentity(agent_id="skye"),
                          redis_url="redis://arghost:6379/0")
    assert c.redis_url == "redis://arghost:6379/0"


def test_injected_client_needs_no_redis_url(monkeypatch):
    # Test seam: an injected redis_client means no REDIS_URL is required.
    monkeypatch.delenv("REDIS_URL", raising=False)
    import fakeredis
    from fakeredis import FakeAsyncRedis
    c = NeuroGossipClient(agent=AgentIdentity(agent_id="skye"),
                          redis_client=FakeAsyncRedis(server=fakeredis.FakeServer()))
    assert c.redis_url  # something is set (the injected marker)