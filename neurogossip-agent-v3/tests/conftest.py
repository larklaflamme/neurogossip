import os
import pytest
import redis.asyncio as aioredis

from neurogossip_agent.session_manager import AgentConversationManager
from neurogossip_agent.transport import MockAgentTransport


@pytest.fixture
async def redis_client() -> aioredis.Redis:
    # Manual parser for workspace .env file
    env = {}
    env_path = "/home/ubuntu/neurogossip/.env"
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()

    redis_url = (
        os.environ.get("NEUROGOSSIP_V3_REDIS_URL")
        or os.environ.get("REDIS_URL")
        or env.get("NEUROGOSSIP_V3_REDIS_URL")
        or env.get("REDIS_URL")
        or "redis://localhost:6379/0"
    )

    client = aioredis.from_url(redis_url, decode_responses=True)
    # Flush database to avoid test pollution
    await client.flushdb()
    yield client
    # Clean up
    await client.flushdb()
    await client.aclose()


@pytest.fixture
def mock_transport() -> MockAgentTransport:
    return MockAgentTransport()


@pytest.fixture
def session_manager(redis_client, mock_transport) -> AgentConversationManager:
    return AgentConversationManager(redis_client, mock_transport)
