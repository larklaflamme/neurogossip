"""Exception types for neurogossip-client."""


class NeuroGossipError(Exception):
    """Base class for all neurogossip-client errors."""


class PayloadTooLargeError(NeuroGossipError):
    """Raised when a serialized message exceeds the max payload size."""


class ConnectionError(NeuroGossipError):  # noqa: A001 - intentional shadow for namespacing
    """Raised on Redis connection failures."""


class MessageDecodeError(NeuroGossipError):
    """Raised when an incoming message cannot be parsed into a GossipMessage."""


class NotConnectedError(NeuroGossipError):
    """Raised when an operation is attempted before the client is connected."""


class RedisUrlNotConfiguredError(NeuroGossipError):
    """Raised at construction when no Redis URL is available — neither an explicit
    ``redis_url=`` argument nor a ``REDIS_URL`` environment variable was provided."""