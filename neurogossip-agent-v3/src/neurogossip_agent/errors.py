"""Errors raised by the neurogossip-agent package."""
from __future__ import annotations


class NeuroGossipAgentError(Exception):
    """Base error class for neurogossip-agent exceptions."""


class SessionNotFoundError(NeuroGossipAgentError):
    """Raised when a requested conversation session does not exist."""


class RequestNotFoundError(NeuroGossipAgentError):
    """Raised when a requested agent request does not exist."""


class TimeoutError(NeuroGossipAgentError):
    """Raised when waiting for a response times out."""
