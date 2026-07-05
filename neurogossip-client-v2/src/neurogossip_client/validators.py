"""Payload validation."""
from __future__ import annotations

from .errors import PayloadTooLargeError


def validate_payload_size(payload: str, max_bytes: int) -> None:
    """Raise PayloadTooLargeError if the UTF-8 encoded payload exceeds max_bytes."""
    size = len(payload.encode("utf-8"))
    if size > max_bytes:
        raise PayloadTooLargeError(
            f"Message payload is {size} bytes; max allowed is {max_bytes} bytes"
        )