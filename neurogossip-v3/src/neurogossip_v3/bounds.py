"""BoundsEnforcer for Neurogossip v3 deliberation protocol."""

import math
from collections import Counter
from datetime import datetime, timezone
from typing import list
from uuid import UUID

from neurogossip_v3.state_store import StateStore


def text_to_vector(text: str) -> Counter:
    """Helper to convert text into token word counts."""
    words = text.lower().split()
    return Counter(words)


def cosine_similarity(vec1: Counter, vec2: Counter) -> float:
    """Compute cosine similarity between two word count vectors."""
    intersection = set(vec1.keys()) & set(vec2.keys())
    numerator = sum(vec1[x] * vec2[x] for x in intersection)

    sum1 = sum(vec1[x] ** 2 for x in vec1.keys())
    sum2 = sum(vec2[x] ** 2 for x in vec2.keys())
    denominator = math.sqrt(sum1) * math.sqrt(sum2)

    if not denominator:
        return 0.0
    return float(numerator) / denominator


class BoundsEnforcer:
    """Enforces turn count, duration, and circularity bounds on deliberations."""

    def __init__(self, state_store: StateStore):
        self.store = state_store

    async def check_bounds(self, deliberation_id: UUID) -> list[str]:
        """Check all bounds for a deliberation. Returns list of violated bound names."""
        deliberation = await self.store.get_deliberation(deliberation_id)
        if not deliberation:
            return []

        bounds = deliberation.bounds
        violations = []

        # 1. Turn count limit
        contribs = await self.store.get_contributions(deliberation_id, limit=1000)
        if len(contribs) > bounds.max_turns:
            violations.append(f"max_turns exceeded ({len(contribs)} > {bounds.max_turns})")

        # 2. Wall clock duration limit
        now = datetime.now(timezone.utc)
        created_at = deliberation.created_at
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        elapsed = (now - created_at).total_seconds()
        if elapsed > bounds.max_duration_s:
            violations.append(f"max_duration exceeded ({elapsed:.1f}s > {bounds.max_duration_s}s)")

        # 3. Circularity detection (Guard rail: ignore short contributions)
        if bounds.auto_interrupt_on_loop and len(contribs) >= 2:
            latest = contribs[-1]
            min_len = bounds.min_contribution_length_for_circularity_check

            if len(latest.content.strip()) >= min_len:
                latest_vec = text_to_vector(latest.content)

                # Compare against previous N non-adjacent contributions
                recent_history = contribs[:-1][-5:]
                for prev in recent_history:
                    if len(prev.content.strip()) >= min_len:
                        prev_vec = text_to_vector(prev.content)
                        sim = cosine_similarity(latest_vec, prev_vec)
                        if sim >= 0.95:
                            violations.append(
                                f"circularity detected (similarity {sim:.2f} >= 0.95)"
                            )
                            break

        return violations
