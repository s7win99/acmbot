"""Pure qrating calculation algorithms."""

from __future__ import annotations


DEFAULT_PAIRWISE_ELO_K = 80
DEFAULT_MAX_DELTA = 80
DISPLAY_RULE_VERSION = "v1"


def get_display_offset_v1(count: int) -> int:
    """Return the display offset for the v1 public qrating rule."""
    rated_count = max(0, int(count))
    if rated_count == 0:
        return 200
    if rated_count == 1:
        return 120
    if rated_count == 2:
        return 70
    if rated_count == 3:
        return 40
    if rated_count == 4:
        return 15
    return 0


def calc_display_qrating(internal_qrating: int, rated_contest_count: int) -> int:
    """Calculate the public display qrating from internal qrating and contest count."""
    return max(0, int(internal_qrating) - get_display_offset_v1(rated_contest_count))


def calculate_pairwise_elo_deltas(
    participants: list[dict],
    k: int = DEFAULT_PAIRWISE_ELO_K,
    max_delta: int = DEFAULT_MAX_DELTA,
) -> list[dict]:
    """Calculate internal qrating deltas from contest ranks with Pairwise Elo."""
    participant_count = len(participants)
    if participant_count < 2:
        raise ValueError("Pairwise Elo requires at least 2 participants")

    results: list[dict] = []
    for current in participants:
        actual = 0.0
        expected = 0.0
        current_internal = int(current["old_internal_qrating"])
        current_rank = int(current["rank"])

        for other in participants:
            if other["qq_id"] == current["qq_id"]:
                continue

            other_internal = int(other["old_internal_qrating"])
            other_rank = int(other["rank"])
            expected += 1 / (1 + 10 ** ((other_internal - current_internal) / 400))

            if current_rank < other_rank:
                actual += 1
            elif current_rank == other_rank:
                actual += 0.5

        raw_delta = round(k * (actual - expected) / (participant_count - 1))
        internal_delta = max(-max_delta, min(max_delta, int(raw_delta)))
        new_internal = current_internal + internal_delta

        results.append(
            {
                "qq_id": current["qq_id"],
                "nickname": current["nickname"],
                "rank": current_rank,
                "old_internal_qrating": current_internal,
                "internal_delta": internal_delta,
                "new_internal_qrating": new_internal,
            }
        )

    return sorted(results, key=lambda item: (item["rank"], item["nickname"]))
