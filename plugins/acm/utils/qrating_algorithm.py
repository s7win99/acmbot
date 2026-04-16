"""Pure qrating calculation algorithms."""

from __future__ import annotations


DEFAULT_PAIRWISE_ELO_K = 80
DEFAULT_MAX_DELTA = 80


def calculate_pairwise_elo_deltas(
    participants: list[dict],
    k: int = DEFAULT_PAIRWISE_ELO_K,
    max_delta: int = DEFAULT_MAX_DELTA,
) -> list[dict]:
    """Calculate qrating deltas from contest ranks with Pairwise Elo."""
    participant_count = len(participants)
    if participant_count < 2:
        raise ValueError("Pairwise Elo requires at least 2 participants")

    results: list[dict] = []
    for current in participants:
        actual = 0.0
        expected = 0.0
        current_qrating = int(current["old_qrating"])
        current_rank = int(current["rank"])

        for other in participants:
            if other["qq_id"] == current["qq_id"]:
                continue

            other_qrating = int(other["old_qrating"])
            other_rank = int(other["rank"])
            expected += 1 / (1 + 10 ** ((other_qrating - current_qrating) / 400))

            if current_rank < other_rank:
                actual += 1
            elif current_rank == other_rank:
                actual += 0.5

        raw_delta = round(k * (actual - expected) / (participant_count - 1))
        limited_delta = max(-max_delta, min(max_delta, int(raw_delta)))
        new_qrating = max(0, current_qrating + limited_delta)
        actual_delta = new_qrating - current_qrating

        results.append(
            {
                "qq_id": current["qq_id"],
                "nickname": current["nickname"],
                "rank": current_rank,
                "old_qrating": current_qrating,
                "delta": actual_delta,
                "new_qrating": new_qrating,
            }
        )

    return sorted(results, key=lambda item: (item["rank"], item["nickname"]))
