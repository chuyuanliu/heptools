from itertools import chain, repeat


def balance_split(total: int, target: int) -> list[int]:
    if total <= target:
        return [total]
    groups = total // target
    diffs = []
    for i in (0, 1):
        diffs.append(
            (
                n := groups + i,
                m := total // n,
                d := total % n,
                d * abs(m + 1 - target) + (n - d) * abs(m - target),
            )
        )
    n, m, d, _ = min(diffs, key=lambda diff: diff[3])
    return [*chain(repeat(m + 1, d), repeat(m, n - d))]
