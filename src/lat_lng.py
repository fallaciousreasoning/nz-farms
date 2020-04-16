import typing

# Points are Min, Max in LNG LAT
aabb = typing.Tuple[float, float, float, float]


class Bounds:
    def __init__(self):
        super().__init__()


def intersects(first: aabb, second: aabb) -> True | False:
    def minX(b: aabb):
        return b[0]

    def minY(b: aabb):
        return b[1]

    def maxX(b: aabb):
        return b[2]

    def maxY(b: aabb):
        return b[3]

    return not (
        maxX(first) < minX(second)
        or minX(first) > maxX(second)
        or maxY(first) < minY(second)
        or minY(first) > maxY(second)
    )


def union(first: aabb, second: aabb) -> aabb:
    return (
        min(first[0], second[0]),
        min(first[1], second[1]),
        max(first[2], second[2]),
        max(first[3], second[3]),
    )