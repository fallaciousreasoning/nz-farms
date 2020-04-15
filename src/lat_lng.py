import typing

# Points are Min, Max in LNG LAT
aabb = typing.Tuple[float, float, float, float]

class Bounds:
    def __init__(self):
        super().__init__()

def intersects(first: aabb, second: aabb) -> True | False:
    def maxY(b: aabb): return b[3]
    def minY(b: aabb): return b[1]
    def maxX(b: aabb): return b[2]
    def minX(b: aabb): return b[0]
    
    return not (maxX(first) < minX(second)
        or minX(first) > maxX(second)
        or maxY(first) < minY(second)
        or minY(first) > maxY(second))