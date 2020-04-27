import shapefile
import typing

input_file = "data/urban-areas/urban_areas"#"data/land-cover/land-cover-database-v4-0-class-orders"

sf = shapefile.Reader(input_file)

def num_covers() -> int:
    return len(sf)

def iterate_covers() -> typing.Iterator[shapefile.ShapeRecord]:
    for i in range(len(sf)):
        yield sf.shapeRecord(i)
