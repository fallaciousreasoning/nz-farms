import shapefile
import typing

input_file = 'data/titles/nz-property-titles-including-owners-1'
output_file = 'output/titles'

sf = shapefile.Reader(input_file)

def get_shape(for_record: int):
    return sf.shape(for_record)

def get_record(for_record: int):
    return sf.record(for_record)

def iterate_titles() -> typing.Iterator[shapefile.ShapeRecord]:
    for i in range(len(sf)):
        yield sf.shapeRecord(i)