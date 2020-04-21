import shapefile
import typing

input_file = "data/titles/nz-property-titles-including-owners-1"
output_file = "output/titles"

sf = shapefile.Reader(input_file)


def get_shape(for_record: int):
    return sf.shape(for_record)


def get_record(for_record: int):
    return sf.record(for_record)


def iterate_titles() -> typing.Iterator[shapefile.ShapeRecord]:
    for i in range(len(sf)):
        yield sf.shapeRecord(i)


def num_titles() -> int:
    return sf.numRecords


def get_title_with_group_writer(filename: str):
    w = shapefile.Writer(filename)

    for field in sf.fields:
        w.field(*field)

    w.field('farm_id', 'N')

    return w

def write_title(writer: shapefile.Writer, shape_record: shapefile.ShapeRecord, group: int):
    record = shape_record.record
    shape = shape_record.shape

    record_tuple = tuple(record)

    writer.record(*record, group)
    writer.shape(shape)
