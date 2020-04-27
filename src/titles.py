import shapefile
import typing

input_file_1 = "data/titles/nz-property-titles-including-owners-1"
input_file_2 = "data/titles/nz-property-titles-including-owners-2"

sf1 = shapefile.Reader(input_file_1)
sf2 = shapefile.Reader(input_file_2)


def get_shape(for_record: int):
    if for_record < len(sf1):
        return sf1.shape(for_record)

    return sf2.shape(for_record - len(sf1))


def get_record(for_record: int):
    if for_record < len(sf1):
        return sf1.record(for_record)

    return sf2.record(for_record - len(sf1))


def iterate_titles() -> typing.Iterator[shapefile.ShapeRecord]:
    for i in range(len(sf1)):
        yield sf1.shapeRecord(i)

    for i in range(len(sf2)):
        yield sf2.shapeRecord(i)


def num_titles() -> int:
    return sf1.numRecords + sf2.numRecords


def get_title_with_group_writer(filename: str):
    w = shapefile.Writer(filename)

    for field in sf1.fields:
        w.field(*field)

    w.field('farm_id', 'N')

    return w

def write_title(writer: shapefile.Writer, shape_record: shapefile.ShapeRecord, group: int):
    record = shape_record.record
    shape = shape_record.shape

    record_tuple = tuple(record)

    writer.record(*record, group)
    writer.shape(shape)
