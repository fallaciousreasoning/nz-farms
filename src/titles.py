import shapefile

input_file = 'data/titles/nz-property-titles-including-owners-1'
output_file = 'output/titles'

sf = shapefile.Reader(input_file)

def get_shape(for_record: int):
    return sf.shape(for_record)