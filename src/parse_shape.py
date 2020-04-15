import shapefile
import itertools

sf = shapefile.Reader('data/titles/nz-property-titles-including-owners-1')

w = shapefile.Writer('output/titles')

print("Here?")
for field in sf.fields:
    w.field(*field)

w.field('farm_id', 'N')

print("There?")

for i in range(10000):
    record = sf.record(i)
    shape = sf.shape(i)
    as_tuple = tuple(record)
    w.record(*record, 5)
    w.shape(shape)

# for shape in sf:
#     print(str(shape))
#     print("FOO?")
#     print(record)

print("Done?")
sf.close()
w.close()

sf = shapefile.Reader('output/titles')
print(sf.fields)
print(sf.record(0).farm_id)