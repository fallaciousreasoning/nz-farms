from dbfread import DBF
import itertools

table = DBF('data/titles/nz-property-titles-including-owners-1.dbf')

for record in itertools.islice(table, 10):
    print(record['owners'])
