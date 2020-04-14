from dbfread import DBF
import itertools

from title_owners import title_owners

table = DBF('data/titles/nz-property-titles-including-owners-1.dbf')

for record in itertools.islice(table, 10):
    owners = title_owners(record, filter_companies=False, last_only=True)
    print(" | ".join(owners))
