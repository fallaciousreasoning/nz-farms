from dbfread import DBF

table = DBF('data/titles/nz-property-titles-including-owners-1.dbf')
print(table)

for record in table:
    print(record)
    break