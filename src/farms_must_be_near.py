import itertools
import typing
import uuid
import pandas
import numpy

from titles import get_shape
from names_to_farms import get_farm_database
from lat_lng import intersects

next_farm_id = 1
class FarmGroup:
    def __init__(self):
        global next_farm_id
        super().__init__()
        self.rows = []
        self.bounds = []
        self.id = next_farm_id
        next_farm_id += 1

    def is_near(self, other) -> True | False:
        if not other:
            return False

        for poly in self.bounds:
            for otherPoly in other.bounds:
                if intersects(poly, otherPoly):
                    return True
        return False

    def merge_farm(self, other):
        self.bounds.extend(other.bounds)
        self.rows.extend(other.rows)

    def add(self, row, shape):
        self.rows.append(row)
        self.bounds.append(shape.bbox)

database = get_farm_database()
database['farm_id'] = pandas.Series(0, index=database.index)

farm_groups = database.groupby('group_id')
total_rows = database.shape[0]
processed_rows = 0
farms_count = 0

print("Initial Groups:", 616858) # TODO: Not this

for group_id, group in farm_groups:
    result_groups: typing.List[FarmGroup] = []
    
    for record in group.row:
        shape = get_shape(record)
        farm = FarmGroup()
        farm.add(record, shape)
        result_groups.append(farm)

    def collapse_farms():
        removed = 0
        i = 0
        while i < len(result_groups):
            f1 = result_groups[i]

            j = i + 1
            while j < len(result_groups):
                f2 = result_groups[j]

                if not f1.is_near(f2):
                    j += 1
                    continue

                f1.merge_farm(f2)
                result_groups.pop(j)   
                removed += 1
            i += 1          
        return removed

    while collapse_farms() != 0:
        pass

    for farm in result_groups:
        for row in farm.rows:
            processed_rows += 1
            database._set_value(row, "farm_id", farm.id)
            # print(farm.id, database["farm_id"][row])
            # database.at["farm_id", row] = farm.id

    farms_count += len(result_groups)
    print(f"\rProgress: {round(processed_rows/total_rows*100, 2)}%", end='')

print()
print("End group counts:", farms_count);
database.to_csv('cache/near-farms.csv')