import os

from titles import iterate_titles
from title_owners import title_owners

def iterate_names():
    for title in iterate_titles():
        record = title.record
        names = title_owners(record)

        for name in names:
            yield record.id, name
