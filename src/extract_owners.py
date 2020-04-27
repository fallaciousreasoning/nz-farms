import os

from titles import iterate_titles
from title_owners import title_owners, name_is_company
from companies import get_company

def iterate_names():
    for title in iterate_titles():
        record = title.record
        names = record.owners.upper().split(', ')

        for name in names:
            parts = name.split(' ')

            # Return the name, as it is
            yield record.id, name, False, False

            # Return the last name
            if len(parts) >= 1 and not name_is_company(name):
                yield record.id, parts[-1], True, False

            if name_is_company(name):
                company_names = get_company(name)
                if not company_names:
                    continue

                for director in company_names:
                    director = director.upper()

                    yield record.id, director, False, True

                    parts = director.split(' ')
                    # Return the last name
                    if len(parts) >= 1:
                        yield record.id, parts[-1], True, True


