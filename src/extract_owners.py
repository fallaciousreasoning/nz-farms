import os

from titles import iterate_titles
from title_owners import title_owners, name_is_company
from companies import get_names_for_role

def iterate_names():
    for title in iterate_titles():
        record = title.record
        names = record.owners.split(',')

        for name in names:
            parts = name.split(' ')

            # Return the last name
            if len(parts) >= 1:
                yield record.id, parts[-1], True, False

            # Return the name, as it is
            yield record.id, name, False, False

            if name_is_company(name):
                company_name = get_names_for_role(name, 'DIRECTOR')
                if not company_name:
                    continue

                yield record.id, get_company_name(name), False, True

