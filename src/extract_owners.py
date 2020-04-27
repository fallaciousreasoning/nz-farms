import os
import itertools

from titles import iterate_titles
from title_owners import title_owners, name_is_company
from companies import get_company

def iterate_names():
    for title in iterate_titles():
        seen_owners = set()
        owners = []
        def add_owner(owner, is_company, is_last=False):
            if owner in seen_owners:
                return

            seen_owners.add(owner)
            owners.append((title.record.id, owner, is_last, is_company))

            if not is_last and not name_is_company(owner):
                parts = owner.split(' ')
                if len(parts) >= 1:
                    add_owner(parts[-1], is_company, True)

        record = title.record
        names = record.owners.upper().split(', ')

        for name in names:
            parts = name.split(' ')

            # Add the name
            add_owner(name, False, False)

            if name_is_company(name):
                company_names = get_company(name)
                if not company_names:
                    continue

                for director in company_names:
                    director = director.upper()
                    add_owner(director, True)

        yield from owners
