def title_owners(record, last_only=False, filter_companies=False):
    """Get the owner names for a record in the NZ property titles dbf file"""

    all_names = record['owners'].split(', ')
    result = set()
    for name in all_names:
        if filter_companies and name_is_company(name):
            continue

        if last_only:
            name = last_name(name)

        result.add(name)


    return list(result)

def last_name(name):
    "Get the last name from a name. This uses the simple heuristic of 'The last name is the last space seperated bit of a name. It is probably not true."
    return name.split(' ')[-1]

def name_is_company(name):
    """Heuristic for guessing if a name belongs to a company"""
    return name.endswith('Limited')

