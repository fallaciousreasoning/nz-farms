import os
import itertools
import json

cache_path = 'cache'
if not os.path.exists(cache_path):
    os.makedirs(cache_path)

data_path = 'data/nzbn'
files_count = 1 # 5

database_name = f"{cache_path}/companies.map"

def iterate_companies(progress_callback=None):
    global data_path, files_count
    # Any line describing a company will look like this.
    company_lines_start_with = ' {"australianBusinessNumber":'

    file_names = [ f'{data_path}/20200302_{x+1}.json'
        for x in range(files_count)
    ]

    total_size_bytes = 0
    for file_name in file_names:
        total_size_bytes += os.path.getsize(file_name)

    bytes_read = 0
    
    for file_name in file_names:
        with open(file_name, encoding='utf8') as f:
            index = 0
            for line in f:
                if index % 1000 == 0:
                    print(f"\nLine: {str(index)}")
                index += 1

                # This is just an approximation...
                bytes_read += len(line)
                if not line.startswith(company_lines_start_with):
                    continue

                close = line.rfind(' }') + 2
                line = line[:close]
                try:
                    j = json.loads(line)
                    if progress_callback:
                        progress_callback(bytes_read/total_size_bytes)
                    yield j
                except:
                    print(f'Failed to parse: {line} as json')
                    break


connection = None
def get_root():
    global connection
    if connection:
        return connection.root

    import ZODB, ZODB.FileStorage
    storage = ZODB.FileStorage.FileStorage(database_name)
    db = ZODB.DB(storage)
    connection = db.open()

    return connection.root

def get_companies():
    return get_root().companies

def get_company(name):
    name = name.upper()
    return get_companies()[name]

def has_company(name):
    name = name.upper()
    return get_companies().has_key(name)

def get_names_for_role(company, role):
    roles = company['roles']
    active_roles = filter(lambda r: r['roleStatus'] == 'ACTIVE', roles)
    matching_roles = filter(lambda r: r['roleType'] == role, active_roles)

    people = []
    for r in matching_roles:
        for person in r['rolePerson']:
            first = person['firstName']
            middle = person['middleNames']
            last = person['lastName']

            name = ""
            if first:
                name += first
            if middle:
                name += " " + middle

            if last:
                name += " " + last

            name = name.strip()

            if not name:
                continue

            people.append(name)

    return people

def maybe_build_database():
    if os.path.exists(database_name):
        print("Cache already exists, not rebuilding!")
        return

    from BTrees import OOBTree
    import transaction

    root = get_root()
    root.companies = OOBTree.BTree()

    companies = iterate_companies(lambda progress: print(f'\rProgress: {round(progress * 100, 2)}%', end=''))
    
    commit_every_n = 5000
    company_number = 0
    for company in companies:
        name = company['entityName'].upper()
        root.companies[name] = get_names_for_role(company, 'Director')

        # Don't commit every change.
        if company_number % commit_every_n == 0:
            transaction.commit()
        company_number += 1

    # Commit everything remaining.
    transaction.commit()

maybe_build_database()
print(has_company('DATARA CONTRACTING LIMITED'))