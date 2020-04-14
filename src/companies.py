import os
import itertools
import json

data_path = 'data/nzbn'
files_count = 1 # 5

database_name = f"{data_path}/companies.map"

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
        with open(file_name) as f:
            for line in f:
                # This is just an approximation...
                bytes_read += len(line)
                if not line.startswith(company_lines_start_with):
                    continue

                line = line.strip('\n').strip(',')
                j = json.loads(line)
                if progress_callback:
                    progress_callback(bytes_read/total_size_bytes)
                yield j


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

def maybe_build_database():
    from BTrees import OOBTree
    import transaction

    root = get_root()
    root.companies = OOBTree.BTree()

    companies = iterate_companies(lambda progress: print(f'\rProgress: {round(progress * 100, 2)}', end=''))
    
    commit_every_n = 10000
    company_number = 0
    for company in companies:
        name = company['entityName'].upper()
        root.companies[name] = company

        # Don't commit every change.
        if company_number % commit_every_n == 0:
            transaction.commit()
        company_number += 1

    # Commit everything remaining.
    transaction.commit()

maybe_build_database()
print(has_company('LBS TRUSTEE LIMITED'))