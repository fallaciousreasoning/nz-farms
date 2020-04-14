import os
import ZODB

data_path = 'data/nzbn'
files_count = 1 # 5
file_names = [ f'{data_path}/20200302_{x+1}.json'
    for x in range(files_count)
]

database_name = f"{data_path}/companies.map"

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
    return get_companies()[name]

def maybe_build_database():
    from BTrees import OOBTree
    root = get_root()
    root.companies = OOBTree.BTree()

    for file_name in file_names:
        pass

maybe_build_database()
print(get_company('foo'))