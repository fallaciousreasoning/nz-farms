from dbfread import DBF
import itertools
from title_owners import title_owners
import persistent
import uuid
import transaction

class Title(persistent.Persistent):
    def __init__(self, names, title_no):
        self.names = names
        self.title_no = title_no

    def __repr__(self):
        return f'Owners: [{", ".join(self.names)}], TitleNo: {self.title_no}'

    def __eq__(self, value):
        return value is not None and value.title_no == self.title_no

    def __hash__(self):
        return self.title_no.__hash__()

class Group(persistent.Persistent):
    def __init__(self):
        self.id = uuid.uuid4().int
        self.titles: Set[Title] = set()

connection = None
def get_root():
    global connection
    if connection:
        return connection.root

    import ZODB
    connection = ZODB.connection('cache/name_groups')
    return connection.root

def get_titles():
    if not hasattr(get_root(), 'titles'):
        from BTrees import OOBTree
        get_root().titles = OOBTree.BTree()
    return get_root().titles

def get_names():
    if not hasattr(get_root(), 'names'):
        from BTrees import OOBTree
        get_root().names = OOBTree.BTree()

    return get_root().names

def get_groups():
    if not hasattr(get_root(), 'groups'):
        from BTrees import OOBTree
        get_root().groups = OOBTree.BTree()

    return get_root().groups

def group_for_name(name):
    names = get_names()
    if not names.has_key(name):
        return None

    group_id = names[name]
    return get_groups()[group_id]

def merge_groups(a: Group, b: Group):
    for title in b.titles:
        add_title(a, title)

    a._p_changed = True

    # There shouldn't be any reference to b now...
    # get_groups().remove(b.id)
    return a

def add_title(to: Group, title: Title):
    get_titles()[title.title_no] = to.id
    for name in title.names:
        get_names()[name] = to.id

    to.titles.add(title)
    to._p_changed = True

def iterate_titles(progress_callback=None):
    table = DBF('data/titles/nz-property-titles-including-owners-1.dbf', encoding='utf8')
    num_rows = len(table)
    row_num = 0
    for record in table:
        yield Title(title_owners(record), record['title_no'])
        
        row_num += 1
        if progress_callback:
            progress_callback(row_num/num_rows)

def build_name_groups():
    save_every_n = 100000
    iteration = 0
    for title in iterate_titles(lambda progress: print(f'\rBuilding name database: {round(progress*100, 2)}%', end='')):
        last_group = None
        for name in title.names:
            group = group_for_name(name)
            if not group: continue
        
            if last_group:
                 last_group = merge_groups(last_group, group)
            else:
                last_group = group

        if not last_group:
            last_group = Group()
            get_groups()[last_group.id] = last_group
        
        add_title(last_group, title)

        if iteration % save_every_n == 0:
            transaction.commit()

        iteration += 1
    transaction.commit()


build_name_groups()
