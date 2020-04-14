from dbfread import DBF
import itertools
from title_owners import title_owners
import persistent
import uuid
import transaction

class Title:
    def __init__(self, names, title_no):
        self.names = names
        self.title_no = title_no

    def __repr__(self):
        return f'Owners: [{", ".join(self.names)}], TitleNo: {self.title_no}'

class Group(persistent.Persistent):
    def __init__(self):
        self.id = uuid.uuid4().int
        self.names = set()
        self.titles = set()

connection = None
def get_root():
    global connection
    if connection:
        return connection.root

    import ZODB
    connection = ZODB.connection('cache/name_groups')
    return connection.root

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
    for name in b.names:
        a.names.add(name)
        get_names()[name] = a.id
    a.titles.update(b.titles)
    a._p_changed = True

    # There shouldn't be any reference to b now...
    # get_groups().remove(b.id)
    return a

def add_title(to: Group, title: Title):
    for name in title.names:
        try:
            get_names()[name] = to.id
            to.names.update(title.names)
        except:
            print(to.id)
            raise

    to.titles.add(title.title_no)
    to._p_changed = True

def iterate_titles():
    table = DBF('data/titles/nz-property-titles-including-owners-1.dbf', encoding='utf8')

    for record in itertools.islice(table, 250000):
        yield Title(title_owners(record), record['title_no'])

def build_name_groups():
    save_every_n = 10000
    iteration = 0
    for title in iterate_titles():
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
