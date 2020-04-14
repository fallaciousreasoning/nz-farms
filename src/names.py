from dbfread import DBF
import itertools
from title_owners import title_owners

class Title:
    def __init__(self, names, title_no):
        self.names = names
        self.title_no = title_no

    def __repr__(self):
        return f'Owners: [{", ".join(self.names)}], TitleNo: {self.title_no}'

name_group_map = {}

class Group:
    def __init__(self):
        global name_group_map

        super().__init__()
        self.names = set()
        self.titles = []

    def merge_in(self, group):
        global name_group_map
        for name in group.names:
            self.names.add(name)
            name_group_map[name] = self

        self.titles.extend(group.titles)

    def add_title(self, title):
        for name in title.names:
            self.names.add(name)
            name_group_map[name] = self

        self.titles.append(title.title_no)

def iterate_titles():
    table = DBF('data/titles/nz-property-titles-including-owners-1.dbf')

    for record in itertools.islice(table, 25000):
        yield Title(title_owners(record), record['title_no'])

def build_name_groups():
    for title in iterate_titles():
        last_group = None
        for name in title.names:
            group = name_group_map[name] if name in name_group_map else None
            if not group: continue
        
            if last_group:
                 group.merge_in(last_group)
            last_group = group

        if not last_group:
            last_group = Group()
        
        last_group.add_title(title)

build_name_groups()
