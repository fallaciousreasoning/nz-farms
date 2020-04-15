import typing
import uuid
import itertools
import os

from names import iterate_titles, Title

names_map: typing.Dict[str, int] = {}
group_map = {}

class Group:
    def __init__(self):
        self.names: typing.Set[str] = set()
        self.titles: typing.Set[Title] = set()
        self.id = uuid.uuid4().int
        group_map[self.id] = self

    def add_title(self, title: Title):
        for name in title.names:
            names_map[name] = self.id
            self.names.add(name)
        # self.names.update(title.names)
        self.titles.add(title)

    def merge_in(self, group):
        # Don't merge with ourself
        if group.id == self.id:
            return

        for name in group.names:
            names_map[name] = self.id
            self.names.add(name)

        self.titles.update(group.titles)
        del group_map[group.id]

database_name = 'cache/farms.csv'
def maybe_build_farm_database():
    if os.path.exists(database_name):
        return
    titles = iterate_titles(lambda p: print(f'\r{round(p*100,2)}%', end=""))
    for title in itertools.islice(titles, None):
        last_group: Group = None
        for name in title.names:
            group = group_map[names_map[name]] if name in names_map else None
            if last_group and group:
                last_group.merge_in(group)
            else: last_group = group

        if not last_group:
            last_group = Group()

        last_group.add_title(title)

    del names_map
    rows = []
    for key in group_map.keys():
        group = group_map[key]
        for title in group.titles:
            rows.append((title.row, title.title_no, group.id))
        del group.names
        del group.titles
    del group_map

    rows.sort()

    with open(database_name, mode='w') as f:
        f.write('row,title_no,group_id\n')

        for row, title_no, group_id in rows:
            f.write(f'{row},{title_no},{group_id}\n')

def get_farm_database():
    maybe_build_farm_database()
    import pandas
    return pandas.read_csv(database_name)