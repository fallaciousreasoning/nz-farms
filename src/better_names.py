import typing
import uuid
import itertools

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

merged_groups = 0
most_titles = 0
total_groups = len(group_map)
total_titles = 0

for group in group_map.values():
    num_titles = len(group.titles)
    if num_titles > 1:
        merged_groups += 1
    most_titles = max(num_titles, most_titles)
    total_titles += num_titles

print()
# 130510 123723 616560 1133494
print(merged_groups, most_titles, total_groups, total_titles)

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

with open('cache/farms.csv', mode='w') as f:
    f.write('row, title_no, group_id\n')

    for row, title_no, group_id in rows:
        f.write(f'{row},{title_no},{group_id}\n')