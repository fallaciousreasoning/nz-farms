import jqtree
import itertools
import typing

import time

from lat_lng import union, intersects, aabb
from titles import iterate_titles, num_titles
from title_owners import title_owners

nz_bounds = (-47.401915, 172.266598, -33.984178, 177.715816)
quad_tree = jqtree.Index(bbox=nz_bounds, max_depth=40, max_items=100)

nodes = {}
next_node_id = 0
class Node:
    def __init__(self, titles: typing.Set[str], names: typing.Set[str], bounds: aabb):
        global next_node_id
        self.bounds = bounds
        self.titles = titles
        self.names = names
        self.qtn = None
        self.qt = None
        self.id = next_node_id
        next_node_id += 1
        nodes[self.id] = self

    def has_title(self, title: str) -> True | False:
        return title in self.titles

    def names_overlap(self, names: typing.List[str]) -> True | False:
        for name in names:
            if name in self.names:
                return True
        return False

def merge_nodes(nodes: typing.List[Node]) -> Node:
    new_bounds = None
    titles = set()
    names = set()

    for node in nodes:
        if not new_bounds:
            new_bounds = node.bounds
        else:
            new_bounds = union(new_bounds, node.bounds)

        titles.update(node.titles)
        names.update(node.names)
    return Node(titles, names, new_bounds)

start = time.time()

# Build the tree
slice = num_titles()
for shapeRecord in itertools.islice(iterate_titles(), slice):
    node = Node(set([shapeRecord.record.oid]), title_owners(shapeRecord.record), shapeRecord.shape.bbox)
    quad_tree.insert(node, node.bounds)
    print(f"\rBuilding Initial Tree: {round((shapeRecord.record.oid + 1)/slice * 100,2):.2f}%", end='')
print(f'Took {round(time.time() - start, 2)} seconds')
print("Finding nearby...")
merges = 0
nodes_processed = 0
started_processing_at = time.time()
note_every = 100

def human_time(time: float):
    hour_mul = 60 * 60
    minute_mul = 60
    hours = int(time / hour_mul)
    time -= hours * hour_mul

    minutes = int(time / minute_mul)
    time -= minutes * minute_mul
    seconds = int(time)

    return f'{hours}h {minutes}m {seconds}s'
for shapeRecord in itertools.islice(iterate_titles(), slice):
    record = shapeRecord.record

    # Find the nearby nodes
    nearby = quad_tree.intersect(shapeRecord.shape.bbox)
    nodes_to_merge = []
    owners = title_owners(record)

    for node in nearby:
        # If this is the node containing the title, it will need to be merged.
        if node.has_title(record['title_no']):
            nodes_to_merge.append(node)
            continue

        # If this is a neighbouring node with the same name, it too should be
        # merged.
        if node.names_overlap(owners):
            nodes_to_merge.append(node)

    nodes_processed += 1
    if nodes_processed % note_every == 0:
        progress = nodes_processed / slice
        current_time = time.time()
        dt = current_time - started_processing_at
        predicted_remaining = dt / progress - dt
        print(f"\rProgress: {round(progress*100, 2):.2f}%. {human_time(predicted_remaining)} remaining\t", end='')

    # If the only node we found is the one this title already belongs to, do
    # nothing.
    if len(nodes_to_merge) <= 1:
        continue

    # Merge all the candidates together.
    merged_node = merge_nodes(nodes_to_merge)

    # Remove the old nodes from the tree
    for node in nodes_to_merge:
        quad_tree.remove(node, node.bounds)
        del nodes[node.id]
        merges += 1

    # Insert the new, merged node.
    quad_tree.insert(merged_node, merged_node.bounds)
print("Merged", merges, "nodes")
print(f'Total time {round(time.time() - start, 2)} seconds')

print("Saving to CSV")
with open('cache/near-farms.csv', mode='w') as f:
    f.write("farm_id,record_no\n")

    for node in nodes.values():
        for record in node.titles:
            f.write(f'{node.id},{record}\n')
    
# for each node
#   place in tree

# for each node
#   for each nearby node
#       if name is similar
#           group(node, nearby)