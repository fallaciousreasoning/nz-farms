import os
os.environ['PATH'] = os.path.abspath('lib/') + ';' + os.environ['PATH']
import itertools
import time
import typing

from geodaisy import converters, GeoObject

from titles import iterate_titles, num_titles, get_title_with_group_writer, write_title
from land_cover import num_covers, iterate_covers
from progress import print_progress
from extract_owners import iterate_names

import spatialite
import shapefile

title_pairs_file = "cache/title_pairs.csv"
farms_file = "cache/farms.csv"
farm_titles_shapefile = "output/titles"

def to_wkt(shape: shapefile.Shape):
    if shape.shapeTypeName != "POLYGON":
       raise Error("Unknown shape " + shape.shapeTypeName)

    result = "POLYGON (("
    for point in shape.points:
        if result[-1] != "(":
            result += ", "
        result += f"{point[0]} {point[1]}"
    result += "))"
    return result

db_name = "cache/data.db"
db = spatialite.connect(db_name)

def table_exists(table_name):
    exists = db.execute(f"SELECT name FROM sqlite_master WHERE name='{table_name}'").fetchone()

    return not not exists

print(db.execute('SELECT spatialite_version()').fetchone()[0])
if not table_exists('spatial_ref_sys'):
    cursor = db.cursor()
    cursor.execute("BEGIN ;")
    cursor.execute('SELECT InitSpatialMetaData();')
    cursor.execute("END ;")

def insert_titles():
    """We always drop the table when inserting data, to ensure everything is fresh"""
    if table_exists("TITLES"):
        db.execute("SELECT DisableSpatialIndex('TITLES', 'Geometry')")
        db.execute("SELECT DiscardGeometryColumn('TITLES', 'Geometry')")
        db.execute("DROP TABLE IF EXISTS TITLES")
    db.execute("""CREATE TABLE TITLES
        (id INTEGER PRIMARY KEY,
        title_no TEXT,
        status TEXT,
        type TEXT,
        land_distr TEXT,
        issue_date TEXT,
        guarantee_ TEXT,
        estate_des TEXT,
        owners TEXT,
        spatial_ex TEXT)""")
    db.execute("SELECT AddGeometryColumn('TITLES', 'geometry', 4326, 'POLYGON', 'XY')")

    print("Inserting titles....")
    titles_count = num_titles()
    start_time = time.time()

    values = []
    batch_size = 10000
    report_progress = 100

    def commit_batch():
        db.executemany("""INSERT INTO TITLES VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, PolygonFromText(?, 4326))""",
        values)
        db.commit()
        values.clear()

    for title in itertools.islice(iterate_titles(), titles_count):
        shape: shapefile.Shape = title.shape
        record = title.record
        wkt = to_wkt(shape)

        # Add the parameters for this query.
        values.append((record.id, record.title_no, record.status, record.type,record.land_distr,
            record.issue_date,
            record.guarantee_,
            record.estate_des,
            record.owners,
            record.spatial_ex,
            wkt))

        if len(values) >= batch_size:
            commit_batch()
        
        if (record.oid + 1) % report_progress == 0:
            print_progress((record.oid + 1)/titles_count, start_time)

    # Commit any remaining records.
    commit_batch()
    create_title_indexes()

def create_title_indexes():
    print("Creating TITLES indexes...")
    db.execute("CREATE INDEX TITLE_title_no ON TITLES(title_no)")
    db.execute("SELECT CreateSpatialIndex('TITLES', 'Geometry')")

    db.commit()

def maybe_insert_titles():
    if table_exists("TITLES"):
        print("Titles already loaded!")
        return

    insert_titles()

def insert_owners():
    print("Inserting names....")
    db.execute("DROP TABLE IF EXISTS OWNERS")
    db.execute("""CREATE TABLE OWNERS
        (id INTEGER PRIMARY KEY AUTOINCREMENT,
        title_id INTEGER,
        name TEXT,
        is_last_name BOOLEAN,
        is_company_director BOOLEAN)""")

    values = []
    batch_size = 1000
    def commit_batch():
        db.executemany("INSERT INTO OWNERS VALUES (NULL, ?, ?, ?, ?)", values)
        db.commit()
        values.clear()

    for title_id, name, is_last_name, is_from_company in iterate_names():
        values.append((title_id, name, is_last_name, is_from_company))

        if len(values) > batch_size:
            commit_batch()

    commit_batch()

    print("Creating name indexes")
    db.execute("CREATE INDEX OWNER_names on OWNERS(name)")
    db.execute("CREATE INDEX OWNER_title_id on OWNERS(title_id)")
    db.execute("CREATE INDEX OWNER_is_last_name on OWNERS(is_last_name)")
    db.execute("CREATE INDEX OWNER_is_company_director on OWNERS(is_company_director)")
    db.commit()

def maybe_insert_owners():
    if table_exists("OWNERS"):
        print("Owners already loaded!")
        return

    insert_owners()

def maybe_create_title_owners_view():
    db.execute("""CREATE VIEW IF NOT EXISTS TITLE_OWNERS AS
        SELECT title.id as title_id, owner.name, title.geometry
        FROM TITLES title
        INNER JOIN OWNERS owner
        ON title.id=owner.title_id""")
    db.commit()

def maybe_find_title_pairs():
    if os.path.exists(title_pairs_file):
        print("Already found title pairs!")
        return

    query = db.execute("""SELECT DISTINCT title.title_id, other_title.title_id
        FROM TITLE_OWNERS title
        INNER JOIN TITLE_OWNERS other_title
        on not title.title_id = other_title.title_id
            /*Only check one way*/
            and title.title_id < other_title.title_id
            and title.name = other_title.name
            and Intersects(title.geometry, other_title.geometry)""")

    batch_size = 100
    f = open(title_pairs_file, mode='w')

    f.write("title_0,title_1\n")
    while True:
        results = query.fetchmany(batch_size)
        for result in results:
            f.write(f"{result[0]},{result[1]}\n")
        if len(results) < batch_size:
            break

    f.close()
    print("Found title pairs!")

def maybe_build_farms():
    if os.path.exists(farms_file):
        print("Farms are already grouped!")
        return

    next_group_id = 0
    title_to_group: typing.Dict[int, int] = {}
    groups: typing.Dict[int, typing.Set[int]] = {}

    def join_farms(ids: typing.List[int]):
        nonlocal next_group_id
        group_id = None
        for title in ids:
            title_group_id = title_to_group[title] if title in title_to_group else None

            # We need to join the groups 
            if group_id and title_group_id and group_id != title_group_id:
                groups[group_id].update(groups[title_group_id])
                del groups[title_group_id]

            # Make sure we store the group we've seen.
            if not group_id:
                group_id = title_group_id

        # If no group exists, make a new group
        if not group_id:
            group_id = next_group_id
            groups[group_id] = set()
            next_group_id += 1

        # Add the farms to the group.
        groups[group_id].update(ids)

        # Point all the farms at their group.
        for title in groups[group_id]:
            title_to_group[title] = group_id 
    
    total_bytes = os.path.getsize(title_pairs_file)
    read_bytes = 0
    start_time = time.time()
    print_every = 1000
    line_number = 0

    with open(title_pairs_file) as f:
        # Skip the header line
        for line in itertools.islice(f, 1, None):
            read_bytes += len(line)
            ids = [int(id) for id in line.split(',')]
            join_farms(ids)

            if line_number % print_every == 0:
                print_progress(read_bytes/total_bytes, start_time)
            line_number += 1
    print()

    with open(farms_file, mode='w') as f:
        f.write('group_id,title_id\n')
        for group_id, group in groups.items():
            for title in group:
                f.write(f'{group_id},{title}\n')

    print("Wrote farms to", farms_file)

def output_titles_with_groups():
    print("Outputting titles shape file with farm ids")
    title_to_group_id = {}
    with open(farms_file) as f:
        for line in itertools.islice(f, 1, None):
            parts = [int(x) for x in line.split(',')]

            # Map from farm_id to group_id
            title_to_group_id[parts[1]] = parts[0]

    start_time = time.time()
    print_progress_every = 1000

    writer = get_title_with_group_writer(farm_titles_shapefile)
    for shape_record in iterate_titles():
        title_id = shape_record.record.id
        farm_id = title_to_group_id[title_id] if title_id in title_to_group_id else None
        write_title(writer, shape_record, farm_id)

        if (shape_record.record.oid + 1) % print_progress_every == 0:
            print_progress((shape_record.record.oid + 1)/num_titles(), start_time)

    writer.close()
    print("Wrote titles shape file to: " + farm_titles_shapefile)

maybe_insert_titles()
maybe_insert_owners()
maybe_create_title_owners_view()
maybe_find_title_pairs()
maybe_build_farms()
output_titles_with_groups()