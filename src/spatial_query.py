import os
os.environ['PATH'] = os.path.abspath('lib/') + ';' + os.environ['PATH']
import itertools
import time

from titles import iterate_titles, num_titles
from progress import print_progress
from extract_owners import iterate_names

import spatialite
import shapefile

def to_wkt(shape: shapefile.Shape):
    geo = shape.__geo_interface__
    if shape.shapeTypeName != "POLYGON":
        raise Error("Unknown shape " + shape.shapeTypeName)

    result = "POLYGON (("
    for point in shape.points:
        if result[-1] != "(":
            result += ", "
        result += f"{point[0]} {point[1]}"
    result += "))"
    return result

def to_wkb(shape: shapefile.Shape):
    if shape.shapeType != 5:
        raise Error("Unknown shape" + shape.shapeTypeName)

    result = 0
    return result

db_name = "cache/data.db"
db = spatialite.connect(db_name)
print(db.execute('SELECT spatialite_version()').fetchone()[0])

def table_exists(table_name):
    exists = db.execute("SELECT name FROM sqlite_master WHERE name='TITLES'").fetchone()

    return not not exists

def insert_titles():
    """We always drop the table when inserting data, to ensure everything is fresh"""
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
        spatial_ex TEXT,
        Geometry MULTIPOLYGON)""")

    print("Inserting titles....")
    titles_count = num_titles()
    start_time = time.time()

    values = []
    batch_size = 10000
    report_progress = 100

    def commit_batch():
        db.executemany("""INSERT INTO TITLES VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, PolygonFromText(?))""",
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
    db.execute("CREATE INDEX TITLES_geometry ON TITLES(Geometry)")
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
        name TEXT)""")

    values = []
    batch_size = 1000
    def commit_batch():
        db.executemany("INSERT INTO OWNERS VALUES (NULL, ?, ?)", values)
        db.commit()
        values.clear()

    for title_id, name in iterate_names():
        values.append((title_id, name))

        if len(values) > batch_size:
            commit_batch()

    commit_batch()

    print("Creating name indexes")
    db.execute("CREATE INDEX OWNER_names on OWNERS(name)")
    db.execute("CREATE INDEX OWNER_title_id on OWNERS(title_id)")
    db.commit()

def maybe_insert_owners():
    if table_exists("OWNERS"):
        print("Owners already loaded!")
        return

    insert_owners()

def create_title_owners_view():
    db.execute("""CREATE VIEW IF NOT EXISTS TITLE_OWNERS AS
        SELECT title.id as title_id, owner.name, title.geometry
        FROM TITLES title
        INNER JOIN OWNERS owner
        ON title.id=owner.title_id""")
    db.commit()

def find_farms():
    ouput_file = "output/farms.csv"

    query = db.execute("""SELECT DISTINCT title.title_id, other_title.title_id
        FROM TITLE_OWNERS title
        INNER JOIN TITLE_OWNERS other_title
        on not title.title_id = other_title.title_id
            /*Only check one way*/
            and title.title_id < other_title.title_id
            and title.name = other_title.name
            and Intersects(title.geometry, other_title.geometry)""")

    batch_size = 100
    f = open(ouput_file, mode='w')

    f.write("title_0,title_1\n")
    while True:
        results = query.fetchmany(batch_size)
        for result in results:
            f.write(f"{result[0]},{result[1]}\n")
        if len(results) < batch_size:
            break

    f.close()

maybe_insert_titles()
maybe_insert_owners()
create_title_owners_view()
find_farms()