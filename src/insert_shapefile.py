import shapefile
import sqlite3

def insert_shapefile(file, db: sqlite3.Connection, table_name):
    sf = sf.Reader(file)

    cursor = db.cursor()

    cursor.execute(f"DROP TABLE {table_name}")
    cursor.execute("""CREATE TABLE ?
        ()""",
    (table_name))



    db.commit()