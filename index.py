import sys
import argparse
import os
import yaml

from psycopg2 import connect, sql
from psycopg2.extras import RealDictCursor

# DROP TABLE cleanup;
# CREATE TABLE cleanup (
#     id SERIAL,
#     storage_box_id INTEGER NOT NULL,
#     file_size BIGINT NOT NULL,
#     uri TEXT NOT NULL,
#     PRIMARY KEY (id)
# );
# CREATE UNIQUE INDEX cleanup_uq ON cleanup (storage_box_id, uri);
# GRANT ALL PRIVILEGES ON TABLE cleanup TO mytardis;
# GRANT USAGE, SELECT ON SEQUENCE cleanup_id_seq TO mytardis;

cache = []


def get_parser():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--config",
        default="settings.yaml",
        help="Config file location (default: settings.yaml)."
    )

    parser.add_argument(
        "location",
        metavar="location",
        help="Location value of storage box to scan."
    )

    return parser


def get_dataset_id(sbid, path):
    q = sql.SQL("""
        SELECT DISTINCT df.dataset_id 
        FROM tardis_portal_datafileobject AS dfo
        LEFT JOIN tardis_portal_datafile AS df
        ON df.id=dfo.datafile_id 
        WHERE dfo.storage_box_id={sbid} AND dfo.uri LIKE {path}
    """).format(
        sbid=sql.Literal(sbid),
        path=sql.Literal("{}%".format(path)))
    cur.execute(q)
    rows = cur.fetchall()
    if len(rows) == 1:
        return rows[0]["dataset_id"]
    return None


def get_dataset_uris(dsid):
    q = sql.SQL("""
        SELECT dfo.uri
        FROM tardis_portal_datafileobject AS dfo
        LEFT JOIN tardis_portal_datafile AS df
        ON df.id=dfo.datafile_id 
        WHERE df.dataset_id={dsid}
    """).format(
        dsid=sql.Literal(dsid))
    cur.execute(q)
    data = []
    for row in cur.fetchall():
        data.append(row["uri"])
    return data


def walk_the_line(sbid, location, uri):
    global cache
    this_location = os.path.join(location, uri)
    if len(uri) == 0:
        print(this_location)
    if len(uri) != 0:
        path = uri.split("/")
        if len(path) == 1:
            print(this_location)
            dsid = get_dataset_id(sbid, path[0])
            if not dsid:
                print("Can't find dataset.")
                return False
            cache = get_dataset_uris(dsid)
    for fname in os.listdir(this_location):
        fname_abs = os.path.join(this_location, fname)
        fname_uri = os.path.join(uri, fname)
        if os.path.isfile(fname_abs):
            if fname_uri not in cache:
                fsize = os.stat(fname_abs).st_size
                q = sql.SQL("""
                    INSERT INTO cleanup (storage_box_id, file_size, uri)
                    VALUES ({sbid}, {fsize}, {uri})
                    ON CONFLICT DO NOTHING
                """).format(
                    sbid=sql.Literal(sbid),
                    fsize=sql.Literal(fsize),
                    uri=sql.Literal(fname_uri))
                cur.execute(q)
                con.commit()
        elif os.path.isdir(fname_abs):
            walk_the_line(sbid, location, fname_uri)
    return True


args = get_parser().parse_args()

if os.path.isfile(args.config):
    with open(args.config) as f:
        settings = yaml.load(f, Loader=yaml.Loader)
else:
    sys.exit("Can't find settings.")

try:
    con = connect(
        host=settings["database"]["host"],
        port=settings["database"]["port"],
        user=settings["database"]["username"],
        password=settings["database"]["password"],
        database=settings["database"]["database"]
    )
except Exception as e:
    sys.exit("Can't connect to the database - {}.".format(str(e)))

cur = con.cursor(cursor_factory=RealDictCursor)

q = sql.SQL("""
    SELECT storage_box_id
    FROM tardis_portal_storageboxoption
    WHERE key='location' AND value={location}
""").format(location=sql.Literal(args.location))
cur.execute(q)
rows = cur.fetchall()

if len(rows) == 1:
    try:
        print("Walking {}".format(args.location))
        walk_the_line(rows[0]["storage_box_id"], args.location, "")
        print("Completed.")
    except Exception as e:
        print("Error - {}.".format(str(e)))
else:
    print("Can't find storage box.")

cur.close()
con.close()
