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

sbid = 0
cache = []


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        default="settings.yaml",
        help="Config file location (default: settings.yaml)."
    )
    return parser


def get_info(sbids, path):
    q = sql.SQL("""
        SELECT DISTINCT dfo.storage_box_id, df.dataset_id 
        FROM tardis_portal_datafileobject AS dfo
        LEFT JOIN tardis_portal_datafile AS df
        ON df.id=dfo.datafile_id 
        WHERE dfo.storage_box_id IN {sbids} AND dfo.uri LIKE {path}
    """).format(
        sbids=sql.Literal(tuple(sbids)),
        path=sql.Literal("{}%".format(path)))
    cur.execute(q)
    rows = cur.fetchall()
    if len(rows) == 1:
        return {
            "sbid": rows[0]["storage_box_id"],
            "dsid": rows[0]["dataset_id"]
        }
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


def get_storage_boxes():
    q = sql.SQL("""
        SELECT sb.name, sbo.value AS location
        FROM tardis_portal_storagebox AS sb
        JOIN tardis_portal_storageboxoption AS sbo
        ON sbo.storage_box_id = sb.id AND sbo.key = 'location'
        WHERE
            sb.status != 'deleted' AND
            sb.master_box_id IS NULL AND
            sb.name NOT LIKE 'fast%' AND
            sb.name NOT LIKE '%test%' AND
            sb.name NOT LIKE '%temp%' AND
            sb.name NOT LIKE '%vault%' AND
            sbo.value NOT LIKE '%vault%'
    """)
    cur.execute(q)
    return cur.fetchall()


def walk_the_line(sbids, location, uri):
    global sbid
    global cache
    this_location = os.path.join(location, uri)
    if len(uri) == 0:
        print(this_location)
    if len(uri) != 0:
        path = uri.split("/")
        if len(path) == 1:
            print(this_location)
            info = get_info(sbids, path[0])
            if not info:
                print("Can't find dataset.")
                return False
            sbid = info["sbid"]
            cache = get_dataset_uris(info["dsid"])
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
            walk_the_line(sbids, location, fname_uri)
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

sbs = get_storage_boxes()
for sb in sbs:
    print("Scanning {}".format(sb["name"]))
    q = sql.SQL("""
        SELECT storage_box_id
        FROM tardis_portal_storageboxoption
        WHERE key='location' AND value={location}
    """).format(location=sql.Literal(sb["location"]))
    cur.execute(q)
    rows = cur.fetchall()

    if len(rows) != 0:
        try:
            print("Walking {}".format(sb["location"]))
            walk_the_line([row["storage_box_id"] for row in rows], sb["location"], "")
            print("Completed.")
        except Exception as e:
            print("Error - {}.".format(str(e)))
    else:
        print("Can't find storage boxes for this location.")

cur.close()
con.close()
