import sys
import argparse
import os
import yaml

from psycopg2 import connect, sql
from psycopg2.extras import RealDictCursor

# CREATE TABLE cleanup (
# 	id SERIAL,
# 	storage_box_id INTEGER NOT NULL,
#   file_size BIGINT NOT NULL,
# 	uri TEXT NOT NULL,
# 	PRIMARY KEY (id)
# );
# CREATE INDEX cleanup_storage_box_id ON cleanup (storage_box_id);


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


def walk_the_line(sbid, location, uri):
    this_location = os.path.join(location, uri)
    for fname in os.listdir(this_location):
        fname_abs = os.path.join(this_location, fname)
        fname_uri = os.path.join(uri, fname)
        if os.path.isfile(fname_abs):
            q = sql.SQL("""
                SELECT id
                FROM tardis_portal_datafileobject
                WHERE storage_box_id={sbid} AND uri={uri}
                LIMIT 1
            """).format(sbid=sql.Literal(sbid), uri=sql.Literal(fname_uri))
            cur.execute(q)
            dfo = cur.fetchone()
            if dfo is None:
                q = sql.SQL("""
                    SELECT id
                    FROM cleanup
                    WHERE storage_box_id={sbid} AND uri={uri}
                    LIMIT 1
                """).format(sbid=sql.Literal(sbid), uri=sql.Literal(fname_uri))
                cur.execute(q)
                id = cur.fetchone()
                if id is None:
                    fsize = os.stat(fname_abs).st_size
                    q = sql.SQL("""
                        INSERT INTO cleanup (storage_box_id, file_size, uri)
                        VALUES ({sbid}, {fsize}, {uri})
                    """).format(
                        sbid=sql.Literal(sbid),
                        fsize=sql.Literal(fsize),
                        uri=sql.Literal(fname_uri))
                    cur.execute(q)
                    con.commit()
                    print(fsize, fname_abs)
        elif os.path.isdir(fname_abs):
            walk_the_line(sbid, location, fname_uri)


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
