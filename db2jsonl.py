# jsonl2db
#
# Convert JSON Lines (jsonlines.org) to an equivalent sqlite3 database.
#
import json
import sqlite3
import sys


def db2jsonl(db_filename, table_name):
    conn = sqlite3.connect(db_filename)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    for row in c.execute(f'select * from {table_name}'):
        print(json.dumps(dict(row)))


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: python3 db2jsonl.py results.db perf_results"
              " > perf_results.jsonl")
    else:
        db2jsonl(sys.argv[1], sys.argv[2])
