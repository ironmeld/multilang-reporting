# jsonl2db
#
# Convert JSON Lines (jsonlines.org) to an equivalent sqlite3
# database.
#
import json
import os
import re
import sqlite3
import sys


def read_results(jsonl_filename):
    results = []
    with open(jsonl_filename) as jsonl_file:
        for json_line in jsonl_file:
            results.append(json.loads(json_line))
    return results


def get_columns(results):
    valid_column_name = re.compile("^[A-Za-z0-9_]+$")
    columns = {}
    for result in results:
        for field_name in result:
            columns[field_name] = True

    for column_name in columns:
        if not valid_column_name.match(column_name):
            print(f"ERROR: invalid column: {column_name}", file=sys.stderr)
            sys.exit(1)
    return columns


def jsonl2db(jsonl_filename, db_filename):
    if not jsonl_filename.endswith('.jsonl'):
        print("ERROR: jsonl file must end with .jsonl", file=sys.stderr)
        sys.exit(1)
    table_name = os.path.basename(jsonl_filename)[:-len('.jsonl')]

    results = read_results(jsonl_filename)
    columns = get_columns(results)

    conn = sqlite3.connect(db_filename)
    c = conn.cursor()

    c.execute(f"create table if not exists {table_name} " +
              f"({', '.join(list(columns.keys()))})")

    marks = ",".join(["?" for field in results[0]])
    c.executemany(f"insert into {table_name} values ({marks})",
                  [tuple(result.values()) for result in results])

    conn.commit()
    c.close()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: python3 jsonl2db.py perf_results.jsonl results.db")
    else:
        jsonl2db(sys.argv[1], sys.argv[2])
