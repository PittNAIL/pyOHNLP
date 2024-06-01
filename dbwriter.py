import csv
import json
import psycopg2

from io import StringIO

from util import parse_args

args = parse_args()

with open(args.db_conf, "r") as f:
    config = json.load(f)

def write_to_db(dtc, config):
    columns = list(dtc.keys())
    rows = zip(*[dtc[col] for col in columns])
    with open(config, "r") as f:
        config = json.load(f)["write_to"]
    db, user, host, password = (
        config["database"],
        config["user"],
        config["host"],
        config["password"],
    )
    table = config["to_table"]
    output = StringIO()
    csv_writer = csv.writer(output)
    csv_writer.writerow(columns)
    csv_writer.writerows(rows)
    output.seek(0)

    if config["db_type"] == "postgresql":
        connect = psycopg2.connect(database=db, user=user, host=host, password=password)
        cursor = connect.cursor()
        cursor.copy_expert(
            f"copy {table} ({', '.join(columns)}) FROM STDIN WITH CSV HEADER", output
        )
        connect.commit()
        cursor.close()
        connect.close()
    if config["db_type"] != "postgresql":
        raise ValueError(f"Write capability for {config["db_type"]} not yet implemented. Only
                         PostgreSQL compatibility enabled at this time.")
