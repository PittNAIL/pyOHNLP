import csv
import json
import psycopg2

from io import StringIO

from util import parse_args

args = parse_args()

with open(args.db_conf, "r") as f:
    config = json.load(f)

def write_to_db(dataframe, config_path):
    columns = list(dataframe.columns)
    output = StringIO()
    dataframe.to_csv(output, index=False, header=True)
    output.seek(0)
    
    with open(config_path, "r") as f:
        config = json.load(f)["write_to"]

    db_type = config["db_type"]
    if db_type != "postgresql":
        raise ValueError("Only postgresql supported at the moment.")
    
    db_config = config[db_type]

    db, user, host, password = (
        db_config["database"],
        db_config["user"],
        db_config["host"],
        db_config["password"],
    )
    table = db_config["to_table"]
    if config["db_type"] == "postgresql":
        connect = psycopg2.connect(database=db, user=user, host=host, password=password)
        cursor = connect.cursor()
        cursor.copy_expert(
            f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH CSV HEADER", output
        )
        connect.commit()
        cursor.close()
        connect.close()
    if config["db_type"] != "postgresql":
        raise ValueError("Only postgresql supported at the moment.")
