import json
import os
import psycopg2
import multiprocessing

import pandas as pd

from util import parse_args

args = parse_args()
batch_size = 1000
lock = multiprocessing.Lock()

num_processes = multiprocessing.cpu_count()


def append_ent_data(ent, source, md):
    if ent._.is_negated:
        certainty = "Negated"
    elif ent._.is_possible:
        certainty = "Possible"
    elif ent._.is_hypothetical:
        certainty = "Hypothetical"
    else:
        certainty = "Positive"
    if ent._.fam_experiencer:
        experiencer = "Family"
    else:
        experiencer = "Patient"
    if ent._.is_historical:
        status = "History"
    if ent._.hypo_experienced | ent._.hist_experienced:
        status = "Other History"
    else:
        status = "Present"
    return {
        "ent": str(ent),
        "certainty": certainty,
        "status": status,
        "experiencer": experiencer,
        "source": source,
        "rule": str(ent._.literal),
    } | md


def process_text(text, nlp, source, md):
    doc = nlp(text)
    results = []
    for ent in doc.ents:
        results.append(append_ent_data(ent, source, md))
    return results


def process_records(args):
    text, source, nlp, shared_dtc, md = args
    results = process_text(text, nlp, source, md)
    for result in results:
        with lock:
            for key in shared_dtc.keys():
                shared_dtc[key].append(result[key])


def collect_data(nlp):

    with open(args.db_conf, "r") as f:
        config = json.load(f)

    row_to_read, metadata = config["read_from"]["text_col"], config["read_from"]["meta_data"]

    data_to_collate = {
        "ent": [],
        "certainty": [],
        "status": [],
        "experiencer": [],
        "source": [],
        "rule": [],
    }

    if metadata is not None:
        for md in metadata:
            data_to_collate[md] = []

    # TODO: ADD METADATA AND CUSTOM FLAGS, CUSTOM FLAGS MAY BE DOABLE IN LOADER.PY, BUT
    # METADATA WILL NEED TO BE DEFINED IN HERE.

    manager = multiprocessing.Manager()
    shared_dtc = manager.dict({key: manager.list() for key in data_to_collate.keys()})

    if (args.db_conf is None) & (args.file_path is None):
        raise ValueError("No input to process! --file_path or --db_conf argument needed!")

    if args.file_path is not None:
        if os.path.isdir(args.file_path):
            texts = []
            for file in os.listdir(args.file_path):
                with open(os.path.join(args.file_path, file), "r") as f:
                    txt = f.read()
                texts.append((txt, file, nlp, shared_dtc))
            pool = multiprocessing.Pool(processes=num_processes)
            pool.map(process_records, texts)
            data_to_collate = {key: list(value) for key, value in shared_dtc.items()}

        if os.path.isfile(args.file_path):
            if args.file_path.endswith(".csv"):
                chunk_size = 1000
                for chunk in pd.read_csv(args.file_path, chunksize=chunk_size):
                    note_text = [(row[row_to_read], {md: row[md] for md in metadata}) for _, row in
                                 chunk.iterrows()]
                    args_list = [
                        (text[0], args.file_path, nlp, shared_dtc, text[1]) for text in note_text
                    ]
                    pool = multiprocessing.Pool(processes=num_processes)
                    pool.map(process_records, args_list)
                    pool.close()
                    pool.join()
                data_to_collate = {key: list(value) for key, value in shared_dtc.items()}

            else:
                raise ValueError(".zip compatibility not yet implemented.")

    if (args.db_conf is not None) & (args.file_path is None):
        if os.path.isfile(args.db_conf):
            if not (args.db_conf).endswith(".json"):
                raise ValueError("Database Config file must be .json!")

            if (args.db_conf).endswith("json"):
                conn_details = config["read_from"]
                db_used = conn_details["db_type"]

                if db_used == "postgresql":
                    db, user, host = (
                        conn_details["database"],
                        conn_details["user"],
                        conn_details["host"],
                    )
                    password = conn_details["password"]
                    connect = psycopg2.connect(database=db, user=user, host=host, password=password)
                    table = conn_details["input_table"]
                    text_col = conn_details["text_col"]
                    ident = conn_details["id_col"]
                    cursor = connect.cursor()
                    cursor.execute(f"select {text_col}, {ident} from {table} limit 1500")

                    with multiprocessing.Pool(num_processes) as pool:
                        while True:
                            records = cursor.fetchmany(batch_size)
                            if not records:
                                break
                            pool.map(
                                process_records,
                                [(record[0], record[1], nlp, shared_dtc) for record in records],
                            )

                    connect.close()
                    data_to_collate = {key: list(value) for key, value in shared_dtc.items()}

                if db_used != "postgresql":
                    raise ValueError(
                        f"Database type {db_used} not supported! Please consider using postgresql if you'd like to read or write to database"
                    )

    return data_to_collate
