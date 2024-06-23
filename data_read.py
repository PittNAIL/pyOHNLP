import json
import os
import psycopg2
import multiprocessing
import time
import logging
import sqlite3

import pandas as pd

from util import parse_args, get_versioning

args = parse_args()
batch_size = 1000
lock = multiprocessing.Lock()

num_processes = multiprocessing.cpu_count()

version = get_versioning("versions.json", args.db_conf)

def append_ent_data(ent, source, md:None, idx):
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
        "dose_status": ent._.dose_dec,
        "source": source,
        "rule": str(ent._.literal),
        "offset": ent.start_char,
        "version": version,
        "index": idx,
        } | md

def process_text(text, nlp, source, md, idx):
    doc = nlp(text)
    results = []
    for ent in doc.ents:
        results.append(append_ent_data(ent, source, md, idx))
    if len(doc.ents) == 0:
        results.append(
            {
                "ent": "no entities found",
                "certainty": "not found",
                "status": "not found",
                "experiencer": "not found",
                "dose_status": "not found",
                "source": "not found",
                "rule": "not found",
                "offset": "not found",
                "version": version,
                "index": idx,
            }
            | md
        )
    return results


def process_records(args):
    text, source, nlp, shared_dtc, md, idx = args
    results = process_text(text, nlp, source, md, idx)
    for result in results:
        with lock:
            for key in shared_dtc.keys():
                shared_dtc[key].append(result[key])


def log_shared_dtc_lengths(shared_dtc):
    while True:
        lengths = {key: len(shared_dtc[key]) for key in shared_dtc.keys()}
        logging.info(f"Lengths of shared_dtc: {lengths}")
        time.sleep(10)


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


def collect_data(nlp):

    with open(args.db_conf, "r") as f:
        config = json.load(f)

    row_to_read, metadata = config["read_from"]["text_col"], config["read_from"]["meta_data"]

    data_to_collate = {
        "ent": [],
        "certainty": [],
        "status": [],
        "experiencer": [],
        "dose_status": [],
        "source": [],
        "rule": [],
        "offset": [],
        "version": [],
        "index": [],
    }

    if metadata is not None:
        for md in metadata:
            data_to_collate[md] = []

    manager = multiprocessing.Manager()
    shared_dtc = manager.dict({key: manager.list() for key in data_to_collate.keys()})

    logger_process = multiprocessing.Process(target=log_shared_dtc_lengths, args=(shared_dtc,))
    logger_process.start()

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
                    note_text = [
                        (row[row_to_read], {md: row[md] for md in metadata}, idx)
                        for idx, row in chunk.iterrows()
                    ]
                    args_list = [
                        (text[0], args.file_path, nlp, shared_dtc, text[1], text[2])
                        for text in note_text
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
                if db_used == "sqlite":
                    connect = sqlite3.connect(database=db)
                table = conn_details["input_table"]
                text_col = conn_details["text_col"]
                ident = conn_details["id_col"]
                grab = [text_col, ident]
                md = conn_details["meta_data"]
                grab.extend(md)
                grab = ", ".join(grab)
                cursor = connect.cursor()
                cursor.execute(f"select {grab} from {table} limit 15")

                with multiprocessing.Pool(num_processes) as pool:
                    while True:
                        records = cursor.fetchmany(batch_size)
                        if not records:
                            break
                        pool.map(
                            process_records,
                            [
                                (
                                    record[0],
                                    table,
                                    nlp,
                                    shared_dtc,
                                    {md[i]: record[i + 2] for i in range(len(md))},
                                    record[1],
                                )
                                for record in records
                            ],
                        )

                connect.close()
                data_to_collate = {key: list(value) for key, value in shared_dtc.items()}

            if (db_used != "postgresql") & (db_used != "sqlite"):
                raise ValueError(
                    f"Database type {db_used} not supported! Please consider using postgresql if you'd like to read or write to database"
                )

    logger_process.terminate()

    return data_to_collate
