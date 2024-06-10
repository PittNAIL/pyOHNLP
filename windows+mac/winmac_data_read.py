import json
import os
import psycopg2
import multiprocessing
import pandas as pd
import threading
import medspacy

from util import parse_args, set_extensions, compile_target_rules, get_context_rules

CONTEXT_ATTRS = {
    "NEG": {"is_negated": True},
    "POSS": {"is_possible": True},
    "HYPO": {"is_hypothetical": True},
    "HIST": {"is_historical": True},
    "EXP_FAMILY": {"fam_experiencer": True},
    "HISTEXP": {"hist_experienced": True},
    "HYPOEXP": {"hypo_experienced": True},
    "DOSE": {"dose_exp": True},
}

set_extensions(CONTEXT_ATTRS)

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
        "dose_status": ent._.dose_exp,
    } | md

def process_text(text, nlp, source, md):
    doc = nlp(text)
    results = []
    for ent in doc.ents:
        results.append(append_ent_data(ent, source, md))
    return results

def process_records(args):
    fixit()
    text, source, nlp, shared_dtc, md = args
    results = process_text(text, nlp, source, md)
    for result in results:
        with lock:
            for key in shared_dtc.keys():
                shared_dtc[key].append(result[key])

def fixit():
    args = parse_args()
    with open(args.db_conf, "r") as f:
        conf = json.load(f)
    nlp = medspacy.load()
    nlp.remove_pipe("medspacy_context")
    CONTEXT_ATTRS = {
        "NEG": {"is_negated": True},
        "POSS": {"is_possible": True},
        "HYPO": {"is_hypothetical": True},
        "HIST": {"is_historical": True},
        "EXP_FAMILY": {"fam_experiencer": True},
        "HISTEXP": {"hist_experienced": True},
        "HYPOEXP": {"hypo_experienced": True},
        "DOSE": {"dose_exp": True},
    }
    set_extensions(CONTEXT_ATTRS)
    context_rules_list = get_context_rules(args.context_file)
    context = nlp.add_pipe("medspacy_context", config={"rules": None, "span_attrs": CONTEXT_ATTRS})
    context.add(context_rules_list)

    rule_files = [
        os.path.join(conf["ruleset_dir"], file) for file in os.listdir(conf["ruleset_dir"])
    ]
    target_matcher = nlp.get_pipe("medspacy_target_matcher")
    for file in rule_files:
        target_matcher.add(compile_target_rules(file))

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
    }

    if metadata is not None:
        for md in metadata:
            data_to_collate[md] = []

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

        if os.path.isfile(args.file_path):
            if args.file_path.endswith(".csv"):
                chunk_size = 100
                for chunk in pd.read_csv(args.file_path, chunksize=chunk_size):
                    note_text = [(row[row_to_read], {md: row[md] for md in metadata}) for _, row in chunk.iterrows()]
                    args_list = [(text[0], args.file_path, nlp, shared_dtc, text[1]) for text in note_text]
                    pool = multiprocessing.Pool(processes=num_processes)
                    pool.map(process_records, args_list)
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

                    while True:
                        records = cursor.fetchmany(batch_size)
                        if not records:
                            break
                        pool = multiprocessing.Pool(processes=num_processes)
                        pool.map(
                            process_records,
                            [(record[0], record[1], nlp, shared_dtc) for record in records],
                        )

                    connect.close()

    return {key: list(value) for key, value in shared_dtc.items()}

