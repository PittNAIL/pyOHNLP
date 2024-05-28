import json
import tqdm
import os
import psycopg2
import tqdm
import multiprocessing

from functools import partial

from util import parse_args

args = parse_args()
batch_size = 1000

data_to_collate = {'ent' : [], 'negated': [], 'possible' : [], 'hypothetical': [], 'historical': [], 'is exp': [], 'hist exp': [], 'hypo exp': [], 'source': [], 'rule': []}


num_processes = multiprocessing.cpu_count()

def append_ent_data(ent, source):
    return {
        'ent': str(ent),
        'negated': ent._.is_negated,
        'possible': ent._.is_possible,
        'hypothetical': ent._.is_hypothetical,
        'historical': ent._.is_historical,
        'is exp': ent._.is_experiencer,
        'hist exp': ent._.hist_experienced,
        'hypo exp': ent._.hypo_experienced,
        'source': source,
        'rule': str(ent._.literal)
    }

def process_text(text, nlp, source):
    doc = nlp(text)
    results = []
    for ent in doc.ents:
        results.append(append_ent_data(ent, source))
    return results

def process_records(args):
    text, source, nlp, shared_dtc = args
    results = process_text(text, nlp, source)
    for result in results:
        for key in data_to_collate:
            shared_dtc[key].append(result[key])

def collect_data(nlp):


    data_to_collate = {'ent' : [], 'negated': [], 'possible' : [], 'hypothetical': [], 'historical': [], 'is exp': [], 'hist exp': [], 'hypo exp': [], 'source': [], 'rule': []}

    if (args.db_conf == None) & (args.file_path == None):
        raise ValueError("No input to process! --file_path or --db_conf argument needed!")

    if (args.file_path != None):
        if os.path.isdir(args.file_path):
            for file in tqdm.tqdm(os.listdir(args.file_path)):
                with open(os.path.join(args.file_path, file), 'r') as f:
                   txt = f.read()
                process_text(txt, nlp, file)

        if os.path.isfile(args.file_path):
            raise ValueError(".zip and .csv compatibility not yet implemented.")

    if (args.db_conf != None):
        if os.path.isfile(args.db_conf):
            if not (args.db_conf).endswith('.json'):
                raise ValueError("Database Config file must be .json!")

            if (args.db_conf).endswith('json'):
                with open(args.db_conf, 'r') as f:
                    conn_details = json.load(f)['config']

                if conn_details['db_type'] == 'postgresql':
                    db, user, host = conn_details['database'], conn_details['user'], conn_details['host'],
                    password = conn_details['password']
                    connect = psycopg2.connect(database = db, user = user, host = host, password =
                                               password)
                    cursor = connect.cursor()
                    table = conn_details['input_table']
                    text_col = conn_details['text_col']
                    ident = conn_details['id_col']
                    cursor.execute(f"select {text_col}, {ident} from {table} ")

                    manager = multiprocessing.Manager()
                    shared_dtc = manager.dict({key: manager.list() for key in data_to_collate.keys()})

                    with multiprocessing.Pool(num_processes) as pool:
                        while True:
                            records = cursor.fetchmany(batch_size)
                            if not records:
                                break
                            pool.map(process_records, [(record[0], record[1], nlp, shared_dtc) for
                                                                 record in records])

                    cursor.close()
                    connect.close()
                    data_to_collate = {key: list(value) for key, value in shared_dtc.items()}
    return data_to_collate 

