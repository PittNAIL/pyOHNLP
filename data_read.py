import json
import tqdm
import os
import psycopg2
import multiprocessing

from util import parse_args

args = parse_args()
batch_size = 1000

data_to_collate = {'ent' : [], 'negated': [], 'possible' : [], 'hypothetical': [], 'historical': [], 'is exp': [], 'hist exp': [], 'hypo exp': [], 'source': [], 'rule': []}


num_processes = multiprocessing.cpu_count()

def append_dtc(ent, source, dtc):
    dtc['ent'].append(str(ent))
    dtc['negated'].append(ent._.is_negated)
    dtc['possible'].append(ent._.is_possible)
    dtc['hypothetical'].append(ent._.is_hypothetical)
    dtc['historical'].append(ent._.is_historical)
    dtc['is exp'].append(ent._.is_experiencer)
    dtc['hist exp'].append(ent._.hist_experienced)
    dtc['hypo exp'].append(ent._.hypo_experienced)
    dtc['source'].append(source)
    dtc['rule'].append(str(ent._.literal))

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

    if (args.db_conf is None) & (args.file_path is None):
        raise ValueError("No input to process! --file_path or --db_conf argument needed!")

    if (args.file_path is not None):
        if os.path.isdir(args.file_path):
            for file in tqdm.tqdm(os.listdir(args.file_path)):
                with open(os.path.join(args.file_path, file), 'r') as f:
                   txt = f.read()
                doc = nlp(txt)
                for ent in doc.ents:
                    append_dtc(ent, file, data_to_collate)

        if os.path.isfile(args.file_path):
            raise ValueError(".zip and .csv compatibility not yet implemented.")

    if (args.db_conf is not None):
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
                    cursor.execute(f"select {text_col}, {ident} from {table}")

                    manager = multiprocessing.Manager()
                    shared_dtc = manager.dict({key: manager.list() for key in data_to_collate.keys()})

                    with multiprocessing.Pool(num_processes) as pool:
                        while True:
                            records = cursor.fetchmany(batch_size)
                            if not records:
                                break
                            pool.map(process_records, [(record[0], record[1], nlp,
                                                        shared_dtc) for
                                                                 record in records])

                    cursor.close()
                    connect.close()
                    data_to_collate = {key: list(value) for key, value in shared_dtc.items()}

    return data_to_collate

