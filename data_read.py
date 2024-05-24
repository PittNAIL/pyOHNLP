import pandas as pd
import json
import tqdm
import os
import psycopg2

from util import parse_args

args = parse_args()
batch_size = 10

data_to_collate = {
'ent' : [], 'negated': [], 'possible' : [], 'hypothetical': [], 'historical': [], 'is exp': [], 'hist exp': [], 'hypo exp': [], 'source': [], 'rule': []}

def append_ent_data(ent, source):
    data_to_collate['ent'].append(ent),
    data_to_collate['negated'].append(ent._.is_negated),
    data_to_collate['possible'].append(ent._.is_possible),
    data_to_collate['hypothetical'].append(ent._.is_hypothetical),
    data_to_collate['historical'].append(ent._.is_historical),
    data_to_collate['is exp'].append(ent._.is_experiencer),
    data_to_collate['hist exp'].append(ent._.hist_experienced),
    data_to_collate['hypo exp'].append(ent._.hypo_experienced),
    data_to_collate['source'].append(source),
    data_to_collate['rule'].append(ent._.literal)



def collect_data(nlp):

    if (args.db_conf == None) & (args.file_path == None):
        raise ValueError("No input to process! --file_path or --db_conf argument needed!")

    if (args.file_path != None):
        if os.path.isdir(args.file_path):
            for file in tqdm.tqdm(os.listdir(args.file_path)):
                with open(os.path.join(args.file_path, file), 'r') as f:
                   txt = f.read()
                doc = nlp(txt)

                for ent in doc.ents:
                    append_ent_data(ent, file)

        if os.path.isfile(args.file_path):
            raise ValueError(".zip and .csv compatibility not yet implemented.")

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
                cursor.execute(f"select {text_col}, {ident} from {table} limit 10")
                records = cursor.fetchall()
                for record in records:
                    doc = nlp(record[0])
                    source = record[1]
                    for ent in doc.ents:
                        append_ent_data(ent, source)
                cursor.close()
                connect.close()

    return data_to_collate

