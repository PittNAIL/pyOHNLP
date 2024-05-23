import pandas as pd
import os
import psycopg2

from util import parse_args

args = parse_args()
batch_size = 10

if args.data_source == 'filesystem':
    txt_dir = input('Text file directory:')
    for file in os.listdir():
        #CONNECT THIS TO OUR MAIN PROCESS WHERE WE GET THE STUFF....
        # :-)
if args.data_source == 'database':
    config = input("Directory to your .json config:")
    with open(config, 'r') as f:
        conn_details = json.read(config)['config']
    if conn_details['config']['db_type'] == 'postgresql':
        db, user, host = conn_details['database'], conn_details['user'], conn_details['host'],
        pass = conn_details['password']
        connect = psycopg2.connect(database = db, user = user, host = host, password = pass)
        #Implement batch behavior, sqlite, other connection behavior.
