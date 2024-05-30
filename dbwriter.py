import json
import psycopg2

from util import parse_args

args = parse_args()

with open(args.db_conf, 'r') as f:
    config = json.load(f)

print(config)
