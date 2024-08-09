#!/bin/bash
#
python ./gui/root_window.py

python main.py --db_conf ./user_conf.json --file_path /home/jordan/Downloads/eval_files/HOUSING_DATA_GOLDSTANDARD.csv
