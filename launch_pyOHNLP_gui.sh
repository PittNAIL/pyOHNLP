#!/bin/bash
#
python ./gui/root_window.py

echo What is the path of the file/folder you want to run pyOHNLP on?

read file_dir

python main.py --db_conf ./user_conf.json --file_path $file_dir
