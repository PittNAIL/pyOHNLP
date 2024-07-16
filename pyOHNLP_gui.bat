@echo off
rem Run Python script for GUI
python gui\root_window.py

rem Run Python script with arguments
python loader.py --db_conf user_conf.json --file_path "C:\Users\jordan\Downloads\eval_files\HOUSING_DATA_GOLDSTANDARD.csv"
