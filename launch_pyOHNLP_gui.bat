@echo off
REM Run the Python script for the GUI
python .\gui\root_window.py

REM Prompt the user for the path of the file or folder
set /p file_dir=What is the path of the file/folder you want to run pyOHNLP on?

REM Run the main Python script with the provided file path
python main.py --db_conf .\user_conf.json --file_path %file_dir%

pause
