# pyOHNLP
The pyOHNLP Toolkit is a python-based open health natural language processing software using
[MedSpaCy][medspacy] as a framework for NER in clinical notes using ConText algorithm modifiers. Supports
read/write capabilities for local filesystems (.txt, .zip, .csv) and SQL databases (PostgreSQL,
SQLite)

<p align=center>
    <img src="https://img.shields.io/badge/build-partial-orange" alt="Build Status">
    <img src="https://img.shields.io/badge/Version-1.12-green" alt="Version">
    <img src="https://img.shields.io/badge/license-MIT-green" alt="MIT License">
</p>

# How To:
## File system:

Reading from a filesystem (either a folder with .txt files, or directly from a .csv or .zip file)
can be performed with the following command:

```python
python loader.py --db_conf <CONFIG JSON FILE> --file_path <PATH TO FOLDER OR FILE>
```

## Databases (PostgreSQL and SQLite only as of 6/21/24):
Connecting to databases can be done with a .json file with your desired configuration, a template is
provided below:

```json
{"read_from" : {
    "db_type": "postgresql",
    "database": "DB_NAME",
    "user": "DB_USER",
    "password": "USER_PASSWORD",
    "host": "HOST",
    "input_table": "TABLE_TO_GET_NOTES_FROM",
    "text_col": "TEXT_COLUMN_OF_TABLE",
    "id_col": "IDENTIFIER_COLUMN_OF_TABLE",
    "meta_data": "LIST_OF_METADATA_COLS"
    },
"write_to": {
    "db_type": "postgresql",
    "database": "DB_NAME",
    "user": "DB_USER",
    "password": "USER_PASSWORD",
    "host": "HOST",
    "to_table": "OUTPUT_WRITE_TABLE",
    "to_csv" : "TRUE or FALSE",
    },
"ruleset_dir": "DIRECTORY TO RULESET",
"enact": "TRUE or FALSE"
}

```

Execution of the pipeline with your database can be achieved with the command
NOTE: The ruleset_dir and context_file arguments are optional, as there is a default context file,
and the ruleset_dir can be included in your db_conf.json.
```python
python loader.py --db_conf <PATH TO DB CONFIG>
```

# Not Yet Implemented:
Compatibility with .zip files has yet to be added, as well as writing to/from mysql databases.

# Implementation
MediTag has been implemented by [Jordan Hilsman][jordan] from [PittNail][pn] at the [University of Pittsburgh][pitt].

[medspacy]: https://github.com/medspacy/medspacy
[jordan]: https://jordanhilsman.github.io
[pn]: https://pittnail.github.io
[pitt]: https://shrs.pitt.edu
