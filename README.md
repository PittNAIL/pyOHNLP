# MediTag
[MedSpaCy][medspacy] based NER for clinical notes using ConText algorithm modifiers. Supports
read/write capabilities for local filesystems (.txt, .zip, .csv) and SQL databases (PostgreSQL,
SQLite)

<p align=center>
    <img src="https://img.shields.io/badge/build-partial-orange" alt="Build Status">
    <img src="https://img.shields.io/badge/Version-0.37-orange" alt="Version">
    <img src="https://img.shields.io/badge/license-MIT-green" alt="MIT License">
</p>

# How To:
## File system:

Reading from a filesystem (either a folder with .txt files, or directly from a .csv or .zip file)
can be performed with the following command:

```python
python loader.py --ruleset_dir <PATH TO RULESET> --context_file <PATH TO CONTEXT FILE> --file_path
<PATH TO FOLDER OR FILE>
```

## Databases (PostgreSQL only as of 5/23/24):
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
    "to_csv" : "TRUE" or "FALSE", will write output to .csv.
    },
"ruleset_dir": "DIRECTORY TO RULESET"
}

```

Execution of the pipeline with your database can be achieved with the command
NOTE: The ruleset_dir and context_file arguments are optional, as there is a default context file,
and the ruleset_dir can be included in your db_conf.json.
```python
python loader.py --ruleset_dir <PATH TO RULESET> --context_file <PATH TO CONTEXT FILE> --db_conf
<PATH TO DB CONFIG>
```
# Implementation
MediTag has been implemented by [Jordan Hilsman][jordan] from [PittNail][pn] at the [University of Pittsburgh][pitt].

[medspacy]: https://github.com/medspacy/medspacy
[jordan]: https://jordanhilsman.github.io
[pn]: https://pittnail.github.io
[pitt]: https://shrs.pitt.edu
