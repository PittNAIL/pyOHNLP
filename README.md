### MediTag
[MedSpaCy][medspacy] based NER for clinical notes using ConText algorithm modifiers. Supports
read/write capabilities for local filesystems (.txt, .zip, .csv) and SQL databases (PostgreSQL,
SQLite)

## How To:
# File system:

# Databases (PostgreSQL only as of 5/23/24):
Connecting to databases can be done with a .json file with your desired configuration, a template is
provided below:
```json
{"config" : {
    "db_type": "postgresql",
    "database": "DB_NAME",
    "user": "DB_USER",
    "password": "USER_PASSWORD",
    "host": "HOST",
    "input_table": "TABLE_TO_GET_NOTES_FROM",
    "text_col": "TEXT_COLUMN_OF_TABLE",
    "id_col": "IDENTIFIER_COLUMN_OF_TABLE"
    }
}
```

Execution of the pipeline with your database can be achieved with the command
```python
python loader.py --ruleset_dir <PATH TO RULESET> --context_file <PATH TO CONTEXT FILE> --db_conf
<PATH TO DB CONFIG>
```
## Implementation
MediTag has been implemented by [Jordan Hilsman][jordan] from [PittNail][pn] at the [University of Pittsburgh][pitt].

[medspacy]: https://github.com/medspacy/medspacy
[jordan]: https://github.com/jordanhilsman
[pn]: https://pittnail.github.io
[pitt]: https://shrs.pitt.edu
