# NEM12 to SQL

## Overview

This project parses NEM12 meter data files and generates SQL `INSERT` statements for interval consumption data.

The processing pipeline is:

1. Read the file sequentially.
2. Use `200` records to establish the active NMI and interval length.
3. Reconstruct wrapped multi-line `300` records into logical records.
4. Validate record structure and interval payloads.
5. Expand interval values into timestamped rows.
6. Write rows as batched SQL `INSERT` statements.

The implementation uses the Python standard library. `pytest` is used for tests.

## How to Run

The current entry point is `main.py`.

```bash
python3 main.py
```

The script currently reads from `sample.txt` and writes SQL output to `output.sql`.

## How to Run Tests

Install test dependencies:

```bash
python3 -m pip install -r requirements.txt
```

Run the test suite:

```bash
python3 -m pytest
```

## Design Decisions

- Streaming processing: the parser reads the input file line by line and does not load the full file into memory.
- Logical record buffering: wrapped `300` records are reconstructed before validation so downstream code operates on complete daily interval records.
- Context-driven processing: `200` records establish the active NMI and interval length used to interpret subsequent `300` records.
- Validation is separated from parsing: parsing extracts raw fields, while validation converts and normalizes values.
- Error handling is split by severity:
  - fatal structural issues stop processing
  - invalid `300` records are treated as recoverable and skipped
- SQL output is batched to reduce write overhead.
- Logging is structured through the standard `logging` module with a consistent formatter.

## Assumptions and Limitations

- The current entry point uses fixed input and output file names in `main.py`.
- Interval lengths are limited to `5`, `15`, and `30` minutes.
- A valid `300` record must contain exactly one day of interval values for the active interval length.
- Dates must be in `YYYYMMDD` format.
- Consumption values must be numeric and non-negative.
- Output is generated for a table named `meter_readings` with columns `(id, nmi, "timestamp", consumption)`.
- The generated SQL uses `gen_random_uuid()` for `id`, so the target database is expected to provide that function.
