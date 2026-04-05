"""Write transformed rows as batched SQL INSERT statements."""

import logging
from typing import TextIO


logger = logging.getLogger(__name__)


class SQLBatchWriter:
    # Buffer rows so the output stays compact and write frequency remains low.
    def __init__(self, output_file: TextIO, batch_size: int = 1000) -> None:
        self.output_file = output_file
        self.batch_size = batch_size
        self.buffer: list[dict] = []

    # Add one row and flush when the batch threshold is reached.
    def add_row(self, row: dict) -> None:
        self.buffer.append(row)
        if len(self.buffer) >= self.batch_size:
            self.flush()

    # Accept iterables from the transformer without forcing eager materialization.
    def add_rows(self, rows) -> None:
        for row in rows:
            self.add_row(row)

    # Emit one INSERT statement for the current buffer and then clear it.
    def flush(self) -> None:
        if not self.buffer:
            return

        logger.debug("Flushing SQL batch with %s rows", len(self.buffer))

        self.output_file.write(
            'INSERT INTO meter_readings (id, nmi, "timestamp", consumption) VALUES\n'
        )

        values_sql = []
        for row in self.buffer:
            timestamp_str = row["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
            nmi = row["nmi"]
            consumption = row["consumption"]

            values_sql.append(
                f"(gen_random_uuid(), '{nmi}', '{timestamp_str}', {consumption})"
            )

        self.output_file.write(",\n".join(values_sql))
        self.output_file.write(
            '\nON CONFLICT ("nmi", "timestamp") DO NOTHING;\n\n'
        )
        self.buffer.clear()

    # Flush any remaining buffered rows before the writer goes out of scope.
    def close(self) -> None:
        logger.info("Closing SQL writer")
        self.flush()
