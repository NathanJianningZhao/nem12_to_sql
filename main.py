import logging

from exceptions import FatalFileError, RecoverableRecordError
from logging_config import configure_logging
from parser import parse_file
from sql_writer import SQLBatchWriter
from transformer import expand_300_record
from validator import validate_200_record, validate_300_record


logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging()

    input_file_path = "sample.txt"
    output_file_path = "output.sql"

    current_nmi = None
    current_interval = None
    skipped_records = 0

    logger.info("Starting NEM12 processing")
    logger.info("Input file: %s", input_file_path)
    logger.info("Output file: %s", output_file_path)

    try:
        with open(output_file_path, "w", encoding="utf-8") as output_file:
            writer = SQLBatchWriter(output_file, batch_size=1000)

            for record in parse_file(input_file_path):
                if record["type"] == "200":
                    validated_200 = validate_200_record(record)
                    current_nmi = validated_200["nmi"]
                    current_interval = validated_200["interval"]

                    logger.info(
                        "Loaded 200 record context on line %s: NMI=%s interval=%s",
                        record["line_number"],
                        current_nmi,
                        current_interval,
                    )

                elif record["type"] == "300":
                    if current_nmi is None or current_interval is None:
                        raise FatalFileError(
                            f"Line {record['line_number']}: encountered 300 record before any valid 200 record"
                        )

                    try:
                        validated_300 = validate_300_record(record, current_interval)
                    except RecoverableRecordError as exc:
                        skipped_records += 1
                        logger.warning("Skipping invalid 300 record: %s", exc)
                        continue

                    rows = expand_300_record(current_nmi, current_interval, validated_300)
                    writer.add_rows(rows)

                elif record["type"] in {"100", "400", "500", "900"}:
                    logger.debug(
                        "Ignoring unsupported/non-target record type %s on line %s",
                        record["type"],
                        record["line_number"],
                    )

                else:
                    logger.warning(
                        "Skipping unknown record type on line %s",
                        record["line_number"],
                    )

            writer.close()

    except FatalFileError as exc:
        logger.error("Fatal error during processing: %s", exc)
        raise

    logger.info("Finished processing. Skipped records: %s", skipped_records)


if __name__ == "__main__":
    main()