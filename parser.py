import logging
from decimal import Decimal, InvalidOperation
from typing import Iterator


logger = logging.getLogger(__name__)

KNOWN_RECORD_TYPES = {"100", "200", "300", "400", "500", "900"}


def _is_numeric(value: str) -> bool:
    try:
        Decimal(value)
    except InvalidOperation:
        return False

    return True


def _extract_interval_values(parts: list[str]) -> list[str]:
    values: list[str] = []

    for part in parts:
        if not _is_numeric(part):
            break

        values.append(part)

    return values


def _parse_record(record_text: str, line_number: int) -> dict:
    parts = record_text.split(",")
    record_type = parts[0]

    if record_type == "200":
        return {
            "type": "200",
            "line_number": line_number,
            "nmi": parts[1] if len(parts) > 1 else None,
            "interval": parts[8] if len(parts) > 8 else None,
        }

    if record_type == "300":
        return {
            "type": "300",
            "line_number": line_number,
            "date": parts[1] if len(parts) > 1 else None,
            "values": _extract_interval_values(parts[2:]),
        }

    return {
        "type": record_type,
        "line_number": line_number,
        "raw": parts,
    }


def parse_file(file_path: str) -> Iterator[dict]:
    logger.info("Opening input file: %s", file_path)

    buffered_record: str | None = None
    buffered_line_number: int | None = None
    buffered_type: str | None = None

    with open(file_path, "r", encoding="utf-8") as file_obj:
        for line_number, raw_line in enumerate(file_obj, start=1):
            stripped_line = raw_line.strip()

            if not stripped_line:
                logger.debug("Skipping blank line %s", line_number)
                continue

            record_type = stripped_line.split(",", 1)[0]

            if record_type in KNOWN_RECORD_TYPES:
                if buffered_record is not None and buffered_line_number is not None:
                    yield _parse_record(buffered_record, buffered_line_number)

                buffered_record = stripped_line
                buffered_line_number = line_number
                buffered_type = record_type
                continue

            if buffered_type == "300" and buffered_record is not None:
                buffered_record += stripped_line
                continue

            if buffered_record is not None and buffered_line_number is not None:
                yield _parse_record(buffered_record, buffered_line_number)
                buffered_record = None
                buffered_line_number = None
                buffered_type = None

            logger.warning("Unknown record type '%s' on line %s", record_type, line_number)
            yield {
                "type": "UNKNOWN",
                "line_number": line_number,
                "raw": stripped_line.split(","),
            }

    if buffered_record is not None and buffered_line_number is not None:
        yield _parse_record(buffered_record, buffered_line_number)
