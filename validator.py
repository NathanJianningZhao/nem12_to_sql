"""Validate and normalize parsed NEM12 records before transformation."""

from datetime import datetime
from decimal import Decimal, InvalidOperation

from exceptions import FatalFileError, RecoverableRecordError


VALID_INTERVALS = {5, 15, 30}
NMI_LENGTH = 10


# NEM12 interval payloads are daily, so the valid count is derived from the active interval length rather than stored explicitly in the file.
def expected_interval_count(interval_minutes: int) -> int:
    if interval_minutes not in VALID_INTERVALS:
        raise FatalFileError(
            f"Unsupported interval length: {interval_minutes}. Expected one of {sorted(VALID_INTERVALS)}"
        )
    return 1440 // interval_minutes


# Validate the active 200 record context and normalize interval length to int so later stages can treat it as trusted state.
def validate_200_record(record: dict) -> dict:
    raw_nmi = record.get("nmi")
    raw_interval = record.get("interval")
    line_number = record.get("line_number")

    if raw_nmi is None or not str(raw_nmi).strip():
        raise FatalFileError(f"Line {line_number}: 200 record missing NMI")

    nmi = str(raw_nmi).strip()

    if len(nmi) != NMI_LENGTH:
        raise FatalFileError(
            f"Line {line_number}: invalid NMI '{nmi}'. Expected {NMI_LENGTH} characters"
        )

    if raw_interval is None:
        raise FatalFileError(f"Line {line_number}: 200 record missing interval length")

    try:
        interval = int(raw_interval)
    except ValueError as exc:
        raise FatalFileError(
            f"Line {line_number}: invalid interval length '{raw_interval}'"
        ) from exc

    if interval not in VALID_INTERVALS:
        raise FatalFileError(
            f"Line {line_number}: invalid interval length {interval}. Expected one of {sorted(VALID_INTERVALS)}"
        )

    return {
        "type": "200",
        "line_number": line_number,
        "nmi": nmi,
        "interval": interval,
    }


# Validate a 300 record against the active interval context and return parsed Decimal values so timestamp expansion can stay free of data-quality checks.
def validate_300_record(record: dict, interval_minutes: int) -> dict:
    line_number = record.get("line_number")
    date_str = record.get("date")
    raw_values = record.get("values")

    if not date_str:
        raise RecoverableRecordError(f"Line {line_number}: 300 record missing interval date")

    try:
        datetime.strptime(date_str, "%Y%m%d")
    except ValueError as exc:
        raise RecoverableRecordError(
            f"Line {line_number}: invalid date '{date_str}'"
        ) from exc

    if raw_values is None:
        raise RecoverableRecordError(f"Line {line_number}: 300 record missing interval values")

    expected_count = expected_interval_count(interval_minutes)
    actual_count = len(raw_values)

    # A 300 record must represent one complete day for the active interval.
    if actual_count != expected_count:
        raise RecoverableRecordError(
            f"Line {line_number}: 300 record has {actual_count} interval values, expected {expected_count}"
        )

    parsed_values = []
    for index, raw_value in enumerate(raw_values, start=1):
        if raw_value == "":
            raise RecoverableRecordError(
                f"Line {line_number}: empty consumption value at interval {index}"
            )

        try:
            value = Decimal(raw_value)
        except InvalidOperation as exc:
            raise RecoverableRecordError(
                f"Line {line_number}: invalid consumption value '{raw_value}' at interval {index}"
            ) from exc

        # Negative reads are rejected here so the SQL output path only handles semantically valid consumption data.
        if value < 0:
            raise RecoverableRecordError(
                f"Line {line_number}: negative consumption {value} at interval {index}"
            )

        parsed_values.append(value)

    return {
        "type": "300",
        "line_number": line_number,
        "date": date_str,
        "values": parsed_values,
    }
