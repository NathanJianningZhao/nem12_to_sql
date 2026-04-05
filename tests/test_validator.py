from decimal import Decimal

import pytest

from exceptions import FatalFileError, RecoverableRecordError
from validator import expected_interval_count, validate_200_record, validate_300_record


def test_expected_interval_count_for_30_minutes() -> None:
    assert expected_interval_count(30) == 48


def test_validate_200_record_returns_normalized_record() -> None:
    record = {
        "type": "200",
        "line_number": 1,
        "nmi": "NEM1201009",
        "interval": "30",
    }

    validated = validate_200_record(record)

    assert validated["type"] == "200"
    assert validated["line_number"] == 1
    assert validated["nmi"] == "NEM1201009"
    assert validated["interval"] == 30


def test_validate_200_record_raises_for_invalid_interval() -> None:
    record = {
        "type": "200",
        "line_number": 1,
        "nmi": "NEM1201009",
        "interval": "17",
    }

    with pytest.raises(FatalFileError):
        validate_200_record(record)


def test_validate_200_record_raises_for_invalid_nmi_length() -> None:
    record = {
        "type": "200",
        "line_number": 1,
        "nmi": "NEM120100",
        "interval": "30",
    }

    with pytest.raises(FatalFileError):
        validate_200_record(record)


def test_validate_200_record_strips_whitespace_from_nmi() -> None:
    record = {
        "type": "200",
        "line_number": 1,
        "nmi": " NEM1201009 ",
        "interval": "30",
    }

    validated = validate_200_record(record)

    assert validated["nmi"] == "NEM1201009"


def test_validate_200_record_raises_for_missing_interval() -> None:
    record = {
        "type": "200",
        "line_number": 1,
        "nmi": "NEM1201009",
        "interval": None,
    }

    with pytest.raises(FatalFileError):
        validate_200_record(record)


def test_validate_200_record_raises_for_non_numeric_interval() -> None:
    record = {
        "type": "200",
        "line_number": 1,
        "nmi": "NEM1201009",
        "interval": "abc",
    }

    with pytest.raises(FatalFileError):
        validate_200_record(record)


def test_validate_300_record_rejects_wrong_interval_count() -> None:
    record = {
        "type": "300",
        "line_number": 2,
        "date": "20050301",
        "values": ["0"] * 10,
    }

    with pytest.raises(RecoverableRecordError):
        validate_300_record(record, 30)


def test_validate_300_record_rejects_missing_date() -> None:
    record = {
        "type": "300",
        "line_number": 2,
        "date": None,
        "values": ["0"] * 48,
    }

    with pytest.raises(RecoverableRecordError):
        validate_300_record(record, 30)


def test_validate_300_record_accepts_valid_30_minute_day() -> None:
    record = {
        "type": "300",
        "line_number": 2,
        "date": "20050301",
        "values": ["0"] * 48,
    }

    validated = validate_300_record(record, 30)

    assert validated["type"] == "300"
    assert validated["line_number"] == 2
    assert validated["date"] == "20050301"
    assert len(validated["values"]) == 48
    assert validated["values"][0] == Decimal("0")


def test_validate_300_record_rejects_non_numeric_value() -> None:
    record = {
        "type": "300",
        "line_number": 2,
        "date": "20050301",
        "values": ["0"] * 47 + ["abc"],
    }

    with pytest.raises(RecoverableRecordError):
        validate_300_record(record, 30)


def test_validate_300_record_rejects_negative_value() -> None:
    record = {
        "type": "300",
        "line_number": 2,
        "date": "20050301",
        "values": ["0"] * 47 + ["-1.0"],
    }

    with pytest.raises(RecoverableRecordError):
        validate_300_record(record, 30)


def test_validate_300_record_rejects_bad_date() -> None:
    record = {
        "type": "300",
        "line_number": 2,
        "date": "20051301",
        "values": ["0"] * 48,
    }

    with pytest.raises(RecoverableRecordError):
        validate_300_record(record, 30)
