from datetime import datetime
from decimal import Decimal

from transformer import expand_300_record


def test_expand_300_record_generates_expected_timestamps() -> None:
    record = {
        "type": "300",
        "line_number": 2,
        "date": "20050301",
        "values": [
            Decimal("0"),
            Decimal("0.461"),
            Decimal("0.810"),
        ],
    }

    rows = list(expand_300_record("NEM1201009", 30, record))

    assert len(rows) == 3

    assert rows[0]["nmi"] == "NEM1201009"
    assert rows[0]["timestamp"] == datetime(2005, 3, 1, 0, 0)
    assert rows[0]["consumption"] == Decimal("0")

    assert rows[1]["timestamp"] == datetime(2005, 3, 1, 0, 30)
    assert rows[1]["consumption"] == Decimal("0.461")

    assert rows[2]["timestamp"] == datetime(2005, 3, 1, 1, 0)
    assert rows[2]["consumption"] == Decimal("0.810")