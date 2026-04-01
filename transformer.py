"""Expand validated interval values into timestamped consumption rows."""

from datetime import datetime, timedelta
from typing import Iterator


# Convert one validated 300 record into row-shaped data for SQL writing.
# Each interval advances from the base date by the active interval length.
def expand_300_record(nmi: str, interval_minutes: int, record: dict) -> Iterator[dict]:
    base_date = datetime.strptime(record["date"], "%Y%m%d")

    for index, value in enumerate(record["values"]):
        timestamp = base_date + timedelta(minutes=index * interval_minutes)

        yield {
            "nmi": nmi,
            "timestamp": timestamp,
            "consumption": value,
        }
