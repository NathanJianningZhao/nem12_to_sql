from datetime import datetime, timedelta
from typing import Iterator


def expand_300_record(nmi: str, interval_minutes: int, record: dict) -> Iterator[dict]:
    base_date = datetime.strptime(record["date"], "%Y%m%d")

    for index, value in enumerate(record["values"]):
        timestamp = base_date + timedelta(minutes=index * interval_minutes)

        yield {
            "nmi": nmi,
            "timestamp": timestamp,
            "consumption": value,
        }