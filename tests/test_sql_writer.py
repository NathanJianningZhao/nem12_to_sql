from datetime import datetime
from decimal import Decimal
from io import StringIO

from sql_writer import SQLBatchWriter


def test_sql_writer_flush_writes_insert_statement() -> None:
    output = StringIO()
    writer = SQLBatchWriter(output, batch_size=1000)

    writer.add_row(
        {
            "nmi": "NEM1201009",
            "timestamp": datetime(2005, 3, 1, 0, 0),
            "consumption": Decimal("0.461"),
        }
    )
    writer.add_row(
        {
            "nmi": "NEM1201009",
            "timestamp": datetime(2005, 3, 1, 0, 30),
            "consumption": Decimal("0.810"),
        }
    )

    writer.close()

    sql = output.getvalue()

    assert 'INSERT INTO meter_readings (id, nmi, "timestamp", consumption) VALUES' in sql
    assert "gen_random_uuid()" in sql
    assert "'NEM1201009'" in sql
    assert "'2005-03-01 00:00:00'" in sql
    assert "'2005-03-01 00:30:00'" in sql
    assert "0.461" in sql
    assert "0.810" in sql
    assert sql.strip().endswith(";")