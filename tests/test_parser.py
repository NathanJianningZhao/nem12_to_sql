from pathlib import Path

from parser import parse_file


def test_parse_file_yields_200_and_300_records(tmp_path: Path) -> None:
    sample_file = tmp_path / "sample.txt"
    sample_file.write_text(
        "200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610\n"
        "300,20050301,0,0,0.461,0.810\n",
        encoding="utf-8",
    )

    records = list(parse_file(str(sample_file)))

    assert len(records) == 2

    assert records[0]["type"] == "200"
    assert records[0]["nmi"] == "NEM1201009"
    assert records[0]["interval"] == "30"

    assert records[1]["type"] == "300"
    assert records[1]["date"] == "20050301"
    assert records[1]["values"] == ["0", "0", "0.461", "0.810"]


def test_parse_file_handles_wrapped_300_record(tmp_path: Path) -> None:
    sample_file = tmp_path / "sample.txt"
    sample_file.write_text(
        "200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610\n"
        "300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,\n"
        "1.\n"
        "271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,A,,,20050310121004,20050310182204\n",
        encoding="utf-8",
    )

    records = list(parse_file(str(sample_file)))

    assert len(records) >= 2
    assert records[1]["type"] == "300"
    assert records[1]["date"] == "20050301"

    values = records[1]["values"]
    assert len(values) == 48
    assert values[0] == "0"
    assert values[11] == "0"
    assert values[12] == "0.461"
    assert values[-1] == "0.231"