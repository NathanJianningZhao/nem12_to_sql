from pathlib import Path

from parser import parse_file
from transformer import expand_300_record
from validator import validate_200_record, validate_300_record


def test_end_to_end_parsing_validation_and_transformation(tmp_path: Path) -> None:
    sample_file = tmp_path / "sample.txt"
    sample_file.write_text(
        "100,NEM12,200506081149,UNITEDDP,NEMMCO\n"
        "200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610\n"
        "300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,\n"
        "1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,\n"
        "0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,A,,,20050310121004,20050310182204\n"
        "900\n",
        encoding="utf-8",
    )

    current_nmi = None
    current_interval = None
    output_rows = []

    for record in parse_file(str(sample_file)):
        if record["type"] == "200":
            validated_200 = validate_200_record(record)
            current_nmi = validated_200["nmi"]
            current_interval = validated_200["interval"]

        elif record["type"] == "300":
            validated_300 = validate_300_record(record, current_interval)
            output_rows.extend(
                expand_300_record(current_nmi, current_interval, validated_300)
            )

    assert len(output_rows) == 48
    assert output_rows[0]["nmi"] == "NEM1201009"
    assert output_rows[0]["timestamp"].strftime("%Y-%m-%d %H:%M:%S") == "2005-03-01 00:00:00"
    assert output_rows[-1]["timestamp"].strftime("%Y-%m-%d %H:%M:%S") == "2005-03-01 23:30:00"
