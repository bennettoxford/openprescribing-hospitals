import csv
from datetime import date, datetime
from decimal import Decimal

import pytest

from pipeline.sample.dump_sample_data import (
    QUERIES_DIR,
    dump_sample_data,
    format_cell,
    load_queries,
    write_csv,
)

def test_load_queries_reads_every_query_file():
    queries = load_queries(QUERIES_DIR)

    expected = sorted(
        path.stem + ".csv"
        for path in QUERIES_DIR.glob("*.sql")
        if not path.name.startswith("_")
    )
    assert [name for name, _query in queries] == expected
    for _name, query in queries:
        assert "SELECT" in query
        assert "{{" not in query
        assert not query.endswith(";")


def test_load_queries_includes_fragments(tmp_path):
    (tmp_path / "_bounds.sql").write_text("bounds AS (SELECT 1 AS n)\n")
    (tmp_path / "trusts.sql").write_text("WITH {{ bounds }}\nSELECT n FROM bounds;\n")

    assert load_queries(tmp_path) == [
        ("trusts.csv", "WITH bounds AS (SELECT 1 AS n)\nSELECT n FROM bounds")
    ]


def test_load_queries_includes_a_nested_scope(tmp_path):
    (tmp_path / "_bounds.sql").write_text("bounds AS (SELECT 1 AS n)\n")
    (tmp_path / "_products.sql").write_text("products AS (SELECT n FROM bounds)\n")
    (tmp_path / "_scope.sql").write_text("{{ bounds }},\n{{ products }}\n")
    (tmp_path / "products.sql").write_text("WITH {{ scope }}\nSELECT n FROM products;\n")

    assert load_queries(tmp_path) == [
        (
            "products.csv",
            "WITH bounds AS (SELECT 1 AS n),\n"
            "products AS (SELECT n FROM bounds)\n"
            "SELECT n FROM products",
        )
    ]


def test_load_queries_rejects_an_unknown_fragment(tmp_path):
    (tmp_path / "trusts.sql").write_text("WITH {{ bounds }}\nSELECT 1;\n")

    with pytest.raises(ValueError, match="unknown fragment"):
        load_queries(tmp_path)


def test_format_cell_matches_the_sample_loader():
    assert format_cell(None) == ""
    assert format_cell(True) == "true"
    assert format_cell(False) == "false"
    assert format_cell(date(2024, 8, 1)) == "2024-08-01"
    assert format_cell(datetime(2024, 8, 1, 0, 0)) == "2024-08-01"
    assert format_cell(Decimal("3.0")) == "3"
    assert format_cell(287600.0) == "287600"


def test_dump_sample_data_writes_query_results(tmp_path):
    queries_dir = tmp_path / "queries"
    queries_dir.mkdir()
    (queries_dir / "data_status.sql").write_text(
        "-- months\nSELECT year_month, file_type FROM status;\n"
    )
    output_dir = tmp_path / "out"

    class Result:
        schema = [_Field("year_month"), _Field("file_type")]

        def __iter__(self):
            yield {
                "year_month": date(2024, 8, 1),
                "file_type": "final",
            }

    class Job:
        def result(self, timeout):
            assert timeout == 600
            return Result()

    class Client:
        def __init__(self):
            self.queries = []

        def query(self, query):
            self.queries.append(query)
            return Job()

    client = Client()
    written = dump_sample_data(queries_dir, output_dir, client)

    assert client.queries == ["-- months\nSELECT year_month, file_type FROM status"]
    assert written == [output_dir / "data_status.csv"]
    with written[0].open(newline="", encoding="utf-8") as handle:
        assert list(csv.DictReader(handle)) == [
            {"year_month": "2024-08-01", "file_type": "final"}
        ]


def test_write_csv_replaces_the_destination(tmp_path):
    path = tmp_path / "trusts.csv"
    path.write_text("old\n")

    write_csv(path, ["in_shelford_group"], [{"in_shelford_group": True}])

    assert path.read_text() == "in_shelford_group\ntrue\n"
    assert not path.with_suffix(".csv.tmp").exists()


class _Field:
    def __init__(self, name):
        self.name = name
