import pytest
import pandas as pd

from unittest.mock import patch
from pipeline.load_data.load_ddd import (
    extract_ddd_data,
    transform_ddd_data,
    load_ddd_data,
)
from viewer.models import DDD, VMP, WHORoute


@pytest.fixture
def sample_ddd_bigquery_data():
    return pd.DataFrame(
        {
            "vmp_code": ["12345", "67890", "11111", "22222"],
            "vmp_name": ["Drug A", "Drug B", "Drug C", "Drug D"],
            "selected_ddd_value": [1.5, 2.0, None, 3.0],
            "selected_ddd_unit": ["mg", "g", None, "ml"],
            "selected_ddd_route_code": ["O", "P", None, "IM"],
        }
    )


@pytest.fixture
def sample_extracted_data():
    return [
        {
            "vmp_code": "12345",
            "vmp_name": "Drug A",
            "ddd": 1.5,
            "ddd_unit": "mg",
            "who_route": "O",
        },
        {
            "vmp_code": "67890",
            "vmp_name": "Drug B",
            "ddd": 2.0,
            "ddd_unit": "g",
            "who_route": "P",
        },
    ]


@pytest.fixture
def sample_transformed_data():
    return [
        {
            "vmp_code": "12345",
            "vmp_name": "Drug A",
            "ddd": 1.5,
            "unit_type": "mg",
            "who_route": "O",
        },
        {
            "vmp_code": "67890",
            "vmp_name": "Drug B",
            "ddd": 2.0,
            "unit_type": "g",
            "who_route": "P",
        },
    ]


class TestLoadDDD:
    @patch("pipeline.load_data.load_ddd.fetch_table_data_from_bq")
    def test_extract_ddd_data(self, mock_fetch, sample_ddd_bigquery_data):

        mock_fetch.return_value = sample_ddd_bigquery_data

        result = extract_ddd_data()

        mock_fetch.assert_called_once()
        assert isinstance(result, list)
        assert len(result) == 3

        extracted_codes = {item["vmp_code"] for item in result}
        assert "12345" in extracted_codes
        assert "67890" in extracted_codes
        assert "11111" not in extracted_codes
        assert "22222" in extracted_codes

        first_record = result[0]
        assert all(
            key in first_record
            for key in ["vmp_code", "vmp_name", "ddd", "ddd_unit", "who_route"]
        )

    def test_transform_ddd_data(self, sample_extracted_data):
        result = transform_ddd_data(sample_extracted_data)

        assert isinstance(result, list)
        assert len(result) == 2

        first_record = result[0]
        assert first_record["unit_type"] == "mg"
        assert isinstance(first_record["ddd"], float)

        required_fields = {"vmp_code", "vmp_name", "ddd", "unit_type", "who_route"}
        assert all(field in first_record for field in required_fields)

    @pytest.mark.django_db
    def test_load_ddd_data(self, sample_transformed_data):

        who_routes = [
            WHORoute.objects.create(code="O", name="Oral"),
            WHORoute.objects.create(code="P", name="Parenteral"),
        ]

        vmps = [
            VMP.objects.create(code="12345", name="Drug A"),
            VMP.objects.create(code="67890", name="Drug B"),
        ]

        initial_ddd = DDD.objects.create(
            vmp=vmps[0], ddd=1.0, unit_type="mg", who_route=who_routes[0]
        )

        result = load_ddd_data(sample_transformed_data)

        assert result["deleted"] == 1
        assert result["created"] == 2
        assert result["skipped"] == 0
        assert result["total_records"] == 2

        ddds = DDD.objects.all().order_by("vmp__code")
        assert ddds.count() == 2

        first_ddd = ddds[0]
        assert first_ddd.vmp.code == "12345"
        assert first_ddd.ddd == 1.5
        assert first_ddd.unit_type == "mg"
        assert first_ddd.who_route.code == "O"

        second_ddd = ddds[1]
        assert second_ddd.vmp.code == "67890"
        assert second_ddd.ddd == 2.0
        assert second_ddd.unit_type == "g"
        assert second_ddd.who_route.code == "P"

    @pytest.mark.django_db
    def test_load_ddd_data_missing_references(self, sample_transformed_data):

        WHORoute.objects.create(code="O", name="Oral")
        WHORoute.objects.create(code="P", name="Parenteral")
        VMP.objects.create(code="12345", name="Drug A")

        result = load_ddd_data(sample_transformed_data)

        assert result["deleted"] == 0
        assert result["created"] == 1
        assert result["skipped"] == 1
        assert result["total_records"] == 2

        ddds = DDD.objects.all()
        assert ddds.count() == 1

        ddd = ddds[0]
        assert ddd.vmp.code == "12345"
        assert ddd.who_route.code == "O"

    @pytest.mark.django_db
    def test_load_ddd_data_missing_who_routes(self, sample_transformed_data):

        VMP.objects.create(code="12345", name="Drug A")
        VMP.objects.create(code="67890", name="Drug B")

        with pytest.raises(ValueError) as exc_info:
            load_ddd_data(sample_transformed_data)

        assert "Missing WHO routes in database" in str(exc_info.value)
        assert DDD.objects.count() == 0
