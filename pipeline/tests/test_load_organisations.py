import pytest

from unittest.mock import patch
from pipeline.flows.load_organisations import (
    extract_organisations,
    transform_organisations,
    load_organisations,
)
from viewer.models import Organisation


@pytest.fixture
def sample_bigquery_data():
    """Fixture providing sample BigQuery response data"""
    return [
        {
            "ods_code": "ABC123",
            "ods_name": "Test Hospital 1",
            "region": "North",
            "icb": "ICB1",
            "successors": ["DEF456"],
            "ultimate_successors": ["DEF456"],
        },
        {
            "ods_code": "DEF456",
            "ods_name": "Test Hospital 2",
            "region": "South",
            "icb": "ICB2",
            "successors": [],
            "ultimate_successors": [],
        },
        {
            "ods_code": "GHI789",
            "ods_name": "Test Hospital 3",
            "region": "East",
            "icb": "ICB3",
            "successors": [],
            "ultimate_successors": [],
        },
    ]


@pytest.fixture
def sample_transformed_data():
    """Fixture providing sample transformed data"""
    return [
        {
            "ods_code": "ABC123",
            "ods_name": "Test Hospital 1",
            "region": "North",
            "icb": "ICB1",
            "successor_code": "DEF456",
        },
        {
            "ods_code": "DEF456",
            "ods_name": "Test Hospital 2",
            "region": "South",
            "icb": "ICB2",
            "successor_code": None,
        },
        {
            "ods_code": "GHI789",
            "ods_name": "Test Hospital 3",
            "region": "East",
            "icb": "ICB3",
            "successor_code": None,
        },
    ]


class TestLoadOrganisations:
    @patch("pipeline.flows.load_organisations.execute_bigquery_query")
    def test_extract_organisations(self, mock_execute_query, sample_bigquery_data):
        mock_execute_query.return_value = sample_bigquery_data

        result = extract_organisations()

        mock_execute_query.assert_called_once()
        assert isinstance(result, list)
        assert len(result) == 3
        assert all(isinstance(org, dict) for org in result)
        assert all(
            key in result[0]
            for key in [
                "ods_code",
                "ods_name",
                "region",
                "icb",
                "successors",
                "ultimate_successors",
            ]
        )

    def test_transform_organisations(self, sample_bigquery_data):
        """Test the transform_organisations task"""
        result = transform_organisations(sample_bigquery_data)

        assert isinstance(result, list)
        assert len(result) == 3
        assert all(isinstance(org, dict) for org in result)

        assert result[0]["successor_code"] == "DEF456"  # First org has a successor
        assert result[1]["successor_code"] is None  # Second org has no successor
        assert all(
            key in result[0]
            for key in ["ods_code", "ods_name", "region", "icb", "successor_code"]
        )

    @pytest.mark.django_db
    def test_load_organisations_with_real_db(self, sample_transformed_data):

        initial_orgs = [
            Organisation(
                ods_code="OLD123", ods_name="Old Hospital", region="West", icb="ICB_OLD"
            )
        ]
        Organisation.objects.bulk_create(initial_orgs)
        assert Organisation.objects.count() == 1

        result = load_organisations(sample_transformed_data)

        assert result["deleted"] == 1
        assert result["created"] == 3
        assert result["total_records"] == 3

        stored_orgs = Organisation.objects.all().order_by("ods_code")
        assert stored_orgs.count() == 3

        orgs_list = list(stored_orgs)
        assert orgs_list[0].ods_code == "ABC123"
        assert orgs_list[0].ods_name == "Test Hospital 1"
        assert orgs_list[0].region == "North"
        assert orgs_list[0].icb == "ICB1"

        assert (
            orgs_list[0].successor == orgs_list[1]
        )  # ABC123's successor should be DEF456
        assert orgs_list[1].successor is None  # DEF456 should have no successor
        assert orgs_list[2].successor is None  # GHI789 should have no successor
