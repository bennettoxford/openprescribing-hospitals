import pytest

from unittest.mock import patch
from pipeline.flows.load_organisations import (
    extract_organisations,
    transform_organisations,
    create_trust_types,
    load_organisations,
)
from viewer.models import Organisation, TrustType, Region, ICB


@pytest.fixture
def sample_bigquery_data():
    """Fixture providing sample BigQuery response data"""
    return [
        {
            "ods_code": "ABC123",
            "ods_name": "Test Hospital 1",
            "region": "North",
            "region_code": "REG001",
            "icb": "ICB North",
            "icb_code": "ICB1",
            "successors": ["DEF456"],
            "ultimate_successors": ["DEF456"],
            "trust_type": "ACUTE - TEACHING",
        },
        {
            "ods_code": "DEF456",
            "ods_name": "Test Hospital 2",
            "region": "South",
            "region_code": "REG002",
            "icb": "ICB South",
            "icb_code": "ICB2",
            "successors": [],
            "ultimate_successors": [],
            "trust_type": "COMMUNITY",
        },
        {
            "ods_code": "GHI789",
            "ods_name": "Test Hospital 3",
            "region": "East",
            "region_code": "REG003",
            "icb": "ICB East",
            "icb_code": "ICB3",
            "successors": [],
            "ultimate_successors": [],
            "trust_type": None,
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
            "region_code": "REG001",
            "icb": "ICB North",
            "icb_code": "ICB1",
            "successor_code": "DEF456",
            "trust_type": "ACUTE - TEACHING",
        },
        {
            "ods_code": "DEF456",
            "ods_name": "Test Hospital 2",
            "region": "South",
            "region_code": "REG002",
            "icb": "ICB South",
            "icb_code": "ICB2",
            "successor_code": None,
            "trust_type": "COMMUNITY",
        },
        {
            "ods_code": "GHI789",
            "ods_name": "Test Hospital 3",
            "region": "East",
            "region_code": "REG003",
            "icb": "ICB East",
            "icb_code": "ICB3",
            "successor_code": None,
            "trust_type": None,
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
                "region_code",
                "icb",
                "icb_code",
                "successors",
                "ultimate_successors",
                "trust_type",
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
            for key in ["ods_code", "ods_name", "region", "region_code", "icb", "icb_code", "successor_code", "trust_type"]
        )

    @pytest.mark.django_db
    def test_create_trust_types(self, sample_transformed_data):
        """Test the create_trust_types task"""
        result = create_trust_types(sample_transformed_data)
        
        assert isinstance(result, dict)
        assert len(result) == 2  # Should have 2 unique trust types
        assert "ACUTE - TEACHING" in result
        assert "COMMUNITY" in result
        
        assert TrustType.objects.count() == 2
        assert TrustType.objects.filter(name="ACUTE - TEACHING").exists()
        assert TrustType.objects.filter(name="COMMUNITY").exists()
   
    @pytest.mark.django_db
    def test_load_organisations_with_real_db(self, sample_transformed_data):

        old_region = Region.objects.create(code="REG_OLD", name="West")
        old_icb = ICB.objects.create(code="ICB_OLD", name="ICB Old", region=old_region)
        initial_orgs = [
            Organisation(
                ods_code="OLD123", ods_name="Old Hospital", region=old_region, icb=old_icb
            )
        ]
        Organisation.objects.bulk_create(initial_orgs)
        assert Organisation.objects.count() == 1

        trust_type_lookup = create_trust_types(sample_transformed_data)
        assert len(trust_type_lookup) == 2

        result = load_organisations(sample_transformed_data, trust_type_lookup)

        assert result["deleted"] == 1
        assert result["created"] == 3
        assert result["total_records"] == 3

        stored_orgs = Organisation.objects.all().order_by("ods_code")
        assert stored_orgs.count() == 3

        orgs_list = list(stored_orgs)
        assert orgs_list[0].ods_code == "ABC123"
        assert orgs_list[0].ods_name == "Test Hospital 1"
        assert orgs_list[0].region.name == "North"
        assert orgs_list[0].region.code == "REG001"
        assert orgs_list[0].icb.name == "ICB North"
        assert orgs_list[0].icb.code == "ICB1"
        assert orgs_list[0].trust_type.name == "ACUTE - TEACHING"

        assert orgs_list[1].trust_type.name == "COMMUNITY"
        assert orgs_list[2].trust_type is None  # GHI789 has no trust type

        assert (
            orgs_list[0].successor == orgs_list[1]
        )  # ABC123's successor should be DEF456
        assert orgs_list[1].successor is None  # DEF456 should have no successor
        assert orgs_list[2].successor is None  # GHI789 should have no successor
