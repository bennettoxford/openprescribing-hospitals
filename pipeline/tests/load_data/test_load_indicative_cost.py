import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from pipeline.load_data.load_indicative_cost import (
    get_unique_vmps,
    extract_indicative_cost_by_vmps,
    clear_existing_data,
    cache_foreign_keys,
    transform_and_load_chunk,
)
from viewer.models import IndicativeCost, VMP, Organisation, Region, ICB


@pytest.fixture
def sample_indicative_cost_data():
    return pd.DataFrame(
        {
            "year_month": ["2024-01-01", "2024-01-01", "2024-02-01"],
            "vmp_code": ["12345", "67890", "12345"],
            "ods_code": ["ORG1", "ORG1", "ORG2"],
            "indicative_cost": [15.50, 25.75, 18.90],
        }
    )


@pytest.fixture
def sample_foreign_keys(db):
    vmps = [
        VMP.objects.create(code="12345", name="Test Drug 1"),
        VMP.objects.create(code="67890", name="Test Drug 2"),
    ]

    region = Region.objects.create(code="REG1", name="Test Region")
    icb = ICB.objects.create(code="ICB1", name="Test ICB", region=region)

    orgs = [
        Organisation.objects.create(
            ods_code="ORG1", ods_name="Test Org 1", region=region, icb=icb
        ),
        Organisation.objects.create(
            ods_code="ORG2", ods_name="Test Org 2", region=region, icb=icb
        ),
    ]

    return {
        "vmps": {vmp.code: vmp.id for vmp in vmps},
        "organisations": {org.ods_code: org.id for org in orgs},
    }


class TestLoadIndicativeCost:
    @patch("pipeline.load_data.load_indicative_cost.get_bigquery_client")
    def test_get_unique_vmps(self, mock_client):
        mock_query = MagicMock()
        mock_query.to_dataframe.return_value = pd.DataFrame(
            {"vmp_code": ["12345", "67890", "11111"]}
        )
        mock_client.return_value.query.return_value = mock_query

        result = get_unique_vmps()

        assert isinstance(result, list)
        assert len(result) == 3
        assert "12345" in result
        assert "67890" in result
        assert "11111" in result

    @patch("pipeline.load_data.load_indicative_cost.get_bigquery_client")
    def test_extract_indicative_cost_by_vmps(
        self, mock_client, sample_indicative_cost_data
    ):
        mock_query = MagicMock()
        mock_query.to_dataframe.return_value = sample_indicative_cost_data
        mock_client.return_value.query.return_value = mock_query

        result = extract_indicative_cost_by_vmps(["12345", "67890"], 1, 2)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert all(
            col in result.columns
            for col in [
                "year_month",
                "vmp_code",
                "ods_code",
                "indicative_cost",
            ]
        )

    @pytest.mark.django_db
    def test_clear_existing_data(self):
        vmp1 = VMP.objects.create(code="12345", name="Test Drug 1")
        vmp2 = VMP.objects.create(code="67890", name="Test Drug 2")
        region = Region.objects.create(code="REG1", name="Test Region")
        icb = ICB.objects.create(code="ICB1", name="Test ICB", region=region)
        org = Organisation.objects.create(
            ods_code="ORG1", ods_name="Test Org", region=region, icb=icb
        )

        IndicativeCost.objects.create(
            vmp=vmp1, organisation=org, data=[["2024-01-01", "15.50"]]
        )
        IndicativeCost.objects.create(
            vmp=vmp2, organisation=org, data=[["2024-02-01", "18.90"]]
        )

        deleted_count = clear_existing_data()

        assert deleted_count == 2
        assert IndicativeCost.objects.count() == 0

    @pytest.mark.django_db
    def test_cache_foreign_keys(self, sample_foreign_keys):
        result = cache_foreign_keys()

        assert isinstance(result, dict)
        assert "vmps" in result
        assert "organisations" in result
        assert len(result["vmps"]) == 2
        assert len(result["organisations"]) == 2

    @pytest.mark.django_db
    def test_transform_and_load_chunk(
        self, sample_indicative_cost_data, sample_foreign_keys
    ):
        result = transform_and_load_chunk(
            sample_indicative_cost_data, sample_foreign_keys, 1, 2
        )

        assert isinstance(result, dict)
        assert "created" in result
        assert "updated" in result
        assert "skipped" in result
        assert result["created"] > 0

        indicative_costs = IndicativeCost.objects.all()
        assert indicative_costs.count() > 0

        cost_record = indicative_costs.first()
        assert isinstance(cost_record.data, list)
        assert len(cost_record.data) > 0
        assert all(
            isinstance(entry, list) and len(entry) == 2
            for entry in cost_record.data
        )

    @pytest.mark.django_db
    def test_transform_and_load_chunk_invalid_data(self, sample_foreign_keys):
        invalid_data = pd.DataFrame(
            {
                "year_month": [None, "2024-01-01", "2024-02-01"],
                "vmp_code": ["12345", None, "99999"],
                "ods_code": ["ORG1", "ORG1", "ORG3"],
                "indicative_cost": [15.50, 25.75, None],
            }
        )

        result = transform_and_load_chunk(invalid_data, sample_foreign_keys, 1, 2)

        assert result["skipped"] > 0
        assert result["created"] >= 0

    @pytest.mark.django_db
    def test_transform_and_load_chunk_missing_foreign_keys(self):
        """Test handling when foreign keys are missing"""
        data = pd.DataFrame(
            {
                "year_month": ["2024-01-01"],
                "vmp_code": ["99999"],  # Not in database
                "ods_code": ["ORG999"],  # Not in database
                "indicative_cost": [15.50],
            }
        )

        foreign_keys = {"vmps": {}, "organisations": {}}

        result = transform_and_load_chunk(data, foreign_keys, 1, 1)

        assert result["created"] == 0
        assert result["skipped"] == 1

    @pytest.mark.django_db
    def test_transform_and_load_chunk_replacement(self, sample_foreign_keys):

        sample_data = pd.DataFrame(
            {
                "year_month": ["2024-01-01"],
                "vmp_code": ["12345"],
                "ods_code": ["ORG1"],
                "indicative_cost": [15.50],
            }
        )

        result1 = transform_and_load_chunk(sample_data, sample_foreign_keys, 1, 2)
        assert result1["created"] == 1
        assert IndicativeCost.objects.count() == 1

        updated_data = pd.DataFrame(
            {
                "year_month": ["2024-02-01"],
                "vmp_code": ["12345"],
                "ods_code": ["ORG1"],
                "indicative_cost": [18.90],
            }
        )

        transform_and_load_chunk(updated_data, sample_foreign_keys, 1, 2)

        assert IndicativeCost.objects.count() == 1

    @pytest.mark.django_db
    def test_clear_existing_data_chunking(self):

        vmps = []
        orgs = []
        
        region = Region.objects.create(code="REG1", name="Test Region")
        icb = ICB.objects.create(code="ICB1", name="Test ICB", region=region)
        
        for i in range(5):
            vmps.append(VMP.objects.create(code=f"VMP{i}", name=f"Test Drug {i}"))
            orgs.append(Organisation.objects.create(
                ods_code=f"ORG{i}", ods_name=f"Test Org {i}", region=region, icb=icb
            ))

        count = 0
        for vmp in vmps:
            for org in orgs:
                if count >= 15:
                    break
                IndicativeCost.objects.create(
                    vmp=vmp,
                    organisation=org,
                    data=[[f"2024-{count+1:02d}-01", "15.50"]],
                )
                count += 1

        initial_count = IndicativeCost.objects.count()
        assert initial_count == 15

        deleted_count = clear_existing_data()

        assert deleted_count == 15
        assert IndicativeCost.objects.count() == 0

    @pytest.mark.django_db
    def test_data_type_conversion(self, sample_foreign_keys):

        data = pd.DataFrame(
            {
                "year_month": ["2024-01-01", "2024-02-01"],
                "vmp_code": ["12345", "12345"],
                "ods_code": ["ORG1", "ORG1"],
                "indicative_cost": [15.505, np.float64(25.999)],
            }
        )

        result = transform_and_load_chunk(data, sample_foreign_keys, 1, 1)

        assert result["created"] == 1  # Should be grouped into one record

        cost_record = IndicativeCost.objects.first()
   
        for entry in cost_record.data:
            cost_str = entry[1]
            assert isinstance(cost_str, str)
            cost_float = float(cost_str)
            assert cost_float in [15.5, 26.0]
