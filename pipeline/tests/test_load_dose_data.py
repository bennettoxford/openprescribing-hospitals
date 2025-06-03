import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from pipeline.flows.load_dose_data import (
    get_unique_vmps_with_dose_data,
    extract_dose_data_by_vmps,
    clear_existing_dose_data,
    cache_foreign_keys,
    transform_and_load_chunk,
    ensure_proper_types,
)
from viewer.models import Dose, SCMDQuantity, VMP, Organisation


@pytest.fixture
def sample_dose_data():
    return pd.DataFrame(
        {
            "year_month": ["2024-01-01", "2024-01-01", "2024-02-01"],
            "vmp_code": ["12345", "67890", "12345"],
            "ods_code": ["ORG1", "ORG1", "ORG2"],
            "dose_quantity": [1.0, 2.0, 3.0],
            "dose_unit": ["pre-filled disposable injection", "capsules", "spoonful"],
            "scmd_quantity": [10.0, 2.0, 15.0],
            "scmd_basis_unit_name": ["ml", "capsules", "ml"],
        }
    )


@pytest.fixture
def sample_foreign_keys(db):
    vmps = [
        VMP.objects.create(code="12345", name="Test Drug 1"),
        VMP.objects.create(code="67890", name="Test Drug 2"),
    ]

    orgs = [
        Organisation.objects.create(
            ods_code="ORG1", ods_name="Test Org 1", region="Test Region"
        ),
        Organisation.objects.create(
            ods_code="ORG2", ods_name="Test Org 2", region="Test Region"
        ),
    ]

    return {
        "vmps": {vmp.code: vmp.id for vmp in vmps},
        "organisations": {org.ods_code: org.id for org in orgs},
    }


class TestLoadDoseData:
    def test_ensure_proper_types(self):
        test_data = [
            [pd.Timestamp("2024-01-01"), np.float64(1.5), "pre-filled disposable injection"],
            ["2024-02-01", "2.0", "capsules"],
            [pd.Timestamp("2024-03-01"), 3, "spoonful"],
        ]

        result = ensure_proper_types(test_data)

        for entry in result:
            assert isinstance(entry[0], str)
            assert isinstance(entry[1], float)
            assert isinstance(entry[2], str)

    @patch("pipeline.flows.load_dose_data.get_bigquery_client")
    def test_get_unique_vmps_with_dose_data(self, mock_client):
        mock_query = MagicMock()
        mock_query.to_dataframe.return_value = pd.DataFrame(
            {"vmp_code": ["12345", "67890"]}
        )
        mock_client.return_value.query.return_value = mock_query

        result = get_unique_vmps_with_dose_data()

        assert isinstance(result, list)
        assert len(result) == 2
        assert "12345" in result
        assert "67890" in result

    @patch("pipeline.flows.load_dose_data.get_bigquery_client")
    def test_extract_dose_data_by_vmps(self, mock_client, sample_dose_data):
        mock_query = MagicMock()
        mock_query.to_dataframe.return_value = sample_dose_data
        mock_client.return_value.query.return_value = mock_query

        result = extract_dose_data_by_vmps(["12345", "67890"], 1, 2)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert all(
            col in result.columns
            for col in [
                "year_month",
                "vmp_code",
                "ods_code",
                "dose_quantity",
                "dose_unit",
                "scmd_quantity",
                "scmd_basis_unit_name",
            ]
        )

    @pytest.mark.django_db
    def test_clear_existing_dose_data(self):
        vmp = VMP.objects.create(code="12345", name="Test Drug")
        org = Organisation.objects.create(
            ods_code="ORG1", ods_name="Test Org", region="Test"
        )

        Dose.objects.create(vmp=vmp, organisation=org, data=[["2024-01-01", 1.5, "mg"]])
        SCMDQuantity.objects.create(
            vmp=vmp, organisation=org, data=[["2024-01-01", 10.0, "tablets"]]
        )

        dose_deleted, scmd_deleted = clear_existing_dose_data()

        assert dose_deleted == 1
        assert scmd_deleted == 1
        assert Dose.objects.count() == 0
        assert SCMDQuantity.objects.count() == 0

    @pytest.mark.django_db
    def test_cache_foreign_keys(self, sample_foreign_keys):
        result = cache_foreign_keys()

        assert isinstance(result, dict)
        assert "vmps" in result
        assert "organisations" in result
        assert len(result["vmps"]) == 2
        assert len(result["organisations"]) == 2

    @pytest.mark.django_db
    def test_transform_and_load_chunk(self, sample_dose_data, sample_foreign_keys):
        result = transform_and_load_chunk(sample_dose_data, sample_foreign_keys, 1, 2)

        assert isinstance(result, dict)
        assert result["dose_created"] > 0
        assert result["scmd_created"] > 0

        doses = Dose.objects.all()
        scmd = SCMDQuantity.objects.all()

        # We expect 2 unique VMP/org combinations from sample data:
        # VMP 12345 with ORG1, VMP 12345 with ORG2, VMP 67890 with ORG1
        assert doses.count() == 3
        assert scmd.count() == 3

        dose = doses.first()
        assert isinstance(dose.data, list)
        assert len(dose.data) > 0
        assert all(isinstance(entry, list) and len(entry) == 3 for entry in dose.data)

        all_dose_data = []
        for dose in doses:
            all_dose_data.extend(dose.data)
        
        dose_units = [entry[2] for entry in all_dose_data]
        expected_units = ["pre-filled disposable injection", "capsules", "spoonful"]
        for unit in dose_units:
            assert unit in expected_units

    @pytest.mark.django_db
    def test_transform_and_load_chunk_invalid_data(self, sample_foreign_keys):
        invalid_data = pd.DataFrame(
            {
                "year_month": [None, "2024-01-01"],
                "vmp_code": ["12345", None],
                "ods_code": ["ORG1", "ORG1"],
                "dose_quantity": [1.5, 2.0],
                "dose_unit": ["pre-filled disposable injection", "capsules"],
                "scmd_quantity": [None, None],
                "scmd_basis_unit_name": [None, None],
            }
        )

        result = transform_and_load_chunk(invalid_data, sample_foreign_keys, 1, 2)

        assert result["skipped"] > 0
        assert result["dose_created"] == 0
        assert result["scmd_created"] == 0
