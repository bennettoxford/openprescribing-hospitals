import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from pipeline.flows.load_dose_data import (
    get_dose_calculation_logic,
    get_unique_vmps_with_dose_data,
    extract_dose_data_by_vmps,
    clear_existing_dose_data,
    cache_foreign_keys,
    transform_and_load_chunk,
    load_dose_logic,
    ensure_proper_types,
)
from viewer.models import Dose, SCMDQuantity, VMP, Organisation, CalculationLogic, Region, ICB


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
def sample_dose_logic_dict():
    return {
        "12345": "Dose calculated from unit dose form size and unit dose unit of measure for Test Drug 1",
        "67890": "Dose calculated from unit dose form size and unit dose unit of measure for Test Drug 2",
    }


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
    def test_get_dose_calculation_logic(self, mock_client):
        with patch("pipeline.flows.load_dose_data.task", lambda x: x):
            mock_query = MagicMock()
            mock_query.to_dataframe.return_value = pd.DataFrame({
                "vmp_code": ["12345", "67890"],
                "logic": [
                    "Dose calculated from unit dose form size and unit dose unit of measure for Test Drug 1",
                    "Dose calculated from unit dose form size and unit dose unit of measure for Test Drug 2"
                ]
            })
            mock_client.return_value.query.return_value = mock_query

            result = get_dose_calculation_logic()

            assert isinstance(result, dict)
            assert len(result) == 2
            assert result["12345"] == "Dose calculated from unit dose form size and unit dose unit of measure for Test Drug 1"
            assert result["67890"] == "Dose calculated from unit dose form size and unit dose unit of measure for Test Drug 2"

    @patch("pipeline.flows.load_dose_data.get_bigquery_client")
    def test_get_unique_vmps_with_dose_data(self, mock_client):
        with patch("pipeline.flows.load_dose_data.task", lambda x: x):
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
        with patch("pipeline.flows.load_dose_data.task", lambda x: x):
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
            assert "calculation_logic" not in result.columns

    @pytest.mark.django_db
    def test_clear_existing_dose_data(self):
        with patch("pipeline.flows.load_dose_data.task", lambda x: x):
            vmp = VMP.objects.create(code="12345", name="Test Drug")
            region = Region.objects.create(code="REG1", name="Test Region")
            icb = ICB.objects.create(code="ICB1", name="Test ICB", region=region)
            org = Organisation.objects.create(
                ods_code="ORG1", ods_name="Test Org", region=region, icb=icb
            )

            Dose.objects.create(vmp=vmp, organisation=org, data=[["2024-01-01", 1.5, "mg"]])
            SCMDQuantity.objects.create(
                vmp=vmp, organisation=org, data=[["2024-01-01", 10.0, "tablets"]]
            )
            CalculationLogic.objects.create(
                vmp=vmp, logic_type="dose", logic="Test logic", ingredient=None
            )

            dose_deleted, scmd_deleted, logic_deleted = clear_existing_dose_data()

            assert dose_deleted == 1
            assert scmd_deleted == 1
            assert logic_deleted == 1
            assert Dose.objects.count() == 0
            assert SCMDQuantity.objects.count() == 0
            assert CalculationLogic.objects.filter(logic_type="dose").count() == 0

    @pytest.mark.django_db
    def test_cache_foreign_keys(self, sample_foreign_keys):
        with patch("pipeline.flows.load_dose_data.task", lambda x: x):
            result = cache_foreign_keys()

            assert isinstance(result, dict)
            assert "vmps" in result
            assert "organisations" in result
            assert len(result["vmps"]) == 2
            assert len(result["organisations"]) == 2

    @pytest.mark.django_db
    def test_load_dose_logic(self, sample_dose_data, sample_foreign_keys, sample_dose_logic_dict):
        with patch("pipeline.flows.load_dose_data.task", lambda x: x):
            result = load_dose_logic(
                sample_dose_data, sample_foreign_keys, sample_dose_logic_dict, 1, 2
            )

            assert isinstance(result, dict)
            assert "logic_created" in result
            assert "logic_conflicts" in result
            assert result["logic_created"] == 2
            assert result["logic_conflicts"] == 0

            logic_records = CalculationLogic.objects.filter(logic_type="dose")
            assert logic_records.count() == 2

            logic_values = {record.vmp.code: record.logic for record in logic_records}
            assert logic_values["12345"] == "Dose calculated from unit dose form size and unit dose unit of measure for Test Drug 1"
            assert logic_values["67890"] == "Dose calculated from unit dose form size and unit dose unit of measure for Test Drug 2"
            
            for logic_record in logic_records:
                assert logic_record.ingredient is None

    @pytest.mark.django_db
    def test_load_dose_logic_missing_logic(self, sample_foreign_keys):
        with patch("pipeline.flows.load_dose_data.task", lambda x: x):
            test_data = pd.DataFrame({
                "year_month": ["2024-01-01", "2024-01-01"],
                "vmp_code": ["12345", "99999"],
                "ods_code": ["ORG1", "ORG1"],
                "dose_quantity": [1.0, 2.0],
                "dose_unit": ["capsules", "tablets"],
                "scmd_quantity": [10.0, 20.0],
                "scmd_basis_unit_name": ["ml", "ml"],
            })
            
            # Only provide logic for one VMP
            logic_dict = {"12345": "Test logic for 12345"}

            result = load_dose_logic(
                test_data, sample_foreign_keys, logic_dict, 1, 2
            )

            assert result["logic_created"] == 1
            assert result["logic_conflicts"] == 0

            logic_records = CalculationLogic.objects.filter(logic_type="dose")
            assert logic_records.count() == 1
            assert logic_records.first().vmp.code == "12345"

    @pytest.mark.django_db
    def test_load_dose_logic_empty_data(self, sample_foreign_keys, sample_dose_logic_dict):
        with patch("pipeline.flows.load_dose_data.task", lambda x: x):
            empty_df = pd.DataFrame()
            
            result = load_dose_logic(
                empty_df, sample_foreign_keys, sample_dose_logic_dict, 1, 2
            )

            assert result["logic_created"] == 0
            assert result["logic_conflicts"] == 0

    @pytest.mark.django_db
    def test_transform_and_load_chunk(self, sample_dose_data, sample_foreign_keys, sample_dose_logic_dict):
        with patch("pipeline.flows.load_dose_data.task", lambda x: x):
            result = transform_and_load_chunk(
                sample_dose_data, sample_foreign_keys, sample_dose_logic_dict, 1, 2
            )

            assert isinstance(result, dict)
            assert result["dose_created"] > 0
            assert result["scmd_created"] > 0
            assert "logic_created" in result
            assert "logic_conflicts" in result

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

            logic_records = CalculationLogic.objects.filter(logic_type="dose")
            assert logic_records.count() == 2

    @pytest.mark.django_db
    def test_transform_and_load_chunk_invalid_data(self, sample_foreign_keys, sample_dose_logic_dict):
        with patch("pipeline.flows.load_dose_data.task", lambda x: x):
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

            result = transform_and_load_chunk(
                invalid_data, sample_foreign_keys, sample_dose_logic_dict, 1, 2
            )

            assert result["skipped"] > 0
            assert result["dose_created"] == 0
            assert result["scmd_created"] == 0
            # Logic still created for valid VMP codes even if dose/scmd data is invalid
            assert "logic_created" in result
            assert "logic_conflicts" in result

    @pytest.mark.django_db
    def test_transform_and_load_chunk_empty_data(self, sample_foreign_keys, sample_dose_logic_dict):
        with patch("pipeline.flows.load_dose_data.task", lambda x: x):
            empty_data = pd.DataFrame()

            result = transform_and_load_chunk(
                empty_data, sample_foreign_keys, sample_dose_logic_dict, 1, 2
            )

            assert result["dose_created"] == 0
            assert result["scmd_created"] == 0
            assert result["skipped"] == 0
            assert result["logic_created"] == 0
            assert result["logic_conflicts"] == 0
