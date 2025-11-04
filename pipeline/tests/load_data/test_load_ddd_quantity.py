import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pipeline.load_data.load_ddd_quantity import (
    get_ddd_calculation_logic,
    get_all_vmps_with_ddd_logic,
    get_unique_vmps_with_ddd_data,
    extract_ddd_data_by_vmps,
    clear_existing_ddd_data,
    cache_foreign_keys,
    transform_and_load_ddd_quantity_chunk,
    load_ddd_logic_for_vmps,
)
from viewer.models import (
    DDDQuantity,
    VMP,
    Organisation,
    CalculationLogic,
    Region,
    ICB,
)


@pytest.fixture
def sample_ddd_data():
    return pd.DataFrame(
        {
            "vmp_code": ["12345", "67890", "12345"],
            "year_month": ["2024-01-01", "2024-01-01", "2024-02-01"],
            "ods_code": ["ORG1", "ORG1", "ORG2"],
            "ddd_quantity": [1.5, 2.0, 3.0],
            "ddd_value": [1.0, 2.0, 1.0],
            "ddd_unit": ["mg", "g", "mg"],
        }
    )


@pytest.fixture
def sample_ddd_logic_dict():
    return {
        "12345": "DDD calculated using WHO DDD value for Test Drug 1",
        "67890": "DDD calculated using WHO DDD value for Test Drug 2",
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


class TestLoadDDDQuantity:
    @patch("pipeline.load_data.load_ddd_quantity.get_bigquery_client")
    def test_get_ddd_calculation_logic(self, mock_client):
        with patch("pipeline.load_data.load_ddd_quantity.task", lambda x: x):
            mock_query = MagicMock()
            mock_query.to_dataframe.return_value = pd.DataFrame({
                "vmp_code": ["12345", "67890"],
                "logic": [
                    "DDD calculated using WHO DDD value for Test Drug 1",
                    "DDD calculated using WHO DDD value for Test Drug 2"
                ]
            })
            mock_client.return_value.query.return_value = mock_query

            result = get_ddd_calculation_logic()

            assert isinstance(result, dict)
            assert len(result) == 2
            assert result["12345"] == "DDD calculated using WHO DDD value for Test Drug 1"
            assert result["67890"] == "DDD calculated using WHO DDD value for Test Drug 2"

    @patch("pipeline.load_data.load_ddd_quantity.get_bigquery_client")
    def test_get_all_vmps_with_ddd_logic(self, mock_client):
        with patch("pipeline.load_data.load_ddd_quantity.task", lambda x: x):
            mock_query = MagicMock()
            mock_query.to_dataframe.return_value = pd.DataFrame(
                {"vmp_code": ["12345", "67890", "11111"]}
            )
            mock_client.return_value.query.return_value = mock_query

            result = get_all_vmps_with_ddd_logic()

            assert isinstance(result, list)
            assert len(result) == 3
            assert "12345" in result
            assert "67890" in result
            assert "11111" in result

    @patch("pipeline.load_data.load_ddd_quantity.get_bigquery_client")
    def test_get_unique_vmps_with_ddd_data(self, mock_client):
        with patch("pipeline.load_data.load_ddd_quantity.task", lambda x: x):
            mock_query = MagicMock()
            mock_query.to_dataframe.return_value = pd.DataFrame(
                {"vmp_code": ["12345", "67890"]}
            )
            mock_client.return_value.query.return_value = mock_query

            result = get_unique_vmps_with_ddd_data()

            assert isinstance(result, list)
            assert len(result) == 2
            assert "12345" in result
            assert "67890" in result

    @patch("pipeline.load_data.load_ddd_quantity.get_bigquery_client")
    def test_extract_ddd_data_by_vmps(self, mock_client, sample_ddd_data):
        with patch("pipeline.load_data.load_ddd_quantity.task", lambda x: x):
            mock_query = MagicMock()
            mock_query.to_dataframe.return_value = sample_ddd_data
            mock_client.return_value.query.return_value = mock_query

            result = extract_ddd_data_by_vmps(["12345", "67890"], 1, 2)

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 3
            assert all(
                col in result.columns
                for col in [
                    "vmp_code",
                    "year_month",
                    "ods_code",
                    "ddd_quantity",
                    "ddd_value",
                    "ddd_unit",
                ]
            )
            assert "calculation_logic" not in result.columns

    @pytest.mark.django_db
    def test_clear_existing_ddd_data(self):
        with patch("pipeline.load_data.load_ddd_quantity.task", lambda x: x):
            vmp = VMP.objects.create(code="12345", name="Test Drug")
            region = Region.objects.create(code="REG1", name="Test Region")
            icb = ICB.objects.create(code="ICB1", name="Test ICB", region=region)
            org = Organisation.objects.create(
                ods_code="ORG1", ods_name="Test Org", region=region, icb=icb
            )

            DDDQuantity.objects.create(
                vmp=vmp, 
                organisation=org, 
                data=[["2024-01-01", "1.5", "DDD (1.0 mg)"]]
            )
            CalculationLogic.objects.create(
                vmp=vmp, logic_type="ddd", logic="Test DDD logic", ingredient=None
            )

            deleted_count, logic_deleted_count = clear_existing_ddd_data()

            assert deleted_count == 1
            assert logic_deleted_count == 1
            assert DDDQuantity.objects.count() == 0
            assert CalculationLogic.objects.filter(logic_type="ddd").count() == 0

    @pytest.mark.django_db
    def test_cache_foreign_keys(self):
        with patch("pipeline.load_data.load_ddd_quantity.task", lambda x: x):
            vmp1 = VMP.objects.create(code="12345", name="Test Drug 1")
            vmp2 = VMP.objects.create(code="67890", name="Test Drug 2")
            region = Region.objects.create(code="REG1", name="Test Region")
            icb = ICB.objects.create(code="ICB1", name="Test ICB", region=region)
            org1 = Organisation.objects.create(
                ods_code="ORG1", ods_name="Test Org 1", region=region, icb=icb
            )
            org2 = Organisation.objects.create(
                ods_code="ORG2", ods_name="Test Org 2", region=region, icb=icb
            )

            result = cache_foreign_keys()

            assert isinstance(result, dict)
            assert "vmps" in result
            assert "organisations" in result
            
            assert result["vmps"]["12345"] == vmp1.id
            assert result["vmps"]["67890"] == vmp2.id
            assert result["organisations"]["ORG1"] == org1.id
            assert result["organisations"]["ORG2"] == org2.id

    @pytest.mark.django_db
    def test_load_ddd_logic_for_vmps(self, sample_foreign_keys, sample_ddd_logic_dict):
        with patch("pipeline.load_data.load_ddd_quantity.task", lambda x: x):
            vmp_codes = ["12345", "67890"]
            
            result = load_ddd_logic_for_vmps(
                vmp_codes, sample_foreign_keys, sample_ddd_logic_dict, 1, 2
            )

            assert isinstance(result, dict)
            assert "logic_created" in result
            assert "logic_missing" in result
            assert result["logic_created"] == 2
            assert result["logic_missing"] == 0

            logic_records = CalculationLogic.objects.filter(logic_type="ddd")
            assert logic_records.count() == 2

            logic_values = {record.vmp.code: record.logic for record in logic_records}
            assert logic_values["12345"] == "DDD calculated using WHO DDD value for Test Drug 1"
            assert logic_values["67890"] == "DDD calculated using WHO DDD value for Test Drug 2"
            
            for logic_record in logic_records:
                assert logic_record.ingredient is None

    @pytest.mark.django_db
    def test_load_ddd_logic_for_vmps_missing_logic(self, sample_foreign_keys):
        with patch("pipeline.load_data.load_ddd_quantity.task", lambda x: x):
            vmp_codes = ["12345", "99999"]
            
            # Only provide logic for one VMP
            logic_dict = {"12345": "Test logic for 12345"}

            result = load_ddd_logic_for_vmps(
                vmp_codes, sample_foreign_keys, logic_dict, 1, 2
            )

            assert result["logic_created"] == 1
            assert result["logic_missing"] == 1

            logic_records = CalculationLogic.objects.filter(logic_type="ddd")
            assert logic_records.count() == 1
            assert logic_records.first().vmp.code == "12345"

    @pytest.mark.django_db
    def test_load_ddd_logic_for_vmps_empty_list(self, sample_foreign_keys, sample_ddd_logic_dict):
        with patch("pipeline.load_data.load_ddd_quantity.task", lambda x: x):
            vmp_codes = []
            
            result = load_ddd_logic_for_vmps(
                vmp_codes, sample_foreign_keys, sample_ddd_logic_dict, 1, 2
            )

            assert result["logic_created"] == 0
            assert result["logic_missing"] == 0

    @pytest.mark.django_db
    def test_transform_and_load_ddd_quantity_chunk(
        self, sample_ddd_data, sample_foreign_keys
    ):
        with patch("pipeline.load_data.load_ddd_quantity.task", lambda x: x):
            result = transform_and_load_ddd_quantity_chunk(
                sample_ddd_data, sample_foreign_keys, 1, 2
            )

            assert isinstance(result, dict)
            assert result["created"] > 0
            assert "skipped" in result
            assert DDDQuantity.objects.count() > 0

            ddd_quantity = DDDQuantity.objects.first()
            assert isinstance(ddd_quantity.data, list)
            assert len(ddd_quantity.data) > 0
            assert all(
                isinstance(entry, list) and len(entry) == 3
                for entry in ddd_quantity.data
            )

    @pytest.mark.django_db
    def test_transform_and_load_ddd_quantity_chunk_empty_data(self, sample_foreign_keys):
        with patch("pipeline.load_data.load_ddd_quantity.task", lambda x: x):
            empty_df = pd.DataFrame()
            
            result = transform_and_load_ddd_quantity_chunk(
                empty_df, sample_foreign_keys, 1, 1
            )

            assert result["created"] == 0
            assert result["skipped"] == 0
            assert DDDQuantity.objects.count() == 0

    @pytest.mark.django_db
    def test_transform_and_load_ddd_quantity_chunk_invalid_foreign_keys(
        self, sample_ddd_data
    ):
        with patch("pipeline.load_data.load_ddd_quantity.task", lambda x: x):
            empty_cache = {"vmps": {}, "organisations": {}}
            
            result = transform_and_load_ddd_quantity_chunk(
                sample_ddd_data, empty_cache, 1, 1
            )

            assert result["created"] == 0
            assert DDDQuantity.objects.count() == 0

    @pytest.mark.django_db 
    def test_transform_and_load_ddd_quantity_chunk_missing_data(self, sample_foreign_keys):
        with patch("pipeline.load_data.load_ddd_quantity.task", lambda x: x):
            invalid_data = pd.DataFrame({
                "vmp_code": ["12345", None, "67890"],
                "year_month": ["2024-01-01", "2024-01-01", None], 
                "ods_code": ["ORG1", "ORG1", "ORG2"],
                "ddd_quantity": [1.5, 2.0, None],
                "ddd_value": [1.0, 2.0, 1.0],
                "ddd_unit": ["mg", "g", "mg"],
            })
            
            result = transform_and_load_ddd_quantity_chunk(
                invalid_data, sample_foreign_keys, 1, 1
            )

            assert result["created"] == 1
            assert result["skipped"] == 2 
            assert DDDQuantity.objects.count() == 1
