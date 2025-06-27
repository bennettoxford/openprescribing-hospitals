import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from pipeline.flows.load_ingredient_quantity import (
    get_ingredient_calculation_logic,
    get_unique_vmps_with_ingredient_data,
    extract_ingredient_data_by_vmps,
    clear_existing_ingredient_data,
    transform_and_load_chunk,
    load_ingredient_logic,
)
from viewer.models import IngredientQuantity, Ingredient, VMP, Organisation, CalculationLogic


@pytest.fixture
def sample_ingredient_data():
    return pd.DataFrame(
        {
            "vmp_code": ["12345", "67890", "12345"],
            "year_month": ["2024-01-01", "2024-01-01", "2024-02-01"],
            "ods_code": ["ORG1", "ORG1", "ORG2"],
            "ingredients": [
                [
                    {
                        "ingredient_code": "ING1",
                        "ingredient_quantity_basis": 100.0,
                        "ingredient_basis_unit": "mg",
                    },
                    {
                        "ingredient_code": "ING2",
                        "ingredient_quantity_basis": 50.0,
                        "ingredient_basis_unit": "mg",
                    },
                ],
                [
                    {
                        "ingredient_code": "ING3",
                        "ingredient_quantity_basis": 200.0,
                        "ingredient_basis_unit": "ml",
                    }
                ],
                [
                    {
                        "ingredient_code": "ING1",
                        "ingredient_quantity_basis": 150.0,
                        "ingredient_basis_unit": "mg",
                    }
                ],
            ],
        }
    )


@pytest.fixture
def sample_ingredient_logic_dict():
    return {
        ("12345", "ING1"): "Ingredient quantity calculated from strength numerator/denominator for Test Drug 1 + Test Ingredient 1",
        ("12345", "ING2"): "Ingredient quantity calculated from strength numerator/denominator for Test Drug 1 + Test Ingredient 2",
        ("67890", "ING3"): "Ingredient quantity calculated using unit conversion for Test Drug 2 + Test Ingredient 3",
    }


@pytest.fixture
def sample_foreign_keys(db):
    ingredients = [
        Ingredient.objects.create(code="ING1", name="Test Ingredient 1"),
        Ingredient.objects.create(code="ING2", name="Test Ingredient 2"),
        Ingredient.objects.create(code="ING3", name="Test Ingredient 3"),
    ]

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
        "ingredients": {ing.code: ing.id for ing in ingredients},
        "vmps": {vmp.code: vmp.id for vmp in vmps},
        "organisations": {org.ods_code: org.id for org in orgs},
    }


class TestLoadIngredientQuantity:
    @patch("pipeline.flows.load_ingredient_quantity.get_bigquery_client")
    def test_get_ingredient_calculation_logic(self, mock_client):
        with patch("pipeline.flows.load_ingredient_quantity.task", lambda x: x):
            mock_query = MagicMock()
            mock_query.to_dataframe.return_value = pd.DataFrame({
                "vmp_code": ["12345", "12345", "67890"],
                "ingredient_code": ["ING1", "ING2", "ING3"],
                "logic": [
                    "Ingredient quantity calculated from strength numerator/denominator for Test Drug 1 + Test Ingredient 1",
                    "Ingredient quantity calculated from strength numerator/denominator for Test Drug 1 + Test Ingredient 2",
                    "Ingredient quantity calculated using unit conversion for Test Drug 2 + Test Ingredient 3"
                ]
            })
            mock_client.return_value.query.return_value = mock_query

            result = get_ingredient_calculation_logic()

            assert isinstance(result, dict)
            assert len(result) == 3
            assert result[("12345", "ING1")] == "Ingredient quantity calculated from strength numerator/denominator for Test Drug 1 + Test Ingredient 1"
            assert result[("12345", "ING2")] == "Ingredient quantity calculated from strength numerator/denominator for Test Drug 1 + Test Ingredient 2"
            assert result[("67890", "ING3")] == "Ingredient quantity calculated using unit conversion for Test Drug 2 + Test Ingredient 3"

    @patch("pipeline.flows.load_ingredient_quantity.execute_bigquery_query")
    def test_get_unique_vmps_with_ingredient_data(self, mock_execute_query):
        with patch("pipeline.flows.load_ingredient_quantity.task", lambda x: x):
            mock_execute_query.return_value = [{"vmp_code": "12345"}, {"vmp_code": "67890"}]

            result = get_unique_vmps_with_ingredient_data()

            assert isinstance(result, list)
            assert len(result) == 2
            assert "12345" in result
            assert "67890" in result

    @patch("pipeline.flows.load_ingredient_quantity.get_bigquery_client")
    def test_extract_ingredient_data_by_vmps(self, mock_client, sample_ingredient_data):
        with patch("pipeline.flows.load_ingredient_quantity.task", lambda x: x):
            mock_query_job = mock_client.return_value.query.return_value
            mock_query_job.to_dataframe.return_value = sample_ingredient_data

            result = extract_ingredient_data_by_vmps(["12345", "67890"], 1, 2)

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 3
            assert all(
                col in result.columns
                for col in ["vmp_code", "year_month", "ods_code", "ingredients"]
            )

            for _, row in result.iterrows():
                for ingredient in row["ingredients"]:
                    assert "calculation_logic" not in ingredient

    @pytest.mark.django_db
    def test_clear_existing_ingredient_data(self):
        with patch("pipeline.flows.load_ingredient_quantity.task", lambda x: x):
            ingredient = Ingredient.objects.create(code="ING1", name="Test Ingredient")
            vmp = VMP.objects.create(code="12345", name="Test Drug")
            org = Organisation.objects.create(
                ods_code="ORG1", ods_name="Test Org", region="Test"
            )

            IngredientQuantity.objects.create(
                ingredient=ingredient,
                vmp=vmp,
                organisation=org,
                data=[["2024-01-01", "100.0", "mg"]],
            )
            CalculationLogic.objects.create(
                vmp=vmp, 
                ingredient=ingredient, 
                logic_type="ingredient", 
                logic="Test ingredient logic"
            )

            deleted_count, logic_deleted_count = clear_existing_ingredient_data()

            assert deleted_count == 1
            assert logic_deleted_count == 1
            assert IngredientQuantity.objects.count() == 0
            assert CalculationLogic.objects.filter(logic_type="ingredient").count() == 0

    @pytest.mark.django_db
    def test_load_ingredient_logic(self, sample_ingredient_data, sample_foreign_keys, sample_ingredient_logic_dict):
        with patch("pipeline.flows.load_ingredient_quantity.task", lambda x: x):
            sample_ingredient_data["ingredients"] = sample_ingredient_data["ingredients"].apply(lambda x: np.array(x))
            
            result = load_ingredient_logic(
                sample_ingredient_data, sample_foreign_keys, sample_ingredient_logic_dict, 1, 2
            )

            assert isinstance(result, dict)
            assert "logic_created" in result
            assert "logic_conflicts" in result
            assert result["logic_created"] == 3
            assert result["logic_conflicts"] == 0

            logic_records = CalculationLogic.objects.filter(logic_type="ingredient")
            assert logic_records.count() == 3

            for logic_record in logic_records:
                key = (logic_record.vmp.code, logic_record.ingredient.code)
                assert key in sample_ingredient_logic_dict
                assert logic_record.logic == sample_ingredient_logic_dict[key]
                assert logic_record.ingredient is not None

    @pytest.mark.django_db
    def test_load_ingredient_logic_missing_logic(self, sample_foreign_keys):
        with patch("pipeline.flows.load_ingredient_quantity.task", lambda x: x):
            test_data = pd.DataFrame({
                "vmp_code": ["12345", "12345"],
                "year_month": ["2024-01-01", "2024-02-01"],
                "ods_code": ["ORG1", "ORG1"],
                "ingredients": [
                    np.array([
                        {
                            "ingredient_code": "ING1",
                            "ingredient_quantity_basis": 100.0,
                            "ingredient_basis_unit": "mg",
                        }
                    ]),
                    np.array([
                        {
                            "ingredient_code": "ING999",
                            "ingredient_quantity_basis": 150.0,
                            "ingredient_basis_unit": "mg",
                        }
                    ]),
                ],
            })
            
            # Only provide logic for one combination
            logic_dict = {("12345", "ING1"): "Test logic for 12345 + ING1"}

            result = load_ingredient_logic(
                test_data, sample_foreign_keys, logic_dict, 1, 2
            )

            assert result["logic_created"] == 1
            assert result["logic_conflicts"] == 0

            logic_records = CalculationLogic.objects.filter(logic_type="ingredient")
            assert logic_records.count() == 1
            record = logic_records.first()
            assert record.vmp.code == "12345"
            assert record.ingredient.code == "ING1"

    @pytest.mark.django_db
    def test_load_ingredient_logic_empty_data(self, sample_foreign_keys, sample_ingredient_logic_dict):
        with patch("pipeline.flows.load_ingredient_quantity.task", lambda x: x):
            empty_df = pd.DataFrame()
            
            result = load_ingredient_logic(
                empty_df, sample_foreign_keys, sample_ingredient_logic_dict, 1, 2
            )

            assert result["logic_created"] == 0
            assert result["logic_conflicts"] == 0

    @pytest.mark.django_db
    def test_load_ingredient_logic_missing_foreign_keys(self, sample_ingredient_data, sample_ingredient_logic_dict):
        with patch("pipeline.flows.load_ingredient_quantity.task", lambda x: x):
            empty_cache = {"ingredients": {}, "vmps": {}, "organisations": {}}
            
            sample_ingredient_data["ingredients"] = sample_ingredient_data["ingredients"].apply(lambda x: np.array(x))
            
            result = load_ingredient_logic(
                sample_ingredient_data, empty_cache, sample_ingredient_logic_dict, 1, 2
            )

            assert result["logic_created"] == 0
            assert result["logic_conflicts"] == 0

    @pytest.mark.django_db
    def test_transform_and_load_chunk(
        self, sample_ingredient_data, sample_foreign_keys, sample_ingredient_logic_dict
    ):
        with patch("pipeline.flows.load_ingredient_quantity.task", lambda x: x):
            sample_ingredient_data["ingredients"] = sample_ingredient_data[
                "ingredients"
            ].apply(lambda x: np.array(x))

            result = transform_and_load_chunk(
                sample_ingredient_data, sample_foreign_keys, sample_ingredient_logic_dict, 1, 2
            )

            assert isinstance(result, dict)
            assert result["created"] > 0
            assert "logic_created" in result
            assert "logic_conflicts" in result
            assert IngredientQuantity.objects.count() > 0

            iq = IngredientQuantity.objects.first()
            assert isinstance(iq.data, list)
            assert len(iq.data) > 0
            assert all(isinstance(entry, list) and len(entry) == 3 for entry in iq.data)

            logic_records = CalculationLogic.objects.filter(logic_type="ingredient")
            assert logic_records.count() == 3

    @pytest.mark.django_db
    def test_transform_and_load_chunk_invalid_data(self, sample_foreign_keys, sample_ingredient_logic_dict):
        with patch("pipeline.flows.load_ingredient_quantity.task", lambda x: x):
            import numpy as np

            invalid_data = pd.DataFrame(
                {
                    "vmp_code": ["12345"],
                    "year_month": ["2024-01-01"],
                    "ods_code": ["ORG1"],
                    "ingredients": [
                        np.array(
                            [
                                {
                                    "ingredient_code": "INVALID",
                                    "ingredient_quantity_basis": 100.0,
                                    "ingredient_basis_unit": "mg",
                                }
                            ]
                        )
                    ],
                }
            )

            result = transform_and_load_chunk(
                invalid_data, sample_foreign_keys, sample_ingredient_logic_dict, 1, 2
            )

            assert result["created"] == 0
            assert result["skipped"] > 0
            assert "logic_created" in result
            assert "logic_conflicts" in result

    @pytest.mark.django_db
    def test_transform_and_load_chunk_empty_data(self, sample_foreign_keys, sample_ingredient_logic_dict):
        with patch("pipeline.flows.load_ingredient_quantity.task", lambda x: x):
            empty_data = pd.DataFrame()

            result = transform_and_load_chunk(
                empty_data, sample_foreign_keys, sample_ingredient_logic_dict, 1, 2
            )

            assert result["created"] == 0
            assert result["skipped"] == 0
            assert result["logic_created"] == 0
            assert result["logic_conflicts"] == 0
            assert IngredientQuantity.objects.count() == 0

    @pytest.mark.django_db
    def test_transform_and_load_chunk_missing_fields(self, sample_foreign_keys, sample_ingredient_logic_dict):
        with patch("pipeline.flows.load_ingredient_quantity.task", lambda x: x):
            invalid_data = pd.DataFrame(
                {
                    "vmp_code": ["12345", None],
                    "year_month": ["2024-01-01", "2024-01-01"],
                    "ods_code": ["ORG1", "ORG1"],
                    "ingredients": [
                        np.array([
                            {
                                "ingredient_code": "ING1",
                                "ingredient_quantity_basis": 100.0,
                                "ingredient_basis_unit": "mg",
                            }
                        ]),
                        np.array([
                            {
                                "ingredient_code": "ING2",
                                "ingredient_quantity_basis": 50.0,
                                "ingredient_basis_unit": "mg",
                            }
                        ]),
                    ],
                }
            )

            result = transform_and_load_chunk(
                invalid_data, sample_foreign_keys, sample_ingredient_logic_dict, 1, 2
            )

            assert result["created"] == 1
            assert result["skipped"] == 1
            assert IngredientQuantity.objects.count() == 1
