import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch
from pipeline.flows.load_ingredient_quantity import (
    get_unique_vmps_with_ingredient_data,
    extract_ingredient_data_by_vmps,
    clear_existing_ingredient_data,
    transform_and_load_chunk,
)
from viewer.models import IngredientQuantity, Ingredient, VMP, Organisation


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
    @patch("pipeline.flows.load_ingredient_quantity.execute_bigquery_query")
    def test_get_unique_vmps_with_ingredient_data(self, mock_execute_query):
        mock_execute_query.return_value = [{"vmp_code": "12345"}, {"vmp_code": "67890"}]

        result = get_unique_vmps_with_ingredient_data()

        assert isinstance(result, list)
        assert len(result) == 2
        assert "12345" in result
        assert "67890" in result

    @patch("pipeline.flows.load_ingredient_quantity.get_bigquery_client")
    def test_extract_ingredient_data_by_vmps(self, mock_client, sample_ingredient_data):
        mock_query_job = mock_client.return_value.query.return_value
        mock_query_job.to_dataframe.return_value = sample_ingredient_data

        result = extract_ingredient_data_by_vmps(["12345", "67890"], 1, 2)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert all(
            col in result.columns
            for col in ["vmp_code", "year_month", "ods_code", "ingredients"]
        )

    @pytest.mark.django_db
    def test_clear_existing_ingredient_data(self):
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

        deleted_count = clear_existing_ingredient_data()

        assert deleted_count == 1
        assert IngredientQuantity.objects.count() == 0

    @pytest.mark.django_db
    def test_transform_and_load_chunk(
        self, sample_ingredient_data, sample_foreign_keys
    ):
        sample_ingredient_data["ingredients"] = sample_ingredient_data[
            "ingredients"
        ].apply(lambda x: np.array(x))

        result = transform_and_load_chunk(
            sample_ingredient_data, sample_foreign_keys, 1, 2
        )

        assert isinstance(result, dict)
        assert result["created"] > 0
        assert IngredientQuantity.objects.count() > 0

        iq = IngredientQuantity.objects.first()
        assert isinstance(iq.data, list)
        assert len(iq.data) > 0
        assert all(isinstance(entry, list) and len(entry) == 3 for entry in iq.data)

    @pytest.mark.django_db
    def test_transform_and_load_chunk_invalid_data(self, sample_foreign_keys):
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

        result = transform_and_load_chunk(invalid_data, sample_foreign_keys, 1, 2)

        assert result["created"] == 0
        assert result["skipped"] > 0
