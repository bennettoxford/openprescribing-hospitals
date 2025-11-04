import pytest
import pandas as pd
from unittest.mock import patch
from pipeline.load_data.load_atc import (
    extract_atc_codes,
    transform_atc_codes,
    load_atc_codes,
)
from viewer.models import ATC


@pytest.fixture
def sample_atc_data():
    """Fixture providing sample ATC data as a pandas DataFrame"""
    return pd.DataFrame(
        {
            "atc_code": ["A", "A02", "A02A", "A02AA", "A02AA01"],
            "atc_name": [
                "Alimentary Tract and Metabolism",
                "Drugs For Acid Related Disorders",
                "Antacids",
                "Magnesium Compounds",
                "Magnesium Carbonate",
            ],
            "anatomical_main_group": [
                "Alimentary Tract and Metabolism",
                "Alimentary Tract and Metabolism",
                "Alimentary Tract and Metabolism",
                "Alimentary Tract and Metabolism",
                "Alimentary Tract and Metabolism",
            ],
            "therapeutic_subgroup": [
                None,
                "Drugs For Acid Related Disorders",
                "Drugs For Acid Related Disorders",
                "Drugs For Acid Related Disorders",
                "Drugs For Acid Related Disorders",
            ],
            "pharmacological_subgroup": [
                None,
                None,
                "Antacids",
                "Antacids",
                "Antacids",
            ],
            "chemical_subgroup": [
                None,
                None,
                None,
                "Magnesium Compounds",
                "Magnesium Compounds",
            ],
            "chemical_substance": [None, None, None, None, "Magnesium Carbonate"],
        }
    )


@pytest.fixture
def sample_transformed_data():
    """Fixture providing sample transformed ATC data"""
    return [
        {
            "code": "A",
            "name": "Alimentary Tract and Metabolism",
            "level_1": "Alimentary Tract and Metabolism",
            "level_2": None,
            "level_3": None,
            "level_4": None,
            "level_5": None,
        },
        {
            "code": "A02",
            "name": "Drugs For Acid Related Disorders",
            "level_1": "Alimentary Tract and Metabolism",
            "level_2": "Drugs For Acid Related Disorders",
            "level_3": None,
            "level_4": None,
            "level_5": None,
        },
        {
            "code": "A02A",
            "name": "Antacids",
            "level_1": "Alimentary Tract and Metabolism",
            "level_2": "Drugs For Acid Related Disorders",
            "level_3": "Antacids",
            "level_4": None,
            "level_5": None,
        },
        {
            "code": "A02AA",
            "name": "Magnesium Compounds",
            "level_1": "Alimentary Tract and Metabolism",
            "level_2": "Drugs For Acid Related Disorders",
            "level_3": "Antacids",
            "level_4": "Magnesium Compounds",
            "level_5": None,
        },
        {
            "code": "A02AA01",
            "name": "Magnesium Carbonate",
            "level_1": "Alimentary Tract and Metabolism",
            "level_2": "Drugs For Acid Related Disorders",
            "level_3": "Antacids",
            "level_4": "Magnesium Compounds",
            "level_5": "Magnesium Carbonate",
        },
    ]


class TestLoadATC:
    @patch("pipeline.load_data.load_atc.fetch_table_data_from_bq")
    def test_extract_atc_codes(self, mock_fetch, sample_atc_data):
        mock_fetch.return_value = sample_atc_data

        result = extract_atc_codes()

        mock_fetch.assert_called_once()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5
        assert all(
            col in result.columns
            for col in [
                "atc_code",
                "atc_name",
                "anatomical_main_group",
                "therapeutic_subgroup",
                "pharmacological_subgroup",
                "chemical_subgroup",
                "chemical_substance",
            ]
        )

    def test_transform_atc_codes(self, sample_atc_data):
        result = transform_atc_codes(sample_atc_data)

        assert isinstance(result, list)
        assert len(result) == 5
        assert all(isinstance(atc, dict) for atc in result)

        assert result[0]["code"] == "A"
        assert result[-1]["code"] == "A02AA01"

        magnesium_carbonate = next(atc for atc in result if atc["code"] == "A02AA01")
        assert magnesium_carbonate["level_1"] == "Alimentary Tract and Metabolism"
        assert magnesium_carbonate["level_2"] == "Drugs For Acid Related Disorders"
        assert magnesium_carbonate["level_3"] == "Antacids"
        assert magnesium_carbonate["level_4"] == "Magnesium Compounds"
        assert magnesium_carbonate["level_5"] == "Magnesium Carbonate"

    @pytest.mark.django_db
    def test_load_atc_codes_with_real_db(self, sample_transformed_data):

        initial_atcs = [
            ATC(
                code="OLD123",
                name="Old ATC",
                level_1="Test Level 1",
                level_2="Test Level 2",
                level_3=None,
                level_4=None,
                level_5=None,
            )
        ]
        ATC.objects.bulk_create(initial_atcs)
        assert ATC.objects.count() == 1

        result = load_atc_codes(sample_transformed_data)

        assert result["deleted"] == 1
        assert result["created"] == 5
        assert result["total_records"] == 5

        stored_atcs = ATC.objects.all().order_by("code")
        assert stored_atcs.count() == 5

        atcs_list = list(stored_atcs)
        assert atcs_list[0].code == "A"
        assert atcs_list[0].name == "Alimentary Tract and Metabolism"
        assert atcs_list[0].level_1 == "Alimentary Tract and Metabolism"
        assert atcs_list[0].level_2 is None

        magnesium_carbonate = atcs_list[-1]
        assert magnesium_carbonate.code == "A02AA01"
        assert magnesium_carbonate.name == "Magnesium Carbonate"
        assert magnesium_carbonate.level_1 == "Alimentary Tract and Metabolism"
        assert magnesium_carbonate.level_2 == "Drugs For Acid Related Disorders"
        assert magnesium_carbonate.level_3 == "Antacids"
        assert magnesium_carbonate.level_4 == "Magnesium Compounds"
        assert magnesium_carbonate.level_5 == "Magnesium Carbonate"

        for atc in atcs_list:
            assert atc.code.startswith("A")
            assert len(atc.code) in [1, 3, 4, 5, 7]
