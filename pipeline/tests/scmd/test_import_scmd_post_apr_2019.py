import pytest
import pandas as pd
from datetime import date
from unittest.mock import patch, Mock

from pipeline.scmd.import_scmd_post_apr_2019 import (
    fetch_dataset_urls,
    process_month_data,
    get_months_to_update,
    map_columns,
)

@pytest.fixture
def sample_csv_content():
    return """YEAR_MONTH,ODS_CODE,VMP_SNOMED_CODE,VMP_PRODUCT_NAME,UNIT_OF_MEASURE_IDENTIFIER,UNIT_OF_MEASURE_NAME,TOTAL_QUANITY_IN_VMP_UNIT,INDICATIVE_COST
202401,ABC123,12345678,Test Product,001,milligram,100.0,50.0
202401,XYZ789,87654321,Another Product,002,millilitre,200.0,75.0"""


@pytest.fixture
def renamed_csv_content():
    return """YEAR_MONTH,ODS_CODE,VMP_SNOMED_CODE,VMP_PRODUCT_NAME,VMP_UDFS_UNIT_OF_MEASURE_IDENTIFIER,VMP_UDFS_UNIT_OF_MEASURE_NAME,TOTAL_QUANTITY_IN_VMP_UDFS_UNIT_OF_MEASURE,INDICATIVE_COST,VMP_UNIT_DOSE_UNIT_OF_MEASURE_NAME,TOTAL_QUANTITY_IN_VMP_UNIT_DOSE_UNIT_OF_MEASURE
202606,ABC123,012345678,Test Product,001,milligram,100.0,50.0,tablet,10.0"""

class TestDatasetURLFetching:
    @pytest.fixture
    def mock_provisional_api_response(self):
        return {
            "result": {
                "resources": [
                    {
                        "format": "CSV",
                        "url": "https://example.com/scmd_provisional_202401.csv"
                    },
                    {
                        "format": "CSV",
                        "url": "https://example.com/scmd_provisional_202402.csv"
                    }
                ]
            }
        }

    @pytest.fixture
    def mock_finalised_api_response(self):
        return {
            "result": {
                "resources": [
                    {
                        "format": "CSV",
                        "url": "https://example.com/scmd_final_202401.csv"
                    }
                ]
            }
        }

    def test_fetch_dataset_urls_success(self, mock_provisional_api_response, mock_finalised_api_response):
        with patch("requests.get") as mock_get:
            mock_response1 = Mock()
            mock_response1.json.return_value = mock_provisional_api_response
            mock_response1.raise_for_status.return_value = None
            
            mock_response2 = Mock()
            mock_response2.json.return_value = mock_finalised_api_response
            mock_response2.raise_for_status.return_value = None
            
            mock_get.side_effect = [mock_response1, mock_response2]

            result = fetch_dataset_urls()

            assert isinstance(result, dict)
            assert "provisional" in result
            assert "finalised" in result
            assert isinstance(result["provisional"], dict)
            assert isinstance(result["finalised"], dict)
            assert "2024-01-01" in result["provisional"]
            assert result["provisional"]["2024-01-01"]["file_type"] == "provisional"
            assert "2024-02-01" in result["provisional"]
            assert "2024-01-01" in result["finalised"]
            assert result["finalised"]["2024-01-01"]["file_type"] == "final"


class TestDataProcessing:
    def test_process_month_data_success(self, sample_csv_content):
        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.text = sample_csv_content
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            result = process_month_data("2024-01-01", "https://example.com/test.csv")

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert list(result.columns) == [
                "YEAR_MONTH", "ODS_CODE", "VMP_SNOMED_CODE", "VMP_PRODUCT_NAME",
                "UNIT_OF_MEASURE_IDENTIFIER", "UNIT_OF_MEASURE_NAME",
                "TOTAL_QUANITY_IN_VMP_UNIT", "INDICATIVE_COST"
            ]
            assert result["YEAR_MONTH"].iloc[0] == date(2024, 1, 1)
            assert result["UNIT_OF_MEASURE_IDENTIFIER"].iloc[0] == "001"

    def test_process_month_data_renamed_columns(self, renamed_csv_content):
        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.text = renamed_csv_content
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            result = process_month_data("2026-06-01", "https://example.com/test.csv")

            assert result["VMP_SNOMED_CODE"].iloc[0] == "012345678"
            assert result["VMP_UDFS_UNIT_OF_MEASURE_IDENTIFIER"].iloc[0] == "001"
            assert result["TOTAL_QUANTITY_IN_VMP_UDFS_UNIT_OF_MEASURE"].iloc[0] == 100.0
            assert "VMP_UNIT_DOSE_UNIT_OF_MEASURE_NAME" in result.columns


class TestDataTransformation:
    def test_map_columns_with_typo_column(self):
        """Test mapping when CSV has typo TOTAL_QUANITY_IN_VMP_UNIT"""
        input_df = pd.DataFrame({
            "YEAR_MONTH": ["2024-01-01"],
            "ODS_CODE": ["ABC123"],
            "VMP_SNOMED_CODE": ["12345678"],
            "VMP_PRODUCT_NAME": ["Test Product"],
            "UNIT_OF_MEASURE_IDENTIFIER": ["001"],
            "UNIT_OF_MEASURE_NAME": ["MILLIGRAM"],
            "TOTAL_QUANITY_IN_VMP_UNIT": [100.0],
            "INDICATIVE_COST": [50.0]
        })

        result = map_columns(input_df.copy())

        expected_columns = [
            "year_month", "ods_code", "vmp_snomed_code", "vmp_product_name",
            "unit_of_measure_identifier", "unit_of_measure_name",
            "total_quantity_in_vmp_unit", "indicative_cost"
        ]
        assert list(result.columns.values) == expected_columns
        assert result["unit_of_measure_name"].iloc[0] == "milligram"

    def test_map_columns_with_correct_column(self):
        """Test mapping when CSV has correct TOTAL_QUANTITY_IN_VMP_UNIT"""
        input_df = pd.DataFrame({
            "YEAR_MONTH": ["2024-01-01"],
            "ODS_CODE": ["ABC123"],
            "VMP_SNOMED_CODE": ["12345678"],
            "VMP_PRODUCT_NAME": ["Test Product"],
            "UNIT_OF_MEASURE_IDENTIFIER": ["001"],
            "UNIT_OF_MEASURE_NAME": ["MILLIGRAM"],
            "TOTAL_QUANTITY_IN_VMP_UNIT": [100.0],
            "INDICATIVE_COST": [50.0]
        })

        result = map_columns(input_df.copy())

        expected_columns = [
            "year_month", "ods_code", "vmp_snomed_code", "vmp_product_name",
            "unit_of_measure_identifier", "unit_of_measure_name",
            "total_quantity_in_vmp_unit", "indicative_cost"
        ]
        assert list(result.columns.values) == expected_columns
        assert result["unit_of_measure_name"].iloc[0] == "milligram"

    def test_map_columns_with_renamed_udfs_columns(self):
        input_df = pd.DataFrame({
            "YEAR_MONTH": ["2026-06-01"],
            "ODS_CODE": ["ABC123"],
            "VMP_SNOMED_CODE": ["012345678"],
            "VMP_PRODUCT_NAME": ["Test Product"],
            "VMP_UDFS_UNIT_OF_MEASURE_IDENTIFIER": ["001"],
            "VMP_UDFS_UNIT_OF_MEASURE_NAME": ["MILLIGRAM"],
            "TOTAL_QUANTITY_IN_VMP_UDFS_UNIT_OF_MEASURE": [100.0],
            "INDICATIVE_COST": [50.0],
            "VMP_UNIT_DOSE_UNIT_OF_MEASURE_NAME": ["tablet"],
            "TOTAL_QUANTITY_IN_VMP_UNIT_DOSE_UNIT_OF_MEASURE": [10.0],
        })

        result = map_columns(input_df.copy())

        expected_columns = [
            "year_month", "ods_code", "vmp_snomed_code", "vmp_product_name",
            "unit_of_measure_identifier", "unit_of_measure_name",
            "total_quantity_in_vmp_unit", "indicative_cost"
        ]
        assert list(result.columns.values) == expected_columns
        assert result["unit_of_measure_identifier"].iloc[0] == "001"
        assert result["unit_of_measure_name"].iloc[0] == "milligram"
        assert result["total_quantity_in_vmp_unit"].iloc[0] == 100.0
        assert result["vmp_snomed_code"].iloc[0] == "012345678"

    def test_map_columns_missing_columns(self):
        input_df = pd.DataFrame({
            "YEAR_MONTH": ["2024-01-01"],
            "ODS_CODE": ["ABC123"]
        })

        result = map_columns(input_df.copy())
        assert "year_month" in result.columns
        assert "ods_code" in result.columns

class TestGetMonthsToUpdate:
    def test_get_months_to_update(self):
        urls_by_month = {
            "2024-01-01": {"url": "https://example.com/202401.csv", "file_type": "final"},
            "2024-02-01": {"url": "https://example.com/202402.csv", "file_type": "provisional"},
            "2024-03-01": {"url": "https://example.com/202403.csv", "file_type": "final"},
            "2024-04-01": {"url": "https://example.com/202404.csv", "file_type": "final"},
        }
        existing_dates = {"2024-04-01"}

        result = get_months_to_update(urls_by_month, existing_dates)

        expected_months = ["2024-01-01", "2024-02-01", "2024-03-01"]
        
        assert isinstance(result, list)
        assert len(result) == 3
        assert sorted(result) == sorted(expected_months)

    def test_get_months_to_update_no_updates_needed(self):
        urls_by_month = {
            "2024-01-01": {"url": "https://example.com/202401.csv", "file_type": "final"}
        }
        existing_dates = {"2024-01-01"}

        result = get_months_to_update(urls_by_month, existing_dates)
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_get_months_to_update_all_new_months(self):
        urls_by_month = {
            "2024-01-01": {"url": "https://example.com/202401.csv", "file_type": "final"},
            "2024-02-01": {"url": "https://example.com/202402.csv", "file_type": "provisional"}
        }
        existing_dates = set()

        result = get_months_to_update(urls_by_month, existing_dates)
        
        expected_months = ["2024-01-01", "2024-02-01"]
        assert isinstance(result, list)
        assert len(result) == 2
        assert sorted(result) == sorted(expected_months)
