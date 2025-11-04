import pytest
import pandas as pd
from datetime import date
from unittest.mock import patch

from pipeline.scmd.import_scmd_post_apr_2019 import (
    fetch_dataset_urls,
    process_month_data,
    get_months_to_update,
    map_columns,
)

@pytest.fixture
def sample_csv_content():
    return """YEAR_MONTH,ODS_CODE,VMP_SNOMED_CODE,VMP_PRODUCT_NAME,UNIT_OF_MEASURE_IDENTIFIER,UNIT_OF_MEASURE_NAME,TOTAL_QUANTITY_IN_VMP_UNIT,INDICATIVE_COST
202401,ABC123,12345678,Test Product,001,milligram,100.0,50.0
202401,XYZ789,87654321,Another Product,002,millilitre,200.0,75.0"""

class TestDatasetURLFetching:
    @pytest.fixture
    def mock_api_response(self):
        return {
            "result": {
                "resources": [
                    {
                        "format": "CSV",
                        "url": "https://example.com/scmd_provisional_202401.csv"
                    },
                    {
                        "format": "CSV",
                        "url": "https://example.com/scmd_final_202401.csv"
                    },
                    {
                        "format": "CSV",
                        "url": "https://example.com/scmd_provisional_202402.csv"
                    }
                ]
            }
        }

    def test_fetch_dataset_urls_success(self, mock_api_response):
        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_api_response
            mock_get.return_value.status_code = 200

            result = fetch_dataset_urls()

            assert isinstance(result, dict)
            assert len(result) == 2
            assert "2024-01-01" in result
            assert result["2024-01-01"]["file_type"] == "final"
            assert "2024-02-01" in result
            assert result["2024-02-01"]["file_type"] == "provisional"


class TestDataProcessing:
    def test_process_month_data_success(self, sample_csv_content):
        with patch("requests.get") as mock_get:
            mock_get.return_value.text = sample_csv_content
            mock_get.return_value.status_code = 200

            result = process_month_data("2024-01-01", "https://example.com/test.csv")

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert list(result.columns) == [
                "YEAR_MONTH", "ODS_CODE", "VMP_SNOMED_CODE", "VMP_PRODUCT_NAME",
                "UNIT_OF_MEASURE_IDENTIFIER", "UNIT_OF_MEASURE_NAME",
                "TOTAL_QUANTITY_IN_VMP_UNIT", "INDICATIVE_COST"
            ]
            assert result["YEAR_MONTH"].iloc[0] == date(2024, 1, 1)

 
class TestDataTransformation:
    def test_map_columns_all_columns(self):
        input_df = pd.DataFrame({
            "YEAR_MONTH": ["2024-01-01"],
            "ODS_CODE": ["ABC123"],
            "VMP_SNOMED_CODE": ["12345678"],
            "VMP_PRODUCT_NAME": ["Test Product"],
            "UNIT_OF_MEASURE_IDENTIFIER": ["001"],
            "UNIT_OF_MEASURE_NAME": ["milligram"],
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

    def test_map_columns_missing_columns(self):
        input_df = pd.DataFrame({
            "YEAR_MONTH": ["2024-01-01"],
            "ODS_CODE": ["ABC123"]
        })

        result = map_columns(input_df.copy())
        assert "year_month" in result.columns
        assert "ods_code" in result.columns

class TestGetMonthsToUpdate:
    @pytest.fixture
    def sample_urls_df(self):
        return pd.DataFrame({
            'year_month': pd.to_datetime([
                '2024-01-01',  # New final month
                '2024-02-01',  # New provisional month
                '2024-03-01',  # Update from provisional to final
                '2024-04-01',  # Already exists as final
            ]),
            'file_type_new': [
                'final',
                'provisional',
                'final',
                'final'
            ]
        })

    @pytest.fixture
    def sample_existing_status_df(self):
        return pd.DataFrame({
            'year_month': pd.to_datetime([
                '2024-03-01',  # Currently provisional, will be updated
                '2024-04-01',  # Already final, no update needed
            ]),
            'file_type_existing': [
                'provisional',
                'final'
            ]
        })

    def test_get_months_to_update(self, sample_urls_df, sample_existing_status_df):
        result = get_months_to_update(sample_urls_df, sample_existing_status_df)

        # Should include:
        # - 2024-01-01 (new final)
        # - 2024-02-01 (new provisional)
        # - 2024-03-01 (update from provisional to final)
        expected_months = ['2024-01-01', '2024-02-01', '2024-03-01']
        
        assert isinstance(result, list)
        assert len(result) == 3
        assert sorted(result) == sorted(expected_months)

    def test_get_months_to_update_no_updates_needed(self):
        urls_df = pd.DataFrame({
            'year_month': pd.to_datetime(['2024-01-01']),
            'file_type_new': ['final']
        })
        
        existing_df = pd.DataFrame({
            'year_month': pd.to_datetime(['2024-01-01']),
            'file_type_existing': ['final']
        })

        result = get_months_to_update(urls_df, existing_df)
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_get_months_to_update_all_new_months(self):
        urls_df = pd.DataFrame({
            'year_month': pd.to_datetime(['2024-01-01', '2024-02-01']),
            'file_type_new': ['final', 'provisional']
        })
        
        existing_df = pd.DataFrame({
            'year_month': [],
            'file_type_existing': []
        })

        result = get_months_to_update(urls_df, existing_df)
        
        expected_months = ['2024-01-01', '2024-02-01']
        assert isinstance(result, list)
        assert len(result) == 2
        assert sorted(result) == sorted(expected_months)
