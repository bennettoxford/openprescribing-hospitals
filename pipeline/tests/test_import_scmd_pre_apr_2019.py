import pytest
import pandas as pd
from datetime import date
from unittest.mock import patch, Mock

from pipeline.flows.import_scmd_pre_apr_2019 import (
    fetch_scmd_data_for_resource,
    transform_scmd_data,
    process_scmd_month,
)


class TestFetchSCMDData:
    @pytest.fixture
    def mock_api_response_first_page(self):
        return {
            "success": True,
            "result": {
                "records": [
                    {
                        "YEAR_MONTH": "201901",
                        "ODS_CODE": "ABC123",
                        "VMP_SNOMED_CODE": "12345678",
                        "VMP_PRODUCT_NAME": "Test Product",
                        "UNIT_OF_MEASURE_IDENTIFIER": "001",
                        "UNIT_OF_MEASURE_NAME": "milligram",
                        "TOTAL_QUANITY_IN_VMP_UNIT": 100.0
                    }
                ],
                "total": 1
            }
        }

    @pytest.fixture
    def mock_api_response_empty(self):
        return {
            "success": True,
            "result": {
                "records": [],
                "total": 0
            }
        }

    def test_fetch_scmd_data_for_resource_success(self, mock_api_response_first_page):
        """Test successful data fetching"""
        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = mock_api_response_first_page
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            result = fetch_scmd_data_for_resource("SCMD_201901")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert result.iloc[0]["ODS_CODE"] == "ABC123"


class TestTransformSCMDData:
    @pytest.fixture
    def sample_raw_data(self):
        return pd.DataFrame({
            "YEAR_MONTH": ["2019-01-01"],
            "ODS_CODE": ["ABC123"],
            "VMP_SNOMED_CODE": [12345678],
            "VMP_PRODUCT_NAME": ["Test Product"],
            "UNIT_OF_MEASURE_IDENTIFIER": [1],
            "UNIT_OF_MEASURE_NAME": ["milligram"],
            "TOTAL_QUANITY_IN_VMP_UNIT": [100.0]
        })

    def test_transform_scmd_data_success(self, sample_raw_data):
        """Test successful data transformation"""
        with patch("pipeline.flows.import_scmd_pre_apr_2019.SCMD_RAW_TABLE_SPEC") as mock_table_spec:
            mock_field1 = Mock()
            mock_field1.name = "year_month"
            mock_field2 = Mock()
            mock_field2.name = "ods_code"
            mock_field3 = Mock()
            mock_field3.name = "vmp_snomed_code"
            mock_field4 = Mock()
            mock_field4.name = "vmp_product_name"
            mock_field5 = Mock()
            mock_field5.name = "unit_of_measure_identifier"
            mock_field6 = Mock()
            mock_field6.name = "unit_of_measure_name"
            mock_field7 = Mock()
            mock_field7.name = "total_quantity_in_vmp_unit"
            mock_field8 = Mock()
            mock_field8.name = "indicative_cost"
            
            mock_table_spec.schema = [
                mock_field1, mock_field2, mock_field3, mock_field4,
                mock_field5, mock_field6, mock_field7, mock_field8
            ]
            
            result = transform_scmd_data(sample_raw_data)
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert "year_month" in result.columns
            assert "ods_code" in result.columns
            assert "indicative_cost" in result.columns
            assert pd.isna(result["indicative_cost"].iloc[0])
            assert isinstance(result["year_month"].iloc[0], date)


class TestProcessSCMDMonth:

    def test_process_scmd_month_success(self):
        """Test successful month processing"""
        with patch("pipeline.flows.import_scmd_pre_apr_2019.check_data_exists_for_month") as mock_check, \
             patch("pipeline.flows.import_scmd_pre_apr_2019.fetch_scmd_data_for_resource") as mock_fetch, \
             patch("pipeline.flows.import_scmd_pre_apr_2019.transform_scmd_data") as mock_transform, \
             patch("pipeline.flows.import_scmd_pre_apr_2019.upload_to_bigquery") as mock_upload, \
             patch("pipeline.flows.import_scmd_pre_apr_2019.update_data_status") as mock_update:
            
            mock_check.return_value = False

            sample_df = pd.DataFrame({
                "year_month": [date(2019, 1, 1)],
                "ods_code": ["ABC123"],
                "vmp_snomed_code": ["12345678"],
                "vmp_product_name": ["Test Product"],
                "unit_of_measure_identifier": ["001"],
                "unit_of_measure_name": ["milligram"],
                "total_quantity_in_vmp_unit": [100.0],
                "indicative_cost": [None]
            })
            
            mock_fetch.return_value = sample_df
            mock_transform.return_value = sample_df
            
            result = process_scmd_month("201901")
            
            assert result["month"] == "201901"
            assert result["status"] == "success"
            assert result["rows"] == 1
            assert result["file_type"] == "finalised"
            
            mock_fetch.assert_called_once_with("SCMD_201901")
            mock_transform.assert_called_once()
            mock_upload.assert_called()
            mock_update.assert_called_once_with("201901", "finalised")

