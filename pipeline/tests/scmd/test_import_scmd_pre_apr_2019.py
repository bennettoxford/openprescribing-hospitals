import pytest
import pandas as pd
from datetime import date
from unittest.mock import patch, Mock

from pipeline.scmd.import_scmd_pre_apr_2019 import (
    get_csv_download_url_for_month,
    download_csv_data,
    transform_scmd_data,
    process_scmd_month,
)


class TestGetCSVDownloadURL:
    @pytest.fixture
    def mock_package_show_response(self):
        return {
            "success": True,
            "result": {
                "resources": [
                    {
                        "name": "SCMD_201901",
                        "format": "CSV",
                        "url": "https://opendata.nhsbsa.net/dataset/test/resource/test-id/download/scmd_201901.csv"
                    },
                    {
                        "name": "SCMD_201902", 
                        "format": "CSV",
                        "url": "https://opendata.nhsbsa.net/dataset/test/resource/test-id/download/scmd_201902.csv"
                    },
                    {
                        "name": "OTHER_DATA",
                        "format": "JSON",
                        "url": "https://example.com/other.json"
                    }
                ]
            }
        }

    def test_get_csv_download_url_success(self, mock_package_show_response):
        """Test successful CSV URL retrieval"""
        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = mock_package_show_response
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            result = get_csv_download_url_for_month("201901")
            
            assert result == "https://opendata.nhsbsa.net/dataset/test/resource/test-id/download/scmd_201901.csv"
            mock_get.assert_called_once_with("https://opendata.nhsbsa.net/api/3/action/package_show?id=secondary-care-medicines-data")

    def test_get_csv_download_url_not_found(self, mock_package_show_response):
        """Test when CSV URL is not found"""
        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = mock_package_show_response
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            result = get_csv_download_url_for_month("201912")
            
            assert result is None


class TestDownloadCSVData:
    @pytest.fixture
    def mock_csv_content(self):
        return """YEAR_MONTH,ODS_CODE,VMP_SNOMED_CODE,VMP_PRODUCT_NAME,UNIT_OF_MEASURE_IDENTIFIER,UNIT_OF_MEASURE_NAME,TOTAL_QUANITY_IN_VMP_UNIT
2019-01,ABC123,12345678,Test Product,001,milligram,100.0
2019-01,DEF456,87654321,Another Product,002,tablet,50.0"""

    def test_download_csv_data_success(self, mock_csv_content):
        """Test successful CSV download and parsing"""
        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.text = mock_csv_content
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            result = download_csv_data("https://example.com/test.csv", "201901")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert result.iloc[0]["ODS_CODE"] == "ABC123"
            assert result.iloc[1]["ODS_CODE"] == "DEF456"
            assert result.iloc[0]["TOTAL_QUANITY_IN_VMP_UNIT"] == 100.0


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
        with patch("pipeline.scmd.import_scmd_pre_apr_2019.SCMD_RAW_TABLE_SPEC") as mock_table_spec:
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
            
            result = transform_scmd_data(sample_raw_data, "201901")
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert "year_month" in result.columns
            assert "ods_code" in result.columns
            assert "indicative_cost" in result.columns
            assert pd.isna(result["indicative_cost"].iloc[0])
            assert isinstance(result["year_month"].iloc[0], date)
            assert result["year_month"].iloc[0] == date(2019, 1, 1)


class TestProcessSCMDMonth:

    def test_process_scmd_month_success(self):
        """Test successful month processing"""
        with patch("pipeline.scmd.import_scmd_pre_apr_2019.check_data_exists_for_month") as mock_check, \
             patch("pipeline.scmd.import_scmd_pre_apr_2019.get_csv_download_url_for_month") as mock_get_url, \
             patch("pipeline.scmd.import_scmd_pre_apr_2019.download_csv_data") as mock_download, \
             patch("pipeline.scmd.import_scmd_pre_apr_2019.transform_scmd_data") as mock_transform, \
             patch("pipeline.scmd.import_scmd_pre_apr_2019.upload_to_bigquery") as mock_upload, \
             patch("pipeline.scmd.import_scmd_pre_apr_2019.update_data_status") as mock_update:
            
            mock_check.return_value = False
            mock_get_url.return_value = "https://example.com/scmd_201901.csv"

            raw_sample_df = pd.DataFrame({
                "YEAR_MONTH": ["2019-01"],
                "ODS_CODE": ["ABC123"],
                "VMP_SNOMED_CODE": ["12345678"],
                "VMP_PRODUCT_NAME": ["Test Product"],
                "UNIT_OF_MEASURE_IDENTIFIER": ["001"],
                "UNIT_OF_MEASURE_NAME": ["milligram"],
                "TOTAL_QUANITY_IN_VMP_UNIT": [100.0]
            })

            transformed_sample_df = pd.DataFrame({
                "year_month": [date(2019, 1, 1)],
                "ods_code": ["ABC123"],
                "vmp_snomed_code": ["12345678"],
                "vmp_product_name": ["Test Product"],
                "unit_of_measure_identifier": ["001"],
                "unit_of_measure_name": ["milligram"],
                "total_quantity_in_vmp_unit": [100.0],
                "indicative_cost": [None]
            })
            
            mock_download.return_value = raw_sample_df
            mock_transform.return_value = transformed_sample_df
            
            result = process_scmd_month("201901")
            
            assert result["month"] == "201901"
            assert result["status"] == "success"
            assert result["rows"] == 1
            assert result["file_type"] == "finalised"
            
            mock_get_url.assert_called_once_with("201901")
            mock_download.assert_called_once_with("https://example.com/scmd_201901.csv", "201901")
            mock_transform.assert_called_once_with(raw_sample_df, "201901")
            mock_upload.assert_called()
            mock_update.assert_called_once_with("201901", "finalised")

    def test_process_scmd_month_no_csv_url(self):
        """Test when no CSV URL is found"""
        with patch("pipeline.scmd.import_scmd_pre_apr_2019.check_data_exists_for_month") as mock_check, \
             patch("pipeline.scmd.import_scmd_pre_apr_2019.get_csv_download_url_for_month") as mock_get_url:
            
            mock_check.return_value = False
            mock_get_url.return_value = None
            
            result = process_scmd_month("201901")
            
            assert result["month"] == "201901"
            assert result["status"] == "failed"
            assert result["reason"] == "no_csv_url"

