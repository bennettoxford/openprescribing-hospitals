import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from viewer.management.commands.import_scmd import (
    DataFetcher, 
    UnitConverter, 
    BigQueryTableManager, 
    SCMDImporter,
    DatasetURL,
)
from google.api_core.exceptions import NotFound

class TestDataFetcher:
    @pytest.fixture
    def mock_requests_get(self):
        with patch('requests.get') as mock_get:
            yield mock_get

    def test_iter_dataset_urls(self, mock_requests_get):
        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "result": {
                "resources": [
                    {"url": "https://opendata.nhsbsa.net/dataset/dataset_id/resource/resource_id/download/scmd_unclassified_202405.csv", "format": "CSV"},
                    {"url": "https://opendata.nhsbsa.net/dataset/dataset_id/resource/resource_id/download/scmd_provisional_202405.csv", "format": "CSV"},
                    {"url": "https://opendata.nhsbsa.net/dataset/dataset_id/resource/resource_id/download/scmd_final_202406.csv", "format": "CSV"},
                ]
            }
        }
        mock_requests_get.return_value = mock_response

        urls = list(DataFetcher.iter_dataset_urls())
        assert len(urls) == 2
        assert "scmd_provisional_202405.csv" in urls[0]
        assert "scmd_final_202406.csv" in urls[1]

    def test_iter_months(self):
        urls = [
            "https://opendata.nhsbsa.net/dataset/dataset_id/resource/resource_id/download/scmd_provisional_202405.csv",
            "https://opendata.nhsbsa.net/dataset/dataset_id/resource/resource_id/download/scmd_final_202406.csv"
        ]
        dataset_urls = list(DataFetcher.iter_months(urls))
        assert len(dataset_urls) == 2
        assert dataset_urls[0].month == "2024-05"
        assert dataset_urls[0].file_type == "provisional"
        assert dataset_urls[1].month == "2024-06"
        assert dataset_urls[1].file_type == "final"

class TestUnitConverter:
    @pytest.fixture
    def mock_bigquery_client(self):
        with patch('google.cloud.bigquery.Client') as mock_client:
            yield mock_client

    def test_fetch_conversion_factors(self, mock_bigquery_client):
        mock_client = mock_bigquery_client.return_value
        mock_client.query.return_value.result.return_value = [
            MagicMock(unit="mg", basis="g", conversion_factor=0.001, unit_id=1, basis_id=2),
            MagicMock(unit="microlitre", basis="litre", conversion_factor=0.000001, unit_id=3, basis_id=4)
        ]

        converter = UnitConverter(mock_client)
        result = converter.fetch_conversion_factors()

        assert isinstance(result, pd.DataFrame)
        assert result.shape[0] == 2
        assert set(result.columns) == {'unit', 'basis', 'conversion_factor', 'unit_id', 'basis_id'}

    def test_convert_units(self):
        # Mock data
        df = pd.DataFrame({
            'unit_of_measure_identifier': [1, pd.NA],
            'unit_of_measure_name': ['mg', pd.NA],
            'total_quanity_in_vmp_unit': [1000.0, 10]
        })
        conversion_factors = pd.DataFrame({
            'unit': ['mg'],
            'basis': ['g'],
            'unit_id': [1],
            'basis_id': [2],
            'conversion_factor': [0.001],
        })

        with patch.object(UnitConverter, 'fetch_conversion_factors', return_value=conversion_factors):
            converter = UnitConverter(None)
            converted_df = converter.convert_units(df)
            
            # assert the row with a nan unit of measure identifier is dropped
            assert converted_df.shape[0] == 1

            assert converted_df.iloc[0]['total_quanity_in_vmp_unit'] == 1.0
            assert converted_df.iloc[0]['unit_of_measure_identifier'] == 2

class TestBigQueryTableManager:
    @pytest.fixture
    def mock_bigquery_client(self):
        with patch('google.cloud.bigquery.Client') as mock_client:
            yield mock_client

    def test_create_table(self, mock_bigquery_client):
        mock_client = mock_bigquery_client.return_value
        manager = BigQueryTableManager(mock_client, "project.dataset.table", [])

        manager.create_table(description="Test Table", partition_field="year_month")
        mock_client.create_table.assert_called_once()

    def test_load_dataframe(self, mock_bigquery_client):
        mock_client = mock_bigquery_client.return_value
        manager = BigQueryTableManager(mock_client, "project.dataset.table", [])

        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        manager.load_dataframe(df)
        mock_client.load_table_from_dataframe.assert_called_once()

class TestSCMDImporter:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.urls_by_month = {
            "2022-01-01": DatasetURL(month="2022-01", file_type="final", url="https://opendata.nhsbsa.net/dataset/dataset_id/resource/resource_id/download/scmd_final_202201.csv"),
            "2022-02-01": DatasetURL(month="2022-02", file_type="provisional", url="https://opendata.nhsbsa.net/dataset/dataset_id/resource/resource_id/download/scmd_provisional_202202.csv")
        }
        with patch('viewer.management.commands.import_scmd.SCMDImporter._get_bigquery_client', return_value=MagicMock()):
            self.importer = SCMDImporter(self.urls_by_month)
            self.importer.data_status_table_full_id = "project.dataset.data_status"

    @pytest.fixture(autouse=True)
    def mock_bigquery_client(self):
        with patch('google.cloud.bigquery.Client') as mock_client:
            self.mock_client = mock_client
            yield mock_client

    @pytest.fixture(autouse=True)
    def mock_requests_get(self):
        with patch('requests.get') as mock_get:
            self.mock_requests_get = mock_get
            yield mock_get

    @pytest.fixture(autouse=True)
    def mock_service_account(self):
        with patch('google.oauth2.service_account.Credentials.from_service_account_file', return_value=MagicMock()):
            yield

    def test_get_bigquery_client(self):
        client = self.importer._get_bigquery_client()
        assert client is not None

    def test_get_existing_data(self):
        mock_row1 = MagicMock()
        mock_row1.year_month = "2022-01-01"
        mock_row1.file_type = "final"
        
        mock_row2 = MagicMock()
        mock_row2.year_month = "2022-02-01"
        mock_row2.file_type = "provisional"
        
        mock_result = [mock_row1, mock_row2]
        
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = mock_result
        
        self.importer.client.query.return_value = mock_query_job
        self.importer.data_status_table_full_id = "project.dataset.data_status"

        existing_data = self.importer._get_existing_data()
        
        assert existing_data.shape[0] == 2
        assert existing_data["year_month"].dt.strftime("%Y-%m-%d").tolist() == ["2022-01-01", "2022-02-01"]
        assert existing_data["file_type"].tolist() == ["final", "provisional"]

    def test_process_month_data(self):
        mock_response = MagicMock()
        mock_response.text = "YEAR_MONTH,ODS_CODE,VMP_SNOMED_CODE,VMP_PRODUCT_NAME,UNIT_OF_MEASURE_IDENTIFIER,UNIT_OF_MEASURE_NAME,TOTAL_QUANITY_IN_VMP_UNIT,INDICATIVE_COST\n2022-01-01,RP1,42206411000001101,Apixaban 2.5mg tablets,1,mg,1000,10.0"
        self.mock_requests_get.return_value = mock_response

        with patch.object(UnitConverter, 'fetch_conversion_factors', return_value=pd.DataFrame({
            'unit': ['mg'],
            'basis': ['g'],
            'conversion_factor': [0.001],
            'unit_id': [1],
            'basis_id': [2]
        })):
            processed_data = self.importer._process_month_data(pd.Timestamp("2022-01-01"))
            assert set(processed_data.columns) == {'year_month', 'ods_code', 'vmp_snomed_code', 'vmp_product_name', 'unit_of_measure_identifier', 'unit_of_measure_name', 'total_quanity_in_vmp_unit', 'indicative_cost'}
            assert processed_data['year_month'].dtype == 'datetime64[ns]'
            assert processed_data['vmp_snomed_code'].dtype == 'int64'
            assert processed_data['unit_of_measure_identifier'].dtype == 'int64'
            assert processed_data['total_quanity_in_vmp_unit'].dtype == 'float64'
            assert processed_data['indicative_cost'].dtype == 'float64'

    def test_update_tables_creates_new_tables(self):
        with patch.object(self.importer, '_get_existing_data', side_effect=NotFound("Table not found")):
            with patch.object(self.importer, '_create_new_tables') as mock_create_new_tables:
                new_status_data = pd.DataFrame({
                    "year_month": pd.to_datetime(["2022-01-01", "2022-02-01"]),
                    "file_type": ["final", "final"]
                })
                self.importer.update_tables(new_status_data)
                mock_create_new_tables.assert_called_once()

    def test_create_new_tables(self):
        with patch.object(self.importer.scmd_manager, 'create_table') as mock_create_scmd_table, \
             patch.object(self.importer.data_status_manager, 'create_table') as mock_create_status_table, \
             patch.object(self.importer, '_update_month_data') as mock_update_month_data:
            status_data = pd.DataFrame({
                "year_month": pd.to_datetime(["2022-01-01", "2022-02-01"]),
                "file_type": ["provisional", "final"]
            })
            self.importer._create_new_tables(status_data)
            mock_create_scmd_table.assert_called_once()
            mock_create_status_table.assert_called_once()
            assert mock_update_month_data.call_count == 2

    def test_update_month_data(self):
        with patch.object(self.importer, '_process_month_data', return_value=pd.DataFrame({'dummy': [1]})) as mock_process_month_data, \
             patch.object(self.importer.scmd_manager, 'load_dataframe') as mock_load_dataframe:
            self.importer._update_month_data(pd.Timestamp("2022-01-01"))
            mock_process_month_data.assert_called_once()
            mock_load_dataframe.assert_called_once()

    def test_update_existing_tables(self):
        with patch.object(self.importer, '_update_month_data') as mock_update_month_data, \
             patch('viewer.management.commands.import_scmd.logger.info') as mock_logger:
            new_data = pd.DataFrame({
                "year_month": pd.to_datetime(["2022-01-01", "2022-02-01", "2022-03-01", "2022-04-01"]),
                "file_type": ["final", "wip", "final", "provisional"]
            })
            existing_data = pd.DataFrame({
                "year_month": pd.to_datetime(["2022-01-01", "2022-02-01", "2022-03-01"]),
                "file_type": ["final", "provisional", "wip"]
            })
            
            self.importer._update_existing_tables(new_data, existing_data)

            mock_logger.assert_called_with("Found 1 new months and 2 out-of-date months")
            assert mock_update_month_data.call_count == 3