import pytest
import pandas as pd
from unittest.mock import Mock, patch
from pipeline.flows.import_ddd_expressed_as import (
    load_expressed_as_from_csv,
    validate_expressed_as_units,
    import_expressed_as,
    import_expressed_as_flow
)
from pipeline.utils.config import PROJECT_ID, DATASET_ID
from pipeline.bq_tables import VMP_EXPRESSED_AS_TABLE_SPEC


class TestImportDddExpressedAs:
    @pytest.fixture
    def mock_bigquery_client(self):
        with patch('pipeline.flows.import_ddd_expressed_as.get_bigquery_client') as mock:
            client = Mock()
            client.load_table_from_json.return_value.result.return_value = None
            mock.return_value = client
            yield client

    @pytest.fixture
    def sample_csv_data(self):
        return pd.DataFrame({
            "vmp_id": ["38893611000001108", "12345678901234567"],
            "vmp_name": [
                "Aclidinium bromide 375micrograms/dose dry powder inhale",
                "Test drug 500mg tablets"
            ],
            "ddd_comment": [
                "Expressed as aclidinium, delivered dose",
                "Expressed as active ingredient"
            ],
            "expressed_as_strnt_nmrtr": [322, 500.0],
            "expressed_as_strnt_nmrtr_uom": ["258685003", "258684004"],
            "expressed_as_strnt_nmrtr_uom_name": ["micrograms", "mg"]
        })

    @pytest.fixture
    def mock_csv_path(self, tmp_path, sample_csv_data):
        csv_path = tmp_path / "test_vmp_expressed_as.csv"
        sample_csv_data.to_csv(csv_path, index=False)
        return csv_path

    def test_load_expressed_as_from_csv(self, mock_csv_path, sample_csv_data):
        """Test the load_expressed_as_from_csv task"""
        result = load_expressed_as_from_csv(mock_csv_path)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == list(sample_csv_data.columns)
    
        first_row = result.iloc[0]
        assert first_row["vmp_id"] == "38893611000001108"
        assert first_row["vmp_name"] == "Aclidinium bromide 375micrograms/dose dry powder inhale"
        assert first_row["ddd_comment"] == "Expressed as aclidinium, delivered dose"
        assert first_row["expressed_as_strnt_nmrtr"] == 322
        assert str(first_row["expressed_as_strnt_nmrtr_uom"]) == "258685003"
        assert first_row["expressed_as_strnt_nmrtr_uom_name"] == "micrograms"


    def test_load_expressed_as_from_csv_missing_columns(self, tmp_path):
        """Test load_expressed_as_from_csv with missing required columns"""
        csv_path = tmp_path / "invalid.csv"
        invalid_df = pd.DataFrame({
            "vmp_id": ["123"],
            "vmp_name": ["Test"]
        })
        invalid_df.to_csv(csv_path, index=False)
        
        with pytest.raises(ValueError, match="Missing required columns"):
            load_expressed_as_from_csv(csv_path)

    @patch('pipeline.flows.import_ddd_expressed_as.get_bigquery_client')
    def test_validate_expressed_as_units(self, mock_get_client, sample_csv_data):
        """Test the validate_expressed_as_units task"""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        
        mock_row1 = Mock()
        mock_row1.unit_id = "258685003"
        mock_row1.unit = "micrograms"
        mock_row2 = Mock()
        mock_row2.unit_id = "258684004"
        mock_row2.unit = "mg"
        
        mock_result = Mock()
        mock_result.__iter__ = lambda self: iter([mock_row1, mock_row2])
        mock_client.query.return_value.result.return_value = mock_result
        
        result = validate_expressed_as_units(sample_csv_data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        
        mock_client.query.assert_called_once()
        query_call = mock_client.query.call_args[0][0]
        assert "unit_conversion" in query_call
        assert "unit_id" in query_call

    @patch('pipeline.flows.import_ddd_expressed_as.get_bigquery_client')
    def test_validate_expressed_as_units_invalid_unit_id(self, mock_get_client, sample_csv_data):
        """Test validate_expressed_as_units with invalid unit ID"""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        
        mock_row = Mock()
        mock_row.unit_id = "258685003"
        mock_row.unit = "micrograms"
        
        mock_result = Mock()
        mock_result.__iter__ = lambda self: iter([mock_row])
        mock_client.query.return_value.result.return_value = mock_result

        with pytest.raises(ValueError, match="Invalid unit IDs found"):
            validate_expressed_as_units(sample_csv_data)

    @patch('pipeline.flows.import_ddd_expressed_as.get_bigquery_client')
    def test_validate_expressed_as_units_name_mismatch(self, mock_get_client, sample_csv_data):
        """Test validate_expressed_as_units with unit name mismatch"""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        
        # Mock the query results with mismatched unit names
        mock_row1 = Mock()
        mock_row1.unit_id = "258685003"
        mock_row1.unit = "different_name"
        mock_row2 = Mock()
        mock_row2.unit_id = "258684004"
        mock_row2.unit = "mg"
        
        mock_result = Mock()
        mock_result.__iter__ = lambda self: iter([mock_row1, mock_row2])
        mock_client.query.return_value.result.return_value = mock_result
        
        with pytest.raises(ValueError, match="Unit name mismatches found"):
            validate_expressed_as_units(sample_csv_data)

    def test_import_expressed_as(self, mock_bigquery_client, sample_csv_data):
        """Test the import_expressed_as task"""
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{VMP_EXPRESSED_AS_TABLE_SPEC.table_id}"
        
        import_expressed_as(table_id, sample_csv_data)

        mock_bigquery_client.load_table_from_json.assert_called_once()
        
        actual_records = mock_bigquery_client.load_table_from_json.call_args[0][0]
        assert len(actual_records) == 2

        for record in actual_records:
            expected_keys = {
                'vmp_id', 'vmp_name', 'ddd_comment', 
                'expressed_as_strnt_nmrtr', 'expressed_as_strnt_nmrtr_uom', 'expressed_as_strnt_nmrtr_uom_name'
            }
            assert set(record.keys()) == expected_keys
            
            assert isinstance(record['vmp_id'], str)
            assert isinstance(record['vmp_name'], str)
            assert isinstance(record['ddd_comment'], str)
            assert isinstance(record['expressed_as_strnt_nmrtr'], (int, float))
            assert isinstance(record['expressed_as_strnt_nmrtr_uom'], str)
            assert isinstance(record['expressed_as_strnt_nmrtr_uom_name'], str)

        first_record = actual_records[0]
        assert first_record['vmp_id'] == "38893611000001108"
        assert first_record['vmp_name'] == "Aclidinium bromide 375micrograms/dose dry powder inhale"
        assert first_record['ddd_comment'] == "Expressed as aclidinium, delivered dose"
        assert first_record['expressed_as_strnt_nmrtr'] == 322
        assert first_record['expressed_as_strnt_nmrtr_uom'] == "258685003"
        assert first_record['expressed_as_strnt_nmrtr_uom_name'] == "micrograms"
