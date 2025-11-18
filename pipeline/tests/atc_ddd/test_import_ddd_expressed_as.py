import pytest
import pandas as pd
from unittest.mock import Mock, patch
from pipeline.atc_ddd.ddd_comments.import_ddd_expressed_as import (
    load_expressed_as_from_csv,
    validate_expressed_as_vmps,
)
from pipeline.setup.config import VMP_TABLE_ID



class TestImportDddExpressedAs:
    @pytest.fixture
    def mock_bigquery_client(self):
        with patch('pipeline.atc_ddd.ddd_comments.import_ddd_expressed_as.get_bigquery_client') as mock:
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
            "expressed_as_strnt_nmrtr_uom_name": ["micrograms", "mg"],
            "ingredient_code": ["123456789", "987654321"],
            "ingredient_name": ["Aclidinium bromide", "Test ingredient"]
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
        assert str(first_row["ingredient_code"]) == "123456789"
        assert first_row["ingredient_name"] == "Aclidinium bromide"


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

    @patch('pipeline.atc_ddd.ddd_comments.import_ddd_expressed_as.get_bigquery_client')
    def test_validate_expressed_as_vmps(self, mock_get_client, sample_csv_data):
        """Test the validate_expressed_as_vmps task"""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        
        mock_row1 = Mock()
        mock_row1.vmp_code = "38893611000001108"
        mock_row1.vmp_name = "Aclidinium bromide 375micrograms/dose dry powder inhale"
        mock_row2 = Mock()
        mock_row2.vmp_code = "12345678901234567"
        mock_row2.vmp_name = "Test drug 500mg tablets"
        
        mock_result = Mock()
        mock_result.__iter__ = lambda self: iter([mock_row1, mock_row2])
        mock_client.query.return_value.result.return_value = mock_result
        
        result = validate_expressed_as_vmps(sample_csv_data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        
        mock_client.query.assert_called_once()
        query_call = mock_client.query.call_args[0][0]
        assert VMP_TABLE_ID in query_call
        assert "vmp_code" in query_call
        assert "vmp_name" in query_call


 