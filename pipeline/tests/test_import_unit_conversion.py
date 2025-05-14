import pytest
from unittest.mock import Mock, patch
from pipeline.flows.import_unit_conversion import (
    import_unit_conversion,
)
from pipeline.utils.config import PROJECT_ID, DATASET_ID, UNITS_CONVERSION_TABLE_ID


class TestImportUnitConversion:
    @pytest.fixture
    def mock_bigquery_client(self):
        with patch('pipeline.flows.import_unit_conversion.get_bigquery_client') as mock:
            client = Mock()
            client.load_table_from_json.return_value.result.return_value = None
            mock.return_value = client
            yield client

    @pytest.fixture
    def sample_units_dict(self):
        return {
            'gram': {
                'basis': 'gram',
                'conversion_factor': 1,
                'id': '258682000',
                'basis_id': '258682000'
            },
            'mg': {
                'basis': 'gram',
                'conversion_factor': 0.001,
                'id': '258684004',
                'basis_id': '258682000'
            }
        }

    def test_import_unit_conversion(self, mock_bigquery_client, sample_units_dict):
        """Test the import_unit_conversion task"""
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{UNITS_CONVERSION_TABLE_ID}"
        
        import_unit_conversion(table_id, sample_units_dict)

        mock_bigquery_client.load_table_from_json.assert_called_once()
        
        actual_records = mock_bigquery_client.load_table_from_json.call_args[0][0]
        assert len(actual_records) == 2

        for record in actual_records:
            assert set(record.keys()) == {'unit', 'basis', 'conversion_factor', 'unit_id', 'basis_id'}
            assert isinstance(record['conversion_factor'], float)

