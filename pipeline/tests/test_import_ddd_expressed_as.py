import pytest
from unittest.mock import Mock, patch
from pipeline.flows.import_ddd_expressed_as import (
    import_expressed_as,
    create_expressed_as_dict,
)
from pipeline.utils.config import PROJECT_ID, DATASET_ID
from pipeline.bq_tables import VMP_EXPRESSED_AS_TABLE_SPEC


class TestImportDddCommentMapping:
    @pytest.fixture
    def mock_bigquery_client(self):
        with patch('pipeline.flows.import_ddd_comment_mapping.get_bigquery_client') as mock:
            client = Mock()
            client.load_table_from_json.return_value.result.return_value = None
            mock.return_value = client
            yield client

    @pytest.fixture
    def sample_expressed_as_dict(self):
        return {
            "38893611000001108": {
                "vmp_name": "Aclidinium bromide 375micrograms/dose dry powder inhale",
                "ddd_comment": "Expressed as aclidinium, delivered dose",
                "expressed_as_strnt_nmrtr": 322,
                "expressed_as_strnt_nmrtr_uom": "258685003",
                "expressed_as_strnt_nmrtr_uom_name": "micrograms",
            },
            "12345678901234567": {
                "vmp_name": "Test drug 500mg tablets",
                "ddd_comment": "Expressed as active ingredient",
                "expressed_as_strnt_nmrtr": 500.0,
                "expressed_as_strnt_nmrtr_uom": "258684004",
                "expressed_as_strnt_nmrtr_uom_name": "mg",
            }
        }

    def test_create_expressed_as_dict(self):
        """Test the create_expressed_as_dict task"""
        result = create_expressed_as_dict()
        
        assert isinstance(result, dict)
        assert len(result) > 0

        first_key = list(result.keys())[0]
        first_item = result[first_key]
        
        expected_keys = {
            "vmp_name", "ddd_comment", "expressed_as_strnt_nmrtr", 
            "expressed_as_strnt_nmrtr_uom", "expressed_as_strnt_nmrtr_uom_name"
        }
        assert set(first_item.keys()) == expected_keys

    def test_import_expressed_as(self, mock_bigquery_client, sample_expressed_as_dict):
        """Test the import_expressed_as task"""
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{VMP_EXPRESSED_AS_TABLE_SPEC.table_id}"
        
        import_expressed_as(table_id, sample_expressed_as_dict)

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
