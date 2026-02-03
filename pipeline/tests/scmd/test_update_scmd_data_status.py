from unittest.mock import Mock, patch

from pipeline.scmd.update_scmd_data_status import (
    update_data_status_table,
)

class TestUpdateDataStatusTable:
    @patch('pipeline.scmd.update_scmd_data_status.get_run_logger')
    @patch('pipeline.scmd.update_scmd_data_status.get_bigquery_client')
    def test_update_data_status_table_no_changes(self, mock_get_client, mock_logger):
        mock_logger.return_value = Mock()
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        finalised_months = {"2024-01-01", "2024-02-01"}
        provisional_months = {"2024-03-01"}
        existing_status = {
            "2024-01-01": "final",
            "2024-02-01": "final",
            "2024-03-01": "provisional",
        }

        result = update_data_status_table(
            finalised_months, provisional_months, existing_status
        )

        assert result["status"] == "no_updates_needed"
        assert result["updated_months"] == 0
        assert result["finalised_months"] == 2
        assert result["provisional_months"] == 1
        mock_client.load_table_from_json.assert_not_called()

    @patch('pipeline.scmd.update_scmd_data_status.get_run_logger')
    @patch('pipeline.scmd.update_scmd_data_status.get_bigquery_client')
    def test_update_data_status_table_with_changes(self, mock_get_client, mock_logger):
        mock_logger.return_value = Mock()
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_job = Mock()
        mock_job.result.return_value = None
        mock_client.load_table_from_json.return_value = mock_job

        finalised_months = {"2024-01-01", "2024-02-01"}
        provisional_months = {"2024-03-01", "2024-02-01"}
        existing_status = {
            "2024-01-01": "final",
            "2024-02-01": "final",
        }

        result = update_data_status_table(
            finalised_months, provisional_months, existing_status
        )
        assert result["status"] == "completed"
        assert result["updated_months"] == 1
        assert result["finalised_months"] == 2
        assert result["provisional_months"] == 1
        assert result["total_months"] == 3

        mock_client.load_table_from_json.assert_called_once()
        call_args = mock_client.load_table_from_json.call_args
        records = call_args[0][0]
        assert len(records) == 3

        records_dict = {r["year_month"]: r["file_type"] for r in records}
        assert records_dict["2024-01-01"] == "final"
        assert records_dict["2024-02-01"] == "final"
        assert records_dict["2024-03-01"] == "provisional"


    @patch('pipeline.scmd.update_scmd_data_status.get_run_logger')
    @patch('pipeline.scmd.update_scmd_data_status.get_bigquery_client')
    def test_update_data_status_table_new_months(self, mock_get_client, mock_logger):
        mock_logger.return_value = Mock()
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_job = Mock()
        mock_job.result.return_value = None
        mock_client.load_table_from_json.return_value = mock_job

        finalised_months = {"2024-01-01", "2024-02-01"}
        provisional_months = {"2024-03-01"}
        existing_status = {
            "2024-01-01": "final",
            "2024-02-01": "final",
        }

        result = update_data_status_table(
            finalised_months, provisional_months, existing_status
        )

        assert result["updated_months"] == 1
        assert result["total_months"] == 3

        call_args = mock_client.load_table_from_json.call_args
        records = call_args[0][0]
        assert len(records) == 3

