import pytest
import pandas as pd

from unittest.mock import patch
from pipeline.flows.load_data_status import (
    extract_data_status,
    load_data_status,
)
from viewer.models import DataStatus


@pytest.fixture
def sample_data():
    """Fixture providing sample data for testing"""
    return pd.DataFrame(
        {
            "year_month": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "file_type": ["finalised", "finalised", "provisional"],
        }
    )


class TestLoadDataStatus:
    @patch("pipeline.flows.load_data_status.fetch_table_data_from_bq")
    def test_extract_data_status(self, mock_fetch, sample_data):

        mock_fetch.return_value = sample_data
        result = extract_data_status()

        mock_fetch.assert_called_once()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert list(result.columns) == ["year_month", "file_type"]

    @pytest.mark.django_db
    def test_load_data_status(self, sample_data):

        initial_data = [
            DataStatus(year_month="2023-12-01", file_type="finalised"),
            DataStatus(year_month="2024-01-01", file_type="provisional"),
        ]
        DataStatus.objects.bulk_create(initial_data)
        assert DataStatus.objects.count() == 2

        result = load_data_status(sample_data)

        assert result["created"] == 3
        assert result["deleted"] == 2
        assert result["total_records"] == 3

        stored_data = DataStatus.objects.all().order_by("year_month")
        data_list = list(stored_data)
        assert data_list[0].year_month.strftime("%Y-%m-%d") == "2024-01-01"
        assert data_list[0].file_type == "finalised"
        assert data_list[1].year_month.strftime("%Y-%m-%d") == "2024-02-01"
        assert data_list[1].file_type == "finalised"
        assert data_list[2].year_month.strftime("%Y-%m-%d") == "2024-03-01"
        assert data_list[2].file_type == "provisional"
