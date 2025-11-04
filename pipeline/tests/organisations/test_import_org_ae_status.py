import pytest
import pandas as pd
from datetime import date
from unittest.mock import Mock, patch
from pipeline.organisations.import_org_ae_status import (
    fetch_ae_data,
    prepare_dataframe
)

@pytest.fixture
def sample_html_index():
    return """
    <html>
        <body>
            <a href="https://www.england.nhs.uk/statistics/statistical-work-areas/\
ae-waiting-times-and-activity/ae-attendances-and-emergency-admissions-2023-24/">\
Monthly A&E Attendances and Emergency Admissions 2023-24</a>
        </body>
    </html>
    """

@pytest.fixture
def sample_html_year_page():
    return """
    <html>
        <body>
            <a href="Monthly-AE-March-2024.csv">March 2024 CSV</a>
        </body>
    </html>
    """

@pytest.fixture
def sample_new_csv_content():
    return """Period,Org Code,A&E attendances Type 1,A&E attendances Type 2,A&E attendances Other A&E Department
MSitAE-MARCH-2024,RR8,1234,0,0
MSitAE-MARCH-2024,RXH,5678,0,0
Total,ALL,6912,0,0"""

@pytest.fixture
def sample_old_csv_content():
    return """Period,Org Code,Number of A&E attendances Type 1,Number of A&E attendances Type 2,Number of A&E attendances Other A&E Department,Number of A&E attendances Type 3
MSitAE-AUGUST-2020,RR8,1234,0,0,0
MSitAE-AUGUST-2020,RXH,5678,0,0,0
Total,ALL,6912,0,0,0"""

@pytest.fixture
def mock_requests_get():
    with patch("requests.get") as mock_get:
        yield mock_get

def test_fetch_ae_data(mock_requests_get, sample_html_index, sample_html_year_page, sample_new_csv_content):
    mock_responses = [
        Mock(content=sample_html_index.encode()),
        Mock(content=sample_html_year_page.encode()),
        Mock(text=sample_new_csv_content),
    ]
    mock_requests_get.side_effect = mock_responses

    result = fetch_ae_data()

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["period", "ods_code", "has_ae"]
    assert len(result) == 2  # Should not include the Total row
    assert result["ods_code"].tolist() == ["RR8", "RXH"]
    assert all(result["has_ae"])


def test_prepare_dataframe():
    data = {
        "period": [pd.Timestamp("2024-03-01"), pd.Timestamp("2024-03-01")],
        "ods_code": ["RR8", "RXH"],
        "has_ae": [True, True]
    }
    df = pd.DataFrame(data)

    result = prepare_dataframe(df)

    assert isinstance(result["period"].iloc[0], date)
    assert isinstance(result["ods_code"].iloc[0], str)
    assert bool(result["has_ae"].iloc[0]) is True
