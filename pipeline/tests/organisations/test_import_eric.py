import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from pipeline.organisations.import_eric import (
    extract_year_from_url,
    find_latest_eric_data_url,
    download_eric_data,
    check_existing_eric_data,
    clean_eric_data,
)

@pytest.fixture
def sample_eric_publications_html():
    return """
    <html>
        <body>
            <div id="past-publications">
                <h2>Past Publications</h2>
                <a href="/data-and-information/publications/statistical/estates-returns-information-collection/summary-page-and-dataset-for-eric-2022-23">
                    Summary page and dataset for ERIC 2022-23
                </a>
                <a href="/data-and-information/publications/statistical/estates-returns-information-collection/summary-page-and-dataset-for-eric-2023-24">
                    Summary page and dataset for ERIC 2023-24
                </a>
            </div>
        </body>
    </html>
    """

@pytest.fixture
def sample_eric_publication_page_html():
    return """
    <html>
        <body>
            <a href="https://files.digital.nhs.uk/AF/F737A9/ERIC%20-%202023_24%20-%20Trust%20data.csv">ERIC Trust Data 2023-24</a>
        </body>
    </html>
    """

@pytest.fixture
def sample_eric_csv_content():
    return """Trust Code,Trust Name,Trust Type
RR8,Royal Berkshire NHS Foundation Trust,ACUTE - TEACHING
RXH,Royal Devon and Exeter NHS Foundation Trust,ACUTE - TEACHING
R0A,University Hospitals Birmingham NHS Foundation Trust,ACUTE - TEACHING"""

@pytest.fixture
def sample_eric_dataframe():
    return pd.DataFrame({
        'Trust Code': ['RR8', 'RXH', 'R0A'],
        'Trust Name': [
            'Royal Berkshire NHS Foundation Trust',
            'Royal Devon and Exeter NHS Foundation Trust', 
            'University Hospitals Birmingham NHS Foundation Trust'
        ],
        'Trust Type': ['ACUTE - TEACHING', 'ACUTE - TEACHING', 'ACUTE - TEACHING']
    })

@pytest.fixture
def sample_cleaned_eric_dataframe():
    return pd.DataFrame({
        'data_year': ['2023_24', '2023_24', '2023_24'],
        'trust_code': ['RR8', 'RXH', 'R0A'],
        'trust_name': [
            'Royal Berkshire NHS Foundation Trust',
            'Royal Devon and Exeter NHS Foundation Trust',
            'University Hospitals Birmingham NHS Foundation Trust'
        ],
        'trust_type': ['Acute - Teaching', 'Acute - Teaching', 'Acute - Teaching']
    })

@pytest.fixture
def mock_requests_get():
    with patch("requests.get") as mock_get:
        yield mock_get

@pytest.fixture
def mock_bigquery_client():
    with patch("pipeline.organisations.import_eric.get_bigquery_client") as mock_client:
        yield mock_client

@pytest.fixture
def mock_logger():
    with patch("pipeline.organisations.import_eric.get_run_logger") as mock_logger:
        yield mock_logger

def test_extract_year_from_url():
    assert extract_year_from_url("eric-2022-23") == 2223
    assert extract_year_from_url("no-year-here") == 0
    assert extract_year_from_url("") == 0

def test_find_latest_eric_data_url_success(mock_requests_get, sample_eric_publications_html, sample_eric_publication_page_html):
    mock_responses = [
        Mock(content=sample_eric_publications_html.encode()),
        Mock(content=sample_eric_publication_page_html.encode())
    ]
    mock_requests_get.side_effect = mock_responses

    result = find_latest_eric_data_url()

    assert result is not None
    assert isinstance(result, tuple)
    assert len(result) == 2
    url, year = result
    assert "files.digital.nhs.uk" in url
    assert url.endswith(".csv")
    assert year == "2023_24"


def test_download_eric_data_success(mock_requests_get, sample_eric_csv_content):
    mock_response = Mock(text=sample_eric_csv_content, content=sample_eric_csv_content.encode())
    mock_requests_get.return_value = mock_response

    result = download_eric_data("https://example.com/eric-data.csv")

    assert result is not None
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3
    assert "Trust Code" in result.columns
    assert "Trust Name" in result.columns
    assert "Trust Type" in result.columns


def test_check_existing_eric_data_exists(mock_bigquery_client):
    mock_client = MagicMock()
    mock_bigquery_client.return_value = mock_client
    
    mock_query_result = MagicMock()
    mock_query_result.result.return_value = [Mock(count=5)]
    mock_client.query.return_value = mock_query_result

    result = check_existing_eric_data("2023_24")

    assert result is True
    mock_client.query.assert_called_once()

def test_check_existing_eric_data_not_exists(mock_bigquery_client):
    """Test when ERIC data does not exist."""
    mock_client = MagicMock()
    mock_bigquery_client.return_value = mock_client

    mock_query_result = MagicMock()
    mock_query_result.result.return_value = [Mock(count=0)]
    mock_client.query.return_value = mock_query_result

    result = check_existing_eric_data("2023_24")

    assert result is False

def test_clean_eric_data_success(sample_eric_dataframe):
    """Test successful cleaning of ERIC data."""
    result = clean_eric_data(sample_eric_dataframe, "2023_24")

    assert result is not None
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3
    assert "data_year" in result.columns
    assert "trust_code" in result.columns
    assert "trust_name" in result.columns
    assert "trust_type" in result.columns
    assert result["data_year"].iloc[0] == "2023_24"
    assert result["trust_type"].iloc[0] == "Acute - Teaching"

def test_clean_eric_data_with_missing_trust_codes():
    """Test cleaning data with missing trust codes."""
    df_with_missing = pd.DataFrame({
        'Trust Code': ['RR8', '', None, 'R0A'],
        'Trust Name': ['Trust 1', 'Trust 2', 'Trust 3', 'Trust 4'],
        'Trust Type': ['ACUTE', 'ACUTE', 'ACUTE', 'ACUTE']
    })

    result = clean_eric_data(df_with_missing, "2023_24")

    assert len(result) == 2  # Should remove rows with missing trust codes
    assert result["trust_code"].tolist() == ["RR8", "R0A"]
