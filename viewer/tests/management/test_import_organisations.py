import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime
from google.api_core.exceptions import NotFound
from viewer.management.commands.import_organisations import (
    ORDAPIClient,
    OrganisationDataProcessor,
    DataFrameBuilder,
    RegionDataProcessor,
    BigQueryUploader,
    OrganisationImporter,
)

class TestORDAPIClient:
    @pytest.fixture
    def client(self):
        return ORDAPIClient("ttps://directory.spineservices.nhs.uk/ORD/2-0-0/organisations")

    @pytest.fixture
    def mock_requests_get(self):
        with patch('requests.get') as mock_get:
            yield mock_get

    def test_get_all_orgs(self, mock_requests_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "Organisations":[
                {
                    "Name":"NORTH LONDON NHS FOUNDATION TRUST",
                    "OrgId":"G6V2S",
                    "Status":"Active",
                    "OrgRecordClass":"RC1",
                    "PostCode":"NW1 0PE",
                    "LastChangeDate":"2024-08-12",
                    "PrimaryRoleId":"RO197",
                    "PrimaryRoleDescription":"NHS TRUST",
                    "OrgLink":"https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations/G6V2S"
                },
                {
                    "Name":"MANCHESTER UNIVERSITY NHS FOUNDATION TRUST",
                    "OrgId":"R0A",
                    "Status":"Active",
                    "OrgRecordClass":"RC1",
                    "PostCode":"M13 9WL",
                    "LastChangeDate":"2023-11-14",
                    "PrimaryRoleId":"RO197",
                    "PrimaryRoleDescription":"NHS TRUST",
                    "OrgLink":"https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations/R0A"
                },
            ]
        }
        mock_requests_get.return_value = mock_response

        orgs = ORDAPIClient.get_all_orgs(["RO197"])
        assert len(orgs) == 2
        assert orgs == ["G6V2S", "R0A"]

    def test_get_org_details(self, mock_requests_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {"Organisation": {"Name": "NORTH LONDON NHS FOUNDATION TRUST"}}
        mock_requests_get.return_value = mock_response

        details = ORDAPIClient.get_org_details(["G6V2S"])
        assert len(details) == 1
        assert details["G6V2S"]["Organisation"]["Name"] == "NORTH LONDON NHS FOUNDATION TRUST"

class TestOrganisationDataProcessor:
    @pytest.fixture
    def processor(self):
        return OrganisationDataProcessor()

    def test_filter_england(self):
        test_data = {
            "G6V2S": {"Organisation": {"GeoLoc": {"Location": {"Country": "ENGLAND"}}}},
            "RT7": {"Organisation": {"GeoLoc": {"Location": {"Country": "WALES"}}}},
        }
        filtered = OrganisationDataProcessor._filter_england(test_data)
        assert len(filtered) == 1
        assert "G6V2S" in filtered

    def test_process_org_details(self):
        test_data = {
            "RJ1": {
                "Organisation": {
                    "Name": "GUY'S AND ST THOMAS' NHS FOUNDATION TRUST",
                    "GeoLoc": {
                        "Location": {
                            "Country": "ENGLAND"
                        }
                    },
                    "Rels": {
                        "Rel": [
                            {
                                "Target": {
                                    "OrgId": {
                                        "extension": "QKK"
                                    }
                                },
                                "id": "RE5"
                            },
                        ]
                    },
                    "Succs": {
                        "Succ": [
                            {
                                "Type": "Predecessor",
                                "Target": {
                                    "OrgId": {
                                        "extension": "RAV"
                                    }
                                }
                            },
                            {
                                "Date": [
                                    {
                                        "Type": "Legal",
                                        "Start": "2021-02-01"
                                    }
                                ],
                                "Type": "Predecessor",
                                "Target": {
                                    "OrgId": {
                                        "extension": "RT3"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
        
        
        predecessors, icbs = OrganisationDataProcessor.process_org_details(test_data)
        
        assert predecessors == {"RJ1": ["RAV", "RT3"]}
        assert icbs == {"RJ1": "QKK"}

class TestDataFrameBuilder:
    @pytest.fixture
    def builder(self):
        return DataFrameBuilder()

    def test_get_org_details_dict(self):
        org_details = {
            "Organisation": {
                "Name": "GUY'S AND ST THOMAS' NHS FOUNDATION TRUST",
                "OrgId": {"extension": "RJ1"},
                "GeoLoc": {"Location": {"PostCode": "SE1 7EH"}},
                "Date": [
                    {
                        "Type": "Operational",
                        "Start": "1993-04-01"
                    }
                ]
            }
        }
        icbs = {"RJ1": "QKK"}

        result = DataFrameBuilder._get_org_details_dict(org_details, icbs)
        
        assert result["ods_name"] == "GUY'S AND ST THOMAS' NHS FOUNDATION TRUST"
        assert result["postcode"] == "SE1 7EH"
        assert result["operational_open_date"] == "1993-04-01"
        assert result["icb"] == "QKK"

    def test_create_org_mapping_df(self, builder):
        predecessors = {"RJ1": ["RT3"]}
        orgs_details = {
            "RJ1": {
                "Organisation": {
                    "Name": "GUY'S AND ST THOMAS' NHS FOUNDATION TRUST",
                    "OrgId": {"extension": "RJ1"},
                    "GeoLoc": {"Location": {"PostCode": "SE1 7EH"}},
                    "Date": []
                }
            },
            "RT3": {
                "Organisation": {
                    "Name": "ROYAL BROMPTON & HAREFIELD NHS FOUNDATION TRUST",
                    "OrgId": {"extension": "RT3"},
                    "GeoLoc": {"Location": {"PostCode": "SW3 6NP"}},
                    "Date": []
                }
            }
        }
        icbs = {"RJ1": "QKK", "RT3": "QKK"}

        df = builder.create_org_mapping_df(predecessors, orgs_details, icbs)
        
        assert len(df) == 2
        assert "RJ1" in df["ods_code"].values
        assert "RT3" in df["ods_code"].values
        assert df.loc[df["ods_code"] == "RT3", "successor_ods_code"].iloc[0] == "RJ1"


class TestRegionDataProcessor:
    @pytest.fixture
    def mock_api_client(self):
        return MagicMock()

    @pytest.fixture
    def processor(self, mock_api_client):
        return RegionDataProcessor(mock_api_client)

    def test_get_icb_regions(self, processor, mock_api_client):
        mock_api_client.get_org_details.return_value = {
            "QKK": {
                "Organisation": {
                    "Rels": {
                        "Rel": [
                            {
                                "id": "RE2",
                                "Target": {
                                    "OrgId": {
                                        "extension": "Y56"
                                    }
                                }
                            }
                        ]
                    }
                }
            },
        }

        result = processor.get_icb_regions(["QKK"])
        
        assert result == {"QKK": "Y56"}
        mock_api_client.get_org_details.assert_called_once_with(["QKK"])

    def test_get_region_names(self, processor, mock_api_client):
        mock_api_client.get_org_details.return_value = {
            "Y56": {
                "Organisation": {
                    "Name": "LONDON COMMISSIONING REGION"
                }
            },
        }

        result = processor.get_region_names({"Y56"})
        
        assert result == {
            "Y56": "LONDON COMMISSIONING REGION"
        }
        mock_api_client.get_org_details.assert_called_once_with(["Y56"])

    def test_format_org_name(self, processor):
        test_cases = [
            (
                "NORTH EAST AND YORKSHIRE COMMISSIONING REGION",
                "North East And Yorkshire"
            ),
            (
                "NHS ENGLAND LONDON COMMISSIONING REGION",
                "NHS England London"
            ),
            (
                "SOUTH WEST NHS COMMISSIONING REGION",
                "South West NHS"
            ),
            (
                "NHS TRUST NAME",
                "NHS Trust Name"
            )
        ]

        for input_name, expected_output in test_cases:
            assert RegionDataProcessor.format_org_name(input_name) == expected_output


class TestBigQueryUploader:
    @pytest.fixture
    def uploader(self, mock_bigquery_client):
        with patch('viewer.management.utils.get_bigquery_client') as mock_get_client:
            mock_get_client.return_value = mock_bigquery_client.return_value
            return BigQueryUploader("test-project", "test-dataset", "test-table")

    @pytest.fixture
    def mock_bigquery_client(self):
        with patch('google.cloud.bigquery.Client') as mock_client:
            mock_instance = mock_client.return_value
            mock_instance.get_table = MagicMock()
            mock_instance.create_table = MagicMock()
            mock_instance.load_table_from_dataframe = MagicMock()
            yield mock_client

    def test_prepare_df(self, uploader):
        test_df = pd.DataFrame({
            "ods_code": ["ORG1"],
            "ods_name": ["Test Org"],
            "successor_ods_code": [None],
            "legal_closed_date": [datetime(2021, 1, 1)],
            "operational_closed_date": [None],
            "legal_open_date": [None],
            "operational_open_date": [None],
            "succession_date": [None],
            "postcode": ["AB1 2CD"],
            "region": ["REG1"],
            "icb": ["ICB1"]
        })

        prepared_df = uploader._prepare_df(test_df)
        assert prepared_df["ods_code"].dtype == "object"
        assert prepared_df["legal_closed_date"].dtype == "datetime64[ns]"

    def test_upload_new_table(self, uploader, mock_bigquery_client):
        mock_client = mock_bigquery_client.return_value
        mock_client.get_table.side_effect = NotFound("Table not found")

        test_df = pd.DataFrame({
            "ods_code": ["ORG1"],
            "ods_name": ["Test Org"],
            "successor_ods_code": [None],
            "legal_closed_date": [None],
            "operational_closed_date": [None],
            "legal_open_date": [None],
            "operational_open_date": [None],
            "succession_date": [None],
            "postcode": ["AB1 2CD"],
            "region": ["REG1"],
            "icb": ["ICB1"]
        })
        
        uploader.upload(test_df)

        mock_client.create_table.assert_called_once()
        mock_client.load_table_from_dataframe.assert_called_once()

