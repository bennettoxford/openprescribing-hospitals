import pytest
from unittest.mock import Mock, patch
from pipeline.organisations.import_ord_data import (
    fetch_all_trusts_from_ord,
    fetch_org_details,
    process_org_details,
    get_icb_regions,
    format_org_name,
    resolve_ultimate_successors,
    create_org_mapping_df,
)

@pytest.fixture
def sample_org_response_ro197():
    return {
        "Organisations": [
            {
                "Name": "SAMPLE ACUTE NHS FOUNDATION TRUST",
                "OrgId": "ABC1",
                "Status": "Active",
                "OrgRecordClass": "RC1",
                "PostCode": "SW1A 1AA",
                "LastChangeDate": "2023-12-01",
                "PrimaryRoleId": "RO197",
                "PrimaryRoleDescription": "NHS TRUST",
                "OrgLink": "https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations/ABC1"
            },
            {
                "Name": "NEW MERGED TRUST NHS FOUNDATION TRUST",
                "OrgId": "GHI3",
                "Status": "Active",
                "OrgRecordClass": "RC1",
                "PostCode": "E1 6AN",
                "LastChangeDate": "2023-12-01",
                "PrimaryRoleId": "RO197",
                "PrimaryRoleDescription": "NHS TRUST",
                "OrgLink": "https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations/GHI3"
            }
        ]
    }

@pytest.fixture
def sample_org_response_ro24():
    return {
        "Organisations": [
            {
                "Name": "OLD TRUST NHS FOUNDATION TRUST",
                "OrgId": "DEF2",
                "Status": "Inactive",
                "OrgRecordClass": "RC1",
                "PostCode": "E1 6AN",
                "LastChangeDate": "2022-04-01",
                "PrimaryRoleId": "RO24",
                "PrimaryRoleDescription": "MENTAL HEALTH TRUST",
                "OrgLink": "https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations/DEF2"
            },
            {
                "Name": "ANOTHER OLD TRUST",
                "OrgId": "JKL4",
                "Status": "Inactive",
                "OrgRecordClass": "RC1",
                "PostCode": "E2 8AN",
                "LastChangeDate": "2022-04-01",
                "PrimaryRoleId": "RO24",
                "PrimaryRoleDescription": "MENTAL HEALTH TRUST",
                "OrgLink": "https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations/JKL4"
            }
        ]
    }

@pytest.fixture
def sample_non_england_org():
    return {
        "XYZ9": {  # Non-English organisation
            "Organisation": {
                "Name": "WELSH SAMPLE HOSPITAL",
                "Date": [
                    {"Type": "Operational", "Start": "2019-01-01"},
                    {"Type": "Legal", "Start": "2019-01-01"}
                ],
                "OrgId": {"extension": "XYZ9"},
                "Status": "Active",
                "LastChangeDate": "2023-12-01",
                "orgRecordClass": "RC1",
                "GeoLoc": {
                    "Location": {
                        "PostCode": "CF10 1AA",
                        "Country": "WALES",
                    }
                }
            }
        }
    }

@pytest.fixture
def sample_org_details_england_only():
    return {
        "ABC1": {  # Standard English NHS Trust with ICB relationship
            "Organisation": {
                "Name": "SAMPLE HOSPITAL NHS FOUNDATION TRUST",
                "Date": [
                    {"Type": "Operational", "Start": "2020-01-01"},
                    {"Type": "Legal", "Start": "2020-01-01"}
                ],
                "OrgId": {"extension": "ABC1"},
                "Status": "Active",
                "LastChangeDate": "2023-12-01",
                "orgRecordClass": "RC1",
                "GeoLoc": {
                    "Location": {
                        "PostCode": "SW1A 1AA",
                        "Country": "ENGLAND",
                    }
                },
                "Rels": {
                    "Rel": [
                        {
                            "Date": [{"Type": "Operational", "Start": "2020-01-01"}],
                            "Status": "Active",
                            "Target": {"OrgId": {"extension": "ICB1"}},
                            "id": "RE5",
                        }
                    ]
                }
            }
        },
        "DEF2": {  # Organisation with successors
            "Organisation": {
                "Name": "OLD TRUST NHS FOUNDATION TRUST",
                "Date": [
                    {"Type": "Operational", "Start": "2015-01-01", "End": "2022-03-31"},
                    {"Type": "Legal", "Start": "2015-01-01", "End": "2022-03-31"}
                ],
                "OrgId": {"extension": "DEF2"},
                "Status": "Inactive",
                "LastChangeDate": "2022-04-01",
                "orgRecordClass": "RC1",
                "GeoLoc": {
                    "Location": {
                        "PostCode": "E1 6AN",
                        "Country": "ENGLAND",
                    }
                },
                "Rels": {
                    "Rel": [
                        {
                            "Date": [{"Type": "Operational", "Start": "2015-01-01"}],
                            "Status": "Inactive",
                            "Target": {"OrgId": {"extension": "ICB2"}},
                            "id": "RE5",
                        }
                    ]
                },
                "Succs": {
                    "Succ": [
                        {
                            "Date": [{"Type": "Legal", "Start": "2022-04-01"}],
                            "Type": "Successor",
                            "Target": {"OrgId": {"extension": "GHI3"}}
                        }
                    ]
                }
            }
        },
        "GHI3": {  # Organisation with predecessors and ICB
            "Organisation": {
                "Name": "NEW MERGED TRUST NHS FOUNDATION TRUST",
                "Date": [
                    {"Type": "Operational", "Start": "2022-04-01"},
                    {"Type": "Legal", "Start": "2022-04-01"}
                ],
                "OrgId": {"extension": "GHI3"},
                "Status": "Active",
                "LastChangeDate": "2023-12-01",
                "orgRecordClass": "RC1",
                "GeoLoc": {
                    "Location": {
                        "PostCode": "E1 6AN",
                        "Country": "ENGLAND",
                    }
                },
                "Rels": {
                    "Rel": [
                        {
                            "Date": [{"Type": "Operational", "Start": "2022-04-01"}],
                            "Status": "Active",
                            "Target": {"OrgId": {"extension": "ICB2"}},
                            "id": "RE5",
                        }
                    ]
                },
                "Succs": {
                    "Succ": [
                        {
                            "Date": [{"Type": "Legal", "Start": "2022-04-01"}],
                            "Type": "Predecessor",
                            "Target": {"OrgId": {"extension": "DEF2"}}
                        },
                        {
                            "Date": [{"Type": "Legal", "Start": "2022-04-01"}],
                            "Type": "Predecessor",
                            "Target": {"OrgId": {"extension": "JKL4"}}
                        }
                    ]
                }
            }
        },
        "JKL4": {  # Another predecessor organisation
            "Organisation": {
                "Name": "ANOTHER OLD TRUST",
                "Date": [
                    {"Type": "Operational", "Start": "2010-01-01", "End": "2022-03-31"},
                    {"Type": "Legal", "Start": "2010-01-01", "End": "2022-03-31"}
                ],
                "OrgId": {"extension": "JKL4"},
                "Status": "Inactive",
                "LastChangeDate": "2022-04-01",
                "orgRecordClass": "RC1",
                "GeoLoc": {
                    "Location": {
                        "PostCode": "E2 8AN",
                        "Country": "ENGLAND",
                    }
                },
                "Succs": {
                    "Succ": [
                        {
                            "Date": [{"Type": "Legal", "Start": "2022-04-01"}],
                            "Type": "Successor",
                            "Target": {"OrgId": {"extension": "GHI3"}}
                        }
                    ]
                }
            }
        }
    }

@pytest.fixture
def sample_org_details(sample_org_details_england_only, sample_non_england_org):
    """Combined fixture with both England and non-England organisations"""
    return {**sample_org_details_england_only, **sample_non_england_org}

@pytest.fixture
def sample_icb_details():
    return {
        "ICB1": {  # Standard ICB with region relationship
            "Organisation": {
                "Name": "NORTH EAST ICB",
                "Date": [
                    {"Type": "Operational", "Start": "2022-07-01"},
                    {"Type": "Legal", "Start": "2022-07-01"}
                ],
                "OrgId": {"extension": "ICB1"},
                "Status": "Active",
                "LastChangeDate": "2023-12-01",
                "orgRecordClass": "RC1",
                "GeoLoc": {
                    "Location": {
                        "PostCode": "NE1 1AA",
                        "Country": "ENGLAND",
                    }
                },
                "Rels": {
                    "Rel": [
                        {
                            "Date": [{"Type": "Operational", "Start": "2022-07-01"}],
                            "Status": "Active",
                            "Target": {"OrgId": {"extension": "REG1"}},
                            "id": "RE2",
                        }
                    ]
                }
            }
        },
        "ICB2": {  # Another ICB with different region
            "Organisation": {
                "Name": "LONDON ICB",
                "Date": [
                    {"Type": "Operational", "Start": "2022-07-01"},
                    {"Type": "Legal", "Start": "2022-07-01"}
                ],
                "OrgId": {"extension": "ICB2"},
                "Status": "Active",
                "LastChangeDate": "2023-12-01",
                "orgRecordClass": "RC1",
                "GeoLoc": {
                    "Location": {
                        "PostCode": "E1 6AN",
                        "Country": "ENGLAND",
                    }
                },
                "Rels": {
                    "Rel": [
                        {
                            "Date": [{"Type": "Operational", "Start": "2022-07-01"}],
                            "Status": "Active",
                            "Target": {"OrgId": {"extension": "REG2"}},
                            "id": "RE2",
                        }
                    ]
                }
            }
        },
        "ICB3": {  # ICB with multiple relationships
            "Organisation": {
                "Name": "SOUTH WEST ICB",
                "Date": [
                    {"Type": "Operational", "Start": "2022-07-01"},
                    {"Type": "Legal", "Start": "2022-07-01"}
                ],
                "OrgId": {"extension": "ICB3"},
                "Status": "Active",
                "LastChangeDate": "2023-12-01",
                "orgRecordClass": "RC1",
                "GeoLoc": {
                    "Location": {
                        "PostCode": "BS1 1AA",
                        "Country": "ENGLAND",
                    }
                },
                "Rels": {
                    "Rel": [
                        {
                            "Date": [{"Type": "Operational", "Start": "2022-07-01"}],
                            "Status": "Active",
                            "Target": {"OrgId": {"extension": "REG3"}},
                            "id": "RE2",
                        },
                        {
                            "Date": [{"Type": "Operational", "Start": "2022-07-01"}],
                            "Status": "Active",
                            "Target": {"OrgId": {"extension": "OTHER1"}},
                            "id": "RE6",  # Different relationship type
                        }
                    ]
                }
            }
        }
    }



class TestFetchOrganisations:
    @patch('requests.get')
    def test_fetch_all_trusts(self, mock_get, sample_org_response_ro197, sample_org_response_ro24):
        """Test fetching trust ODS codes"""
        mock_get.return_value.raise_for_status = Mock()
        mock_get.return_value.json.side_effect = [
            sample_org_response_ro197,  # First call for RO197
            sample_org_response_ro24    # Second call for RO24
        ]

        result = fetch_all_trusts_from_ord()
        
        assert sorted(result) == ["ABC1", "DEF2", "GHI3", "JKL4"]
        assert mock_get.call_count == 2  # One call for each role
        mock_get.assert_any_call("https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations?Roles=RO197&Limit=1000")
        mock_get.assert_any_call("https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations?Roles=RO24&Limit=1000")

    @patch('requests.get')
    def test_fetch_org_details(self, mock_get, sample_org_details):
        """Test fetching organisation details"""
        mock_get.return_value.json.return_value = sample_org_details["ABC1"]
        result = fetch_org_details(["ABC1"])
        
        assert "ABC1" in result
        assert result["ABC1"]["Organisation"]["Name"] == "SAMPLE HOSPITAL NHS FOUNDATION TRUST"

class TestProcessOrgDetails:
    def test_process_org_details(self, sample_org_details):
        """Test processing organisation details with comprehensive scenarios"""
        icbs, successors, predecessors, filtered_org_details = process_org_details(sample_org_details)
        
        # Test ICB relationships
        assert icbs == {
            "ABC1": "ICB1",
            "DEF2": "ICB2",
            "GHI3": "ICB2"
        }
        
        # Test successor relationships
        assert successors == {
            "ABC1": [],  # No successors
            "DEF2": ["GHI3"],  # One successor
            "GHI3": [],  # No successors
            "JKL4": ["GHI3"],  # One successor
            "RYK": ["TAJ"], # added manually
        }
        
        # Test predecessor relationships
        assert predecessors == {
            "ABC1": [],  # No predecessors
            "DEF2": [],  # No predecessors
            "GHI3": ["DEF2", "JKL4"],  # Two predecessors
            "JKL4": [],  # No predecessors
            "TAJ": ["RYK"], # added manually
        }
        
        # Verify XYZ9 (Welsh org) is not included in any mappings
        assert "XYZ9" not in icbs
        assert "XYZ9" not in successors
        assert "XYZ9" not in predecessors
        assert "XYZ9" not in filtered_org_details

class TestGetICBRegions:
    @patch('pipeline.organisations.import_ord_data.fetch_org_details')
    def test_get_icb_regions(self, mock_fetch_org_details, sample_icb_details):
        """Test getting region mappings for ICBs"""
        mock_fetch_org_details.return_value = sample_icb_details
        
        result = get_icb_regions(["ICB1", "ICB2", "ICB3"])
        
        # Verify the correct region mappings are extracted
        assert result == {
            "ICB1": "REG1",
            "ICB2": "REG2",
            "ICB3": "REG3"
        }
        
        # Verify fetch_org_details was called correctly
        mock_fetch_org_details.assert_called_once_with(["ICB1", "ICB2", "ICB3"])

    @patch('pipeline.organisations.import_ord_data.fetch_org_details')
    def test_get_icb_regions_missing_relationships(self, mock_fetch_org_details):
        """Test handling ICBs with missing region relationships"""
        mock_fetch_org_details.return_value = {
            "ICB4": {  # ICB with no relationships
                "Organisation": {
                    "Name": "TEST ICB",
                    "OrgId": {"extension": "ICB4"},
                    "Status": "Active"
                }
            },
            "ICB5": {  # ICB with empty relationships
                "Organisation": {
                    "Name": "TEST ICB 2",
                    "OrgId": {"extension": "ICB5"},
                    "Status": "Active",
                    "Rels": {}
                }
            }
        }
        
        result = get_icb_regions(["ICB4", "ICB5"])

        mock_fetch_org_details.assert_called_once_with(["ICB4", "ICB5"])
        
        # Verify ICBs without region relationships are not included
        assert result == {}


class TestFormatOrgName:
    @pytest.mark.parametrize("input_name,expected", [
        ("MANCHESTER UNIVERSITY NHS FOUNDATION TRUST", "Manchester University NHS Foundation Trust"),
        ("BRADFORD DISTRICT CARE NHS FOUNDATION TRUST", "Bradford District Care NHS Foundation Trust"),
        ("KING'S COLLEGE HOSPITAL", "King's College Hospital"),
        ("ST MARY'S HOSPITAL", "St Mary's Hospital"),
    ])
    def test_format_org_name(self, input_name, expected):
        """Test organisation name formatting"""
        assert format_org_name(input_name) == expected

class TestResolveUltimateSuccessors:
    def test_simple_succession(self):
        """Test simple succession chain"""
        successors_dict = {
            "RW6": ["R0A"],
            "R0A": []
        }
        result = resolve_ultimate_successors(successors_dict)
        assert result["RW6"] == ["R0A"]
        assert result["R0A"] == []

    def test_multiple_predecessors(self):
        """Test organisation with multiple predecessors"""
        successors_dict = {
            "RM2": ["RW3"],
            "RW3": ["R0A"],
            "R0A": []
        }
        result = resolve_ultimate_successors(successors_dict)
        assert result["RM2"] == ["R0A"]
        assert result["RW3"] == ["R0A"]
        assert result["R0A"] == []

class TestCreateOrgMappingDF:
    def test_create_mapping_dataframe(self, sample_org_details_england_only):
        """Test creation of organisation mapping dataframe"""
        successors = {
            "ABC1": [],  # Active trust with no successors
            "DEF2": ["GHI3"],  # Inactive trust with successor
            "GHI3": [],  # Active merged trust
            "JKL4": ["GHI3"]  # Another inactive trust with successor
        }
        
        predecessors = {
            "ABC1": [],
            "DEF2": [],
            "GHI3": ["DEF2", "JKL4"],
            "JKL4": []
        }
        
        icbs = {
            "ABC1": "ICB1",
            "DEF2": "ICB2",
            "GHI3": "ICB2"
        }
        
        icb_regions = {
            "ICB1": "REG1",
            "ICB2": "REG2"
        }
        
        region_names = {
            "REG1": "North East Region",
            "REG2": "London Region"
        }
        
        icb_names = {
            "ICB1": "North East ICB",
            "ICB2": "London ICB"
        }

        df = create_org_mapping_df(
            successors,
            predecessors,
            sample_org_details_england_only,
            icbs,
            icb_regions,
            region_names,
            icb_names
        )

        assert len(df) == 4
        
        # Test ABC1 row (active trust)
        abc1_row = df[df["ods_code"] == "ABC1"].iloc[0]
        assert abc1_row["ods_name"] == "Sample Hospital NHS Foundation Trust"
        assert abc1_row["icb_code"] == "ICB1"
        assert abc1_row["icb"] == "North East ICB"
        assert abc1_row["region_code"] == "REG1"
        assert abc1_row["region"] == "North East Region"
        assert abc1_row["postcode"] == "SW1A 1AA"
        assert abc1_row["legal_open_date"] == "2020-01-01"
        assert abc1_row["operational_open_date"] == "2020-01-01"
        assert abc1_row["predecessors"] == []
        assert abc1_row["successors"] == []
        
        # Test GHI3 row (merged trust)
        ghi3_row = df[df["ods_code"] == "GHI3"].iloc[0]
        assert ghi3_row["ods_name"] == "New Merged Trust NHS Foundation Trust"
        assert ghi3_row["icb_code"] == "ICB2"
        assert ghi3_row["icb"] == "London ICB"
        assert ghi3_row["region_code"] == "REG2"
        assert ghi3_row["region"] == "London Region"
        assert ghi3_row["postcode"] == "E1 6AN"
        assert ghi3_row["legal_open_date"] == "2022-04-01"
        assert ghi3_row["operational_open_date"] == "2022-04-01"
        assert sorted(ghi3_row["predecessors"]) == ["DEF2", "JKL4"]
        assert ghi3_row["successors"] == []