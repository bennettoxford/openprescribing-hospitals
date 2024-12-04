from unittest.mock import MagicMock, patch
import pandas as pd
import pytest
from viewer.management.commands.import_dmd_supp import Command, TRUDClient, TRUDRelease

@pytest.fixture
def mock_bigquery_client():
    with patch("viewer.management.commands.import_dmd_supp.get_bigquery_client") as mock:
        yield mock

@pytest.fixture
def command():
    return Command()

class TestTRUDClient:
    @patch("requests.get")
    def test_get_latest_release(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "releases": [{
                "releaseDate": "2024-03-01",
                "archiveFileUrl": "https://isd.digital.nhs.uk/download/api/v1/keys/58be60bf41d31c4309136a732a7efe18f1bb5be1/content/items/25/nhsbsa_dmdbonus_12.0.0_20241202000001.zip"
            }]
        }
        mock_get.return_value = mock_response

        client = TRUDClient("test_key")
        release = client.get_latest_release()

        assert isinstance(release, TRUDRelease)
        assert release.release_date == "2024-03-01"
        assert release.download_url == "https://isd.digital.nhs.uk/download/api/v1/keys/58be60bf41d31c4309136a732a7efe18f1bb5be1/content/items/25/nhsbsa_dmdbonus_12.0.0_20241202000001.zip"
        assert release.year == 2024

class TestImportDMDSuppCommand:

    @patch("xml.etree.ElementTree.parse")
    def test_extract_xml_data(self, mock_parse, command):
        mock_root = MagicMock()
        mock_vmp = MagicMock()
        mock_vmp.find.side_effect = lambda x: {
            'VPID': MagicMock(text='12345'),
            'BNF': MagicMock(text='67890'),
            'ATC': MagicMock(text='A01BC')
        }[x]
        mock_root.findall.return_value = [mock_vmp]
        mock_parse.return_value.getroot.return_value = mock_root

        df = command._extract_xml_data(MagicMock())

        assert len(df) == 1
        assert df.iloc[0]['vmp_code'] == '12345'
        assert df.iloc[0]['bnf_code'] == '67890'
        assert df.iloc[0]['atc_code'] == 'A01BC'

    def test_upload_bq_existing_table(self, command, mock_bigquery_client):
        mock_client = mock_bigquery_client.return_value
        mock_table = MagicMock()
        mock_table.num_rows = 10
        mock_client.get_table.return_value = mock_table

        test_df = pd.DataFrame({
            "vmp_code": ["1234"],
            "bnf_code": ["56789"],
            "atc_code": ["A01BC"]
        })

        command.upload_bq(test_df)

        mock_client.get_table.assert_called_once()
        mock_client.create_table.assert_not_called()
        mock_client.load_table_from_dataframe.assert_called_once()


    @patch("requests.get")
    @patch("zipfile.ZipFile")
    def test_fetch_dmd_supp_data(self, mock_zipfile, mock_get, command, tmp_path):
        command.temp_dir = tmp_path

        command.trud_client.get_latest_release = MagicMock(return_value=TRUDRelease(
            release_date="2024-03-01",
            download_url="https://isd.digital.nhs.uk/download/api/v1/keys/58be60bf41d31c4309136a732a7efe18f1bb5be1/content/items/25/nhsbsa_dmdbonus_12.0.0_20241202000001.zip",
            year=2024
        ))

        mock_response = MagicMock()
        mock_response.content = b"zip_content"
        mock_get.return_value = mock_response

        mock_main_zip = MagicMock()
        mock_main_zip.namelist.return_value = ["week092024-BNF.zip"]

        mock_inner_zip = MagicMock()
        mock_inner_zip.namelist.return_value = ["f1_bnf.xml"]
        
        mock_zipfile.return_value.__enter__.side_effect = [mock_main_zip, mock_inner_zip]

        command._extract_xml_data = MagicMock(return_value=pd.DataFrame({
            "vmp_code": ["1234"],
            "bnf_code": ["56789"],
            "atc_code": ["A01BC"]
        }))

        df = command.fetch_dmd_supp_data()

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        mock_get.assert_called_once_with("https://isd.digital.nhs.uk/download/api/v1/keys/58be60bf41d31c4309136a732a7efe18f1bb5be1/content/items/25/nhsbsa_dmdbonus_12.0.0_20241202000001.zip")
        assert mock_zipfile.call_count == 2
        command._extract_xml_data.assert_called_once()
