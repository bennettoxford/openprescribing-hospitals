import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from pathlib import Path
import xml.etree.ElementTree as ET
from google.api_core.exceptions import NotFound
from viewer.management.commands.import_ddd_atc import Command
from google.cloud.bigquery import SchemaField

class TestImportDDDATCCommand:
    @pytest.fixture
    def command(self):
        return Command()

    @pytest.fixture
    def mock_bigquery_client(self):
        with patch('google.cloud.bigquery.Client') as mock_client:
            mock_instance = mock_client.return_value
            mock_instance.get_table = MagicMock()
            mock_instance.create_table = MagicMock()
            mock_instance.load_table_from_dataframe = MagicMock()
            yield mock_client

    def test_parse_xml(self, command):
        test_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <dataroot xmlns:z="#RowsetSchema">
            <z:row ATCCode='A' Name='ALIMENTARY TRACT AND METABOLISM'/>
            <z:row ATCCode='A01AB' Name='Antiinfectives and antiseptics for local oral treatment'/>
            <z:row ATCCode='H04AA02' Name='dasiglucagon' Comment='Used before new code : H01AC01'/>
        </dataroot>
        """
        
        # Parse the XML string directly
        with patch('xml.etree.ElementTree.parse') as mock_parse:
            mock_tree = ET.ElementTree(ET.fromstring(test_xml))
            mock_parse.return_value = mock_tree

            result = command.parse_xml("dummy_path", ["ATCCode", "Name", "Comment"])
            
            assert len(result) == 3
            assert result[0] == ("A", "ALIMENTARY TRACT AND METABOLISM", None)
            assert result[1] == ("A01AB", "Antiinfectives and antiseptics for local oral treatment", None)
            assert result[2] == ("H04AA02", "dasiglucagon", "Used before new code : H01AC01")

    def test_create_or_update_table(self, command, mock_bigquery_client):
        mock_client = mock_bigquery_client.return_value
        mock_table = MagicMock()
        mock_table.num_rows = 10
        mock_client.get_table.return_value = mock_table

        test_df = pd.DataFrame({"test": [1, 2, 3]})
        test_schema = [SchemaField("test", "INTEGER", mode="NULLABLE")]

        command.create_or_update_table(
            mock_client,
            "project.dataset.table",
            test_schema,
            test_df,
            "Test description"
        )

        mock_client.get_table.assert_called_once()
        mock_client.create_table.assert_not_called()
        mock_client.load_table_from_dataframe.assert_called_once()


    @patch('viewer.management.utils.get_bigquery_client')
    def test_handle(self, mock_get_client, command, mock_bigquery_client):
        mock_get_client.return_value = mock_bigquery_client.return_value
        

        with patch.object(Command, 'parse_xml') as mock_parse:
            mock_parse.side_effect = [
                [("A", "ALIMENTARY TRACT AND METABOLISM", None)],  # ATC data
                [("A01AA01", "1.0", "mg", "O", "Test DDD Comment")]  # DDD data
            ]
            
            with patch.object(Command, 'upload_bq') as mock_upload:
                command.handle()
                
                mock_parse.assert_called()
                assert mock_parse.call_count == 2
                mock_upload.assert_called_once()

                args = mock_upload.call_args[0]
                atc_df, ddd_df = args[0], args[1]
                
                assert len(atc_df) == 1
                assert len(ddd_df) == 1
                assert atc_df.iloc[0]["atc_code"] == "A"
                assert ddd_df.iloc[0]["atc_code"] == "A01AA01"
