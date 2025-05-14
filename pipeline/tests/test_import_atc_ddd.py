import pytest
import pandas as pd
from unittest.mock import Mock, patch

from pipeline.flows.import_atc_ddd import (
    parse_xml,
    validate_who_routes,
    get_atc_level,
    convert_atc_name,
)

@pytest.fixture
def sample_atc_xml():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <dataroot xmlns:z="#RowsetSchema">
        <z:row ATCCode="X" Name="MAIN GROUP EXAMPLE" Comment="Level 1 comment"/>
        <z:row ATCCode="X01" Name="SUBGROUP EXAMPLE" Comment="Level 2 comment"/>
        <z:row ATCCode="X01A" Name="THERAPEUTIC EXAMPLE" Comment="Level 3 comment"/>
        <z:row ATCCode="X01AA" Name="Chemical group example" Comment="Level 4 comment"/>
        <z:row ATCCode="X01AA01" Name="test substance" Comment="Level 5 comment"/>
        <z:row ATCCode="X01AB" Name="Another chemical group" Comment=""/>
        <z:row ATCCode="X01AB04" Name="another substance" Comment=""/>
    </dataroot>'''

@pytest.fixture
def sample_ddd_xml():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <dataroot xmlns:z="#RowsetSchema">
        <z:row ATCCode="X01AA01" DDD="1.5" UnitType="mg" AdmCode="O" DDDComment="Test comment with details"/>
        <z:row ATCCode="X01AB04" DDD="2.0" UnitType="g" AdmCode="P" DDDComment=""/>
        <z:row ATCCode="X02AA01" DDD="10" UnitType="ml" AdmCode="N" DDDComment="Another test comment"/>
    </dataroot>'''

class TestXMLParsing:
    def test_parse_atc_xml(self, sample_atc_xml, tmp_path):
        
        xml_file = tmp_path / "atc.xml"
        xml_file.write_text(sample_atc_xml)
        
        df = parse_xml(xml_file)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 7
        assert 'ATCCode' in df.columns
        assert 'Name' in df.columns
        assert 'Comment' in df.columns

    def test_parse_ddd_xml(self, sample_ddd_xml, tmp_path):
        xml_file = tmp_path / "ddd.xml"
        xml_file.write_text(sample_ddd_xml)
        
        df = parse_xml(xml_file)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert all(col in df.columns for col in ['ATCCode', 'DDD', 'UnitType', 'AdmCode', 'DDDComment'])

class TestWHORoutesValidation:
    @pytest.fixture
    def mock_bigquery_client(self):
        with patch('pipeline.flows.import_atc_ddd.get_bigquery_client') as mock:
            client = Mock()
            mock.return_value = client
            yield client

    def test_validate_who_routes_success(self, mock_bigquery_client):
        df = pd.DataFrame({
            'adm_code': ['O', 'P', 'R']
        })
        
        mock_query_result = Mock()
        mock_query_result.result.return_value = []
        mock_bigquery_client.query.return_value = mock_query_result
        
        validate_who_routes(df)

    def test_validate_who_routes_missing(self, mock_bigquery_client):
        df = pd.DataFrame({
            'adm_code': ['O', 'INVALID', 'R']
        })
        
        mock_query_result = Mock()
        mock_query_result.result.return_value = [Mock(adm_code='INVALID')]
        mock_bigquery_client.query.return_value = mock_query_result
        
        with pytest.raises(ValueError) as exc_info:
            validate_who_routes(df)
        assert 'Invalid route codes detected' in str(exc_info.value)

class TestATCHelpers:
    @pytest.mark.parametrize("code,expected_level", [
        ("A", 1),
        ("A01", 2),
        ("A01A", 3),
        ("A01AA", 4),
        ("A01AA01", 5),
        ("", None),
        ("invalid", None),
        ("A01A01", None),
        (None, None),
        ("   A   ", 1),
    ])
    def test_get_atc_level(self, code, expected_level):
        assert get_atc_level(code) == expected_level

    @pytest.mark.parametrize("name,expected_output", [
        ("MAIN GROUP EXAMPLE", "Main Group Example"),
        ("test substance", "Test Substance"),
        ("Mixed CASE example", "Mixed Case Example"),
        ("", ""),
        (None, None),
        ("   PADDED   ", "Padded"),
        ("single", "Single"),
        ("already Properly Formatted", "Already Properly Formatted"),
    ])
    def test_convert_atc_name(self, name, expected_output):
        assert convert_atc_name(name) == expected_output

