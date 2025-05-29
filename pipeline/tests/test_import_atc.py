import pytest
import pandas as pd
from unittest.mock import Mock, patch

from pipeline.flows.import_atc import (
    get_atc_level,
    convert_atc_name,
    create_atc_code_mapping,
    process_atc_data,
    add_atc_hierarchy
)
from pipeline.utils.utils import parse_xml


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
def sample_atc_alterations():
    return pd.DataFrame([
        {
            'substance': 'New Substance',
            'previous_atc_code': None,
            'new_atc_code': 'N01AA01',
            'year_changed': 2023,
            'comment': 'New code'
        },
        {
            'substance': 'Changed Substance',
            'previous_atc_code': 'A01AA01',
            'new_atc_code': 'A01AA02',
            'year_changed': 2022,
            'comment': None
        },
        {
            'substance': 'Updated Name Only',
            'previous_atc_code': 'B01AA01',
            'new_atc_code': 'B01AA01',
            'year_changed': 2023,
            'comment': None
        },
        {
            'substance': 'Chain Change Final',
            'previous_atc_code': 'C01AA02',
            'new_atc_code': 'C01AA03',
            'year_changed': 2024,
            'comment': None
        },
        {
            'substance': 'Chain Change Initial',
            'previous_atc_code': 'C01AA01',
            'new_atc_code': 'C01AA02',
            'year_changed': 2023,
            'comment': None
        }
    ])


@pytest.fixture
def sample_atc_df():
    return pd.DataFrame([
        {
            'atc_code': 'A01AA01',
            'atc_name': 'Old Substance Name',
            'comment': 'Original comment'
        },
        {
            'atc_code': 'B01AA01',
            'atc_name': 'Another Substance',
            'comment': 'Another comment'
        },
        {
            'atc_code': 'C01AA01',
            'atc_name': 'Chain Substance',
            'comment': 'Chain comment'
        }
    ])


@pytest.fixture
def sample_atc_alterations_with_deletions():
    return pd.DataFrame([
        {
            'substance': 'New Substance',
            'previous_atc_code': None,
            'new_atc_code': 'N01AA01',
            'year_changed': 2023,
            'comment': 'New 3rd/4th level code'
        },
        {
            'substance': 'Changed Substance',
            'previous_atc_code': 'A01AA01',
            'new_atc_code': 'A01AA02',
            'year_changed': 2022,
            'comment': ''
        },
        {
            'substance': 'Deleted Substance',
            'previous_atc_code': 'B01AA01',
            'new_atc_code': 'deleted',
            'year_changed': 2023,
            'comment': ''
        }
    ])


@pytest.fixture
def sample_hierarchical_atc_df():
    return pd.DataFrame([
        {
            'atc_code': 'A',
            'atc_name': 'Alimentary Tract And Metabolism',
            'comment': 'Level 1'
        },
        {
            'atc_code': 'A01',
            'atc_name': 'Stomatological Preparations',
            'comment': 'Level 2'
        },
        {
            'atc_code': 'A01A',
            'atc_name': 'Stomatological Preparations',
            'comment': 'Level 3'
        },
        {
            'atc_code': 'A01AA',
            'atc_name': 'Caries Prophylactic Agents',
            'comment': 'Level 4'
        },
        {
            'atc_code': 'A01AA01',
            'atc_name': 'Sodium Fluoride',
            'comment': 'Level 5'
        },
        {
            'atc_code': 'A01AA02',
            'atc_name': 'Sodium Monofluorophosphate',
            'comment': 'Level 5'
        }
    ])


class TestXMLParsing:
    @patch('pipeline.utils.utils.get_run_logger')
    def test_parse_atc_xml(self, mock_logger, sample_atc_xml, tmp_path):
        mock_logger.return_value = Mock()
        xml_file = tmp_path / "atc.xml"
        xml_file.write_text(sample_atc_xml)
        
        df = parse_xml(xml_file)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 7
        assert 'ATCCode' in df.columns
        assert 'Name' in df.columns
        assert 'Comment' in df.columns


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


class TestCreateATCCodeMapping:
    @patch('pipeline.flows.import_atc.get_run_logger')
    def test_create_atc_code_mapping_basic(self, mock_logger, sample_atc_alterations):
        mock_logger.return_value = Mock()
        
        code_mapping, new_codes, deleted_codes = create_atc_code_mapping(sample_atc_alterations)
        
        # Check code mappings
        assert len(code_mapping) == 4  # A01AA01, B01AA01, C01AA02, C01AA01
        assert code_mapping['A01AA01']['new_code'] == 'A01AA02'
        assert code_mapping['A01AA01']['substance'] == 'Changed Substance'
        assert code_mapping['B01AA01']['new_code'] == 'B01AA01'
        assert code_mapping['B01AA01']['substance'] == 'Updated Name Only'
        
        # Check new codes
        assert len(new_codes) == 1
        assert new_codes['N01AA01'] == 'New Substance'
        
        # Check deleted codes
        assert len(deleted_codes) == 0

    @patch('pipeline.flows.import_atc.get_run_logger')
    def test_create_atc_code_mapping_with_deletions(self, mock_logger, sample_atc_alterations_with_deletions):
        mock_logger.return_value = Mock()
        
        code_mapping, new_codes, deleted_codes = create_atc_code_mapping(sample_atc_alterations_with_deletions)
        
        # Check code mappings
        assert len(code_mapping) == 1
        assert code_mapping['A01AA01']['new_code'] == 'A01AA02'
        
        # Check new codes
        assert len(new_codes) == 1
        assert new_codes['N01AA01'] == 'New Substance'
        
        # Check deleted codes
        assert len(deleted_codes) == 1
        assert deleted_codes['B01AA01'] == 'Deleted Substance'

    @patch('pipeline.flows.import_atc.get_run_logger')
    def test_create_atc_code_mapping_empty_dataframe(self, mock_logger):
        mock_logger.return_value = Mock()
        empty_df = pd.DataFrame(columns=['substance', 'previous_atc_code', 'new_atc_code', 'year_changed', 'comment'])
        
        code_mapping, new_codes, deleted_codes = create_atc_code_mapping(empty_df)
        
        assert len(code_mapping) == 0
        assert len(new_codes) == 0
        assert len(deleted_codes) == 0

    @patch('pipeline.flows.import_atc.get_run_logger')
    def test_create_atc_code_mapping_year_ordering(self, mock_logger):
        mock_logger.return_value = Mock()

        alterations = pd.DataFrame([
            {
                'substance': 'Substance A',
                'previous_atc_code': 'A01AA01',
                'new_atc_code': 'A01AA02',
                'year_changed': 2023,
                'comment': None
            },
            {
                'substance': 'Substance B',
                'previous_atc_code': 'A01AA01',
                'new_atc_code': 'A01AA03',
                'year_changed': 2022,
                'comment': None
            }
        ])
        
        code_mapping, new_codes, deleted_codes = create_atc_code_mapping(alterations)
        
        # The later change should override the earlier one
        assert code_mapping['A01AA01']['new_code'] == 'A01AA02'
        assert code_mapping['A01AA01']['substance'] == 'Substance A'


class TestProcessATCData:
    @patch('pipeline.flows.import_atc.get_run_logger')
    def test_process_atc_data_add_new_codes(self, mock_logger, sample_atc_df):
        mock_logger.return_value = Mock()
        
        atc_mapping = {}
        new_codes = {'N01AA01': 'New Substance'}
        deleted_codes = {}
        
        result_df = process_atc_data(sample_atc_df, atc_mapping, new_codes, deleted_codes)
        
        assert len(result_df) == 4  # Original 3 + 1 new
        new_row = result_df[result_df['atc_code'] == 'N01AA01'].iloc[0]
        assert new_row['atc_name'] == 'New Substance'
        assert new_row['comment'] == 'Added from alterations table'

    @patch('pipeline.flows.import_atc.get_run_logger')
    def test_process_atc_data_delete_codes(self, mock_logger, sample_atc_df):
        mock_logger.return_value = Mock()
        
        atc_mapping = {}
        new_codes = {}
        deleted_codes = {'A01AA01': 'Deleted Substance'}
        
        result_df = process_atc_data(sample_atc_df, atc_mapping, new_codes, deleted_codes)
        
        assert len(result_df) == 2  # Original 3 - 1 deleted
        assert 'A01AA01' not in result_df['atc_code'].values

    @patch('pipeline.flows.import_atc.get_run_logger')
    def test_process_atc_data_update_codes(self, mock_logger, sample_atc_df):
        mock_logger.return_value = Mock()
        
        atc_mapping = {
            'A01AA01': {
                'new_code': 'A01AA99',
                'substance': 'Updated Substance Name'
            }
        }
        new_codes = {}
        deleted_codes = {}
        
        result_df = process_atc_data(sample_atc_df, atc_mapping, new_codes, deleted_codes)
        
        assert len(result_df) == 3  # Same number of rows
        updated_row = result_df[result_df['atc_code'] == 'A01AA99']
        assert len(updated_row) == 1
        assert updated_row.iloc[0]['atc_name'] == 'Updated Substance Name'
        assert 'A01AA01' not in result_df['atc_code'].values

    @patch('pipeline.flows.import_atc.get_run_logger')
    def test_process_atc_data_comprehensive(self, mock_logger, sample_atc_df):
        mock_logger.return_value = Mock()
        
        atc_mapping = {
            'B01AA01': {
                'new_code': 'B01AA99',
                'substance': 'Updated Another Substance'
            }
        }
        new_codes = {'N01AA01': 'Brand New Substance'}
        deleted_codes = {'C01AA01': 'Chain Substance'}
        
        result_df = process_atc_data(sample_atc_df, atc_mapping, new_codes, deleted_codes)
        
        # Should have 3 rows: A01AA01 (unchanged), B01AA99 (updated), N01AA01 (new)
        # C01AA01 should be deleted
        assert len(result_df) == 3
        assert 'A01AA01' in result_df['atc_code'].values  # Unchanged
        assert 'B01AA99' in result_df['atc_code'].values  # Updated
        assert 'N01AA01' in result_df['atc_code'].values  # New
        assert 'C01AA01' not in result_df['atc_code'].values  # Deleted
        assert 'B01AA01' not in result_df['atc_code'].values  # Old code should be gone
        
        updated_row = result_df[result_df['atc_code'] == 'B01AA99'].iloc[0]
        assert updated_row['atc_name'] == 'Updated Another Substance'

    @patch('pipeline.flows.import_atc.get_run_logger')
    def test_process_atc_data_name_conversion(self, mock_logger):
        mock_logger.return_value = Mock()
        
        atc_df = pd.DataFrame([
            {
                'atc_code': 'A01AA01',
                'atc_name': 'UPPERCASE NAME',
                'comment': 'Test comment'
            }
        ])
        
        result_df = process_atc_data(atc_df, {}, {}, {})
        
        assert result_df.iloc[0]['atc_name'] == 'Uppercase Name'

    @patch('pipeline.flows.import_atc.get_run_logger')
    def test_process_atc_data_hierarchy_added(self, mock_logger, sample_hierarchical_atc_df):
        mock_logger.return_value = Mock()
        
        result_df = process_atc_data(sample_hierarchical_atc_df, {}, {}, {})
        
        expected_columns = ['level', 'anatomical_main_group', 'therapeutic_subgroup', 
                          'pharmacological_subgroup', 'chemical_subgroup', 'chemical_substance']
        for col in expected_columns:
            assert col in result_df.columns


class TestAddATCHierarchy:
    def test_add_atc_hierarchy_complete(self, sample_hierarchical_atc_df):
        df = sample_hierarchical_atc_df.copy()
        
        add_atc_hierarchy(df)

        expected_columns = ['level', 'anatomical_main_group', 'therapeutic_subgroup', 
                          'pharmacological_subgroup', 'chemical_subgroup', 'chemical_substance']
        for col in expected_columns:
            assert col in df.columns
        
        # Check specific mappings for the substance level code (level 5)
        substance_row = df[df['atc_code'] == 'A01AA01'].iloc[0]
        assert substance_row['level'] == 5
        assert substance_row['anatomical_main_group'] == 'Alimentary Tract And Metabolism'
        assert substance_row['therapeutic_subgroup'] == 'Stomatological Preparations'
        assert substance_row['pharmacological_subgroup'] == 'Stomatological Preparations'
        assert substance_row['chemical_subgroup'] == 'Caries Prophylactic Agents'
        assert substance_row['chemical_substance'] == 'Sodium Fluoride'
        
        # Check level 1 code - only anatomical_main_group should be populated
        level1_row = df[df['atc_code'] == 'A'].iloc[0]
        assert level1_row['level'] == 1
        assert level1_row['anatomical_main_group'] == 'Alimentary Tract And Metabolism'
        assert pd.isna(level1_row['therapeutic_subgroup'])
        assert pd.isna(level1_row['pharmacological_subgroup'])
        assert pd.isna(level1_row['chemical_subgroup'])
        assert pd.isna(level1_row['chemical_substance'])
    