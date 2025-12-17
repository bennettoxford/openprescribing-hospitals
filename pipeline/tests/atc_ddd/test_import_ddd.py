import pytest
import pandas as pd
from unittest.mock import Mock, patch

from pipeline.atc_ddd.import_atc_ddd.import_ddd import (
    split_routes,
    create_ddd_mappings,
    process_ddd_data,
    fetch_ddd_alterations,
    apply_ddd_deletions_and_updates
)
from pipeline.utils.utils import parse_xml


@pytest.fixture
def sample_ddd_xml():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <dataroot xmlns:z="#RowsetSchema">
        <z:row ATCCode="X01AA01" DDD="1.5" UnitType="mg" AdmCode="O" DDDComment="Test comment with details"/>
        <z:row ATCCode="X01AB04" DDD="2.0" UnitType="g" AdmCode="P" DDDComment=""/>
        <z:row ATCCode="X02AA01" DDD="10" UnitType="ml" AdmCode="N" DDDComment="Another test comment"/>
    </dataroot>'''


@pytest.fixture
def sample_ddd_alterations():
    return pd.DataFrame([
        {
            'substance': 'New DDD Substance',
            'atc_code': 'N01AA01',
            'previous_ddd': None,
            'previous_ddd_unit': None,
            'previous_route': None,
            'new_ddd': 5.0,
            'new_ddd_unit': 'mg',
            'new_route': 'O',
            'year_changed': 2023,
            'comment': 'New DDD'
        },
        {
            'substance': 'Updated DDD',
            'atc_code': 'A01AA01',
            'previous_ddd': 10.0,
            'previous_ddd_unit': 'mg',
            'previous_route': 'O',
            'new_ddd': 15.0,
            'new_ddd_unit': 'mg',
            'new_route': 'O',
            'year_changed': 2023,
            'comment': None
        },
        {
            'substance': 'Route Changed',
            'atc_code': 'B01AA01',
            'previous_ddd': 20.0,
            'previous_ddd_unit': 'mg',
            'previous_route': 'O',
            'new_ddd': 20.0,
            'new_ddd_unit': 'mg',
            'new_route': 'P',
            'year_changed': 2023,
            'comment': None
        },
        {
            'substance': 'Multiple Routes',
            'atc_code': 'C01AA01',
            'previous_ddd': 30.0,
            'previous_ddd_unit': 'mg',
            'previous_route': 'O,P',
            'new_ddd': 35.0,
            'new_ddd_unit': 'mg',
            'new_route': 'O,P',
            'year_changed': 2023,
            'comment': None
        },
        {
            'substance': 'Deleted DDD',
            'atc_code': 'D01AA01',
            'previous_ddd': 40.0,
            'previous_ddd_unit': 'mg',
            'previous_route': 'O',
            'new_ddd': None,
            'new_ddd_unit': None,
            'new_route': None,
            'year_changed': 2023,
            'comment': None
        }
    ])


@pytest.fixture
def sample_ddd_df():
    return pd.DataFrame([
        {
            'atc_code': 'A01AA01',
            'ddd': 10.0,
            'ddd_unit': 'mg',
            'adm_code': 'O',
            'comment': 'Original DDD'
        },
        {
            'atc_code': 'B01AA01',
            'ddd': 20.0,
            'ddd_unit': 'mg',
            'adm_code': 'O',
            'comment': 'Route to change'
        },
        {
            'atc_code': 'C01AA01',
            'ddd': 30.0,
            'ddd_unit': 'mg',
            'adm_code': 'O',
            'comment': 'Multiple route test'
        },
        {
            'atc_code': 'C01AA01',
            'ddd': 30.0,
            'ddd_unit': 'mg',
            'adm_code': 'P',
            'comment': 'Multiple route test'
        },
        {
            'atc_code': 'D01AA01',
            'ddd': 40.0,
            'ddd_unit': 'mg',
            'adm_code': 'O',
            'comment': 'To be deleted'
        }
    ])


class TestXMLParsing:
    @patch('pipeline.utils.utils.get_run_logger')
    def test_parse_ddd_xml(self, mock_logger, sample_ddd_xml, tmp_path):
        mock_logger.return_value = Mock()
        xml_file = tmp_path / "ddd.xml"
        xml_file.write_text(sample_ddd_xml)
        
        df = parse_xml(xml_file)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert all(col in df.columns for col in ['ATCCode', 'DDD', 'UnitType', 'AdmCode', 'DDDComment'])


class TestDDDUtilities:
    @pytest.mark.parametrize("route_str,expected_routes", [
        ("O", ["O"]),
        ("O,P", ["O", "P"]),
        ("O, P, R", ["O", "P", "R"]),
        ("", []),
        (None, []),
        ("O,", ["O"]),
        (" O , P ", ["O", "P"]),
    ])
    def test_split_routes(self, route_str, expected_routes):
        assert split_routes(route_str) == expected_routes


class TestWHORoutesValidation:
    @pytest.fixture
    def mock_bigquery_client(self):
        with patch('pipeline.atc_ddd.import_atc_ddd.import_ddd.get_bigquery_client') as mock:
            client = Mock()
            mock.return_value = client
            yield client


class TestFetchDDDAlterations:
    @patch('pipeline.atc_ddd.import_atc_ddd.import_ddd.fetch_table_data_from_bq')
    def test_fetch_ddd_alterations(self, mock_fetch):
        # Mock the fetch function
        expected_df = pd.DataFrame([
            {'substance': 'Test', 'atc_code': 'A01', 'new_ddd': 5.0, 'year_changed': 2023}
        ])
        mock_fetch.return_value = expected_df
        
        result = fetch_ddd_alterations()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]['substance'] == 'Test'
        mock_fetch.assert_called_once()


class TestDDDMappings:
    @patch('pipeline.atc_ddd.import_atc_ddd.import_ddd.get_run_logger')
    def test_create_ddd_mappings_new_ddds(self, mock_logger, sample_ddd_alterations):
        mock_logger.return_value = Mock()
        ddd_updates, new_ddds, ddds_to_delete = create_ddd_mappings(sample_ddd_alterations)
        
        assert len(new_ddds) == 1
        new_ddd = new_ddds[0]
        assert new_ddd['atc_code'] == 'N01AA01'
        assert new_ddd['ddd'] == 5.0
        assert new_ddd['ddd_unit'] == 'mg'
        assert new_ddd['adm_code'] == 'O'
        assert new_ddd['comment'] == 'New DDD'

    @patch('pipeline.atc_ddd.import_atc_ddd.import_ddd.get_run_logger')
    def test_create_ddd_mappings_new_ddds_without_comment(self, mock_logger):
        """Test new DDDs without alterations comment get None comment"""
        mock_logger.return_value = Mock()
        
        # Create alterations with no comment
        alterations = pd.DataFrame([
            {
                'substance': 'New DDD Substance',
                'atc_code': 'N01AA01',
                'previous_ddd': None,
                'previous_ddd_unit': None,
                'previous_route': None,
                'new_ddd': 5.0,
                'new_ddd_unit': 'mg',
                'new_route': 'O',
                'year_changed': 2023,
                'comment': None
            }
        ])
        
        ddd_updates, new_ddds, ddds_to_delete = create_ddd_mappings(alterations)
        
        assert len(new_ddds) == 1
        new_ddd = new_ddds[0]
        assert new_ddd['atc_code'] == 'N01AA01'
        assert new_ddd['ddd'] == 5.0
        assert new_ddd['ddd_unit'] == 'mg'
        assert new_ddd['adm_code'] == 'O'
        assert new_ddd['comment'] is None

    @patch('pipeline.atc_ddd.import_atc_ddd.import_ddd.get_run_logger')
    def test_create_ddd_mappings_updates(self, mock_logger, sample_ddd_alterations):
        mock_logger.return_value = Mock()
        ddd_updates, new_ddds, ddds_to_delete = create_ddd_mappings(sample_ddd_alterations)

        assert ('A01AA01', 'O') in ddd_updates
        update = ddd_updates[('A01AA01', 'O')]
        assert update['new_ddd'] == 15.0
        assert update['new_ddd_unit'] == 'mg'

    @patch('pipeline.atc_ddd.import_atc_ddd.import_ddd.get_run_logger')
    def test_create_ddd_mappings_deletions(self, mock_logger, sample_ddd_alterations):
        mock_logger.return_value = Mock()
        ddd_updates, new_ddds, ddds_to_delete = create_ddd_mappings(sample_ddd_alterations)
        
        assert ('D01AA01', 'O') in ddds_to_delete

    @patch('pipeline.atc_ddd.import_atc_ddd.import_ddd.get_run_logger')
    def test_create_ddd_mappings_multiple_routes(self, mock_logger, sample_ddd_alterations):
        mock_logger.return_value = Mock()
        ddd_updates, new_ddds, ddds_to_delete = create_ddd_mappings(sample_ddd_alterations)
        
        assert ('C01AA01', 'O') in ddd_updates
        assert ('C01AA01', 'P') in ddd_updates


class TestApplyDDDDeletionsAndUpdates:
    @patch('pipeline.atc_ddd.import_atc_ddd.import_ddd.get_run_logger')
    def test_apply_deletions_only(self, mock_logger, sample_ddd_df):
        """Test that deletions are properly applied"""
        mock_logger.return_value = Mock()
        
        ddds_to_delete = {('D01AA01', 'O')}
        ddd_updates = {}
        
        result = apply_ddd_deletions_and_updates(sample_ddd_df, ddd_updates, ddds_to_delete)
        
        deleted_mask = (result['atc_code'] == 'D01AA01') & (result['adm_code'] == 'O')
        assert not deleted_mask.any(), "Deleted entry should not exist in result"
        assert len(result) == len(sample_ddd_df) - 1

    @patch('pipeline.atc_ddd.import_atc_ddd.import_ddd.get_run_logger')
    def test_apply_updates_same_route_with_alterations_comment(self, mock_logger, sample_ddd_df):
        """Test updating DDD values while keeping the same route, with alterations comment"""
        mock_logger.return_value = Mock()
        
        ddd_updates = {
            ('A01AA01', 'O'): {
                'new_ddd': 15.0,
                'new_ddd_unit': 'mg',
                'new_route': 'O',
                'year_changed': 2023,
                'comment': 'DDD value changed'
            }
        }
        ddds_to_delete = set()
        
        result = apply_ddd_deletions_and_updates(sample_ddd_df, ddd_updates, ddds_to_delete)

        updated_row = result[(result['atc_code'] == 'A01AA01') & (result['adm_code'] == 'O')]
        assert len(updated_row) == 1
        assert updated_row.iloc[0]['ddd'] == 15.0
        assert updated_row.iloc[0]['ddd_unit'] == 'mg'

        assert updated_row.iloc[0]['comment'] == 'DDD value changed'

    @patch('pipeline.atc_ddd.import_atc_ddd.import_ddd.get_run_logger')
    def test_apply_updates_route_change(self, mock_logger, sample_ddd_df):
        """Test updating DDD with route change creates new row and removes old"""
        mock_logger.return_value = Mock()
        
        ddd_updates = {
            ('B01AA01', 'O'): {
                'new_ddd': 25.0,
                'new_ddd_unit': 'mg',
                'new_route': 'P',
                'year_changed': 2023,
                'comment': ''
            }
        }
        ddds_to_delete = set()
        
        result = apply_ddd_deletions_and_updates(sample_ddd_df, ddd_updates, ddds_to_delete)
        
        # Check that old route entry is removed
        old_route_mask = (result['atc_code'] == 'B01AA01') & (result['adm_code'] == 'O')
        assert not old_route_mask.any(), "Old route entry should be removed"
        
        # Check that new route entry was created
        new_route_mask = (result['atc_code'] == 'B01AA01') & (result['adm_code'] == 'P')
        assert new_route_mask.any(), "New route entry should be created"
        
        new_row = result[new_route_mask]
        assert len(new_row) == 1
        assert new_row.iloc[0]['ddd'] == 25.0
        assert new_row.iloc[0]['adm_code'] == 'P'
        assert new_row.iloc[0]['comment'] == 'Route to change'

    @patch('pipeline.atc_ddd.import_atc_ddd.import_ddd.get_run_logger')
    def test_apply_multiple_operations(self, mock_logger, sample_ddd_df):
        """Test applying deletions and updates together"""
        mock_logger.return_value = Mock()
        
        ddds_to_delete = {('D01AA01', 'O')}
        ddd_updates = {
            ('A01AA01', 'O'): {
                'new_ddd': 12.0,
                'new_ddd_unit': 'mg',
                'new_route': 'O',
                'year_changed': 2023,
                'comment': ''
            },
            ('B01AA01', 'O'): {
                'new_ddd': 22.0,
                'new_ddd_unit': 'mg',
                'new_route': 'P',
                'year_changed': 2023,
                'comment': ''
            }
        }
        
        result = apply_ddd_deletions_and_updates(sample_ddd_df, ddd_updates, ddds_to_delete)
        
        # Check deletion
        deleted_mask = (result['atc_code'] == 'D01AA01') & (result['adm_code'] == 'O')
        assert not deleted_mask.any()
        
        # Check same-route update
        updated_a01 = result[(result['atc_code'] == 'A01AA01') & (result['adm_code'] == 'O')]
        assert len(updated_a01) == 1
        assert updated_a01.iloc[0]['ddd'] == 12.0
        
        # Check route-change update
        old_b01 = result[(result['atc_code'] == 'B01AA01') & (result['adm_code'] == 'O')]
        new_b01 = result[(result['atc_code'] == 'B01AA01') & (result['adm_code'] == 'P')]
        assert len(old_b01) == 0
        assert len(new_b01) == 1
        assert new_b01.iloc[0]['ddd'] == 22.0

    @patch('pipeline.atc_ddd.import_atc_ddd.import_ddd.get_run_logger')
    def test_no_matching_entries(self, mock_logger, sample_ddd_df):
        """Test that non-matching deletions and updates are ignored"""
        mock_logger.return_value = Mock()
        
        ddds_to_delete = {('X99XX99', 'Z')}
        ddd_updates = {
            ('Y99YY99', 'Z'): {
                'new_ddd': 999.0,
                'new_ddd_unit': 'kg',
                'new_route': 'Z',
                'year_changed': 2023
            }
        }
        
        result = apply_ddd_deletions_and_updates(sample_ddd_df, ddd_updates, ddds_to_delete)

        assert len(result) == len(sample_ddd_df)
        pd.testing.assert_frame_equal(result.reset_index(drop=True), sample_ddd_df.reset_index(drop=True))


class TestProcessDDDData:
    @patch('pipeline.atc_ddd.import_atc_ddd.import_ddd.get_run_logger')
    def test_data_cleaning(self, mock_logger):
        """Test that data cleaning works correctly"""
        mock_logger.return_value = Mock()
        
        dirty_df = pd.DataFrame([
            {
                'atc_code': 'A01AA01',
                'ddd': '10.5',  # String that should be converted to float
                'ddd_unit': 'MG',  # Should be lowercased
                'adm_code': ' O ',  # Should be stripped
                'comment': 'Test'
            },
            {
                'atc_code': 'B01AA01',
                'ddd': None,  # Should be filtered out
                'ddd_unit': 'g',
                'adm_code': 'P',
                'comment': 'Test'
            },
            {
                'atc_code': 'C01AA01',
                'ddd': '',  # Should be filtered out
                'ddd_unit': 'ml',
                'adm_code': 'R',
                'comment': 'Test'
            }
        ])
        
        result = process_ddd_data(dirty_df, {}, set(), [])
        
        assert len(result) == 1
        assert result.iloc[0]['ddd'] == 10.5
        assert result.iloc[0]['ddd_unit'] == 'mg'
        assert result.iloc[0]['adm_code'] == 'O'

    @patch('pipeline.atc_ddd.import_atc_ddd.import_ddd.get_run_logger')
    def test_with_new_ddds(self, mock_logger, sample_ddd_df):
        """Test adding new DDDs"""
        mock_logger.return_value = Mock()
        
        new_ddds = [
            {
                'atc_code': 'N01AA01',
                'ddd': 5.0,
                'ddd_unit': 'mg',
                'adm_code': 'O',
                'comment': 'New DDD'
            },
            {
                'atc_code': 'N02AA01',
                'ddd': 7.5,
                'ddd_unit': 'g',
                'adm_code': 'P',
                'comment': 'Another new DDD'
            }
        ]
        
        result = process_ddd_data(sample_ddd_df, {}, set(), new_ddds)
        
        assert len(result) == len(sample_ddd_df) + 2
        
        new_n01 = result[result['atc_code'] == 'N01AA01']
        assert len(new_n01) == 1
        assert new_n01.iloc[0]['ddd'] == 5.0
        
        new_n02 = result[result['atc_code'] == 'N02AA01']
        assert len(new_n02) == 1
        assert new_n02.iloc[0]['ddd'] == 7.5

    @patch('pipeline.atc_ddd.import_atc_ddd.import_ddd.get_run_logger')
    def test_comment_handling(self, mock_logger):
        mock_logger.return_value = Mock()
        
        test_df = pd.DataFrame([
            {
                'atc_code': 'A01AA01',
                'ddd': 10.0,
                'ddd_unit': 'mg',
                'adm_code': 'O',
                'comment': ''
            },
            {
                'atc_code': 'A01AA02',
                'ddd': 20.0,
                'ddd_unit': 'mg',
                'adm_code': 'O',
                'comment': '   '
            },
            {
                'atc_code': 'A01AA03',
                'ddd': 30.0,
                'ddd_unit': 'mg',
                'adm_code': 'O',
                'comment': ' Test Comment  '
            }
        ])
        
        result = process_ddd_data(test_df, {}, set(), [])
        
        assert result[result['atc_code'] == 'A01AA01']['comment'].iloc[0] is None
        assert result[result['atc_code'] == 'A01AA02']['comment'].iloc[0] is None
        assert result[result['atc_code'] == 'A01AA03']['comment'].iloc[0] == 'Test Comment'

    @patch('pipeline.atc_ddd.import_atc_ddd.import_ddd.get_run_logger')
    def test_comment_combination_in_updates(self, mock_logger):
        mock_logger.return_value = Mock()
        
        test_df = pd.DataFrame([
            {
                'atc_code': 'A01AA01',
                'ddd': 10.0,
                'ddd_unit': 'mg',
                'adm_code': 'O',
                'comment': '  Original Comment  '
            }
        ])
        
        ddd_updates = {
            ('A01AA01', 'O'): {
                'new_ddd': 15.0,
                'new_ddd_unit': 'mg',
                'new_route': 'O',
                'year_changed': 2023,
                'comment': '  '
            }
        }
        
        result = process_ddd_data(test_df, ddd_updates, set(), [])

        updated_comment = result[result['atc_code'] == 'A01AA01']['comment'].iloc[0]
        assert updated_comment == 'Original Comment'
