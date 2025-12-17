import pytest
import pandas as pd
from unittest.mock import patch

from pipeline.atc_ddd.import_atc_ddd.import_atc_ddd_alterations import (
    process_new_atc_5th_levels,
    process_new_atc_34_levels,
    process_new_ddds,
    process_atc_name_alterations,
    process_atc_level_alterations,
    process_ddd_alterations,
    import_atc_ddd_alterations,
)

@pytest.fixture
def sample_excel_data():
    """Sample Excel data structure"""
    return {
        'New ATC 5th levels': pd.DataFrame({
            'New ATC code': ['C01AA99', 'C02BB99'],
            'Substance name': ['New Substance 1', 'New Substance 2'],
            'Note': [pd.NA, pd.NA]
        }),
        'New 3rd and 4th levels': pd.DataFrame({
            'ATC code': ['E01', 'E02A'],
            'New ATC level name': ['New Group', 'Another Group']
        }),
        'ATC level alterations': pd.DataFrame({
            'Previous ATC code': ['F01AA01', 'F02BB01'],
            'ATC level name': ['Name 1', 'Name 2'],
            'New ATC code': ['F01AA02', 'F02BB02']
        }),
        'ATC level name alterations': pd.DataFrame({
            'ATC code': ['F01AA01', 'F02BB01'],
            'Previous ATC level name': ['Old Name 1', 'Old Name 2'],
            'New ATC level name': ['New Name 1', 'New Name 2']
        }),
        'New DDDs': pd.DataFrame({
            'ATC level name': ['Test Substance', 'Another Substance'],
            'New DDD': [1.5, 2.0],
            'Unit': ['g', 'mg'],
            'Adm.route': ['O', 'P'],
            'ATC code': ['D01AA01', 'D02BB01'],
            'Note': [pd.NA, pd.NA]
        }),
        'DDDs alterations': pd.DataFrame(
            data=[
                ['F01AA01', 'Name 1', 1.0, 'g', 'O', 1.5, 'g', 'O', pd.NA],
                ['F02BB01', 'Name 2', 2.0, 'mg', 'P', 2.5, 'mg', 'P', 'Refers to SC injection']
            ],
            columns=['ATC code', 'ATC level name', 'Previous DDD', 'Prev Unit', 'Prev Route', 'New DDD', 'New Unit', 'New Route', 'Note']
        ),
        
    }

class TestExcelProcessing:
    def test_process_new_atc_5th_levels(self, sample_excel_data):
        """Test processing new ATC codes from Excel"""
        result = process_new_atc_5th_levels(sample_excel_data, 2025)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

        expected_columns = ['substance', 'previous_atc_code', 'new_atc_code', 'year_changed', 'comment', 'alterations_comment']
        assert all(col in result.columns for col in expected_columns)

        first_row = result.iloc[0]
        assert first_row['substance'] == 'New Substance 1'
        assert pd.isna(first_row['previous_atc_code'])
        assert first_row['new_atc_code'] == 'C01AA99'
        assert first_row['year_changed'] == 2025
        assert pd.isna(first_row['comment'])
        assert first_row['alterations_comment'] == 'New code'

    def test_process_new_atc_34_levels(self, sample_excel_data):
        """Test processing new 3rd and 4th level ATC codes"""
        result = process_new_atc_34_levels(sample_excel_data, 2025)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

        first_row = result.iloc[0]
        assert first_row['substance'] == 'New Group'
        assert first_row['new_atc_code'] == 'E01'
        assert pd.isna(first_row['comment'])
        assert first_row['alterations_comment'] == 'New 3rd/4th level code'

    def test_process_atc_level_alterations(self, sample_excel_data):
        """Test processing ATC level alterations"""
        result = process_atc_level_alterations(sample_excel_data, 2025)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

        first_row = result.iloc[0]
        assert first_row['substance'] == 'Name 1'
        assert first_row['previous_atc_code'] == 'F01AA01'
        assert first_row['new_atc_code'] == 'F01AA02'
        assert pd.isna(first_row['comment'])
        assert first_row['alterations_comment'] == 'Level updated: F01AA01 → F01AA02'

    def test_process_atc_name_alterations(self, sample_excel_data):
        """Test processing ATC name alterations"""
        result = process_atc_name_alterations(sample_excel_data, 2025)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

        first_row = result.iloc[0]
        assert first_row['substance'] == 'New Name 1'
        assert first_row['previous_atc_code'] == 'F01AA01'
        assert first_row['new_atc_code'] == 'F01AA01'
        assert pd.isna(first_row['comment'])
        assert 'Old Name 1 → New Name 1' in first_row['alterations_comment']


    def test_process_new_ddds(self, sample_excel_data):
        """Test processing new DDDs from Excel"""
        result = process_new_ddds(sample_excel_data, 2025)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

        expected_columns = [
            'substance', 'previous_ddd', 'previous_ddd_unit', 'previous_route',
            'new_ddd', 'new_ddd_unit', 'new_route', 'atc_code', 'year_changed', 'comment', 'alterations_comment'
        ]
        assert all(col in result.columns for col in expected_columns)

        first_row = result.iloc[0]
        assert first_row['substance'] == 'Test Substance'
        assert pd.isna(first_row['previous_ddd'])
        assert pd.isna(first_row['previous_ddd_unit'])
        assert pd.isna(first_row['previous_route'])
        assert first_row['new_ddd'] == 1.5
        assert first_row['new_ddd_unit'] == 'g'
        assert first_row['atc_code'] == 'D01AA01'
        assert pd.isna(first_row['comment'])
        assert first_row['alterations_comment'] == 'New DDD'


    def test_process_ddd_alterations(self, sample_excel_data):
        """Test processing DDD alterations"""
        result = process_ddd_alterations(sample_excel_data, 2025)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

        first_row = result.iloc[0]
        assert first_row['substance'] == 'Name 1'
        assert first_row['previous_ddd'] == 1.0
        assert first_row['new_ddd'] == 1.5
        assert pd.isna(first_row['comment'])
        assert first_row['alterations_comment'] == 'DDD alteration'
        
        second_row = result.iloc[1]
        assert second_row['comment'] == 'Refers to SC injection'
        assert second_row['alterations_comment'] == 'DDD alteration'

    def test_process_empty_excel_sheets(self):
        """Test processing when Excel sheets are empty"""
        empty_data = {
            'New ATC 5th levels': pd.DataFrame(),
            'New DDDs': pd.DataFrame(),
            'New 3rd and 4th levels': pd.DataFrame(),
            'ATC level alterations': pd.DataFrame(),
            'ATC level name alterations': pd.DataFrame(),
            'DDDs alterations': pd.DataFrame()
        }

        atc_5th_result = process_new_atc_5th_levels(empty_data, 2025)
        ddd_result = process_new_ddds(empty_data, 2025)
        atc_34_result = process_new_atc_34_levels(empty_data, 2025)
        atc_level_result = process_atc_level_alterations(empty_data, 2025)
        name_result = process_atc_name_alterations(empty_data, 2025)
        ddd_alterations_result = process_ddd_alterations(empty_data, 2025)

        assert atc_5th_result.empty
        assert ddd_result.empty
        assert atc_34_result.empty
        assert atc_level_result.empty
        assert name_result.empty
        assert ddd_alterations_result.empty


class TestMainFlow:
    @patch('pipeline.atc_ddd.import_atc_ddd.import_atc_ddd_alterations.find_alterations_files')
    @patch('pipeline.atc_ddd.import_atc_ddd.import_atc_ddd_alterations.fetch_excel_from_gcs')
    @patch('pipeline.atc_ddd.import_atc_ddd.import_atc_ddd_alterations.load_to_bigquery')
    def test_import_atc_ddd_alterations(self, mock_load, mock_fetch_excel_from_gcs,
                                        mock_find_files, sample_excel_data):
        """Test the main import flow"""
        # Mock finding files - return one file for year 2025
        mock_find_files.return_value = [('who_atc_ddd_op_hosp/ATC_DDD_new_and_alterations_2025.xlsx', 2025)]
        mock_fetch_excel_from_gcs.return_value = (sample_excel_data, 2025)

        loaded_dataframes = []
        def capture_load_calls(df, table_spec):
            loaded_dataframes.append((df.copy(), table_spec))
        mock_load.side_effect = capture_load_calls

        import_atc_ddd_alterations()

        # Should have loaded 2 dataframes: DDD and ATC
        assert len(loaded_dataframes) == 2

        ddd_df, ddd_table_spec = loaded_dataframes[0]
        atc_df, atc_table_spec = loaded_dataframes[1]

        # DDD dataframe should contain both new DDDs and DDD alterations
        assert len(ddd_df) == 4  # 2 new DDDs + 2 DDD alterations

        # Verify new DDDs are present
        new_ddd_rows = ddd_df[ddd_df['alterations_comment'] == 'New DDD']
        assert len(new_ddd_rows) == 2
        assert 'Test Substance' in new_ddd_rows['substance'].tolist()
        assert 'Another Substance' in new_ddd_rows['substance'].tolist()
        assert all(pd.isna(new_ddd_rows['previous_ddd']))

        # Verify DDD alterations are present
        alteration_rows = ddd_df[ddd_df['alterations_comment'] == 'DDD alteration']
        assert len(alteration_rows) == 2
        assert 'Name 1' in alteration_rows['substance'].tolist()
        assert 'Name 2' in alteration_rows['substance'].tolist()
        
        assert alteration_rows[alteration_rows['substance'] == 'Name 2']['comment'].iloc[0] == 'Refers to SC injection'

        # ATC dataframe should contain all ATC alterations
        assert len(atc_df) == 8  # 2 new 5th + 2 new 3rd/4th + 2 level alterations + 2 name alterations

        # Verify new 5th level codes
        new_5th_level = atc_df[atc_df['alterations_comment'] == 'New code']
        assert len(new_5th_level) == 2
        assert 'New Substance 1' in new_5th_level['substance'].tolist()

        # Verify new 3rd/4th level codes
        new_34_level = atc_df[atc_df['alterations_comment'] == 'New 3rd/4th level code']
        assert len(new_34_level) == 2
        assert 'New Group' in new_34_level['substance'].tolist()

        # Verify level alterations
        level_alterations = atc_df[atc_df['alterations_comment'].str.contains('Level updated:', na=False)]
        assert len(level_alterations) == 2
        assert 'Name 1' in level_alterations['substance'].tolist()

        # Verify name alterations
        name_alterations = atc_df[atc_df['alterations_comment'].str.contains('Name updated', na=False)]
        assert len(name_alterations) == 2
        assert 'New Name 1' in name_alterations['substance'].tolist()
        # For name alterations, previous and new ATC codes should be the same
        assert name_alterations['previous_atc_code'].equals(name_alterations['new_atc_code'])
