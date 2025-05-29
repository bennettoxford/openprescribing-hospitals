import pytest
import pandas as pd
from unittest.mock import patch

from pipeline.flows.import_atc_ddd_alterations import (
    parse_ddd_alterations,
    parse_atc_alterations,
    clean_atc,
    process_new_atc_codes,
    process_new_ddds,
    process_new_atc_34_levels,
    process_atc_name_alterations,
    import_atc_ddd_alterations_flow,
)

@pytest.fixture
def sample_ddd_html():
    return '''
    <html>
    <body>
        <table>
            <tr>
                <th>Substance</th>
                <th colspan="3">Previous DDD</th>
                <th colspan="3">New DDD</th>
                <th>Present ATC code</th>
                <th>Year changed</th>
            </tr>
            <tr>
                <td>paracetamol<sup><a title="Comment about change">1</a></sup></td>
                <td>3</td>
                <td>g</td>
                <td>O</td>
                <td>3.5</td>
                <td>g</td>
                <td>O</td>
                <td>N02BE01</td>
                <td>2023</td>
            </tr>
            <tr>
                <td>aspirin</td>
                <td>2</td>
                <td>g</td>
                <td>O</td>
                <td>2.5</td>
                <td>g</td>
                <td>O</td>
                <td>N02BA01</td>
                <td>2024</td>
            </tr>
        </table>
    </body>
    </html>
    '''

@pytest.fixture
def sample_atc_html():
    return '''
    <html>
    <body>
        <table>
            <tr>
                <th>Previous ATC code</th>
                <th>Substance name</th>
                <th>New ATC code</th>
                <th>Year changed</th>
            </tr>
            <tr>
                <td>A01AA01 (existing code)</td>
                <td>test substance<sup><a title="Code change comment">1</a></sup></td>
                <td>A01AA02</td>
                <td>2023</td>
            </tr>
            <tr>
                <td>B02BB01</td>
                <td>another substance</td>
                <td>B02BB02 (existing code)</td>
                <td>2024</td>
            </tr>
        </table>
    </body>
    </html>
    '''

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
        'DDDs alterations': pd.DataFrame({
            'ATC code': ['F01AA01', 'F02BB01'],
            'ATC level name': ['Name 1', 'Name 2'],
            'Previous DDD': [1.0, 2.0],
            'Unit': ['g', 'mg'],
            'Adm.route': ['O', 'P'],
            'New DDD': [1.5, 2.5],
            'Unit': ['g', 'mg'],
            'Adm.route': ['O', 'P'],
            'Note': [pd.NA, 'Refers to SC injection']
        }),
        
    }

class TestDDDAlterationsParsing:
    def test_parse_ddd_alterations_success(self, sample_ddd_html):
        """Test successful parsing of DDD alterations HTML"""
        result = parse_ddd_alterations(sample_ddd_html)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

        expected_columns = [
            'substance', 'previous_ddd', 'previous_ddd_unit', 'previous_route',
            'new_ddd', 'new_ddd_unit', 'new_route', 'atc_code', 'year_changed', 'comment'
        ]
        assert all(col in result.columns for col in expected_columns)
        
        first_row = result.iloc[0]
        assert first_row['substance'] == 'paracetamol'
        assert first_row['previous_ddd'] == 3.0
        assert first_row['new_ddd'] == 3.5
        assert first_row['atc_code'] == 'N02BE01'
        assert first_row['year_changed'] == 2023
        assert 'Comment about change' in first_row['comment']

    def test_parse_ddd_alterations_no_table(self):
        """Test parsing when no table is found"""
        html = "<html><body>No table here</body></html>"
        
        with pytest.raises(ValueError, match="Could not find DDD alterations table in HTML"):
            parse_ddd_alterations(html)

    def test_parse_ddd_alterations_invalid_numbers(self):
        """Test parsing with invalid numeric values"""
        html = '''
        <table>
            <tr><th>Headers</th></tr>
            <tr>
                <td>substance</td>
                <td>invalid</td>
                <td>g</td>
                <td>O</td>
                <td>not_a_number</td>
                <td>g</td>
                <td>O</td>
                <td>N02BE01</td>
                <td>not_a_year</td>
            </tr>
        </table>
        '''
        
        result = parse_ddd_alterations(html)
        
        assert len(result) == 1
        assert pd.isna(result.iloc[0]['previous_ddd'])
        assert pd.isna(result.iloc[0]['new_ddd'])
        assert pd.isna(result.iloc[0]['year_changed'])

class TestATCAlterationsParsing:
    def test_parse_atc_alterations_success(self, sample_atc_html):
        """Test successful parsing of ATC alterations HTML"""
        result = parse_atc_alterations(sample_atc_html)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        
        expected_columns = ['substance', 'previous_atc_code', 'new_atc_code', 'year_changed', 'comment']
        assert all(col in result.columns for col in expected_columns)
        
        first_row = result.iloc[0]
        assert first_row['substance'] == 'test substance'
        assert first_row['previous_atc_code'] == 'A01AA01'
        assert first_row['new_atc_code'] == 'A01AA02'
        assert first_row['year_changed'] == 2023
        assert 'Code change comment' in first_row['comment']

    def test_clean_atc_function(self):
        """Test the clean_atc helper function"""
        assert clean_atc("A01AA01 (existing code)") == "A01AA01"
        assert clean_atc("B02BB01") == "B02BB01"
        assert clean_atc("C03CC03 (existing code) extra text") == "C03CC03"

    def test_parse_atc_alterations_no_table(self):
        """Test parsing when no table is found"""
        html = "<html><body>No table here</body></html>"
        
        with pytest.raises(ValueError, match="Could not find ATC alterations table in HTML"):
            parse_atc_alterations(html)

class TestExcelProcessing:
    def test_process_new_atc_codes(self, sample_excel_data):
        """Test processing new ATC codes from Excel"""
        result = process_new_atc_codes(sample_excel_data, 2025)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        
        expected_columns = ['substance', 'previous_atc_code', 'new_atc_code', 'year_changed', 'comment']
        assert all(col in result.columns for col in expected_columns)
        
        first_row = result.iloc[0]
        assert first_row['substance'] == 'New Substance 1'
        assert pd.isna(first_row['previous_atc_code'])
        assert first_row['new_atc_code'] == 'C01AA99'
        assert first_row['year_changed'] == 2025
        assert first_row['comment'] == 'New code'

    def test_process_new_ddds(self, sample_excel_data):
        """Test processing new DDDs from Excel"""
        result = process_new_ddds(sample_excel_data, 2025)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        
        expected_columns = [
            'substance', 'previous_ddd', 'previous_ddd_unit', 'previous_route',
            'new_ddd', 'new_ddd_unit', 'new_route', 'atc_code', 'year_changed', 'comment'
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

    def test_process_new_atc_34_levels(self, sample_excel_data):
        """Test processing new 3rd and 4th level ATC codes"""
        result = process_new_atc_34_levels(sample_excel_data, 2025)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        
        first_row = result.iloc[0]
        assert first_row['substance'] == 'New Group'
        assert first_row['new_atc_code'] == 'E01'
        assert first_row['comment'] == 'New 3rd/4th level code'

    def test_process_atc_name_alterations(self, sample_excel_data):
        """Test processing ATC name alterations"""
        result = process_atc_name_alterations(sample_excel_data, 2025)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        
        first_row = result.iloc[0]
        assert first_row['substance'] == 'New Name 1'
        assert first_row['previous_atc_code'] == 'F01AA01'
        assert first_row['new_atc_code'] == 'F01AA01'
        assert 'Old Name 1 → New Name 1' in first_row['comment']

    def test_process_empty_excel_sheets(self):
        """Test processing when Excel sheets are empty"""
        empty_data = {
            'New ATC 5th levels': pd.DataFrame(),
            'New DDDs': pd.DataFrame(),
            'New 3rd and 4th levels': pd.DataFrame(),
            'ATC level name alterations': pd.DataFrame()
        }
        
        atc_result = process_new_atc_codes(empty_data, 2025)
        ddd_result = process_new_ddds(empty_data, 2025)
        atc_34_result = process_new_atc_34_levels(empty_data, 2025)
        name_result = process_atc_name_alterations(empty_data, 2025)
        
        assert atc_result.empty
        assert ddd_result.empty
        assert atc_34_result.empty
        assert name_result.empty


class TestMainFlow:
    @patch('pipeline.flows.import_atc_ddd_alterations.fetch_excel')
    @patch('pipeline.flows.import_atc_ddd_alterations.fetch_html')
    @patch('pipeline.flows.import_atc_ddd_alterations.load_to_bigquery')
    def test_import_atc_ddd_alterations_flow(self, mock_load, mock_fetch_html, mock_fetch_excel, 
                                           sample_ddd_html, sample_atc_html, sample_excel_data):
        """Test the main import flow including concatenation of HTML and Excel data"""
        mock_fetch_excel.return_value = (sample_excel_data, 2025)
        mock_fetch_html.side_effect = [sample_ddd_html, sample_atc_html]

        loaded_dataframes = []
        def capture_load_calls(df, table_spec):
            loaded_dataframes.append((df.copy(), table_spec))
        mock_load.side_effect = capture_load_calls
            
        result = import_atc_ddd_alterations_flow()

        ddd_df, ddd_table_spec = loaded_dataframes[0]
        
        # Should contain original HTML data (2 rows) + new DDDs from Excel (2 rows)
        assert len(ddd_df) == 4
        
        # Verify HTML data is present
        html_substances = ddd_df[ddd_df['year_changed'].isin([2023, 2024])]['substance'].tolist()
        assert 'paracetamol' in html_substances
        assert 'aspirin' in html_substances
        
        # Verify Excel data is present (new DDDs with year 2025)
        excel_substances = ddd_df[ddd_df['year_changed'] == 2025]['substance'].tolist()
        assert 'Test Substance' in excel_substances
        assert 'Another Substance' in excel_substances
        
        # Verify new DDDs have None for previous values
        new_ddd_rows = ddd_df[ddd_df['comment'] == 'New DDD']
        assert all(pd.isna(new_ddd_rows['previous_ddd']))
        assert all(pd.isna(new_ddd_rows['previous_ddd_unit']))
        assert all(pd.isna(new_ddd_rows['previous_route']))
        
        # Verify ATC alterations concatenation
        atc_df, atc_table_spec = loaded_dataframes[1]
        
        # Should contain:
        # - Original HTML data (2 rows)
        # - New 5th level codes (2 rows)
        # - New 3rd/4th level codes (2 rows) 
        # - Name alterations (2 rows)
        assert len(atc_df) == 8
        
        # Verify HTML data is present
        html_atc_substances = atc_df[atc_df['year_changed'].isin([2023, 2024])]['substance'].tolist()
        assert 'test substance' in html_atc_substances
        assert 'another substance' in html_atc_substances
        
        # Verify Excel data is present (all with year 2025)
        excel_atc_data = atc_df[atc_df['year_changed'] == 2025]
        
        # Check new 5th level codes
        new_5th_level = excel_atc_data[excel_atc_data['comment'] == 'New code']
        assert len(new_5th_level) == 2
        assert 'New Substance 1' in new_5th_level['substance'].tolist()
        assert 'New Substance 2' in new_5th_level['substance'].tolist()
        assert all(pd.isna(new_5th_level['previous_atc_code']))
        
        # Check new 3rd/4th level codes
        new_34_level = excel_atc_data[excel_atc_data['comment'] == 'New 3rd/4th level code']
        assert len(new_34_level) == 2
        assert 'New Group' in new_34_level['substance'].tolist()
        assert 'Another Group' in new_34_level['substance'].tolist()
        
        # Check name alterations
        name_alterations = excel_atc_data[excel_atc_data['comment'].str.contains('Name updated', na=False)]
        assert len(name_alterations) == 2
        assert 'New Name 1' in name_alterations['substance'].tolist()
        assert 'New Name 2' in name_alterations['substance'].tolist()
        # For name alterations, previous and new ATC codes should be the same
        assert name_alterations['previous_atc_code'].equals(name_alterations['new_atc_code'])
        
        # Check return values reflect final concatenated lengths
        assert result['ddd_alterations'] == 4
        assert result['atc_alterations'] == 8 