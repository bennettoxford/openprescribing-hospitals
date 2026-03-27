import pytest
import pandas as pd
from unittest.mock import Mock, patch
from pipeline.atc_ddd.ddd_comments.import_ddd_combined_products import (
    extract_ud_value,
    extract_ingredients_quantity_unit,
    extract_converted_value_and_unit,
    map_dosage_form_to_form_and_route,
    process_combined_products,
    DOSAGE_FORM_MAPPING,
)


@pytest.fixture
def sample_combined_products_df():
    return pd.DataFrame([
        # Valid: DuoPlavin (B01AC30) - tab, two ingredients
        {
            'ATC Code': 'B01AC30',
            'Brand name': 'DuoPlavin ',
            'Dosage form': 'tab',
            'Active ingredients per unit dose (UD)': 'clopidogrel 75 mg/ acetylsalicylic acid 75 mg ',
            'DDD comb.': '1 UD (=1 tab)'
        },
        # Valid: Pylera (A02BD08) - caps, three ingredients
        {
            'ATC Code': 'A02BD08',
            'Brand name': 'Pylera',
            'Dosage form': 'caps',
            'Active ingredients per unit dose (UD)': 'bismuth subcitrate 0.14 g/ tetracycline 0.125 g/ metronidazole 0.125 g',
            'DDD comb.': '12 UD (=12 caps)'
        },
        # Valid: Movicol Liquid (A06AD65) - concentrate for oral solution, denominator 25 ml
        {
            'ATC Code': 'A06AD65',
            'Brand name': 'Movicol, Movicolon Liquid ',
            'Dosage form': 'concentrate for oral solution',
            'Active ingredients per unit dose (UD)': 'macrogol 13.125 g/  sodium bicarbonate 178.5 mg/ 25 ml',
            'DDD comb.': '10 UD (=50 ml) '
        },
        # Invalid: no dosage form / DDD in UD format
        {
            'ATC Code': 'A10BD',
            'Brand name': 'It has been considered most appropriate to assign fixed DDDs based on the average use...',
            'Dosage form': None,
            'Active ingredients per unit dose (UD)': None,
            'DDD comb.': None
        },
        # Invalid: empty required fields
        {
            'ATC Code': 'A01AA05',
            'Brand name': 'Empty Strings',
            'Dosage form': '',
            'Active ingredients per unit dose (UD)': '',
            'DDD comb.': ''
        }
    ])


class TestExtractUDValue:
    @pytest.mark.parametrize("ddd_combined,expected", [
        ("2 UD (=2 tab)", 2.0),
        ("1.5 UD (=1.5 supp)", 1.5),
        ("12 UD (=12 caps)", 12.0),
        ("0.5 UD (=0.5 tab)", 0.5),
        ("10 UD (=10 doses inhal aer)", 10.0),
        ("1 UD (=1 tab)", 1.0),  # DuoPlavin (B01AC30)
        ("2 UD  (=2 tab)", 2.0),  # Calcichew D3 mite - double space
        ("1 UD (=1 ampoulle)", 1.0),  # Invicorp (G04BE30)
        ("2.5 UD (=2.5 supp)", 2.5),  # Ketogan supp (N02AG02)
        ("", None),
        (None, None),
        ("Invalid format", None),
        ("  2 UD (=2 tab)  ", 2.0),
        ("2UD (=2 tab)", 2.0),
    ])
    def test_extract_ud_value(self, ddd_combined, expected):
        result = extract_ud_value(ddd_combined)
        assert result == expected


class TestExtractIngredientsQuantityUnit:
    @pytest.mark.parametrize("active_ingredients,expected", [
        (
            "paracetamol 500 mg",
            [{
                'ingredient': 'paracetamol 500 mg',
                'numerator_quantity': None,
                'numerator_unit': None,
                'denominator_quantity': None,
                'denominator_unit': None
            }]
        ),
        # Pylera (A02BD08)
        (
            "bismuth subcitrate 0.14 g/ tetracycline 0.125 g/ metronidazole 0.125 g",
            [
                {
                    'ingredient': 'bismuth subcitrate',
                    'numerator_quantity': 0.14,
                    'numerator_unit': 'g',
                    'denominator_quantity': None,
                    'denominator_unit': None
                },
                {
                    'ingredient': 'tetracycline',
                    'numerator_quantity': 0.125,
                    'numerator_unit': 'g',
                    'denominator_quantity': None,
                    'denominator_unit': None
                },
                {
                    'ingredient': 'metronidazole',
                    'numerator_quantity': 0.125,
                    'numerator_unit': 'g',
                    'denominator_quantity': None,
                    'denominator_unit': None
                }
            ]
        ),
        (
            "sodium bicarbonate 178.5 mg/ 25 ml",
            [{
                'ingredient': 'sodium bicarbonate',
                'numerator_quantity': 178.5,
                'numerator_unit': 'mg',
                'denominator_quantity': 25.0,
                'denominator_unit': 'ml'
            }]
        ),
        # Movicol Liquid (A06AD65)
        (
            "macrogol 13.125 g/ sodium bicarbonate 178.5 mg/ 25 ml",
            [
                {
                    'ingredient': 'macrogol',
                    'numerator_quantity': 13.125,
                    'numerator_unit': 'g',
                    'denominator_quantity': None,
                    'denominator_unit': None
                },
                {
                    'ingredient': 'sodium bicarbonate',
                    'numerator_quantity': 178.5,
                    'numerator_unit': 'mg',
                    'denominator_quantity': 25.0,
                    'denominator_unit': 'ml'
                }
            ]
        ),
        # Combizym (A09AA02) - multienzymes, no slash
        (
            "multienzymes (lipase, protease etc.)",
            [{
                'ingredient': 'multienzymes (lipase, protease etc.)',
                'numerator_quantity': None,
                'numerator_unit': None,
                'denominator_quantity': None,
                'denominator_unit': None
            }]
        ),
        (
            "some ingredient name",
            [{
                'ingredient': 'some ingredient name',
                'numerator_quantity': None,
                'numerator_unit': None,
                'denominator_quantity': None,
                'denominator_unit': None
            }]
        ),
        (
            "Comb. of benzylpenicillin",
            [{
                'ingredient': 'benzylpenicillin',
                'numerator_quantity': None,
                'numerator_unit': None,
                'denominator_quantity': None,
                'denominator_unit': None
            }]
        ),
        (
            "",
            []
        ),
        (
            None,
            []
        ),
    ])
    def test_extract_ingredients_quantity_unit(self, active_ingredients, expected):
        result = extract_ingredients_quantity_unit(active_ingredients)
        assert result == expected

    def test_extract_ingredients_with_metered_dose(self):
        """Test ingredient extraction with additional text like '(metered dose)' when there's a '/' separator.
        Real example: Seretide inhal aer - (metered dose) after unit."""
        active_ingredients = "salbutamol 100 micrograms (metered dose)/ other ingredient"
        result = extract_ingredients_quantity_unit(active_ingredients)
        
        assert len(result) == 2
        assert result[0]['ingredient'] == 'salbutamol'
        assert result[0]['numerator_quantity'] == 100.0
        assert result[0]['numerator_unit'] == 'micrograms'
        assert result[1]['ingredient'] == 'other ingredient'

    def test_extract_ingredients_caltrate_style_parens_after_unit(self):
        """Real example: Caltrate (A12AX) - (10 mcg) after unit is stripped by pattern."""
        active_ingredients = "ca2+ 0.6 g/ colecalciferol 400 IU (10 mcg)"
        result = extract_ingredients_quantity_unit(active_ingredients)
        assert len(result) == 2
        assert result[0]['ingredient'] == 'ca2+'
        assert result[0]['numerator_quantity'] == 0.6
        assert result[0]['numerator_unit'] == 'g'
        assert result[1]['ingredient'] == 'colecalciferol'
        assert result[1]['numerator_quantity'] == 400.0
        assert result[1]['numerator_unit'] == 'IU'


class TestExtractConvertedValueAndUnit:
    @pytest.mark.parametrize("ddd_combined,expected_value,expected_unit", [
        ("2 UD (=2 tab)", 2.0, 'tablet'),
        ("12 UD (=12 caps)", 12.0, 'capsule'),
        ("2 UD (=2 grams of powder for injection)", 2.0, 'gram'),
        ("4 UD (=4 doses inhal aer)", 4.0, 'dose'),
        ("2 UD (defined as 2 vials)", 2.0, 'vial'),  # Lidaprim (J01EE03)
        ("1.5 UD (= 1.5 supp)", 1.5, 'suppository'),
        ("10 UD (= 10 ml)", 10.0, 'ml'),
        ("10 UD (=50 ml) ", 50.0, 'ml'),  # Movicol Liquid (A06AD65)
        ("1 UD (=1 ampoulle)", 1.0, 'ampoule'),  # Invicorp (G04BE30)
        ("2.5 UD (=2.5 supp)", 2.5, 'suppository'),  # Ketogan supp (N02AG02)
        ("2 UD (=2 sachets)", 2.0, 'sachet'),
        ("", None, None),
        (None, None, None),
        ("2 UD (invalid format)", None, None),
    ])
    def test_extract_converted_value_and_unit(self, ddd_combined, expected_value, expected_unit):
        value, unit = extract_converted_value_and_unit(ddd_combined)
        assert value == expected_value
        assert unit == expected_unit

    def test_extract_converted_value_with_complex_unit_string(self):
        """Test extraction when unit string contains multiple words"""
        ddd_combined = "2 UD (=2 vials inhal sol)"
        value, unit = extract_converted_value_and_unit(ddd_combined)
        assert value == 2.0
        assert unit == 'vial'

    def test_extract_converted_value_unknown_unit_returns_none_for_unit(self):
        """Test extraction when unit is not in mapping returns value but None for unit"""
        ddd_combined = "2 UD (=2 unknownunit)"
        value, unit = extract_converted_value_and_unit(ddd_combined)
        assert value == 2.0
        assert unit is None


class TestMapDosageFormToFormAndRoute:
    @patch('pipeline.atc_ddd.ddd_comments.import_ddd_combined_products.get_run_logger')
    def test_map_dosage_form_basic(self, mock_logger):
        mock_logger.return_value = Mock()
        df = pd.DataFrame({
            'dosage_form': ['tab', 'caps', 'inj', 'oral sol']
        })
        
        result = map_dosage_form_to_form_and_route(df)
        
        assert 'form' in result.columns
        assert 'route' in result.columns
        
        tab_row = result[result['dosage_form'] == 'tab'].iloc[0]
        assert tab_row['form'] == 'tablet'
        assert tab_row['route'] == 'oral'
        
        caps_row = result[result['dosage_form'] == 'caps'].iloc[0]
        assert caps_row['form'] == 'capsule'
        assert caps_row['route'] == 'oral'
        
        inj_row = result[result['dosage_form'] == 'inj'].iloc[0]
        assert inj_row['form'] == '*injection'
        assert inj_row['route'] == '*'
        
        oral_sol_row = result[result['dosage_form'] == 'oral sol'].iloc[0]
        assert oral_sol_row['form'] == 'solution'
        assert oral_sol_row['route'] == 'oral'


    @patch('pipeline.atc_ddd.ddd_comments.import_ddd_combined_products.get_run_logger')
    def test_map_all_dosage_forms(self, mock_logger):
        """Test that all dosage forms in the mapping work correctly"""
        mock_logger.return_value = Mock()
        df = pd.DataFrame({
            'dosage_form': list(DOSAGE_FORM_MAPPING.keys())
        })
        
        result = map_dosage_form_to_form_and_route(df)
        
        for idx, dosage_form in enumerate(DOSAGE_FORM_MAPPING.keys()):
            expected_mapping = DOSAGE_FORM_MAPPING[dosage_form]
            assert result.iloc[idx]['form'] == expected_mapping['form']
            assert result.iloc[idx]['route'] == expected_mapping['route']


class TestProcessCombinedProducts:
    @patch('pipeline.atc_ddd.ddd_comments.import_ddd_combined_products.get_run_logger')
    def test_process_combined_products_basic(self, mock_logger, sample_combined_products_df):
        mock_logger.return_value = Mock()
        
        result = process_combined_products(sample_combined_products_df.copy())
        
        expected_columns = [
            'atc_code', 'brand_name', 'dosage_form', 'form', 'route',
            'active_ingredients', 'ddd_comb', 'ddd_ud_value',
            'ddd_converted_value', 'ddd_converted_unit'
        ]
        assert all(col in result.columns for col in expected_columns)
        
        # Check that rows with missing data are filtered out
        assert len(result) == 3
        
        # Row 1: DuoPlavin (B01AC30) - tab, two ingredients
        first_row = result.iloc[0]
        assert first_row['atc_code'] == 'B01AC30'
        assert first_row['brand_name'] == 'DuoPlavin '
        assert first_row['form'] == 'tablet'
        assert first_row['route'] == 'oral'
        assert first_row['ddd_ud_value'] == 1.0
        assert first_row['ddd_converted_value'] == 1.0
        assert first_row['ddd_converted_unit'] == 'tablet'
        assert len(first_row['active_ingredients']) == 2
        assert first_row['active_ingredients'][0]['ingredient'] == 'clopidogrel'
        assert first_row['active_ingredients'][0]['numerator_quantity'] == 75.0
        assert first_row['active_ingredients'][1]['ingredient'] == 'acetylsalicylic acid'
        assert first_row['active_ingredients'][1]['numerator_quantity'] == 75.0

        # Row 2: Pylera (A02BD08) - caps, three ingredients
        second_row = result.iloc[1]
        assert second_row['atc_code'] == 'A02BD08'
        assert second_row['brand_name'] == 'Pylera'
        assert second_row['form'] == 'capsule'
        assert second_row['route'] == 'oral'
        assert second_row['ddd_ud_value'] == 12.0
        assert second_row['ddd_converted_value'] == 12.0
        assert second_row['ddd_converted_unit'] == 'capsule'
        assert len(second_row['active_ingredients']) == 3
        assert second_row['active_ingredients'][0]['ingredient'] == 'bismuth subcitrate'
        assert second_row['active_ingredients'][1]['ingredient'] == 'tetracycline'
        assert second_row['active_ingredients'][2]['ingredient'] == 'metronidazole'

        # Row 3: Movicol Liquid (A06AD65) - concentrate for oral solution, denominator 25 ml
        third_row = result.iloc[2]
        assert third_row['atc_code'] == 'A06AD65'
        assert third_row['dosage_form'] == 'concentrate for oral solution'
        assert third_row['form'] == 'solution'
        assert third_row['route'] == 'oral'
        assert third_row['ddd_ud_value'] == 10.0
        assert third_row['ddd_converted_value'] == 50.0
        assert third_row['ddd_converted_unit'] == 'ml'
        assert len(third_row['active_ingredients']) == 2
        assert third_row['active_ingredients'][1]['denominator_quantity'] == 25.0
        assert third_row['active_ingredients'][1]['denominator_unit'] == 'ml'

    @patch('pipeline.atc_ddd.ddd_comments.import_ddd_combined_products.get_run_logger')
    def test_process_combined_products_filtering(self, mock_logger):
        """Filtering uses real-style rows: comment row (A10BD) and C02 C03 style have no UD format."""
        mock_logger.return_value = Mock()
        
        df = pd.DataFrame([
            # Valid row
            {
                'ATC Code': 'B01AC30',
                'Brand name': 'DuoPlavin',
                'Dosage form': 'tab',
                'Active ingredients per unit dose (UD)': 'clopidogrel 75 mg/ acetylsalicylic acid 75 mg',
                'DDD comb.': '1 UD (=1 tab)'
            },
            # Filtered: comment row (A10BD)
            {
                'ATC Code': 'A10BD',
                'Brand name': 'It has been considered most appropriate to assign fixed DDDs...',
                'Dosage form': None,
                'Active ingredients per unit dose (UD)': None,
                'DDD comb.': None
            },
            # Filtered: empty dosage form
            {
                'ATC Code': 'C02KX54',
                'Brand name': 'Opsynvi/Yuvanci',
                'Dosage form': '',
                'Active ingredients per unit dose (UD)': 'macitentan 10 mg/ tadalafil 40 mg',
                'DDD comb.': '1 UD (=1 tab)'
            },
            # Filtered: missing ingredients
            {
                'ATC Code': 'C02 C03 C07 C08 C09 C10',
                'Brand name': 'For fixed combinations in these ATC groups, the DDD is assigned based on dosing frequency only...',
                'Dosage form': None,
                'Active ingredients per unit dose (UD)': None,
                'DDD comb.': None
            },
            # Filtered: missing DDD comb
            {
                'ATC Code': 'B01AC56',
                'Brand name': 'Axanum',
                'Dosage form': 'caps',
                'Active ingredients per unit dose (UD)': 'acetylsalicylic acid 81 mg/ esomeprazole 20 mg',
                'DDD comb.': None
            }
        ])
        
        result = process_combined_products(df)
        
        assert len(result) == 1
        assert result.iloc[0]['atc_code'] == 'B01AC30'
        assert result.iloc[0]['brand_name'] == 'DuoPlavin'

    @patch('pipeline.atc_ddd.ddd_comments.import_ddd_combined_products.get_run_logger')
    def test_process_combined_products_multiple_ingredients(self, mock_logger):
        """Real example: Pylera (A02BD08) - three ingredients from CSV."""
        mock_logger.return_value = Mock()
        
        df = pd.DataFrame([
            {
                'ATC Code': 'A02BD08',
                'Brand name': 'Pylera',
                'Dosage form': 'caps',
                'Active ingredients per unit dose (UD)': 'bismuth subcitrate 0.14 g/ tetracycline 0.125 g/ metronidazole 0.125 g',
                'DDD comb.': '12 UD (=12 caps)'
            }
        ])
        
        result = process_combined_products(df)
        
        assert len(result) == 1
        ingredients = result.iloc[0]['active_ingredients']
        assert len(ingredients) == 3
        assert ingredients[0]['ingredient'] == 'bismuth subcitrate'
        assert ingredients[0]['numerator_quantity'] == 0.14
        assert ingredients[0]['numerator_unit'] == 'g'
        assert ingredients[1]['ingredient'] == 'tetracycline'
        assert ingredients[1]['numerator_quantity'] == 0.125
        assert ingredients[2]['ingredient'] == 'metronidazole'
        assert ingredients[2]['numerator_quantity'] == 0.125

    @patch('pipeline.atc_ddd.ddd_comments.import_ddd_combined_products.get_run_logger')
    def test_process_combined_products_with_denominator(self, mock_logger):
        """Real example: Movicol Liquid (A06AD65) - denominator 25 ml from CSV."""
        mock_logger.return_value = Mock()
        
        df = pd.DataFrame([
            {
                'ATC Code': 'A06AD65',
                'Brand name': 'Movicol, Movicolon Liquid ',
                'Dosage form': 'concentrate for oral solution',
                'Active ingredients per unit dose (UD)': 'macrogol 13.125 g/  sodium bicarbonate 178.5 mg/ 25 ml',
                'DDD comb.': '10 UD (=50 ml) '
            }
        ])
        
        result = process_combined_products(df)
        
        assert len(result) == 1
        ingredients = result.iloc[0]['active_ingredients']
        assert len(ingredients) == 2
        assert ingredients[0]['ingredient'] == 'macrogol'
        assert ingredients[0]['numerator_quantity'] == 13.125
        assert ingredients[1]['ingredient'] == 'sodium bicarbonate'
        assert ingredients[1]['numerator_quantity'] == 178.5
        assert ingredients[1]['denominator_quantity'] == 25.0
        assert ingredients[1]['denominator_unit'] == 'ml'
