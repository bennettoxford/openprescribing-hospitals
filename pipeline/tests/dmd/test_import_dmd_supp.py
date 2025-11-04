import pytest

from pipeline.dmd.import_dmd_supp import (
    parse_xml_data,
    parse_vtm_ingredients_xml,
    parse_dmd_history_xml,
    update_atc_codes
)

@pytest.fixture
def sample_xml_content():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <DMDXML>
        <VMPS>
            <VMP>
                <VPID>12345</VPID>
                <BNF>0403030E0</BNF>
                <ATC>N02BE01</ATC>
                <DDD>1000</DDD>
                <DDD_UOMCD>mg</DDD_UOMCD>
            </VMP>
            <VMP>
                <VPID>67890</VPID>
                <BNF>0403030G0</BNF>
                <ATC>N02BE02</ATC>
            </VMP>
            <VMP>
                <VPID>11111</VPID>
                <BNF>0403030E0</BNF>
                <ATC>N02BE01</ATC>
                <DDD>invalid</DDD>
                <DDD_UOMCD>mg</DDD_UOMCD>
            </VMP>
        </VMPS>
    </DMDXML>'''

@pytest.fixture
def sample_vtm_ingredients_xml_content():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <DMDXML>
        <VTM_INGS>
            <VTM_ING>
                <VTMID>12345</VTMID>
                <ISID>67890</ISID>
            </VTM_ING>
            <VTM_ING>
                <VTMID>54321</VTMID>
                <ISID>98765</ISID>
            </VTM_ING>
        </VTM_INGS>
    </DMDXML>'''

@pytest.fixture
def sample_history_xml_content():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <DMDXML>
        <VTM>
            <IDCURRENT>12345</IDCURRENT>
            <IDPREVIOUS>67890</IDPREVIOUS>
            <STARTDT>2023-01-01</STARTDT>
            <ENDDT>2023-12-31</ENDDT>
        </VTM>
        <VMP>
            <IDCURRENT>54321</IDCURRENT>
            <IDPREVIOUS>98765</IDPREVIOUS>
            <STARTDT>2023-02-01</STARTDT>
        </VMP>
    </DMDXML>'''

@pytest.fixture
def sample_dmd_data():
    return [
        {
            'vmp_code': '12345',
            'bnf_code': '0403030E0',
            'atc_code': 'N02BE01',
            'ddd': 1000.0,
            'ddd_uom': 'mg'
        },
        {
            'vmp_code': '67890',
            'bnf_code': '0403030G0',
            'atc_code': 'N02BE02',
            'ddd': None,
            'ddd_uom': None
        },
        {
            'vmp_code': '11111',
            'bnf_code': '0403030H0',
            'atc_code': 'N02BE03',
            'ddd': 500.0,
            'ddd_uom': 'mg'
        },
        {
            'vmp_code': '22222',
            'bnf_code': '0403030I0',
            'atc_code': None,  # No ATC code
            'ddd': None,
            'ddd_uom': None
        }
    ]

class TestXMLParsing:
    def test_parse_xml_data(self, sample_xml_content, tmp_path):
        xml_file = tmp_path / "test.xml"
        xml_file.write_text(sample_xml_content)
        
        result = parse_xml_data(xml_file)
        
        assert len(result) == 3
        assert result[0] == {
            'vmp_code': '12345',
            'bnf_code': '0403030E0',
            'atc_code': 'N02BE01',
            'ddd': 1000.0,
            'ddd_uom': 'mg'
        }
        assert result[2]['ddd'] is None
        
    def test_parse_vtm_ingredients_xml(self, sample_vtm_ingredients_xml_content, tmp_path):
        xml_file = tmp_path / "test_vtm_ing.xml"
        xml_file.write_text(sample_vtm_ingredients_xml_content)
        
        result = parse_vtm_ingredients_xml(xml_file)
        
        assert len(result) == 2
        assert result[0] == {
            'vtm_id': '12345',
            'ingredient_id': '67890'
        }
        assert result[1] == {
            'vtm_id': '54321',
            'ingredient_id': '98765'
        }
        
    def test_parse_dmd_history_xml(self, sample_history_xml_content, tmp_path):
        xml_file = tmp_path / "test_history.xml"
        xml_file.write_text(sample_history_xml_content)
        
        result = parse_dmd_history_xml(xml_file)
        
        assert len(result) == 2
        assert result[0] == {
            'current_id': '12345',
            'previous_id': '67890',
            'start_date': '2023-01-01',
            'end_date': '2023-12-31',
            'entity_type': 'VTM'
        }
        assert result[1] == {
            'current_id': '54321',
            'previous_id': '98765',
            'start_date': '2023-02-01',
            'end_date': None,
            'entity_type': 'VMP'
        }


class TestATCCodeUpdates:
    def test_update_atc_codes_with_mapping(self, sample_dmd_data):
        """Test updating ATC codes using mapping"""
        atc_mapping = {
            'N02BE01': {
                'new_code': 'N02BE01A',
                'substance': 'Paracetamol'
            }
        }
        deleted_codes = {}
        
        result = update_atc_codes(sample_dmd_data, atc_mapping, deleted_codes)
        
        # Check that the ATC code was updated
        assert len(result) == 4
        assert result[0]['atc_code'] == 'N02BE01A'
        assert result[0]['vmp_code'] == '12345'  # Other fields unchanged
        

    def test_update_atc_codes_with_deletions(self, sample_dmd_data):
        """Test removing records with deleted ATC codes"""
        atc_mapping = {}
        deleted_codes = {
            'N02BE02': 'Aspirin'
        }
        
        result = update_atc_codes(sample_dmd_data, atc_mapping, deleted_codes)
        
        # Check that record with deleted ATC code was removed
        assert len(result) == 3
        atc_codes = [record.get('atc_code') for record in result]
        assert 'N02BE02' not in atc_codes
        assert 'N02BE01' in atc_codes
        assert 'N02BE03' in atc_codes

    def test_update_atc_codes_combined(self, sample_dmd_data):
        """Test both updating and deleting ATC codes"""
        atc_mapping = {
            'N02BE01': {
                'new_code': 'N02BE01A',
                'substance': 'Paracetamol'
            }
        }
        deleted_codes = {
            'N02BE02': 'Aspirin'
        }
        
        result = update_atc_codes(sample_dmd_data, atc_mapping, deleted_codes)
        
        # Check that record with deleted ATC code was removed
        assert len(result) == 3
        
        # Check that the remaining N02BE01 record was updated
        updated_record = next(r for r in result if r['vmp_code'] == '12345')
        assert updated_record['atc_code'] == 'N02BE01A'
        
        # Check that N02BE02 record was removed
        atc_codes = [record.get('atc_code') for record in result]
        assert 'N02BE02' not in atc_codes

    def test_update_atc_codes_no_changes(self, sample_dmd_data):
        """Test with empty mappings - no changes should occur"""
        atc_mapping = {}
        deleted_codes = {}
        
        result = update_atc_codes(sample_dmd_data, atc_mapping, deleted_codes)
        
        # Should be identical to original
        assert len(result) == 4
        assert result == sample_dmd_data

    def test_update_atc_codes_preserves_none_values(self, sample_dmd_data):
        """Test that records with None ATC codes are preserved and not affected"""
        atc_mapping = {
            'N02BE01': {
                'new_code': 'N02BE01A',
                'substance': 'Paracetamol'
            }
        }
        deleted_codes = {
            'N02BE02': 'Aspirin'
        }
        
        result = update_atc_codes(sample_dmd_data, atc_mapping, deleted_codes)
        
        # Find the record with None ATC code
        none_atc_record = next(r for r in result if r['vmp_code'] == '22222')
        assert none_atc_record['atc_code'] is None
        assert none_atc_record in result  # Should still be present

