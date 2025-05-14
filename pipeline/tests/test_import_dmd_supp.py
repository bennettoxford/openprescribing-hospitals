import pytest

from pipeline.flows.import_dmd_supp import (
    parse_xml_data,
    parse_vtm_ingredients_xml,
    parse_dmd_history_xml
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

