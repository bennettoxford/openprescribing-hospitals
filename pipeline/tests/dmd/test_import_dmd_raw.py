import zipfile
from pathlib import Path

import pytest

from pipeline.dmd.import_dmd_raw import (
    parse_dmd_release_zip,
    select_current_uom_rows,
)


VMP_XML = """<?xml version="1.0" encoding="UTF-8"?>
<VIRTUAL_MED_PRODUCTS>
  <VMPS>
    <VMP>
      <VPID>318135008</VPID>
      <VTMID>34186711000001102</VTMID>
      <NM>Co-amilofruse 2.5mg/20mg tablets</NM>
      <DF_INDCD>1</DF_INDCD>
      <UDFS>1</UDFS>
      <UDFS_UOMCD>428673006</UDFS_UOMCD>
      <UNIT_DOSE_UOMCD>428673006</UNIT_DOSE_UOMCD>
    </VMP>
    <VMP>
      <VPID>999</VPID>
      <INVALID>1</INVALID>
      <NM>Product with no VTM</NM>
      <DF_INDCD>2</DF_INDCD>
    </VMP>
  </VMPS>
  <VIRTUAL_PRODUCT_INGREDIENT>
    <VPI>
      <VPID>318135008</VPID>
      <ISID>100</ISID>
      <BASIS_STRNTCD>0001</BASIS_STRNTCD>
      <STRNT_NMRTR_VAL>2.5</STRNT_NMRTR_VAL>
      <STRNT_NMRTR_UOMCD>258684004</STRNT_NMRTR_UOMCD>
      <STRNT_DNMTR_VAL>1</STRNT_DNMTR_VAL>
      <STRNT_DNMTR_UOMCD>430293005</STRNT_DNMTR_UOMCD>
      <BS_SUBID>200</BS_SUBID>
    </VPI>
    <VPI>
      <VPID>999</VPID>
      <ISID>101</ISID>
    </VPI>
  </VIRTUAL_PRODUCT_INGREDIENT>
  <ONT_DRUG_FORM>
    <ONT>
      <VPID>318135008</VPID>
      <FORMCD>385268001</FORMCD>
    </ONT>
  </ONT_DRUG_FORM>
  <DRUG_FORM>
    <DFORM>
      <VPID>318135008</VPID>
      <FORMCD>421026006</FORMCD>
    </DFORM>
  </DRUG_FORM>
</VIRTUAL_MED_PRODUCTS>
"""

AMP_XML = """<?xml version="1.0" encoding="UTF-8"?>
<ACTUAL_MEDICINAL_PRODUCTS>
  <AMPS>
    <AMP>
      <APID>300</APID>
      <VPID>318135008</VPID>
      <NM>Brand name</NM>
      <DESC>Brand name 2.5mg tablets</DESC>
      <AVAIL_RESTRICTCD>0001</AVAIL_RESTRICTCD>
    </AMP>
  </AMPS>
</ACTUAL_MEDICINAL_PRODUCTS>
"""

ING_XML = """<?xml version="1.0" encoding="UTF-8"?>
<INGREDIENT_SUBSTANCES>
  <ING>
    <ISID>100</ISID>
    <NM>Amiloride</NM>
  </ING>
</INGREDIENT_SUBSTANCES>
"""

VTM_XML = """<?xml version="1.0" encoding="UTF-8"?>
<VIRTUAL_THERAPEUTIC_MOIETIES>
  <VTM>
    <VTMID>34186711000001102</VTMID>
    <NM>Co-amilofruse</NM>
  </VTM>
  <VTM>
    <VTMID>100</VTMID>
    <INVALID>1</INVALID>
    <NM>Old name</NM>
  </VTM>
</VIRTUAL_THERAPEUTIC_MOIETIES>
"""

LOOKUP_XML = """<?xml version="1.0" encoding="UTF-8"?>
<LOOKUP>
  <COMBINATION_PACK_IND>
    <INFO><CD>1</CD><DESC>Ignore</DESC></INFO>
  </COMBINATION_PACK_IND>
  <UNIT_OF_MEASURE>
    <INFO>
      <CD>428673006</CD>
      <DESC>tablet</DESC>
    </INFO>
    <INFO>
      <CD>10697611000001108</CD>
      <CDDT>2022-05-10</CDDT>
      <CDPREV>282380000</CDPREV>
      <DESC>%v/v</DESC>
    </INFO>
  </UNIT_OF_MEASURE>
  <FORM>
    <INFO><CD>421026006</CD><DESC>Tablet</DESC></INFO>
  </FORM>
  <ONT_FORM_ROUTE>
    <INFO><CD>385268001</CD><DESC>tablet.oral</DESC></INFO>
  </ONT_FORM_ROUTE>
  <AVAILABILITY_RESTRICTION>
    <INFO><CD>0001</CD><DESC>None</DESC></INFO>
  </AVAILABILITY_RESTRICTION>
  <DF_INDICATOR>
    <INFO><CD>1</CD><DESC>Discrete</DESC></INFO>
  </DF_INDICATOR>
</LOOKUP>
"""


def write_release_zip(path: Path) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("f_vmp2_010126.xml", VMP_XML)
        archive.writestr("f_amp2_010126.xml", AMP_XML)
        archive.writestr("f_ingredient2_010126.xml", ING_XML)
        archive.writestr("f_vtm2_010126.xml", VTM_XML)
        archive.writestr("f_lookup2_010126.xml", LOOKUP_XML)


def test_select_current_uom_rows_drops_replaced_codes():
    rows = [
        {
            "uom_code": "282380000",
            "uom_code_prev": None,
            "change_date": None,
            "description": "old percent",
        },
        {
            "uom_code": "10697611000001108",
            "uom_code_prev": "282380000",
            "change_date": "2022-05-10",
            "description": "%v/v",
        },
        {
            "uom_code": "428673006",
            "uom_code_prev": None,
            "change_date": None,
            "description": "tablet",
        },
    ]

    current_rows = select_current_uom_rows(rows)

    assert [row["uom_code"] for row in current_rows] == [
        "10697611000001108",
        "428673006",
    ]


def test_parse_dmd_release_zip(tmp_path):
    zip_path = tmp_path / "nhsbsa_dmd.zip"
    write_release_zip(zip_path)

    rows = parse_dmd_release_zip(zip_path)

    assert rows["vmp"][0] == {
        "vmp_code": "318135008",
        "vtm_code": "34186711000001102",
        "vmp_name": "Co-amilofruse 2.5mg/20mg tablets",
        "df_ind_code": "1",
        "udfs": 1.0,
        "udfs_uom_code": "428673006",
        "unit_dose_uom_code": "428673006",
        "invalid": False,
    }
    assert rows["vmp"][1]["vtm_code"] is None
    assert rows["vmp"][1]["udfs"] is None
    assert rows["vmp"][1]["invalid"] is True
    assert rows["vpi"][0]["strnt_nmrtr_val"] == 2.5
    assert rows["vpi"][0]["basis_strnt_code"] == "0001"
    assert rows["vpi"][0]["bs_subid"] == "200"
    assert rows["vpi"][1]["strnt_nmrtr_val"] is None
    assert rows["ont"] == [{"vmp_code": "318135008", "form_code": "385268001"}]
    assert rows["dform"] == [{"vmp_code": "318135008", "form_code": "421026006"}]
    assert rows["amp"][0]["description"] == "Brand name 2.5mg tablets"
    assert "amp_name" not in rows["amp"][0]
    assert rows["amp"][0]["avail_restrict_code"] == "0001"
    assert rows["ing"] == [{"ing_code": "100", "ing_name": "Amiloride"}]
    assert rows["vtm"] == [
        {"vtm_code": "34186711000001102", "vtm_name": "Co-amilofruse", "invalid": False},
        {"vtm_code": "100", "vtm_name": "Old name", "invalid": True},
    ]
    assert rows["unit_of_measure"][1] == {
        "uom_code": "10697611000001108",
        "uom_code_prev": "282380000",
        "change_date": "2022-05-10",
        "description": "%v/v",
    }
    assert rows["unit_of_measure"][0]["uom_code_prev"] is None
    assert rows["form"][0]["description"] == "Tablet"
    assert rows["ont_form_route"][0]["description"] == "tablet.oral"
    assert rows["availability_restriction"][0]["description"] == "None"
    assert rows["df_indicator"][0]["description"] == "Discrete"
    assert all("Ignore" not in str(records) for records in rows.values())


def test_parse_dmd_release_zip_requires_one_vmp_file(tmp_path):
    zip_path = tmp_path / "nhsbsa_dmd.zip"
    write_release_zip(zip_path)
    with zipfile.ZipFile(zip_path, "a") as archive:
        archive.writestr("f_vmp2_020126.xml", VMP_XML)

    with pytest.raises(FileNotFoundError, match="f_vmp2_"):
        parse_dmd_release_zip(zip_path)
