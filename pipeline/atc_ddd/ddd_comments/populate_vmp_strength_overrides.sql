CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_STRENGTH_OVERRIDES_TABLE_ID }}`
CLUSTER BY vmp_code
AS

WITH overrides AS (
  SELECT
    '41875311000001102' AS vmp_code,
    '126172005' AS ingredient_code,
    7.5 AS strnt_nmrtr_val,
    24.0 AS strnt_dnmtr_val,
    '258685003' AS strnt_nmrtr_uom,
    '258702006' AS strnt_dnmtr_uom,
    'Using ingredient quantity delivered per 24 hours' AS comments
)
SELECT
  o.vmp_code,
  o.ingredient_code,
  o.strnt_nmrtr_val,
  o.strnt_dnmtr_val,
  o.strnt_nmrtr_uom,
  nmrtr_uom.unit AS strnt_nmrtr_uom_name,
  o.strnt_dnmtr_uom,
  dnmtr_uom.unit AS strnt_dnmtr_uom_name,
  vpi.strnt_nmrtr_val AS original_strnt_nmrtr_val,
  vpi.strnt_dnmtr_val AS original_strnt_dnmtr_val,
  vpi.strnt_nmrtr_uom_code AS original_strnt_nmrtr_uom,
  original_nmrtr_uom.unit AS original_strnt_nmrtr_uom_name,
  vpi.strnt_dnmtr_uom_code AS original_strnt_dnmtr_uom,
  original_dnmtr_uom.unit AS original_strnt_dnmtr_uom_name,
  o.comments
FROM overrides o
LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_VPI_TABLE_ID }}` vpi
  ON o.vmp_code = vpi.vmp_code
  AND o.ingredient_code = vpi.ing_code
LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` nmrtr_uom
  ON o.strnt_nmrtr_uom = nmrtr_uom.unit_id
LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` dnmtr_uom
  ON o.strnt_dnmtr_uom = dnmtr_uom.unit_id
LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` original_nmrtr_uom
  ON vpi.strnt_nmrtr_uom_code = original_nmrtr_uom.unit_id
LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` original_dnmtr_uom
  ON vpi.strnt_dnmtr_uom_code = original_dnmtr_uom.unit_id
