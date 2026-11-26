CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_STRENGTH_OVERRIDES_TABLE_ID }}`
CLUSTER BY vmp_code
AS

WITH overrides AS (
  SELECT
    '41875311000001102' AS vmp_code,
    '126172005' AS ingredient_code,
    7.5 AS strnt_nmrtr_val,
    24.0 AS strnt_dnmtr_val,
    'microgram' AS strnt_nmrtr_uom,
    'hour' AS strnt_dnmtr_uom,
    'Using ingredient quantity delivered per 24 hours' AS comments
)
SELECT
  o.vmp_code,
  o.ingredient_code,
  o.strnt_nmrtr_val,
  o.strnt_dnmtr_val,
  o.strnt_nmrtr_uom,
  o.strnt_dnmtr_uom,
  ing.strnt_nmrtr_val AS original_strnt_nmrtr_val,
  ing.strnt_dnmtr_val AS original_strnt_dnmtr_val,
  ing.strnt_nmrtr_uom_name AS original_strnt_nmrtr_uom,
  ing.strnt_dnmtr_uom_name AS original_strnt_dnmtr_uom,
  o.comments
FROM overrides o
LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_TABLE_ID }}` vmp
  ON o.vmp_code = vmp.vmp_code
LEFT JOIN UNNEST(vmp.ingredients) AS ing
  ON ing.ingredient_code = o.ingredient_code
