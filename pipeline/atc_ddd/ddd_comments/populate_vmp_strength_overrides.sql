CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_STRENGTH_OVERRIDES_TABLE_ID }}`
CLUSTER BY vmp_code
AS

SELECT vmp_code, strnt_nmrtr_val, strnt_dnmtr_val, strnt_nmrtr_uom, strnt_dnmtr_uom, comments
FROM (
  SELECT
    '41875311000001102' AS vmp_code,
    7.5 AS strnt_nmrtr_val,
    24.0 AS strnt_dnmtr_val,
    'microgram' AS strnt_nmrtr_uom,
    'hour' AS strnt_dnmtr_uom,
    'Using ingredient quantity delivered per 24 hours' AS comments
)
