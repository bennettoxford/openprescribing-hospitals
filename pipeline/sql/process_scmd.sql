CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}`
PARTITION BY year_month
CLUSTER BY vmp_code
AS
WITH normalized_vmps AS (
  SELECT 
    CAST(vmp_code AS STRING) as vmp_code,
    CAST(vmp_code_prev AS STRING) as vmp_code_prev,
    vmp_name
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}`
),
normalized_data AS (
  SELECT
    CAST(raw.year_month AS DATE) as year_month,
    CAST(raw.ods_code AS STRING) as ods_code,
    CAST(raw.vmp_snomed_code AS STRING) as vmp_snomed_code,
    CAST(COALESCE(dmd.vmp_code, raw.vmp_snomed_code) AS STRING) as vmp_code,
    CAST(COALESCE(dmd.vmp_name, raw.vmp_product_name) AS STRING) as vmp_name,
    CAST(raw.unit_of_measure_identifier AS STRING) as uom_id,
    CAST(raw.total_quantity_in_vmp_unit AS FLOAT64) as quantity,
    CAST(raw.indicative_cost AS FLOAT64) as indicative_cost
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_RAW_TABLE_ID }}` raw
  LEFT JOIN normalized_vmps dmd
    ON CAST(raw.vmp_snomed_code AS STRING) = dmd.vmp_code_prev
),
unit_conversions AS (
  SELECT 
    CAST(uc.unit_id AS STRING) as unit_id,
    CAST(uc.basis_id AS STRING) as normalised_uom_id,
    CAST(uc.conversion_factor AS FLOAT64) as conversion_factor,
    CAST(uc.unit AS STRING) as uom_name,
    CAST(uc.basis AS STRING) as normalised_uom_name
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` uc
)
SELECT
  year_month,
  ods_code,
  vmp_code,
  vmp_name,
  uom_id,
  CAST(COALESCE(uc.normalised_uom_id, norm.uom_id) AS STRING) as normalised_uom_id,
  quantity,
  CAST(
    CASE 
      WHEN uc.conversion_factor IS NOT NULL THEN norm.quantity * uc.conversion_factor
      ELSE norm.quantity
    END AS FLOAT64
  ) as normalised_quantity,
  indicative_cost,
  CAST(COALESCE(uc.uom_name, '') AS STRING) as uom_name,
  CAST(COALESCE(uc.normalised_uom_name, '') AS STRING) as normalised_uom_name
FROM normalized_data norm
LEFT JOIN unit_conversions uc
  ON norm.uom_id = uc.unit_id