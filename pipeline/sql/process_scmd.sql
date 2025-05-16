CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}`
PARTITION BY year_month
CLUSTER BY vmp_code
AS
WITH history_mappings AS (
  SELECT 
    previous_id,
    current_id,
    entity_type
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_HISTORY_TABLE_ID }}`
  WHERE entity_type IN ('VMP', 'UOM')
  AND previous_id != current_id
),
vmp_mappings AS (
  SELECT previous_id, current_id
  FROM history_mappings
  WHERE entity_type = 'VMP'
),
uom_mappings AS (
  SELECT previous_id, current_id
  FROM history_mappings
  WHERE entity_type = 'UOM'
),
normalized_vmps AS (
  SELECT 
    CAST(vmp_code AS STRING) as vmp_code,
    vmp_name
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}`
),
normalized_uoms AS (
  SELECT
    CAST(uom_code AS STRING) as uom_code,
    description as uom_name
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_UOM_TABLE_ID }}`
),
normalized_data AS (
  SELECT
    CAST(raw.year_month AS DATE) as year_month,
    CAST(raw.ods_code AS STRING) as ods_code,
    CAST(raw.vmp_snomed_code AS STRING) as vmp_snomed_code,
    CAST(COALESCE(vmp_map.current_id, raw.vmp_snomed_code) AS STRING) as vmp_code,
    CAST(COALESCE(dmd.vmp_name, raw.vmp_product_name) AS STRING) as vmp_name,
    CAST(COALESCE(uom_map.current_id, raw.unit_of_measure_identifier) AS STRING) as uom_id,
    CAST(COALESCE(uom.uom_name, raw.unit_of_measure_name) AS STRING) as uom_name,
    CAST(raw.total_quantity_in_vmp_unit AS FLOAT64) as quantity,
    CAST(raw.indicative_cost AS FLOAT64) as indicative_cost
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_RAW_TABLE_ID }}` raw
  LEFT JOIN vmp_mappings vmp_map
    ON CAST(raw.vmp_snomed_code AS STRING) = vmp_map.previous_id
  LEFT JOIN uom_mappings uom_map
    ON CAST(raw.unit_of_measure_identifier AS STRING) = uom_map.previous_id
  LEFT JOIN normalized_vmps dmd
    ON COALESCE(vmp_map.current_id, raw.vmp_snomed_code) = dmd.vmp_code
  LEFT JOIN normalized_uoms uom
    ON COALESCE(uom_map.current_id, raw.unit_of_measure_identifier) = uom.uom_code
),
unit_conversions AS (
  SELECT 
    CAST(uc.unit_id AS STRING) as unit_id,
    CAST(uc.basis_id AS STRING) as normalised_uom_id,
    CAST(uc.conversion_factor AS FLOAT64) as conversion_factor,
    CAST(uc.unit AS STRING) as uom_name,
    CAST(uc.basis AS STRING) as normalised_uom_name
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` uc
),
standardised_units AS (
  SELECT
    vmp_code,
    chosen_unit_id,
    chosen_unit_name,
    conversion_factor
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_UNIT_STANDARDISATION_TABLE_ID }}`
),
standardised_data AS (
  SELECT
    norm.*,
    CASE 
      WHEN su.vmp_code IS NOT NULL AND norm.uom_id != su.chosen_unit_id THEN
        su.chosen_unit_id
      ELSE
        norm.uom_id
    END as standardised_uom_id,
    CASE 
      WHEN su.vmp_code IS NOT NULL AND norm.uom_id != su.chosen_unit_id THEN
        norm.quantity * su.conversion_factor
      ELSE
        norm.quantity
    END as standardised_quantity
  FROM normalized_data norm
  LEFT JOIN standardised_units su
    ON norm.vmp_code = su.vmp_code
)
SELECT
  year_month,
  ods_code,
  vmp_code,
  vmp_name,
  uom_id,
  CAST(std.uom_name AS STRING) as uom_name,
  CAST(COALESCE(uc.normalised_uom_id, std.standardised_uom_id) AS STRING) as normalised_uom_id,
  CAST(COALESCE(uc.normalised_uom_name, '') AS STRING) as normalised_uom_name,
  quantity,
  CAST(
    CASE 
      WHEN uc.conversion_factor IS NOT NULL THEN 
        std.standardised_quantity * uc.conversion_factor
      ELSE 
        std.standardised_quantity
    END AS FLOAT64
  ) as normalised_quantity,
  indicative_cost
FROM standardised_data std
LEFT JOIN unit_conversions uc
  ON std.standardised_uom_id = uc.unit_id