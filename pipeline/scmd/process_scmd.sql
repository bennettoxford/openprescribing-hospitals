CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}`
PARTITION BY year_month
CLUSTER BY vmp_code
AS
WITH history_mappings AS (
  SELECT 
    previous_id,
    current_id,
    entity_type,
    start_date,
    end_date
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_HISTORY_TABLE_ID }}`
  WHERE entity_type IN ('VMP', 'UOM')
  AND previous_id != current_id
),
-- Some codes have multiple entries mapping to the same code for different time periods. We deduplicate by selecting the most recent
deduplicated_history AS (
  SELECT 
    previous_id,
    current_id,
    entity_type,
    ROW_NUMBER() OVER (
      PARTITION BY previous_id, entity_type 
      ORDER BY 
        CASE WHEN end_date IS NULL THEN 1 ELSE 0 END DESC, -- Prefer active mappings (no end date)
        end_date DESC,  -- Then prefer latest end date
        start_date DESC -- Finally prefer latest start date
    ) as rn
  FROM history_mappings
),
vmp_mappings AS (
  SELECT previous_id, current_id
  FROM deduplicated_history
  WHERE entity_type = 'VMP' AND rn = 1
),
uom_mappings AS (
  SELECT previous_id, current_id
  FROM deduplicated_history
  WHERE entity_type = 'UOM' AND rn = 1
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
-- Get data status to determine which months are finalised vs provisional
data_status AS (
  SELECT 
    year_month,
    file_type
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_DATA_STATUS_TABLE_ID }}`
),
finalised_months AS (
  SELECT year_month
  FROM data_status
  WHERE file_type = 'final'
),
-- Combine finalised and provisional data, prioritizing finalised
combined_raw_data AS (
  -- Get all finalised data
  SELECT
    CAST(raw.year_month AS DATE) as year_month,
    CAST(raw.ods_code AS STRING) as ods_code,
    CAST(raw.vmp_snomed_code AS STRING) as vmp_snomed_code,
    CAST(raw.vmp_product_name AS STRING) as vmp_product_name,
    CAST(raw.unit_of_measure_identifier AS STRING) as unit_of_measure_identifier,
    CAST(raw.unit_of_measure_name AS STRING) as unit_of_measure_name,
    CAST(raw.total_quantity_in_vmp_unit AS FLOAT64) as total_quantity_in_vmp_unit,
    CAST(raw.indicative_cost AS FLOAT64) as indicative_cost
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_RAW_FINALISED_TABLE_ID }}` raw
  INNER JOIN finalised_months fm
    ON CAST(raw.year_month AS DATE) = fm.year_month
  
  UNION ALL
  
  -- Get provisional data only for months that don't have finalised data
  SELECT
    CAST(raw.year_month AS DATE) as year_month,
    CAST(raw.ods_code AS STRING) as ods_code,
    CAST(raw.vmp_snomed_code AS STRING) as vmp_snomed_code,
    CAST(raw.vmp_product_name AS STRING) as vmp_product_name,
    CAST(raw.unit_of_measure_identifier AS STRING) as unit_of_measure_identifier,
    CAST(raw.unit_of_measure_name AS STRING) as unit_of_measure_name,
    CAST(raw.total_quantity_in_vmp_unit AS FLOAT64) as total_quantity_in_vmp_unit,
    CAST(raw.indicative_cost AS FLOAT64) as indicative_cost
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_RAW_PROVISIONAL_TABLE_ID }}` raw
  LEFT JOIN finalised_months fm
    ON CAST(raw.year_month AS DATE) = fm.year_month
  WHERE fm.year_month IS NULL
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
  FROM combined_raw_data raw
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