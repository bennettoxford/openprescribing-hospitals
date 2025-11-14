CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DOSE_TABLE_ID }}`
PARTITION BY year_month
CLUSTER BY vmp_code
AS
WITH combined_data AS (
  SELECT
    processed.year_month,
    processed.ods_code,
    processed.vmp_code,
    processed.vmp_name,
    processed.uom_id,
    processed.uom_name,
    processed.normalised_uom_id,
    processed.normalised_uom_name,
    processed.quantity,
    processed.normalised_quantity,
    org.ods_name,
    vmp.df_ind,
    vmp.udfs,
    vmp.udfs_uom,
    calc_logic.can_calculate_dose,
    calc_logic.dose_calculation_logic,
    calc_logic.udfs_basis_quantity,
    calc_logic.udfs_basis_uom,
    calc_logic.unit_dose_uom,
    calc_logic.unit_dose_basis_uom
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}` AS processed
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ ORGANISATION_TABLE_ID }}` AS org
    ON processed.ods_code = org.ods_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_TABLE_ID }}` AS vmp
    ON processed.vmp_code = vmp.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DOSE_CALCULATION_LOGIC_TABLE_ID }}` AS calc_logic
    ON processed.vmp_code = calc_logic.vmp_code
),
calculated_doses AS (
  SELECT 
    *,
    CASE 
      WHEN dose_calculation_logic = 'SCMD quantity / UDFS'
      THEN normalised_quantity / udfs_basis_quantity
      WHEN dose_calculation_logic = 'Direct quantity'
      THEN normalised_quantity
      ELSE NULL
    END AS dose_quantity,
    CASE 
      WHEN can_calculate_dose
      THEN unit_dose_uom
      ELSE NULL
    END AS dose_unit,
    dose_calculation_logic AS calculation_logic
  FROM combined_data
)
SELECT 
  year_month,
  vmp_code,
  vmp_name,
  ods_code,
  ods_name,
  quantity AS scmd_quantity,
  uom_name AS scmd_quantity_unit_name,
  normalised_uom_id AS scmd_basis_unit,
  normalised_uom_name AS scmd_basis_unit_name,
  normalised_quantity AS scmd_quantity_in_basis_units,
  dose_quantity,
  dose_unit,
  calculation_logic
FROM calculated_doses
