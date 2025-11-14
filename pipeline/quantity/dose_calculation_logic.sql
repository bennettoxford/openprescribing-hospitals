CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DOSE_CALCULATION_LOGIC_TABLE_ID }}`
CLUSTER BY vmp_code
AS
WITH vmp_calculation_summary AS (
  SELECT
    vmp.vmp_code,
    vmp.vmp_name,
    vmp.df_ind,
    vmp.udfs_basis_quantity,
    vmp.udfs_basis_uom,
    vmp.unit_dose_uom,
    vmp.unit_dose_basis_uom,
    CASE 
      WHEN vmp.df_ind = 'Discrete' THEN
        CASE
          WHEN vmp.scmd_basis_uom_name IS NOT NULL AND
               vmp.udfs_basis_uom IS NOT NULL AND
               vmp.scmd_basis_uom_name = vmp.udfs_basis_uom
          THEN 'SCMD quantity / UDFS'
          WHEN vmp.scmd_basis_uom_name IS NOT NULL AND
               vmp.unit_dose_uom IS NOT NULL AND
               vmp.scmd_basis_uom_name = vmp.unit_dose_uom
          THEN 'Direct quantity'
          ELSE 'Not calculated: Unit mismatch'
        END
      ELSE 'Not calculated: not a discrete form'
    END AS dose_calculation_logic
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_TABLE_ID }}` AS vmp
),
vmp_with_calculation_flag AS (
  SELECT
    *,
    NOT STARTS_WITH(dose_calculation_logic, 'Not calculated') AS can_calculate_dose
  FROM vmp_calculation_summary
)
SELECT 
  vmp_code,
  vmp_name,
  df_ind,
  can_calculate_dose,
  dose_calculation_logic,
  CASE 
    WHEN can_calculate_dose THEN udfs_basis_quantity 
    ELSE NULL 
  END AS udfs_basis_quantity,
  CASE 
    WHEN can_calculate_dose THEN udfs_basis_uom 
    ELSE NULL 
  END AS udfs_basis_uom,
  CASE 
    WHEN can_calculate_dose THEN unit_dose_uom 
    ELSE NULL 
  END AS unit_dose_uom,
  CASE 
    WHEN can_calculate_dose THEN unit_dose_basis_uom 
    ELSE NULL 
  END AS unit_dose_basis_uom
FROM vmp_with_calculation_flag
