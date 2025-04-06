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
    processed.normalised_uom_id AS unit_of_measure_id,
    processed.normalised_uom_name AS unit_of_measure_name,
    processed.normalised_quantity AS total_quanity_in_vmp_unit,
    org.ods_name,
    dmd.vtm,
    dmd.vtm_name,
    dmd.df_ind,
    dmd.udfs AS converted_udfs,
    dmd.udfs_uom AS udfs_basis,
    dmd.unit_dose_uom,
    dmd.dform_form,
    dmd.vmp_name AS dmd_vmp_name
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}` AS processed
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ ORGANISATION_TABLE_ID }}` AS org
    ON processed.ods_code = org.ods_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` AS dmd
    ON processed.vmp_code = dmd.vmp_code
),
processed_data AS (
  SELECT
    year_month,
    ods_code,
    vmp_code,
    vmp_name,
    unit_of_measure_name as uom_name,
    unit_of_measure_name as uom,
    total_quanity_in_vmp_unit as quantity,
    ods_name,
    vtm,
    vtm_name,
    df_ind,
    converted_udfs AS udfs,
    udfs_basis AS udfs_uom,
    unit_dose_uom,
    dform_form,
    dmd_vmp_name
  FROM combined_data
),
calculated_doses AS (
  SELECT 
    *,
    CASE 
      WHEN df_ind = 'Discrete' THEN
        CASE
          WHEN uom = udfs_uom THEN quantity / udfs
          WHEN uom = unit_dose_uom THEN quantity
          ELSE NULL
        END
      ELSE NULL
    END AS number_of_doses,
    CASE 
      WHEN df_ind = 'Discrete' AND (uom = udfs_uom OR uom = unit_dose_uom) THEN unit_dose_uom
      ELSE NULL
    END AS dose_unit,
    CASE 
      WHEN df_ind = 'Discrete' THEN
        CASE
          WHEN uom = udfs_uom THEN 'Quantity / udfs'
          WHEN uom = unit_dose_uom THEN 'Quantity'
          ELSE 'Not calculated: Discrete but SCMD quantity basis != udfs basis or unit dose form size'
        END
      ELSE 'Not calculated: not a discrete form'
    END AS logic
  FROM processed_data
)
SELECT 
  year_month,
  vmp_code,
  vmp_name,
  ods_code,
  ods_name,
  quantity AS scmd_quantity,
  uom AS scmd_quantity_basis,
  uom_name AS scmd_quantity_basis_name,
  number_of_doses AS dose_quantity,
  dose_unit,
  df_ind,
  logic
FROM calculated_doses
