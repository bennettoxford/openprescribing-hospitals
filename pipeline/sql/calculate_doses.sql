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
    dmd.vtm,
    dmd.vtm_name,
    dmd.df_ind,
    dmd.udfs,
    dmd.udfs_uom,
    dmd.unit_dose_uom,
    dmd.dform_form,
    dmd.vmp_name AS dmd_vmp_name
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}` AS processed
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ ORGANISATION_TABLE_ID }}` AS org
    ON processed.ods_code = org.ods_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` AS dmd
    ON processed.vmp_code = dmd.vmp_code
),
unit_converted_data AS (
  SELECT
    cd.*,
    conv1.conversion_factor AS norm_to_udfs_conversion,
    conv2.conversion_factor AS udfs_to_base_conversion,
    conv2.basis AS udfs_basis_unit,
    conv3.conversion_factor AS unit_dose_to_base_conversion,
    conv3.basis AS unit_dose_basis_unit
  FROM combined_data cd
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` conv1
    ON cd.normalised_uom_name = conv1.unit AND cd.udfs_uom = conv1.basis
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` conv2
    ON cd.udfs_uom = conv2.unit
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` conv3
    ON cd.unit_dose_uom = conv3.unit
),
processed_data AS (
  SELECT
    year_month,
    ods_code,
    vmp_code,
    vmp_name,
    uom_id,
    uom_name,
    normalised_uom_id,
    normalised_uom_name,
    quantity,
    normalised_quantity,
    ods_name,
    vtm,
    vtm_name,
    df_ind,
    udfs,
    udfs_uom,
    udfs * COALESCE(udfs_to_base_conversion, 1.0) AS udfs_basis_quantity,
    COALESCE(udfs_basis_unit, udfs_uom) AS udfs_basis_uom,
    unit_dose_uom,
    COALESCE(unit_dose_basis_unit, unit_dose_uom) AS unit_dose_basis_uom,
    dform_form,
    dmd_vmp_name,
    COALESCE(norm_to_udfs_conversion, 1.0) AS norm_to_udfs_conversion,
    COALESCE(udfs_to_base_conversion, 1.0) AS udfs_to_base_conversion,
    COALESCE(unit_dose_to_base_conversion, 1.0) AS unit_dose_to_base_conversion
  FROM unit_converted_data
),
calculated_doses AS (
  SELECT 
    *,
    CASE 
      WHEN df_ind = 'Discrete' THEN
        CASE
          WHEN normalised_uom_name = udfs_basis_uom THEN 
            normalised_quantity / udfs_basis_quantity
          WHEN normalised_uom_name = unit_dose_uom THEN 
            normalised_quantity

          WHEN normalised_uom_name != udfs_uom AND norm_to_udfs_conversion != 1.0 THEN
            (normalised_quantity * norm_to_udfs_conversion) / udfs
          ELSE NULL
        END
      ELSE NULL
    END AS dose_quantity,
    CASE 
      WHEN df_ind = 'Discrete' AND 
           (normalised_uom_name = udfs_basis_uom OR 
            normalised_uom_name = unit_dose_uom OR 
            (normalised_uom_name != udfs_uom AND norm_to_udfs_conversion != 1.0))
      THEN unit_dose_uom
      ELSE NULL
    END AS dose_unit,
    CASE 
      WHEN df_ind = 'Discrete' THEN
        CASE
          WHEN normalised_uom_name = udfs_basis_uom THEN 
            'Quantity in basis units / udfs in basis units'
          WHEN normalised_uom_name = unit_dose_uom THEN 
            'Direct quantity'
          WHEN normalised_uom_name != udfs_uom AND norm_to_udfs_conversion != 1.0 THEN
            'Converted quantity (factor: ' || CAST(norm_to_udfs_conversion AS STRING) || ') / udfs'
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
  uom_name AS scmd_quantity_unit_name,
  normalised_uom_id AS scmd_basis_unit,
  normalised_uom_name AS scmd_basis_unit_name,
  normalised_quantity AS scmd_quantity_in_basis_units,
  udfs,
  udfs_uom,
  udfs_basis_quantity,
  udfs_basis_uom,
  unit_dose_uom,
  unit_dose_basis_uom,
  dose_quantity,
  dose_unit,
  df_ind,
  logic
FROM calculated_doses
