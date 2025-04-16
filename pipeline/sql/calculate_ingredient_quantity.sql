CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ INGREDIENT_QUANTITY_TABLE_ID }}`
PARTITION BY year_month
CLUSTER BY vmp_code
AS
WITH normalized_data AS (
  SELECT
    processed.year_month,
    processed.ods_code,
    processed.vmp_code,
    processed.vmp_name,
    processed.normalised_uom_id as uom,
    processed.normalised_uom_name as uom_name,
    processed.normalised_quantity as quantity,
    processed.indicative_cost as cost,
    dmd.vtm,
    dmd.vtm_name,
    dmd.df_ind,
    dmd.udfs as converted_udfs,
    dmd.udfs_uom as udfs_basis,
    dmd.unit_dose_uom,
    dmd.dform_form,
    dmd.vmp_name as dmd_vmp_name,
    dmd.ingredients
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}` processed
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` dmd
    ON processed.vmp_code = dmd.vmp_code
),
unit_converted_data AS (
  SELECT
    n.*,
    org.ods_name,
    ing.*,
    conv_qty_to_base.conversion_factor AS qty_to_base_conversion,
    conv_qty_to_base.basis AS qty_basis_unit,
    conv_denom_to_base.conversion_factor AS denom_to_base_conversion,
    conv_denom_to_base.basis AS denom_basis_unit,
    conv_nmrtr_to_base.conversion_factor AS nmrtr_to_base_conversion,
    conv_nmrtr_to_base.basis AS nmrtr_basis_unit,
    conv_qty_to_denom.conversion_factor AS qty_to_denom_conversion
  FROM normalized_data n
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ ORGANISATION_TABLE_ID }}` AS org
    ON n.ods_code = org.ods_code
  LEFT JOIN UNNEST(ingredients) as ing
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` conv_qty_to_base
    ON n.uom_name = conv_qty_to_base.unit
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` conv_denom_to_base
    ON ing.strnt_dnmtr_uom_name = conv_denom_to_base.unit
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` conv_nmrtr_to_base
    ON ing.strnt_nmrtr_uom_name = conv_nmrtr_to_base.unit
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` conv_qty_to_denom
    ON n.uom_name = conv_qty_to_denom.unit AND ing.strnt_dnmtr_uom_name = conv_qty_to_denom.basis
),
processed_calcs AS (
  SELECT
    *,
    COALESCE(qty_to_base_conversion, 1.0) AS norm_qty_to_base_conversion,
    COALESCE(qty_basis_unit, uom_name) AS norm_qty_basis_unit,
    COALESCE(denom_to_base_conversion, 1.0) AS norm_denom_to_base_conversion,
    COALESCE(denom_basis_unit, strnt_dnmtr_uom_name) AS norm_denom_basis_unit,
    COALESCE(nmrtr_to_base_conversion, 1.0) AS norm_nmrtr_to_base_conversion,
    COALESCE(nmrtr_basis_unit, strnt_nmrtr_uom_name) AS norm_nmrtr_basis_unit,
    COALESCE(qty_to_denom_conversion, 1.0) AS norm_qty_to_denom_conversion,
    CASE
        WHEN COALESCE(qty_basis_unit, uom_name) = COALESCE(denom_basis_unit, strnt_dnmtr_uom_name) THEN
            COALESCE(qty_to_base_conversion, 1.0) / COALESCE(denom_to_base_conversion, 1.0)
        WHEN qty_to_denom_conversion IS NOT NULL THEN
            qty_to_denom_conversion
        ELSE 1.0
    END AS effective_conversion_factor,
    CASE
        WHEN ing_code IS NULL THEN NULL
        WHEN strnt_dnmtr_uom_name IS NULL AND strnt_nmrtr_uom_name IS NULL THEN NULL
        WHEN uom_name = strnt_dnmtr_uom_name THEN
            (quantity / strnt_dnmtr_val) * strnt_nmrtr_val
        WHEN COALESCE(qty_basis_unit, uom_name) = COALESCE(denom_basis_unit, strnt_dnmtr_uom_name) THEN
            ((quantity * COALESCE(qty_to_base_conversion, 1.0) / COALESCE(denom_to_base_conversion, 1.0)) / strnt_dnmtr_val) * strnt_nmrtr_val
        WHEN qty_to_denom_conversion IS NOT NULL THEN
            ((quantity * qty_to_denom_conversion) / strnt_dnmtr_val) * strnt_nmrtr_val
        WHEN df_ind = 'Discrete' THEN
            CASE
            WHEN uom_name = strnt_nmrtr_uom_name THEN quantity
            WHEN strnt_dnmtr_uom_name IS NULL THEN quantity * strnt_nmrtr_val
            ELSE NULL
            END
        ELSE NULL
    END AS ingredient_quantity,
    CASE
        WHEN ing_code IS NULL THEN 'Not calculated: No ingredient'
        WHEN strnt_dnmtr_uom_name IS NULL AND strnt_nmrtr_uom_name IS NULL THEN "Not calculated: No strength info"
        WHEN uom_name = strnt_dnmtr_uom_name THEN
            '(SCMD quantity / strength denominator value) * strength numerator value'
        WHEN COALESCE(qty_basis_unit, uom_name) = COALESCE(denom_basis_unit, strnt_dnmtr_uom_name) THEN
            '(SCMD quantity * ' || CAST(COALESCE(qty_to_base_conversion, 1.0) AS STRING) || 
            ' / denom conversion ' || CAST(COALESCE(denom_to_base_conversion, 1.0) AS STRING) || 
            ') / strength denominator value * strength numerator value'
        WHEN qty_to_denom_conversion IS NOT NULL THEN
            '(SCMD quantity * conversion factor: ' || CAST(qty_to_denom_conversion AS STRING) || 
            ' / strength denominator value) * strength numerator value'
        WHEN df_ind = 'Discrete' THEN
            CASE
            WHEN uom_name = strnt_nmrtr_uom_name THEN 'SCMD quantity (direct match with numerator units)'
            WHEN strnt_dnmtr_uom_name IS NULL THEN "SCMD quantity * strength numerator value"
            ELSE "Not calculated: Discrete, no compatible unit conversion found"
            END
        ELSE "Not calculated: Not discrete form and no compatible unit conversion found"
    END AS calculation_logic
  FROM unit_converted_data
)
SELECT 
  vmp_code,
  year_month,
  ods_code,
  ods_name,
  vmp_name,
  quantity as converted_quantity,
  uom as quantity_basis,
  uom_name as quantity_basis_name,
  ARRAY_AGG(
    STRUCT(
      ing_code as ingredient_code,
      ing_name as ingredient_name,
      ingredient_quantity,
      strnt_nmrtr_uom_name as ingredient_unit,
      CASE 
        WHEN ingredient_quantity IS NOT NULL AND norm_nmrtr_to_base_conversion IS NOT NULL 
        THEN ingredient_quantity * norm_nmrtr_to_base_conversion 
        ELSE NULL 
      END as ingredient_quantity_basis,
      norm_nmrtr_basis_unit as ingredient_basis_unit,
      strnt_nmrtr_val as strength_numerator_value,
      strnt_nmrtr_uom_name as strength_numerator_unit,
      strnt_dnmtr_val as strength_denominator_value,
      strnt_dnmtr_uom_name as strength_denominator_unit,
      effective_conversion_factor as quantity_to_denominator_conversion_factor,
      norm_denom_basis_unit as denominator_basis_unit,
      calculation_logic
    )
  ) as ingredients
FROM processed_calcs
GROUP BY 
  vmp_code,
  year_month,
  ods_code,
  ods_name,
  vmp_name,
  quantity,
  uom,
  uom_name