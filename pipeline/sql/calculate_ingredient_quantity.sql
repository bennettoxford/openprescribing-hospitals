CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ INGREDIENT_QUANTITY_TABLE_ID }}`
PARTITION BY year_month
CLUSTER BY vmp_code
AS
WITH normalised_units AS (
  SELECT
    processed.year_month,
    processed.ods_code,
    org.ods_name,
    processed.vmp_code,
    processed.vmp_name,
    processed.normalised_quantity as original_quantity,
    processed.normalised_uom_name as original_unit,
    -- Convert SCMD quantity to basis units
    processed.normalised_quantity * COALESCE(qty_conv.conversion_factor, 1.0) as quantity_in_basis,
    COALESCE(qty_conv.basis, processed.normalised_uom_name) as quantity_basis,
    -- Ingredient info
    ing.ing_code as ingredient_code,
    ing.ing_name as ingredient_name,
    -- Original strength values
    ing.strnt_nmrtr_val as strength_numerator_value,
    ing.strnt_nmrtr_uom_name as strength_numerator_unit,
    ing.strnt_dnmtr_val as strength_denominator_value,
    ing.strnt_dnmtr_uom_name as strength_denominator_unit,
    -- Convert strength values to basis units
    ing.strnt_nmrtr_val * COALESCE(num_conv.conversion_factor, 1.0) as numerator_in_basis,
    COALESCE(num_conv.basis, ing.strnt_nmrtr_uom_name) as numerator_basis,
    ing.strnt_dnmtr_val * COALESCE(denom_conv.conversion_factor, 1.0) as denominator_in_basis,
    COALESCE(denom_conv.basis, ing.strnt_dnmtr_uom_name) as denominator_basis
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}` processed
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` dmd
    ON processed.vmp_code = dmd.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ ORGANISATION_TABLE_ID }}` org
    ON processed.ods_code = org.ods_code
  LEFT JOIN UNNEST(dmd.ingredients) as ing
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` qty_conv
    ON processed.normalised_uom_name = qty_conv.unit
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` num_conv
    ON ing.strnt_nmrtr_uom_name = num_conv.unit
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` denom_conv
    ON ing.strnt_dnmtr_uom_name = denom_conv.unit
),
calculated_quantities AS (
  SELECT
    *,
    CASE
      WHEN ingredient_code IS NULL THEN 
        NULL
      WHEN strength_numerator_value IS NULL THEN
        NULL
      WHEN strength_denominator_value IS NULL THEN
        -- Simple multiplication if no denominator
        quantity_in_basis * numerator_in_basis
      WHEN quantity_basis = denominator_basis THEN
        -- If bases match, can do direct calculation
        (quantity_in_basis / denominator_in_basis) * numerator_in_basis
      ELSE
        NULL
    END as ingredient_quantity,
    CASE
      WHEN ingredient_code IS NULL THEN 
        'Not calculated: No ingredient'
      WHEN strength_numerator_value IS NULL THEN
        'Not calculated: No strength info'
      WHEN strength_denominator_value IS NULL THEN
        'Direct multiplication (Qty * Num)'
      WHEN quantity_basis = denominator_basis THEN
        'Basis unit calculation (Qty/Denom * Num)'
      ELSE
        'Not calculated: Incompatible basis units'
    END as calculation_logic
  FROM normalised_units
)
SELECT 
  vmp_code,
  year_month,
  ods_code,
  ods_name,
  vmp_name,
  original_quantity as converted_quantity,
  original_unit as quantity_basis,
  original_unit as quantity_basis_name,
  ARRAY_AGG(
    STRUCT(
      ingredient_code,
      ingredient_name,
      ingredient_quantity,
      strength_numerator_unit as ingredient_unit,
      -- Convert final result to basis units if we have a result
      CASE 
        WHEN ingredient_quantity IS NOT NULL THEN ingredient_quantity 
        ELSE NULL 
      END as ingredient_quantity_basis,
      numerator_basis as ingredient_basis_unit,
      strength_numerator_value,
      strength_numerator_unit,
      strength_denominator_value,
      strength_denominator_unit,
      CASE
        WHEN quantity_basis = denominator_basis THEN 1.0
        ELSE NULL
      END as quantity_to_denominator_conversion_factor,
      denominator_basis as denominator_basis_unit,
      calculation_logic
    )
  ) as ingredients
FROM calculated_quantities
GROUP BY 
  vmp_code,
  year_month,
  ods_code,
  ods_name,
  vmp_name,
  original_quantity,
  original_unit