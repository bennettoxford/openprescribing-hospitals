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
    processed.normalised_quantity,
    processed.normalised_uom_name,
    ing.ingredient_code,
    ing.ingredient_name,
    ing.strnt_nmrtr_val as strength_numerator_value,
    ing.strnt_nmrtr_uom_name as strength_numerator_unit,
    ing.strnt_nmrtr_basis_val as numerator_basis_value,
    ing.strnt_nmrtr_basis_uom as numerator_basis_unit,
    ing.strnt_dnmtr_val as strength_denominator_value,
    ing.strnt_dnmtr_uom_name as strength_denominator_unit,
    ing.strnt_dnmtr_basis_val as denominator_basis_value,
    ing.strnt_dnmtr_basis_uom as denominator_basis_unit
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}` processed
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_TABLE_ID }}` vmp
    ON processed.vmp_code = vmp.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ ORGANISATION_TABLE_ID }}` org
    ON processed.ods_code = org.ods_code
  LEFT JOIN UNNEST(vmp.ingredients) as ing
),
calculated_quantities AS (
  SELECT
    *,
    CASE
      -- 1. Check if it has an ingredient
      WHEN ingredient_code IS NULL THEN 
        NULL
      -- 2. Check for strength info
      WHEN strength_numerator_value IS NULL THEN
        NULL
      -- 3. No denominator case: multiply SCMD quantity by strength numerator
      WHEN strength_denominator_value IS NULL THEN
        normalised_quantity * strength_numerator_value
      -- 4. With denominator case: where bases match, divide quantity by denominator and multiply by numerator
      WHEN normalised_uom_name = denominator_basis_unit THEN
        (normalised_quantity / denominator_basis_value) * numerator_basis_value
      ELSE
        NULL
    END as ingredient_quantity,
    CASE
      WHEN ingredient_code IS NULL THEN 
        'Not calculated: No ingredients'
      WHEN strength_numerator_value IS NULL THEN
        'Not calculated: No ingredient strength'
      WHEN strength_denominator_value IS NULL THEN
        'SCMD quantity x ingredient strength numerator'
      WHEN normalised_uom_name = denominator_basis_unit THEN
        'SCMD quantity x (ingredient strength numerator / ingredient strength denominator)'
      ELSE
        'Not calculated: Ingredient strength denominator units do not match SCMD quantity units'
    END as calculation_logic
  FROM normalised_units
)
SELECT 
  vmp_code,
  year_month,
  ods_code,
  ods_name,
  vmp_name,
  normalised_quantity as converted_quantity,
  normalised_uom_name as quantity_basis,
  normalised_uom_name as quantity_basis_name,
  ARRAY_AGG(
    STRUCT(
      ingredient_code,
      ingredient_name,
      ingredient_quantity,
      strength_numerator_unit as ingredient_unit,
      ingredient_quantity as ingredient_quantity_basis,
      numerator_basis_unit as ingredient_basis_unit,
      strength_numerator_value,
      strength_numerator_unit,
      strength_denominator_value,
      strength_denominator_unit,
      1.0 as quantity_to_denominator_conversion_factor,
      denominator_basis_unit,
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
  normalised_quantity,
  normalised_uom_name