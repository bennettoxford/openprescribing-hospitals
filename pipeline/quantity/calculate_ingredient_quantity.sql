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
    ing.can_calculate_ingredient,
    ing.ingredient_calculation_logic,
    ing.strength_numerator_value,
    ing.strength_numerator_unit,
    ing.numerator_basis_value,
    ing.numerator_basis_unit,
    ing.strength_denominator_value,
    ing.strength_denominator_unit,
    ing.denominator_basis_value,
    ing.denominator_basis_unit
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}` processed
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ ORGANISATION_TABLE_ID }}` org
    ON processed.ods_code = org.ods_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ INGREDIENT_CALCULATION_LOGIC_TABLE_ID }}` calc_logic
    ON processed.vmp_code = calc_logic.vmp_code
  LEFT JOIN UNNEST(calc_logic.ingredients) as ing
),
calculated_quantities AS (
  SELECT
    *,
    CASE
      WHEN ingredient_calculation_logic = 'SCMD quantity x ingredient strength numerator'
      THEN normalised_quantity * strength_numerator_value
      WHEN ingredient_calculation_logic = 'SCMD quantity x (ingredient strength numerator / ingredient strength denominator)'
      THEN (normalised_quantity / denominator_basis_value) * strength_numerator_value
      ELSE NULL
    END as ingredient_quantity,
    
    CASE
      WHEN ingredient_calculation_logic = 'SCMD quantity x ingredient strength numerator'
      THEN normalised_quantity * numerator_basis_value
      WHEN ingredient_calculation_logic = 'SCMD quantity x (ingredient strength numerator / ingredient strength denominator)'
      THEN (normalised_quantity / denominator_basis_value) * numerator_basis_value
      ELSE NULL
    END as ingredient_quantity_basis,
    
    ingredient_calculation_logic as calculation_logic
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
      ingredient_quantity_basis,
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