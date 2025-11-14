CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ INGREDIENT_CALCULATION_LOGIC_TABLE_ID }}`
CLUSTER BY vmp_code
AS
WITH vmp_ingredients AS (
  SELECT
    vmp.vmp_code,
    vmp.vmp_name,
    vmp.scmd_basis_uom_name,
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
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_TABLE_ID }}` vmp
  LEFT JOIN UNNEST(vmp.ingredients) as ing
),
ingredient_logic AS (
  SELECT
    vmp_code,
    vmp_name,
    ingredient_code,
    ingredient_name,
    strength_numerator_value,
    strength_numerator_unit,
    numerator_basis_value,
    numerator_basis_unit,
    strength_denominator_value,
    strength_denominator_unit,
    denominator_basis_value,
    denominator_basis_unit,
    CASE
      WHEN ingredient_code IS NULL THEN 
        'Not calculated: No ingredients'
      WHEN strength_numerator_value IS NULL THEN
        'Not calculated: No ingredient strength'
      WHEN strength_denominator_value IS NULL THEN
        'SCMD quantity x ingredient strength numerator'
      WHEN scmd_basis_uom_name = denominator_basis_unit THEN
        'SCMD quantity x (ingredient strength numerator / ingredient strength denominator)'
      ELSE
        'Not calculated: Ingredient strength denominator units do not match SCMD quantity units'
    END as ingredient_calculation_logic
  FROM vmp_ingredients
),
vmp_with_calculation_flag AS (
  SELECT
    *,
    NOT STARTS_WITH(ingredient_calculation_logic, 'Not calculated') AS can_calculate_ingredient
  FROM ingredient_logic
)
SELECT
  vmp_code,
  vmp_name,
  ARRAY_AGG(
    STRUCT(
      ingredient_code,
      ingredient_name,
      can_calculate_ingredient,
      ingredient_calculation_logic,
      CASE WHEN can_calculate_ingredient THEN strength_numerator_value ELSE NULL END as strength_numerator_value,
      CASE WHEN can_calculate_ingredient THEN strength_numerator_unit ELSE NULL END as strength_numerator_unit,
      CASE WHEN can_calculate_ingredient THEN numerator_basis_value ELSE NULL END as numerator_basis_value,
      CASE WHEN can_calculate_ingredient THEN numerator_basis_unit ELSE NULL END as numerator_basis_unit,
      CASE WHEN can_calculate_ingredient THEN strength_denominator_value ELSE NULL END as strength_denominator_value,
      CASE WHEN can_calculate_ingredient THEN strength_denominator_unit ELSE NULL END as strength_denominator_unit,
      CASE WHEN can_calculate_ingredient THEN denominator_basis_value ELSE NULL END as denominator_basis_value,
      CASE WHEN can_calculate_ingredient THEN denominator_basis_unit ELSE NULL END as denominator_basis_unit
    )
  ) as ingredients
FROM vmp_with_calculation_flag
GROUP BY vmp_code, vmp_name

