CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_CALCULATION_LOGIC_TABLE_ID }}`
CLUSTER BY (vmp_code)
AS

WITH current_results AS (
  SELECT *
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_CALCULATION_LOGIC_TABLE_ID }}`
),

-- Extract ingredient details for single-ingredient VMPs
vmp_ingredient_details AS (
  SELECT
    vmp_code,
    ing.strnt_nmrtr_basis_uom,
    ing.strnt_dnmtr_basis_uom,
    ing.strnt_dnmtr_val IS NOT NULL AS has_denominator
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_TABLE_ID }}`
  CROSS JOIN UNNEST(ingredients) AS ing
  WHERE ARRAY_LENGTH(ingredients) = 1
),

-- Find VMPs that couldn't be calculated and exist in DDD_ROUTE_COMMENTS table
vmp_ddd_overrides AS (
  SELECT
    cr.vmp_code,
    cr.vmp_name,
    vmp_ddd.ddd AS override_ddd_value,
    vmp_ddd.ddd_uom AS override_ddd_unit,
    vmp_ddd.ddd_comment AS override_ddd_comment,
    unit_ddd.basis AS override_ddd_basis_unit,
    -- Check if SCMD units are compatible
    cr.scmd_basis_uom_name = unit_ddd.basis AS override_has_compatible_scmd_units,
    -- Check if ingredient quantity calculation is possible
    CASE
      WHEN cr.ingredients_info IS NULL OR ARRAY_LENGTH(cr.ingredients_info) != 1 THEN FALSE
      WHEN vid.has_denominator THEN
        -- If there's a denominator: check ing denominator basis = SCMD basis AND ing numerator basis = DDD basis
        vid.strnt_dnmtr_basis_uom = cr.scmd_basis_uom_name AND vid.strnt_nmrtr_basis_uom = unit_ddd.basis
      ELSE
        -- If there's no denominator: check ing numerator basis = DDD basis
        vid.strnt_nmrtr_basis_uom = unit_ddd.basis
    END AS override_has_compatible_ingredient_units,
    -- Check for missing ingredient units
    EXISTS (
      SELECT 1
      FROM UNNEST(cr.ingredients_info)
      WHERE ingredient_unit IS NULL
    ) AS override_has_missing_ingredient_units,
    -- Check if has ingredients at all
    (cr.ingredients_info IS NOT NULL AND ARRAY_LENGTH(cr.ingredients_info) > 0) AS override_has_ingredients,
    -- Check if exactly one ingredient
    (cr.ingredients_info IS NOT NULL AND ARRAY_LENGTH(cr.ingredients_info) = 1) AS override_has_single_ingredient
  FROM current_results cr
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_ROUTE_COMMENTS_TABLE_ID }}` vmp_ddd
    ON cr.vmp_code = vmp_ddd.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` unit_ddd
    ON vmp_ddd.ddd_uom = unit_ddd.unit
  LEFT JOIN vmp_ingredient_details vid ON cr.vmp_code = vid.vmp_code
  WHERE cr.can_calculate_ddd = FALSE
    AND vmp_ddd.vmp_code IS NOT NULL
)

-- Apply overrides to the final results
SELECT
  cr.vmp_code,
  cr.vmp_name,
  COALESCE(
    CASE
      WHEN vdo.vmp_code IS NOT NULL AND vdo.override_has_compatible_scmd_units THEN TRUE
      WHEN vdo.vmp_code IS NOT NULL AND NOT vdo.override_has_compatible_scmd_units AND vdo.override_has_single_ingredient AND vdo.override_has_compatible_ingredient_units THEN TRUE
      ELSE cr.can_calculate_ddd
    END,
    FALSE
  ) AS can_calculate_ddd,
  COALESCE(
    CASE
      WHEN vdo.vmp_code IS NOT NULL AND vdo.override_has_compatible_scmd_units THEN 'SCMD quantity / DDD'
      WHEN vdo.vmp_code IS NOT NULL AND NOT vdo.override_has_compatible_scmd_units AND vdo.override_has_single_ingredient AND vdo.override_has_compatible_ingredient_units THEN 'Ingredient quantity / DDD'
      WHEN vdo.vmp_code IS NOT NULL AND NOT vdo.override_has_compatible_scmd_units AND vdo.override_has_missing_ingredient_units THEN 'Not calculated: missing ingredient unit information, cannot calculate'
      WHEN vdo.vmp_code IS NOT NULL AND NOT vdo.override_has_compatible_scmd_units AND NOT vdo.override_has_ingredients THEN 'Not calculated: DDD unit incompatible with SCMD unit. No ingredients found'
      WHEN vdo.vmp_code IS NOT NULL AND NOT vdo.override_has_compatible_scmd_units AND NOT vdo.override_has_single_ingredient THEN 'Not calculated: DDD unit incompatible with SCMD unit. Multiple ingredients found, fallback not possible'
      WHEN vdo.vmp_code IS NOT NULL AND NOT vdo.override_has_compatible_scmd_units AND NOT vdo.override_has_compatible_ingredient_units THEN 'Not calculated: ingredient basis units incompatible with DDD and SCMD basis units'
      ELSE cr.ddd_calculation_logic
    END,
    'Unknown/unhandled case'
  ) AS ddd_calculation_logic,
  CASE
    WHEN vdo.vmp_code IS NOT NULL AND (vdo.override_has_compatible_scmd_units OR (NOT vdo.override_has_compatible_scmd_units AND vdo.override_has_single_ingredient AND vdo.override_has_compatible_ingredient_units)) THEN vdo.override_ddd_value
    WHEN cr.can_calculate_ddd THEN cr.selected_ddd_value
    ELSE NULL
  END AS selected_ddd_value,
  CASE
    WHEN vdo.vmp_code IS NOT NULL AND (vdo.override_has_compatible_scmd_units OR (NOT vdo.override_has_compatible_scmd_units AND vdo.override_has_single_ingredient AND vdo.override_has_compatible_ingredient_units)) THEN vdo.override_ddd_unit
    WHEN cr.can_calculate_ddd THEN cr.selected_ddd_unit
    ELSE NULL
  END AS selected_ddd_unit,
  CASE
    WHEN vdo.vmp_code IS NOT NULL AND (vdo.override_has_compatible_scmd_units OR (NOT vdo.override_has_compatible_scmd_units AND vdo.override_has_single_ingredient AND vdo.override_has_compatible_ingredient_units)) THEN vdo.override_ddd_basis_unit
    WHEN cr.can_calculate_ddd THEN cr.selected_ddd_basis_unit
    ELSE NULL
  END AS selected_ddd_basis_unit,
  CASE
    WHEN vdo.vmp_code IS NOT NULL THEN NULL
    WHEN cr.can_calculate_ddd THEN cr.selected_ddd_route_code
    ELSE NULL
  END AS selected_ddd_route_code,
  cr.scmd_uom_id,
  cr.scmd_uom_name,
  cr.scmd_basis_uom_id,
  cr.scmd_basis_uom_name,
  cr.atcs,
  cr.routes,
  cr.who_ddds,
  cr.ingredients_info,
  COALESCE(
    CASE WHEN vdo.vmp_code IS NOT NULL THEN vdo.override_ddd_comment ELSE cr.selected_ddd_comment END,
    cr.selected_ddd_comment
  ) AS selected_ddd_comment,
  cr.refers_to_ingredient,
  cr.expressed_as_strnt_nmrtr,
  cr.expressed_as_strnt_nmrtr_uom,
  cr.expressed_as_strnt_nmrtr_uom_name,
  cr.expressed_as_strnt_dnmtr,
  cr.expressed_as_strnt_dnmtr_uom,
  cr.expressed_as_strnt_dnmtr_uom_name,
  cr.expressed_as_strnt_dnmtr_basis_val,
  cr.expressed_as_strnt_dnmtr_basis_uom,
  cr.expressed_as_ingredient_code,
  cr.expressed_as_ingredient_name
FROM current_results cr
LEFT JOIN vmp_ddd_overrides vdo ON cr.vmp_code = vdo.vmp_code