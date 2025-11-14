MERGE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_CALCULATION_LOGIC_TABLE_ID }}` AS target
USING (
WITH
-- Find VMPs that couldn't be calculated due to "refers to" comments
vmps_with_refers_to AS (
  SELECT *
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_CALCULATION_LOGIC_TABLE_ID }}`
  WHERE can_calculate_ddd = FALSE
    AND selected_ddd_comment IS NOT NULL
    AND LOWER(selected_ddd_comment) LIKE 'refers to %'
),

-- Join with the refers_to table to get the target ingredients
vmps_with_refers_to_mapping AS (
  SELECT
    vwrt.*,
    refers.refers_to_ingredient,
    refers.dmd_ingredients,
    ARRAY(
      SELECT DISTINCT route.who_route_code
      FROM UNNEST(vwrt.routes) AS route
      WHERE route.who_route_code IS NOT NULL
    ) AS who_route_codes
  FROM vmps_with_refers_to vwrt
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_REFERS_TO_TABLE_ID }}` refers
    ON vwrt.selected_ddd_comment = refers.ddd_comment
),

-- Check if VMP ingredients match the referred ingredients
ingredient_matching AS (
  SELECT
    vm.*,
    -- Check if the VMP's ingredient matches any of the referred ingredients
    EXISTS(
      SELECT 1
      FROM UNNEST(vm.ingredients_info) AS vmp_ing
      INNER JOIN UNNEST(vm.dmd_ingredients) AS ref_ing
        ON vmp_ing.ingredient_code = ref_ing.dmd_ingredient_code
    ) AS vmp_ingredient_matches_refers_to,
    -- Get the matching ingredient info if applicable
    ARRAY(
      SELECT AS STRUCT vmp_ing.*
      FROM UNNEST(vm.ingredients_info) AS vmp_ing
      INNER JOIN UNNEST(vm.dmd_ingredients) AS ref_ing
        ON vmp_ing.ingredient_code = ref_ing.dmd_ingredient_code
    ) AS matching_ingredients
  FROM vmps_with_refers_to_mapping vm
  WHERE vm.dmd_ingredients IS NOT NULL
),

-- Match DDDs with refers_to comments and routes
ddd_analysis AS (
  SELECT
    im.*,
    ARRAY(
      SELECT AS STRUCT ddd.*
      FROM UNNEST(im.who_ddds) AS ddd
      WHERE ddd.ddd IS NOT NULL
        AND ddd.ddd_comment = im.selected_ddd_comment
        AND ddd.ddd_route_code IN (SELECT code FROM UNNEST(im.who_route_codes) AS code)
    ) AS matching_route_ddds,
    (
      SELECT COUNT(DISTINCT ddd.ddd)
      FROM UNNEST(im.who_ddds) AS ddd
      WHERE ddd.ddd IS NOT NULL
        AND ddd.ddd_comment = im.selected_ddd_comment
        AND ddd.ddd_route_code IN (SELECT code FROM UNNEST(im.who_route_codes) AS code)
    ) = 1 AS all_matching_ddds_same
  FROM ingredient_matching im
),

-- Check unit compatibility
unit_analysis AS (
  SELECT
    da.vmp_code,
    da.vmp_name,
    da.vmp_ingredient_matches_refers_to,
    da.matching_ingredients,
    da.refers_to_ingredient,
    da.selected_ddd_comment,
    da.scmd_uom_id,
    da.scmd_uom_name,
    da.scmd_basis_uom_id,
    da.scmd_basis_uom_name,
    da.atcs,
    da.routes,
    da.who_ddds,
    da.ingredients_info,
    da.matching_route_ddds,
    da.all_matching_ddds_same,
    (SELECT ddd FROM UNNEST(da.matching_route_ddds) LIMIT 1) AS selected_ddd_value,
    (SELECT ddd_unit FROM UNNEST(da.matching_route_ddds) LIMIT 1) AS selected_ddd_unit,
    (SELECT ddd_route_code FROM UNNEST(da.matching_route_ddds) LIMIT 1) AS selected_ddd_route_code,
    unit_ddd.basis AS selected_ddd_basis_unit,
    -- Check if ingredient basis matches DDD basis
    CASE
      WHEN ARRAY_LENGTH(da.matching_ingredients) = 1 
      THEN (SELECT ingredient_basis_unit FROM UNNEST(da.matching_ingredients) LIMIT 1) = unit_ddd.basis
      ELSE FALSE
    END AS ingredient_basis_matches_ddd
  FROM ddd_analysis da
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` unit_ddd
    ON (SELECT ddd_unit FROM UNNEST(da.matching_route_ddds) LIMIT 1) = unit_ddd.unit
  WHERE ARRAY_LENGTH(da.matching_route_ddds) > 0
    AND da.all_matching_ddds_same = TRUE
),

-- Determine final calculation status and values
calculation_determination AS (
  SELECT
    ua.vmp_code,
    ua.vmp_name,
    ua.vmp_ingredient_matches_refers_to
      AND ARRAY_LENGTH(ua.matching_route_ddds) > 0
      AND ua.all_matching_ddds_same
      AND ua.ingredient_basis_matches_ddd AS can_calculate_ddd,
    CASE
      WHEN NOT ua.vmp_ingredient_matches_refers_to THEN 'Not calculated: DDD refers to ingredient not found in VMP'
      WHEN ARRAY_LENGTH(ua.matching_route_ddds) = 0 THEN 'Not calculated: No matching routes between VMP and DDD values'
      WHEN NOT ua.all_matching_ddds_same THEN 'Not calculated: Multiple different DDD values for matching routes'
      WHEN ua.ingredient_basis_matches_ddd THEN CONCAT('Ingredient quantity (Refers to ', ua.refers_to_ingredient, ') / DDD')
      ELSE 'Not calculated: Unit for ingredient DDD refers to does not match DDD unit'
    END AS ddd_calculation_logic,
    CASE WHEN ua.ingredient_basis_matches_ddd THEN ua.selected_ddd_value ELSE NULL END AS selected_ddd_value,
    CASE WHEN ua.ingredient_basis_matches_ddd THEN ua.selected_ddd_unit ELSE NULL END AS selected_ddd_unit,
    CASE WHEN ua.ingredient_basis_matches_ddd THEN ua.selected_ddd_basis_unit ELSE NULL END AS selected_ddd_basis_unit,
    CASE WHEN ua.ingredient_basis_matches_ddd THEN ua.selected_ddd_route_code ELSE NULL END AS selected_ddd_route_code,
    ua.selected_ddd_comment,
    ua.scmd_uom_id,
    ua.scmd_uom_name,
    ua.scmd_basis_uom_id,
    ua.scmd_basis_uom_name,
    ua.atcs,
    ua.routes,
    ua.who_ddds,
    ua.ingredients_info
  FROM unit_analysis ua
)

SELECT
  vmp_code,
  vmp_name,
  can_calculate_ddd,
  ddd_calculation_logic,
  selected_ddd_value,
  selected_ddd_unit,
  selected_ddd_basis_unit,
  selected_ddd_route_code,
  scmd_uom_id,
  scmd_uom_name,
  scmd_basis_uom_id,
  scmd_basis_uom_name,
  atcs,
  routes,
  who_ddds,
  ingredients_info,
  selected_ddd_comment
FROM calculation_determination
) AS source
ON target.vmp_code = source.vmp_code
WHEN MATCHED THEN
  UPDATE SET
    can_calculate_ddd = source.can_calculate_ddd,
    ddd_calculation_logic = source.ddd_calculation_logic,
    selected_ddd_value = source.selected_ddd_value,
    selected_ddd_unit = source.selected_ddd_unit,
    selected_ddd_basis_unit = source.selected_ddd_basis_unit,
    selected_ddd_route_code = source.selected_ddd_route_code,
    selected_ddd_comment = source.selected_ddd_comment

