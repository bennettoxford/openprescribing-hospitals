CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_CALCULATION_LOGIC_TABLE_ID }}`
CLUSTER BY vmp_code
AS

WITH
scmd_vmps AS (
  SELECT DISTINCT vmp_code 
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}`
),

vmp_enriched AS (
  SELECT
    vmp.vmp_code,
    vmp.vmp_name,
    vmp.scmd_uom_id,
    vmp.scmd_uom_name,
    vmp.scmd_basis_uom_id,
    vmp.scmd_basis_uom_name,
    ARRAY_AGG(
      STRUCT(
        atc_struct.atc_code,
        who_atc.atc_name
      )
    ) AS atcs,
    ARRAY_AGG(
      STRUCT(
        route.route_code AS ontformroute_cd,
        route.route_name AS ontformroute_descr,
        route_map.who_route AS who_route_code
      )
    ) AS routes,
    ARRAY_AGG(DISTINCT route_map.who_route IGNORE NULLS) AS who_route_codes,
    ARRAY_AGG(
      STRUCT(
        ddd.ddd,
        ddd.ddd_unit,
        ddd.adm_code AS ddd_route_code,
        ddd.comment AS ddd_comment
      )
    ) AS who_ddds,
    ARRAY(
      SELECT DISTINCT AS STRUCT
        ing.ingredient_code,
        ing.ingredient_name,
        ing.strnt_nmrtr_uom_name AS ingredient_unit,
        ing.strnt_nmrtr_basis_uom AS ingredient_basis_unit
      FROM UNNEST(vmp.ingredients) AS ing
      ORDER BY ing.ingredient_name
    ) AS ingredients_info
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_TABLE_ID }}` vmp
  JOIN scmd_vmps sv ON vmp.vmp_code = sv.vmp_code
  LEFT JOIN UNNEST(vmp.atcs) AS atc_struct
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ WHO_ATC_TABLE_ID }}` who_atc
    ON atc_struct.atc_code = who_atc.atc_code
  LEFT JOIN UNNEST(vmp.ont_form_routes) AS route
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ ADM_ROUTE_MAPPING_TABLE_ID }}` route_map
    ON route.route_name = route_map.dmd_ontformroute
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ WHO_DDD_TABLE_ID }}` ddd
    ON atc_struct.atc_code = ddd.atc_code
  GROUP BY vmp.vmp_code, vmp.vmp_name, vmp.ont_form_routes, vmp.ingredients, vmp.scmd_uom_id, vmp.scmd_uom_name, vmp.scmd_basis_uom_id, vmp.scmd_basis_uom_name
),

ddd_analysis AS (
  SELECT
    v.vmp_code,
    v.vmp_name,
    v.scmd_uom_id,
    v.scmd_uom_name,
    v.scmd_basis_uom_id,
    v.scmd_basis_uom_name,
    v.atcs,
    v.who_ddds,
    v.who_route_codes,
    v.routes,
    v.ingredients_info,
    -- Check if any ATC codes
    (SELECT COUNT(1) FROM UNNEST(v.atcs) WHERE atc_code IS NOT NULL) > 0 AS has_atc_codes,
    -- Check if any WHO DDDs with non-NULL values
    (SELECT COUNT(1) FROM UNNEST(v.who_ddds) WHERE ddd IS NOT NULL) > 0 AS has_ddds,
    -- Check if any mapped WHO routes
    (SELECT COUNT(1) FROM UNNEST(v.routes) WHERE who_route_code IS NOT NULL) > 0 AS has_who_routes,
    -- Find DDDs that match the WHO route codes
    ARRAY(
      SELECT AS STRUCT ddd.*
      FROM UNNEST(v.who_ddds) AS ddd
      WHERE ddd.ddd IS NOT NULL 
        AND ddd.ddd_route_code IN (SELECT code FROM UNNEST(v.who_route_codes) AS code)
    ) AS matching_route_ddds,
    -- Check if all matching DDDs have the same value
    (
      SELECT COUNT(DISTINCT ddd.ddd) 
      FROM UNNEST(ARRAY(
        SELECT AS STRUCT ddd.*
        FROM UNNEST(v.who_ddds) AS ddd
        WHERE ddd.ddd IS NOT NULL
          AND ddd.ddd_route_code IN (SELECT code FROM UNNEST(v.who_route_codes) AS code)
      )) AS ddd
    ) = 1 AS all_matching_ddds_same
  FROM vmp_enriched v
),

ddd_route_selection AS (
  SELECT
    da.*,
    (SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1) AS selected_ddd_comment,
    has_atc_codes 
      AND has_ddds 
      AND has_who_routes 
      AND ARRAY_LENGTH(matching_route_ddds) > 0 
      AND all_matching_ddds_same
      AND (
        (SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1) IS NULL 
        OR TRIM((SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1)) = ''
        OR LOWER(TRIM((SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1))) = 'independent of strength'
        OR LOWER(TRIM((SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1))) = 'new ddd'
      ) AS route_match_ok,
    CASE
      WHEN NOT has_atc_codes THEN 'No ATC codes found'
      WHEN NOT has_ddds THEN 'No DDD values found'
      WHEN NOT has_who_routes THEN 'No WHO routes mapped'
      WHEN ARRAY_LENGTH(matching_route_ddds) = 0 THEN 'No matching routes between VMP and DDD values'
      WHEN NOT all_matching_ddds_same THEN 'Multiple different DDD values for matching routes'
      WHEN (SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1) IS NOT NULL 
        AND TRIM((SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1)) != ''
        AND LOWER(TRIM((SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1))) != 'independent of strength'
        AND LOWER(TRIM((SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1))) != 'new ddd'
        THEN CONCAT('DDD has unsupported comment (', (SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1), ')')
      ELSE NULL
    END AS route_matching_issue,
    (SELECT ddd FROM UNNEST(matching_route_ddds) LIMIT 1) AS selected_ddd_value,
    (SELECT ddd_unit FROM UNNEST(matching_route_ddds) LIMIT 1) AS selected_ddd_unit,
    (SELECT ddd_route_code FROM UNNEST(matching_route_ddds) LIMIT 1) AS selected_ddd_route_code
  FROM ddd_analysis da
),

-- Check for unit compatibility for calculation from SCMD quantity
unit_compatibility AS (
  SELECT
    drs.*,
    unit_ddd.basis AS selected_ddd_basis_unit,
    drs.scmd_basis_uom_name = unit_ddd.basis AS has_compatible_units
  FROM ddd_route_selection drs
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` unit_ddd
    ON drs.selected_ddd_unit = unit_ddd.unit
  WHERE drs.route_match_ok = TRUE
),
unit_and_ingredient_analysis AS (
  SELECT
    uc.vmp_code,
    uc.vmp_name,
    uc.who_ddds,
    uc.selected_ddd_value,
    uc.selected_ddd_unit,
    uc.selected_ddd_route_code,
    uc.selected_ddd_basis_unit,
    uc.ingredients_info,
    uc.routes,
    -- Check if ingredients
    (uc.ingredients_info IS NOT NULL AND ARRAY_LENGTH(uc.ingredients_info) > 0) AS has_ingredients,
    -- Check if exactly one ingredient
    (uc.ingredients_info IS NOT NULL AND ARRAY_LENGTH(uc.ingredients_info) = 1) AS has_single_ingredient,
    -- Get the ingredient basis unit (if there's only one ingredient)
    CASE
      WHEN uc.ingredients_info IS NOT NULL AND ARRAY_LENGTH(uc.ingredients_info) = 1 
      THEN (SELECT ingredient_basis_unit FROM UNNEST(uc.ingredients_info) LIMIT 1)
      ELSE NULL
    END AS single_ingredient_basis_unit,
    -- Check if the ingredient basis unit matches the DDD basis unit
    CASE
      WHEN uc.ingredients_info IS NOT NULL AND ARRAY_LENGTH(uc.ingredients_info) = 1 
      THEN (
        SELECT ingredient_basis_unit FROM UNNEST(uc.ingredients_info) LIMIT 1
      ) = uc.selected_ddd_basis_unit
      ELSE FALSE
    END AS ingredient_basis_matches_ddd,
    -- Check for missing ingredient units
    EXISTS (
      SELECT 1 
      FROM UNNEST(uc.ingredients_info) 
      WHERE ingredient_unit IS NULL
    ) AS has_missing_ingredient_units,
    uc.has_compatible_units
  FROM unit_compatibility uc
),

ddd_calculation_status AS (
  SELECT
    drs.vmp_code,
    drs.vmp_name,
    drs.who_ddds,
    CASE
      WHEN drs.route_match_ok = FALSE THEN FALSE
      WHEN uc.has_compatible_units = TRUE THEN TRUE
      WHEN uia.has_single_ingredient AND uia.ingredient_basis_matches_ddd THEN TRUE
      ELSE FALSE
    END AS can_calculate_ddd,
    CASE
      WHEN drs.route_match_ok = FALSE THEN CONCAT('Not calculated: ', drs.route_matching_issue)
      WHEN uc.has_compatible_units = TRUE THEN 'SCMD quantity / DDD'
      WHEN uia.has_single_ingredient AND uia.ingredient_basis_matches_ddd THEN 'Ingredient quantity / DDD'
      WHEN uia.has_missing_ingredient_units THEN 'Not calculated: missing ingredient unit information, cannot calculate'
      WHEN NOT uia.has_ingredients THEN 'Not calculated: DDD unit incompatible with SCMD unit. No ingredients found.'
      WHEN NOT uia.has_single_ingredient THEN 'Not calculated: DDD unit incompatible with SCMD unit. Multiple ingredients found, fallback not possible.'
      WHEN NOT uia.ingredient_basis_matches_ddd THEN 'Not calculated: DDD unit incompatible with SCMD unit. Ingredient unit does not match DDD unit.'
      ELSE 'Not calculated: unknown route or unit compatibility issue'
    END AS ddd_calculation_logic,
    CASE
      WHEN drs.route_match_ok = FALSE THEN NULL
      WHEN uc.has_compatible_units = TRUE THEN drs.selected_ddd_value
      WHEN uia.has_single_ingredient AND uia.ingredient_basis_matches_ddd THEN drs.selected_ddd_value
      ELSE NULL
    END AS selected_ddd_value,
    CASE
      WHEN drs.route_match_ok = FALSE THEN NULL
      WHEN uc.has_compatible_units = TRUE THEN drs.selected_ddd_unit
      WHEN uia.has_single_ingredient AND uia.ingredient_basis_matches_ddd THEN drs.selected_ddd_unit
      ELSE NULL
    END AS selected_ddd_unit,
    CASE
      WHEN drs.route_match_ok = FALSE THEN NULL
      WHEN uc.has_compatible_units = TRUE THEN drs.selected_ddd_route_code
      WHEN uia.has_single_ingredient AND uia.ingredient_basis_matches_ddd THEN drs.selected_ddd_route_code
      ELSE NULL
    END AS selected_ddd_route_code,
    CASE
      WHEN drs.route_match_ok = FALSE THEN NULL
      WHEN uc.has_compatible_units = TRUE THEN uc.selected_ddd_basis_unit
      WHEN uia.has_single_ingredient AND uia.ingredient_basis_matches_ddd THEN uia.selected_ddd_basis_unit
      ELSE NULL
    END AS selected_ddd_basis_unit,
    drs.selected_ddd_comment
  FROM ddd_route_selection drs
  LEFT JOIN unit_compatibility uc ON drs.vmp_code = uc.vmp_code
  LEFT JOIN unit_and_ingredient_analysis uia ON drs.vmp_code = uia.vmp_code
)
SELECT
  v.vmp_code,
  v.vmp_name,
  COALESCE(dcs.can_calculate_ddd, FALSE) AS can_calculate_ddd,
  COALESCE(dcs.ddd_calculation_logic, 'Unknown/unhandled case') AS ddd_calculation_logic,
  CASE 
    WHEN dcs.can_calculate_ddd THEN dcs.selected_ddd_value
    ELSE NULL 
  END AS selected_ddd_value,
  CASE 
    WHEN dcs.can_calculate_ddd THEN dcs.selected_ddd_unit
    ELSE NULL 
  END AS selected_ddd_unit,
  CASE 
    WHEN dcs.can_calculate_ddd THEN dcs.selected_ddd_basis_unit
    ELSE NULL 
  END AS selected_ddd_basis_unit,
  CASE 
    WHEN dcs.can_calculate_ddd THEN dcs.selected_ddd_route_code
    ELSE NULL 
  END AS selected_ddd_route_code,
  v.scmd_uom_id,
  v.scmd_uom_name,
  v.scmd_basis_uom_id,
  v.scmd_basis_uom_name,
  v.atcs,
  v.routes,
  v.who_ddds,
  v.ingredients_info,
  dcs.selected_ddd_comment,
  CAST(NULL AS STRING) AS refers_to_ingredient
FROM vmp_enriched v
LEFT JOIN ddd_calculation_status dcs ON v.vmp_code = dcs.vmp_code
