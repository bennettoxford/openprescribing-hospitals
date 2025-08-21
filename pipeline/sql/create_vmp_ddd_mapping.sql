CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_DDD_MAPPING_TABLE_ID }}`
CLUSTER BY vmp_code
AS

WITH
scmd_vmps AS (
  SELECT DISTINCT vmp_code 
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}`
),

vmp_atc_mappings AS (
  SELECT
    vmp.vmp_code,
    vmp.vmp_name,
    ARRAY_AGG(
      STRUCT(
        atc.atc_code,
        who_atc.atc_name
      )
    ) AS atcs
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` vmp
  JOIN scmd_vmps sv ON vmp.vmp_code = sv.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_SUPP_TABLE_ID }}` atc
    ON vmp.vmp_code = atc.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ WHO_ATC_TABLE_ID }}` who_atc
    ON atc.atc_code = who_atc.atc_code
  GROUP BY vmp.vmp_code, vmp.vmp_name
),

vmp_routes AS (
  SELECT
    vmp.vmp_code,
    ARRAY_AGG(
      STRUCT(
        route.ontformroute_cd,
        route.ontformroute_descr,
        route_map.who_route AS who_route_code
      )
    ) AS routes
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` vmp,
  UNNEST(ontformroutes) AS route
  JOIN scmd_vmps sv ON vmp.vmp_code = sv.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ ADM_ROUTE_MAPPING_TABLE_ID }}` route_map
    ON route.ontformroute_descr = route_map.dmd_ontformroute
  GROUP BY vmp.vmp_code
),

vmp_who_ddds AS (
  SELECT
    vmp.vmp_code,
    ARRAY_AGG(
      STRUCT(
        ddd.ddd,
        ddd.ddd_unit,
        ddd.adm_code AS ddd_route_code,
        ddd.comment AS ddd_comment
      )
    ) AS who_ddds
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` vmp
  JOIN scmd_vmps sv ON vmp.vmp_code = sv.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_SUPP_TABLE_ID }}` atc
    ON vmp.vmp_code = atc.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ WHO_DDD_TABLE_ID }}` ddd
    ON atc.atc_code = ddd.atc_code
  GROUP BY vmp.vmp_code
),

vmp_uoms AS (
  SELECT
    vmp_code,
    ARRAY_AGG(
      STRUCT(
        uom_id AS uom_id,
        uom_name AS uom_name,
        normalised_uom_id AS basis_id,
        normalised_uom_name AS basis_name
      )
    ) AS uoms
  FROM (
    SELECT DISTINCT
      vmp_code,
      uom_id,
      uom_name,
      normalised_uom_id,
      normalised_uom_name
    FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}`
  )
  GROUP BY vmp_code
),

base_vmps AS (
  SELECT 
    sv.vmp_code,
    vmp.vmp_name
  FROM scmd_vmps sv
  JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` vmp ON sv.vmp_code = vmp.vmp_code
),

vmp_ingredients AS (
  SELECT
    vmp_code,
    ARRAY_AGG(
      STRUCT(
        ingredient_code,
        ingredient_name,
        ingredient_unit,
        ingredient_basis_unit
      )
      ORDER BY ingredient_name
    ) AS ingredients_info
  FROM (
    SELECT DISTINCT
      iq.vmp_code,
      ingredient.ingredient_code,
      ingredient.ingredient_name,
      ingredient.ingredient_unit,
      ingredient.ingredient_basis_unit
    FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ INGREDIENT_QUANTITY_TABLE_ID }}` iq,
    UNNEST(ingredients) AS ingredient
  )
  GROUP BY vmp_code
),

ddd_refers_to_mappings AS (
  SELECT 
    rt.ddd_comment,
    rt.refers_to_ingredient,
    dmd_ing.dmd_ingredient_code,
    dmd_ing.dmd_ingredient_name
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_REFERS_TO_TABLE_ID }}` rt,
  UNNEST(dmd_ingredients) AS dmd_ing
),

route_matches AS (
  SELECT
    vmp_code,
    ARRAY(
      SELECT DISTINCT who_route_code
      FROM UNNEST(routes)
      WHERE who_route_code IS NOT NULL
    ) AS who_route_codes
  FROM vmp_routes
),

ddd_analysis AS (
  SELECT
    v.vmp_code,
    v.vmp_name,
    atc.atcs,
    ddds.who_ddds,
    r.who_route_codes,
    -- Check if any ATC codes
    ARRAY_LENGTH(atc.atcs) > 0 AS has_atc_codes,
    -- Check if any WHO DDDs with non-NULL values
    (SELECT COUNT(1) FROM UNNEST(ddds.who_ddds) WHERE ddd IS NOT NULL) > 0 AS has_ddds,
    -- Check if any mapped WHO routes
    ARRAY_LENGTH(r.who_route_codes) > 0 AS has_who_routes,
    -- Find DDDs that match the WHO route codes
    ARRAY(
      SELECT AS STRUCT ddd.*
      FROM UNNEST(ddds.who_ddds) AS ddd
      WHERE ddd.ddd IS NOT NULL 
        AND ddd.ddd_route_code IN (SELECT code FROM UNNEST(r.who_route_codes) AS code)
    ) AS matching_route_ddds,
    -- Check if all matching DDDs have the same value
    (
      SELECT COUNT(DISTINCT ddd.ddd) 
      FROM UNNEST(ARRAY(
        SELECT AS STRUCT ddd.*
        FROM UNNEST(ddds.who_ddds) AS ddd
        WHERE ddd.ddd IS NOT NULL
          AND ddd.ddd_route_code IN (SELECT code FROM UNNEST(r.who_route_codes) AS code)
      )) AS ddd
    ) = 1 AS all_matching_ddds_same
  FROM base_vmps v
  LEFT JOIN vmp_atc_mappings atc ON v.vmp_code = atc.vmp_code
  LEFT JOIN vmp_who_ddds ddds ON v.vmp_code = ddds.vmp_code
  LEFT JOIN route_matches r ON v.vmp_code = r.vmp_code
),

ddd_route_selection AS (
  SELECT
    da.*,
    CASE
      WHEN NOT has_atc_codes OR NOT has_ddds OR NOT has_who_routes OR ARRAY_LENGTH(matching_route_ddds) = 0 OR NOT all_matching_ddds_same THEN NULL
      ELSE (SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1)
    END AS selected_ddd_comment,
    -- Route matching checks. Also check if the DDD has a comment. If it does, then the route match is not ok.
    CASE
      WHEN NOT has_atc_codes THEN FALSE
      WHEN NOT has_ddds THEN FALSE
      WHEN NOT has_who_routes THEN FALSE
      WHEN ARRAY_LENGTH(matching_route_ddds) = 0 THEN FALSE
      WHEN NOT all_matching_ddds_same THEN FALSE
      WHEN (SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1) IS NOT NULL 
        AND TRIM((SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1)) != '' 
        AND TRIM((SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1)) != 'New DDD'
        AND TRIM((SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1)) != 'Independent of strength'
        AND NOT LOWER(TRIM((SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1))) LIKE 'refers to %' THEN FALSE
      ELSE TRUE
    END AS route_match_ok,
    CASE
      WHEN NOT has_atc_codes THEN 'No ATC codes found'
      WHEN NOT has_ddds THEN 'No DDD values found'
      WHEN NOT has_who_routes THEN 'No WHO routes mapped'
      WHEN ARRAY_LENGTH(matching_route_ddds) = 0 THEN 'No matching routes between VMP and DDD values'
      WHEN NOT all_matching_ddds_same THEN 'Multiple different DDD values for matching routes'
      WHEN (SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1) IS NOT NULL 
        AND TRIM((SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1)) != '' 
        AND TRIM((SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1)) != 'New DDD'
        AND TRIM((SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1)) != 'Independent of strength'
        AND NOT LOWER(TRIM((SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1))) LIKE 'refers to %'
        THEN CONCAT('DDD has unsupported comment: ', (SELECT ddd_comment FROM UNNEST(matching_route_ddds) LIMIT 1))
      ELSE NULL
    END AS route_matching_issue,
    -- Get the selected DDD when routes match
    CASE
      WHEN NOT has_atc_codes OR NOT has_ddds OR NOT has_who_routes OR ARRAY_LENGTH(matching_route_ddds) = 0 OR NOT all_matching_ddds_same THEN NULL
      ELSE (SELECT ddd FROM UNNEST(matching_route_ddds) LIMIT 1)
    END AS selected_ddd_value,
    -- Get the unit of the selected DDD
    CASE
      WHEN NOT has_atc_codes OR NOT has_ddds OR NOT has_who_routes OR ARRAY_LENGTH(matching_route_ddds) = 0 OR NOT all_matching_ddds_same THEN NULL
      ELSE (SELECT ddd_unit FROM UNNEST(matching_route_ddds) LIMIT 1)
    END AS selected_ddd_unit,
    -- Get the route code of the selected DDD
    CASE
      WHEN NOT has_atc_codes OR NOT has_ddds OR NOT has_who_routes OR ARRAY_LENGTH(matching_route_ddds) = 0 OR NOT all_matching_ddds_same THEN NULL
      ELSE (SELECT ddd_route_code FROM UNNEST(matching_route_ddds) LIMIT 1)
    END AS selected_ddd_route_code
  FROM ddd_analysis da
),

-- Check for unit compatibility
unit_compatibility AS (
  SELECT
    drs.*,
    uoms.uoms,
    unit_ddd.basis AS ddd_basis,
    unit_ddd.unit AS ddd_unit,
    EXISTS(
      SELECT 1
      FROM UNNEST(uoms.uoms) AS uom
      JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` unit_scmd
        ON uom.basis_id = unit_scmd.unit_id
      WHERE unit_scmd.basis = unit_ddd.basis
    ) AS has_compatible_units
  FROM ddd_route_selection drs
  LEFT JOIN vmp_uoms uoms ON drs.vmp_code = uoms.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` unit_ddd
    ON drs.selected_ddd_unit = unit_ddd.unit
  WHERE drs.route_match_ok = TRUE
),

vmp_refers_to_ingredients AS (
  SELECT 
    ing.vmp_code,
    uc.selected_ddd_comment,
    STRING_AGG(vmp_ing.ingredient_name, ', ') AS refers_to_ingredient_names
  FROM unit_compatibility uc
  LEFT JOIN vmp_ingredients ing ON uc.vmp_code = ing.vmp_code
  LEFT JOIN UNNEST(ing.ingredients_info) AS vmp_ing
  LEFT JOIN ddd_refers_to_mappings rt 
    ON rt.ddd_comment = uc.selected_ddd_comment
    AND rt.dmd_ingredient_code = vmp_ing.ingredient_code
  WHERE uc.selected_ddd_comment IS NOT NULL 
    AND LOWER(TRIM(uc.selected_ddd_comment)) LIKE 'refers to %'
    AND rt.dmd_ingredient_code IS NOT NULL
  GROUP BY ing.vmp_code, uc.selected_ddd_comment
),

unit_and_ingredient_analysis AS (
  SELECT
    uc.vmp_code,
    uc.vmp_name,
    uc.who_ddds,
    uc.selected_ddd_value,
    uc.selected_ddd_unit,
    uc.selected_ddd_route_code,
    uc.selected_ddd_comment,
    uc.ddd_basis AS selected_ddd_basis_unit,
    ing.ingredients_info,
    -- Check if ingredients
    (ing.ingredients_info IS NOT NULL AND ARRAY_LENGTH(ing.ingredients_info) > 0) AS has_ingredients,
    -- Check if exactly one ingredient
    (ing.ingredients_info IS NOT NULL AND ARRAY_LENGTH(ing.ingredients_info) = 1) AS has_single_ingredient,
    -- Check if DDD comment starts with "Refers to"
    (uc.selected_ddd_comment IS NOT NULL 
     AND LOWER(TRIM(uc.selected_ddd_comment)) LIKE 'refers to %') AS is_refers_to_ddd,
    -- For "Refers to" DDDs, check if VMP has the referred ingredient
    EXISTS(
      SELECT 1
      FROM UNNEST(ing.ingredients_info) AS vmp_ing
      JOIN ddd_refers_to_mappings rt 
        ON rt.ddd_comment = uc.selected_ddd_comment
        AND rt.dmd_ingredient_code = vmp_ing.ingredient_code
    ) AS has_refers_to_ingredient,
    -- Get the specific ingredient name for "refers to" cases
    vri.refers_to_ingredient_names AS refers_to_ingredient_name,
    -- Get the ingredient basis unit (if there's only one ingredient)
    CASE
      WHEN ing.ingredients_info IS NOT NULL AND ARRAY_LENGTH(ing.ingredients_info) = 1 
      THEN (SELECT ingredient_basis_unit FROM UNNEST(ing.ingredients_info) LIMIT 1)
      ELSE NULL
    END AS single_ingredient_basis_unit,
    -- Check if the ingredient basis unit matches the DDD basis unit
    CASE
      WHEN ing.ingredients_info IS NOT NULL AND ARRAY_LENGTH(ing.ingredients_info) = 1 
      THEN (
        SELECT ingredient_basis_unit FROM UNNEST(ing.ingredients_info) LIMIT 1
      ) = uc.ddd_basis
      ELSE FALSE
    END AS ingredient_basis_matches_ddd,
    -- For "Refers to" DDDs, check if the referred ingredient's basis unit matches DDD basis
    CASE
      WHEN uc.selected_ddd_comment IS NOT NULL 
           AND LOWER(TRIM(uc.selected_ddd_comment)) LIKE 'refers to %'
      THEN EXISTS(
        SELECT 1
        FROM UNNEST(ing.ingredients_info) AS vmp_ing
        JOIN ddd_refers_to_mappings rt 
          ON rt.ddd_comment = uc.selected_ddd_comment
          AND rt.dmd_ingredient_code = vmp_ing.ingredient_code
        WHERE vmp_ing.ingredient_basis_unit = uc.ddd_basis
      )
      ELSE FALSE
    END AS refers_to_ingredient_basis_matches_ddd,
    -- Check for route matching
    (
      SELECT COUNT(1) 
      FROM UNNEST(uc.who_ddds) ddd 
      JOIN (SELECT DISTINCT who_route_code FROM UNNEST(r.routes) WHERE who_route_code IS NOT NULL) rt
      ON ddd.ddd_route_code = rt.who_route_code
    ) > 0 AS routes_match,
    -- Check for missing ingredient units
    EXISTS (
      SELECT 1 
      FROM UNNEST(ing.ingredients_info) 
      WHERE ingredient_unit IS NULL
    ) AS has_missing_ingredient_units,
    uc.has_compatible_units
  FROM unit_compatibility uc
  LEFT JOIN vmp_ingredients ing ON uc.vmp_code = ing.vmp_code
  LEFT JOIN vmp_routes r ON uc.vmp_code = r.vmp_code
  LEFT JOIN vmp_refers_to_ingredients vri ON uc.vmp_code = vri.vmp_code
),

ddd_calculation_status AS (
  SELECT
    vmp_code,
    vmp_name,
    who_ddds,
    FALSE AS can_calculate_ddd,
    route_matching_issue AS ddd_calculation_logic,
    NULL AS selected_ddd_value,
    NULL AS selected_ddd_unit,
    NULL AS selected_ddd_route_code,
    NULL AS selected_ddd_basis_unit
  FROM ddd_route_selection
  WHERE route_match_ok = FALSE
  
  UNION ALL

  SELECT
    vmp_code,
    vmp_name,
    who_ddds,
    TRUE AS can_calculate_ddd,
    'SCMD quantity / DDD' AS ddd_calculation_logic,
    selected_ddd_value,
    selected_ddd_unit,
    selected_ddd_route_code,
    ddd_basis AS selected_ddd_basis_unit
  FROM unit_compatibility
  WHERE has_compatible_units = TRUE
  
  UNION ALL
  
  SELECT
    vmp_code,
    vmp_name,
    who_ddds,
    CASE 
      -- Allow single ingredient case
      WHEN has_single_ingredient AND ingredient_basis_matches_ddd THEN TRUE
      -- Allow "Refers to" case with multiple ingredients if the referred ingredient matches
      WHEN is_refers_to_ddd AND has_refers_to_ingredient AND refers_to_ingredient_basis_matches_ddd THEN TRUE
      ELSE FALSE
    END AS can_calculate_ddd,
    CASE
      WHEN has_single_ingredient AND ingredient_basis_matches_ddd 
      THEN 'Ingredient quantity / DDD'
      WHEN is_refers_to_ddd AND has_refers_to_ingredient AND refers_to_ingredient_basis_matches_ddd
      THEN CONCAT('Ingredient quantity / DDD (Refers to ', LOWER(refers_to_ingredient_name), ')')
      WHEN NOT routes_match 
      THEN 'Not calculated: DDD route does not match product route'
      WHEN has_missing_ingredient_units 
      THEN 'Not calculated: missing ingredient unit information, cannot calculate'
      WHEN NOT has_ingredients 
      THEN CONCAT('Not calculated: DDD unit incompatible with SCMD unit. No ingredients found.')
      WHEN is_refers_to_ddd AND NOT has_refers_to_ingredient
      THEN CONCAT('Not calculated: DDD refers to ingredient not found in VMP (', selected_ddd_comment, ')')
      WHEN is_refers_to_ddd AND has_refers_to_ingredient AND NOT refers_to_ingredient_basis_matches_ddd
      THEN CONCAT('Not calculated: referred ingredient unit does not match DDD unit (', selected_ddd_comment, ')')
      WHEN NOT has_single_ingredient AND NOT is_refers_to_ddd
      THEN CONCAT('Not calculated: DDD unit incompatible with SCMD unit. Multiple ingredients found, fallback not possible.')
      WHEN NOT ingredient_basis_matches_ddd 
      THEN CONCAT('Not calculated: DDD unit incompatible with SCMD unit. Ingredient unit does not match DDD unit.')
      ELSE 'Not calculated: unknown route or unit compatibility issue'
    END AS ddd_calculation_logic,
    selected_ddd_value,
    selected_ddd_unit,
    selected_ddd_route_code,
    selected_ddd_basis_unit
  FROM unit_and_ingredient_analysis
  WHERE NOT has_compatible_units
),

missing_cases AS (
  SELECT 
    bv.vmp_code,
    bv.vmp_name,
    ddds.who_ddds,
    FALSE AS can_calculate_ddd,
    'Unknown/unhandled case' AS ddd_calculation_logic,
    CAST(NULL AS FLOAT64) AS selected_ddd_value,
    CAST(NULL AS STRING) AS selected_ddd_unit,
    CAST(NULL AS STRING) AS selected_ddd_route_code,
    CAST(NULL AS STRING) AS selected_ddd_basis_unit
  FROM base_vmps bv
  LEFT JOIN vmp_who_ddds ddds ON bv.vmp_code = ddds.vmp_code
  LEFT JOIN ddd_calculation_status dcs ON bv.vmp_code = dcs.vmp_code
  WHERE dcs.vmp_code IS NULL
),

vmp_ddd_final_mapping AS (
  SELECT
    bv.vmp_code,
    bv.vmp_name,
    uoms.uoms,
    atc.atcs,
    routes.routes,
    ddds.who_ddds,
    ing.ingredients_info,
    COALESCE(dcs.can_calculate_ddd, ms.can_calculate_ddd, FALSE) AS can_calculate_ddd,
    COALESCE(dcs.ddd_calculation_logic, ms.ddd_calculation_logic, 'No reason specified') AS ddd_calculation_logic,
    COALESCE(dcs.selected_ddd_value, ms.selected_ddd_value) AS selected_ddd_value,
    COALESCE(dcs.selected_ddd_unit, ms.selected_ddd_unit) AS selected_ddd_unit,
    COALESCE(dcs.selected_ddd_basis_unit, ms.selected_ddd_basis_unit) AS selected_ddd_basis_unit,
    COALESCE(dcs.selected_ddd_route_code, ms.selected_ddd_route_code) AS selected_ddd_route_code
  FROM base_vmps bv
  LEFT JOIN vmp_atc_mappings atc ON bv.vmp_code = atc.vmp_code
  LEFT JOIN vmp_routes routes ON bv.vmp_code = routes.vmp_code
  LEFT JOIN vmp_who_ddds ddds ON bv.vmp_code = ddds.vmp_code
  LEFT JOIN vmp_uoms uoms ON bv.vmp_code = uoms.vmp_code
  LEFT JOIN vmp_ingredients ing ON bv.vmp_code = ing.vmp_code
  LEFT JOIN ddd_calculation_status dcs ON bv.vmp_code = dcs.vmp_code
  LEFT JOIN missing_cases ms ON bv.vmp_code = ms.vmp_code
)

SELECT 
  vmp_code,
  vmp_name,
  uoms,
  atcs,
  routes,
  who_ddds,
  ingredients_info,
  can_calculate_ddd,
  ddd_calculation_logic,
  selected_ddd_value,
  selected_ddd_unit,
  selected_ddd_basis_unit,
  selected_ddd_route_code
FROM vmp_ddd_final_mapping