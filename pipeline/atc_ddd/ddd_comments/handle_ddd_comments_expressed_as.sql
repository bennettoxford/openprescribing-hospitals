MERGE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_CALCULATION_LOGIC_TABLE_ID }}` AS target
USING (
WITH
-- Find VMPs that couldn't be calculated due to "expressed as" comments
vmps_with_expressed_as AS (
  SELECT 
    ddd_logic.* EXCEPT(expressed_as_strnt_nmrtr, expressed_as_strnt_nmrtr_uom, expressed_as_strnt_nmrtr_uom_name, expressed_as_strnt_dnmtr, expressed_as_strnt_dnmtr_uom, expressed_as_strnt_dnmtr_uom_name, expressed_as_strnt_dnmtr_basis_val, expressed_as_strnt_dnmtr_basis_uom, expressed_as_ingredient_code, expressed_as_ingredient_name),
    expressed.vmp_id AS expressed_vmp_id,
    expressed.vmp_name AS expressed_vmp_name,
    expressed.ddd_comment AS expressed_ddd_comment,
    expressed.expressed_as_strnt_nmrtr,
    expressed.expressed_as_strnt_nmrtr_uom,
    expressed.expressed_as_strnt_nmrtr_uom_name,
    expressed.expressed_as_strnt_dnmtr,
    expressed.expressed_as_strnt_dnmtr_uom,
    expressed.expressed_as_strnt_dnmtr_uom_name,
    expressed.expressed_as_strnt_dnmtr_basis_val,
    expressed.expressed_as_strnt_dnmtr_basis_uom,
    expressed.ingredient_code AS expressed_ingredient_code,
    expressed.ingredient_name AS expressed_ingredient_name
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_CALCULATION_LOGIC_TABLE_ID }}` ddd_logic
  INNER JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_EXPRESSED_AS_TABLE_ID }}` expressed
    ON ddd_logic.vmp_code = expressed.vmp_id
  WHERE ddd_logic.can_calculate_ddd = FALSE
    AND ddd_logic.selected_ddd_comment IS NOT NULL
    AND ddd_logic.selected_ddd_comment = expressed.ddd_comment
),

-- Check if the expressed_as ingredient is in the VMP's ingredients
ingredient_matching AS (
  SELECT
    vwea.*,
    -- Check if the expressed_as ingredient code matches any VMP ingredient
    EXISTS(
      SELECT 1
      FROM UNNEST(vwea.ingredients_info) AS vmp_ing
      WHERE vmp_ing.ingredient_code = vwea.expressed_ingredient_code
    ) AS expressed_ingredient_in_vmp,
    -- Get the matching ingredient info if applicable
    ARRAY(
      SELECT AS STRUCT vmp_ing.*
      FROM UNNEST(vwea.ingredients_info) AS vmp_ing
      WHERE vmp_ing.ingredient_code = vwea.expressed_ingredient_code
      LIMIT 1
    ) AS matching_ingredients
  FROM vmps_with_expressed_as vwea
),

-- Use denominator information from vmp_expressed_as table
expressed_as_with_denominator AS (
  SELECT
    im.*
  FROM ingredient_matching im
  WHERE im.expressed_ingredient_in_vmp = TRUE
),
-- Convert expressed_as unit to basis unit
expressed_as_with_basis AS (
  SELECT
    eawd.* EXCEPT(expressed_as_strnt_dnmtr_uom),
    unit_conv.basis AS expressed_as_basis_unit,
    unit_conv.conversion_factor AS expressed_as_conversion_factor,
    eawd.expressed_as_strnt_nmrtr * COALESCE(unit_conv.conversion_factor, 1.0) AS expressed_as_strnt_nmrtr_basis_val,
    -- Get denominator unit identifier if needed
    denom_unit_conv.unit_id AS expressed_as_strnt_dnmtr_uom
  FROM expressed_as_with_denominator eawd
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` unit_conv
    ON eawd.expressed_as_strnt_nmrtr_uom_name = unit_conv.unit
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` denom_unit_conv
    ON eawd.expressed_as_strnt_dnmtr_uom_name = denom_unit_conv.unit
  WHERE eawd.expressed_ingredient_in_vmp = TRUE
),

-- Get WHO route codes for matching
route_analysis AS (
  SELECT
    eawb.*,
    ARRAY(
      SELECT DISTINCT route.who_route_code
      FROM UNNEST(eawb.routes) AS route
      WHERE route.who_route_code IS NOT NULL
    ) AS who_route_codes
  FROM expressed_as_with_basis eawb
),

-- Match DDDs with expressed_as comments and routes
ddd_analysis AS (
  SELECT
    ra.*,
    ARRAY(
      SELECT AS STRUCT ddd.*
      FROM UNNEST(ra.who_ddds) AS ddd
      WHERE ddd.ddd IS NOT NULL
        AND ddd.ddd_comment = ra.expressed_ddd_comment
        AND ddd.ddd_route_code IN (SELECT code FROM UNNEST(ra.who_route_codes) AS code)
    ) AS matching_route_ddds,
    (
      SELECT COUNT(DISTINCT ddd.ddd)
      FROM UNNEST(ra.who_ddds) AS ddd
      WHERE ddd.ddd IS NOT NULL
        AND ddd.ddd_comment = ra.expressed_ddd_comment
        AND ddd.ddd_route_code IN (SELECT code FROM UNNEST(ra.who_route_codes) AS code)
    ) = 1 AS all_matching_ddds_same
  FROM route_analysis ra
),

-- Check unit compatibility - expressed_as basis unit vs DDD basis unit
unit_analysis AS (
  SELECT
    da.vmp_code,
    da.vmp_name,
    da.expressed_ingredient_in_vmp,
    da.expressed_as_strnt_nmrtr,
    da.expressed_as_strnt_nmrtr_uom,
    da.expressed_as_strnt_nmrtr_uom_name,
    da.expressed_as_basis_unit,
    da.expressed_as_strnt_nmrtr_basis_val,
    da.expressed_as_strnt_dnmtr,
    da.expressed_as_strnt_dnmtr_uom,
    da.expressed_as_strnt_dnmtr_uom_name,
    da.expressed_as_strnt_dnmtr_basis_val,
    da.expressed_as_strnt_dnmtr_basis_uom,
    da.expressed_ingredient_code,
    da.expressed_ingredient_name,
    da.expressed_ddd_comment,
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
    -- Check if expressed_as basis unit matches DDD basis unit
    da.expressed_as_basis_unit = unit_ddd.basis AS expressed_as_basis_matches_ddd
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
    ua.expressed_ingredient_in_vmp
      AND ARRAY_LENGTH(ua.matching_route_ddds) > 0
      AND ua.all_matching_ddds_same
      AND ua.expressed_as_basis_matches_ddd AS can_calculate_ddd,
    CASE
      WHEN NOT ua.expressed_ingredient_in_vmp THEN 'Not calculated: Expressed as ingredient not found in VMP'
      WHEN ARRAY_LENGTH(ua.matching_route_ddds) = 0 THEN 'Not calculated: No matching routes between VMP and DDD values'
      WHEN NOT ua.all_matching_ddds_same THEN 'Not calculated: Multiple different DDD values for matching routes'
      WHEN ua.expressed_as_basis_matches_ddd THEN 
        CONCAT(
          'Ingredient quantity (',
          CASE 
            WHEN LOWER(ua.expressed_ddd_comment) LIKE '%expressed as%' 
            THEN LOWER(ua.expressed_ddd_comment)
            ELSE CONCAT('expressed as ', LOWER(ua.expressed_ddd_comment))
          END,
          ') / DDD'
        )
      ELSE 'Not calculated: Unit for expressed as ingredient does not match DDD unit'
    END AS ddd_calculation_logic,
    CASE WHEN ua.expressed_as_basis_matches_ddd THEN ua.selected_ddd_value ELSE NULL END AS selected_ddd_value,
    CASE WHEN ua.expressed_as_basis_matches_ddd THEN ua.selected_ddd_unit ELSE NULL END AS selected_ddd_unit,
    CASE WHEN ua.expressed_as_basis_matches_ddd THEN ua.selected_ddd_basis_unit ELSE NULL END AS selected_ddd_basis_unit,
    CASE WHEN ua.expressed_as_basis_matches_ddd THEN ua.selected_ddd_route_code ELSE NULL END AS selected_ddd_route_code,
    ua.expressed_ddd_comment AS selected_ddd_comment,
    ua.expressed_as_strnt_nmrtr AS expressed_as_strnt_nmrtr,
    ua.expressed_as_strnt_nmrtr_uom AS expressed_as_strnt_nmrtr_uom,
    ua.expressed_as_strnt_nmrtr_uom_name AS expressed_as_strnt_nmrtr_uom_name,
    ua.expressed_as_strnt_dnmtr AS expressed_as_strnt_dnmtr,
    ua.expressed_as_strnt_dnmtr_uom AS expressed_as_strnt_dnmtr_uom,
    ua.expressed_as_strnt_dnmtr_uom_name AS expressed_as_strnt_dnmtr_uom_name,
    ua.expressed_as_strnt_dnmtr_basis_val AS expressed_as_strnt_dnmtr_basis_val,
    ua.expressed_as_strnt_dnmtr_basis_uom AS expressed_as_strnt_dnmtr_basis_uom,
    ua.expressed_ingredient_code AS expressed_as_ingredient_code,
    ua.expressed_ingredient_name AS expressed_as_ingredient_name,
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
  selected_ddd_comment,
  NULL AS refers_to_ingredient,
  expressed_as_strnt_nmrtr,
  expressed_as_strnt_nmrtr_uom,
  expressed_as_strnt_nmrtr_uom_name,
  expressed_as_strnt_dnmtr,
  expressed_as_strnt_dnmtr_uom,
  expressed_as_strnt_dnmtr_uom_name,
  expressed_as_strnt_dnmtr_basis_val,
  expressed_as_strnt_dnmtr_basis_uom,
  expressed_as_ingredient_code,
  expressed_as_ingredient_name
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
    selected_ddd_comment = source.selected_ddd_comment,
    expressed_as_strnt_nmrtr = source.expressed_as_strnt_nmrtr,
    expressed_as_strnt_nmrtr_uom = source.expressed_as_strnt_nmrtr_uom,
    expressed_as_strnt_nmrtr_uom_name = source.expressed_as_strnt_nmrtr_uom_name,
    expressed_as_strnt_dnmtr = source.expressed_as_strnt_dnmtr,
    expressed_as_strnt_dnmtr_uom = source.expressed_as_strnt_dnmtr_uom,
    expressed_as_strnt_dnmtr_uom_name = source.expressed_as_strnt_dnmtr_uom_name,
    expressed_as_strnt_dnmtr_basis_val = source.expressed_as_strnt_dnmtr_basis_val,
    expressed_as_strnt_dnmtr_basis_uom = source.expressed_as_strnt_dnmtr_basis_uom,
    expressed_as_ingredient_code = source.expressed_as_ingredient_code,
    expressed_as_ingredient_name = source.expressed_as_ingredient_name

