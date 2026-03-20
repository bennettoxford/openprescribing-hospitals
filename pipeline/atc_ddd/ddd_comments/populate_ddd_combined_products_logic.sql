CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_COMBINED_PRODUCTS_LOGIC_TABLE_ID }}`
CLUSTER BY vmp_code
AS

WITH base_candidates AS (
  SELECT
    dcl.vmp_code,
    dcl.vmp_name,
    dcl.can_calculate_ddd,
    dcl.ddd_calculation_logic,
    atc.atc_code,
    atc.atc_name,
    wdc.atc_code AS who_atc_code,
    wdc.brand_name,
    wdc.dosage_form,
    wdc.form,
    wdc.route,
    wdc.active_ingredients,
    wdc.ddd_comb,
    wdc.ddd_ud_value,
    wdc.ddd_converted_value,
    wdc.ddd_converted_unit,
    vmp.ingredients,
    vmp.ont_form_routes,
    vmp.scmd_uom_id,
    vmp.scmd_uom_name,
    vmp.scmd_basis_uom_id,
    vmp.scmd_basis_uom_name,
    uc_ddd.basis AS ddd_converted_basis_unit,
    uc_scmd.basis AS scmd_basis_unit,
    uc_ddd.conversion_factor AS ddd_conversion_factor
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_CALCULATION_LOGIC_TABLE_ID }}` AS dcl
  CROSS JOIN UNNEST(dcl.atcs) AS atc
  INNER JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ WHO_DDD_COMBINED_PRODUCTS_TABLE_ID }}` AS wdc
    ON atc.atc_code = wdc.atc_code
  INNER JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_TABLE_ID }}` AS vmp
    ON dcl.vmp_code = vmp.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` AS uc_ddd
    ON uc_ddd.unit = wdc.ddd_converted_unit
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` AS uc_scmd
    ON uc_scmd.unit = vmp.scmd_uom_name
  WHERE dcl.can_calculate_ddd = FALSE
),
-- Convert WHO active_ingredients (per UD) to per-unit so they are comparable to VMP ingredients
with_conversion AS (
  SELECT
    bc.*,
    SAFE_DIVIDE(bc.ddd_converted_value, bc.ddd_ud_value) AS ud_unit_conversion,
    (SELECT ARRAY_AGG(
      STRUCT(
        ai.ingredient,
        SAFE_DIVIDE(ai.numerator_quantity, SAFE_DIVIDE(bc.ddd_converted_value, bc.ddd_ud_value)) AS numerator_quantity,
        ai.numerator_unit,
        SAFE_DIVIDE(ai.denominator_quantity, SAFE_DIVIDE(bc.ddd_converted_value, bc.ddd_ud_value)) AS denominator_quantity,
        ai.denominator_unit
      )
    )
    FROM UNNEST(bc.active_ingredients) AS ai
    ) AS active_ingredients_per_unit
  FROM base_candidates bc
),
-- Explode to one row per converted active_ingredient and match to VMP ingredient by name + basis quantity.
-- For tablet-style (no denominator): allow proportional match (VMP strength = k * WHO strength) and output strength_ratio.
exploded AS (
  SELECT
    wc.vmp_code,
    wc.atc_code,
    wc.form,
    wc.route,
    wc.brand_name,
    wc.ddd_ud_value,
    wc.ddd_converted_value,
    wc.ddd_converted_unit,
    ARRAY_LENGTH(wc.active_ingredients_per_unit) AS n_active_ingredients,
    ARRAY_LENGTH(wc.ingredients) AS n_vmp_ingredients,
    SAFE_DIVIDE(ing.strnt_nmrtr_basis_val, ai.numerator_quantity * COALESCE(uc_ai.conversion_factor, 1.0)) AS strength_ratio
  FROM with_conversion wc
  CROSS JOIN UNNEST(wc.active_ingredients_per_unit) AS ai
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` AS uc_ai
    ON uc_ai.unit = ai.numerator_unit
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` AS uc_ai_denom
    ON uc_ai_denom.unit = ai.denominator_unit
  INNER JOIN UNNEST(wc.ingredients) AS ing
    ON (
      LOWER(TRIM(ing.ingredient_name)) = LOWER(TRIM(ai.ingredient))
      OR STARTS_WITH(LOWER(TRIM(ing.ingredient_name)), LOWER(TRIM(ai.ingredient)))
      OR (ing.basis_of_strength_name IS NOT NULL
          AND (LOWER(TRIM(ing.basis_of_strength_name)) = LOWER(TRIM(ai.ingredient))
               OR STARTS_WITH(LOWER(TRIM(ing.basis_of_strength_name)), LOWER(TRIM(ai.ingredient)))))
    )
    AND ing.strnt_nmrtr_basis_uom = uc_ai.basis
    AND (
      -- Tablet-style (no denominator): allow proportional match (same ratio for all ingredients, enforced in ingredient_matched).
      (ai.denominator_quantity IS NULL AND ai.denominator_unit IS NULL AND ing.strnt_dnmtr_basis_val IS NULL AND ing.strnt_dnmtr_basis_uom IS NULL
        AND SAFE_DIVIDE(ing.strnt_nmrtr_basis_val, ai.numerator_quantity * COALESCE(uc_ai.conversion_factor, 1.0)) > 0)
      OR (
        -- WHO has denominator: exact match on numerator and denominator.
        ing.strnt_nmrtr_basis_val = ai.numerator_quantity * COALESCE(uc_ai.conversion_factor, 1.0)
        AND ai.denominator_quantity IS NOT NULL AND ai.denominator_unit IS NOT NULL
        AND ing.strnt_dnmtr_basis_val = ai.denominator_quantity * COALESCE(uc_ai_denom.conversion_factor, 1.0)
        AND ing.strnt_dnmtr_basis_uom = uc_ai_denom.basis
      )
      OR (
        -- WHO no denominator, VMP per 1 [DDD unit] (e.g. per ml): exact strength match.
        ai.denominator_quantity IS NULL AND ai.denominator_unit IS NULL
        AND wc.ddd_converted_basis_unit IS NOT NULL
        AND ing.strnt_dnmtr_basis_val = 1.0
        AND ing.strnt_dnmtr_basis_uom = wc.ddd_converted_basis_unit
        AND ing.strnt_nmrtr_basis_val = ai.numerator_quantity * COALESCE(uc_ai.conversion_factor, 1.0)
      )
      OR (
        -- WHO no denominator, VMP per 1 [DDD unit]: proportional match.
        ai.denominator_quantity IS NULL AND ai.denominator_unit IS NULL
        AND wc.ddd_converted_basis_unit IS NOT NULL
        AND ing.strnt_dnmtr_basis_val = 1.0
        AND ing.strnt_dnmtr_basis_uom = wc.ddd_converted_basis_unit
        AND SAFE_DIVIDE(ing.strnt_nmrtr_basis_val, ai.numerator_quantity * COALESCE(uc_ai.conversion_factor, 1.0)) > 0
      )
    )
),
-- Keep only (vmp_code, atc_code, wdc row) where every ingredient matched and (for proportional match) same strength_ratio for all.
-- Round strength_ratio to 6 decimal places for comparison to avoid float precision issues.
ingredient_matched AS (
  SELECT
    vmp_code,
    atc_code,
    form,
    route,
    brand_name,
    ddd_ud_value,
    ddd_converted_value,
    ddd_converted_unit,
    strength_ratio
  FROM (
    SELECT
      vmp_code,
      atc_code,
      form,
      route,
      brand_name,
      ddd_ud_value,
      ddd_converted_value,
      ddd_converted_unit,
      n_active_ingredients,
      n_vmp_ingredients,
      ROUND(MAX(strength_ratio), 6) AS strength_ratio,
      COUNT(*) AS match_count,
      COUNT(DISTINCT ROUND(strength_ratio, 6)) AS distinct_ratio_count
    FROM exploded
    WHERE strength_ratio IS NOT NULL AND strength_ratio > 0
    GROUP BY vmp_code, atc_code, form, route, brand_name, ddd_ud_value, ddd_converted_value, ddd_converted_unit, n_active_ingredients, n_vmp_ingredients
  )
  WHERE match_count = n_active_ingredients
    AND n_active_ingredients = n_vmp_ingredients
    AND distinct_ratio_count = 1
),
base AS (
  SELECT
    wc.*,
    im.vmp_code IS NOT NULL AS ingredient_match,
    im.strength_ratio AS strength_ratio
  FROM with_conversion wc
  LEFT JOIN ingredient_matched im
    ON wc.vmp_code = im.vmp_code
    AND wc.atc_code = im.atc_code
    AND COALESCE(wc.form, '') = COALESCE(im.form, '')
    AND COALESCE(wc.route, '') = COALESCE(im.route, '')
    AND COALESCE(wc.brand_name, '') = COALESCE(im.brand_name, '')
    AND COALESCE(CAST(wc.ddd_ud_value AS STRING), '') = COALESCE(CAST(im.ddd_ud_value AS STRING), '')
    AND COALESCE(CAST(wc.ddd_converted_value AS STRING), '') = COALESCE(CAST(im.ddd_converted_value AS STRING), '')
    AND COALESCE(wc.ddd_converted_unit, '') = COALESCE(im.ddd_converted_unit, '')
),
reasons AS (
  SELECT
    base.*,
    CASE WHEN ARRAY_LENGTH(base.ingredients) <= 1
      THEN 'Less than 2 ingredients for this product in the dm+d' ELSE NULL END AS reason_single_ingredient,
    CASE
      WHEN base.ingredient_match = FALSE
      THEN 'Combined product ingredients and/or strengths do not match VMP ingredients' ELSE NULL END AS reason_ingredient_mismatch,
    CASE
      WHEN NOT EXISTS (SELECT 1 FROM UNNEST(base.ont_form_routes) AS ofr WHERE LOWER(ofr.route_name) LIKE LOWER(CONCAT(COALESCE(REPLACE(TRIM(IFNULL(base.form, '*')), '*', '%'), '%'), '.', COALESCE(REPLACE(TRIM(IFNULL(base.route, '*')), '*', '%'), '%'))))
      THEN 'Form/route specified for the DDD does not match the form/route for this product from the dm+d' ELSE NULL END AS reason_form_route_mismatch,
    CASE WHEN base.ddd_converted_unit IS NOT NULL AND base.ddd_converted_basis_unit IS NULL
      THEN 'Combined DDD unit not supported' ELSE NULL END AS reason_ddd_unit_unknown,
    CASE WHEN base.scmd_uom_name IS NOT NULL AND base.scmd_basis_unit IS NULL
      THEN 'SCMD unit not supported' ELSE NULL END AS reason_scmd_unit_unknown,
    CASE WHEN base.ddd_converted_basis_unit IS NOT NULL AND base.scmd_basis_unit IS NOT NULL AND base.ddd_converted_basis_unit != base.scmd_basis_unit
      THEN 'Combined DDD unit and SCMD unit not compatible' ELSE NULL END AS reason_basis_mismatch
  FROM base
)
SELECT
  vmp_code,
  vmp_name,
  atc_code,
  atc_name,
  form,
  route,
  active_ingredients,
  ddd_ud_value,
  ddd_converted_value,
  ddd_converted_unit,
  ud_unit_conversion,
  active_ingredients_per_unit,
  ingredients,
  ont_form_routes,
  scmd_uom_id,
  scmd_uom_name,
  scmd_basis_uom_id,
  scmd_basis_uom_name,
  ddd_converted_basis_unit,
  scmd_basis_unit,
  strength_ratio,
  NULLIF(TRIM(ARRAY_TO_STRING(
    (SELECT ARRAY_AGG(x) FROM UNNEST([reason_single_ingredient, reason_ingredient_mismatch, reason_form_route_mismatch, reason_ddd_unit_unknown, reason_scmd_unit_unknown, reason_basis_mismatch]) AS x WHERE x IS NOT NULL),
    ', '
  )), '') AS why_ddd_not_chosen,
  CASE
    WHEN reason_single_ingredient IS NULL AND reason_ingredient_mismatch IS NULL AND reason_form_route_mismatch IS NULL
     AND reason_ddd_unit_unknown IS NULL AND reason_scmd_unit_unknown IS NULL AND reason_basis_mismatch IS NULL
    THEN (ddd_converted_value * ddd_conversion_factor) / COALESCE(strength_ratio, 1.0)
    ELSE NULL
  END AS chosen_ddd_value,
  CASE
    WHEN reason_single_ingredient IS NULL AND reason_ingredient_mismatch IS NULL AND reason_form_route_mismatch IS NULL
     AND reason_ddd_unit_unknown IS NULL AND reason_scmd_unit_unknown IS NULL AND reason_basis_mismatch IS NULL
    THEN ddd_converted_basis_unit
    ELSE NULL
  END AS chosen_ddd_unit
FROM reasons
