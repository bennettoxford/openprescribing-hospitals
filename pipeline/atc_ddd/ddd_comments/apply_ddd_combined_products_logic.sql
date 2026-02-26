MERGE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_CALCULATION_LOGIC_TABLE_ID }}` AS target
USING (
  WITH
  -- Aggregate distinct full reason strings per vmp_code, joined with ' OR ' when reasons differ across matched combos
  why_not_agg AS (
    SELECT
      vmp_code,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT TRIM(why_ddd_not_chosen) ORDER BY TRIM(why_ddd_not_chosen)), ' OR ') AS why_not_agg
    FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_COMBINED_PRODUCTS_LOGIC_TABLE_ID }}`
    WHERE why_ddd_not_chosen IS NOT NULL AND TRIM(why_ddd_not_chosen) != ''
    GROUP BY vmp_code
  ),
  full_agg AS (
    SELECT
      dcl.vmp_code,
      MAX(CASE WHEN dcl.why_ddd_not_chosen IS NULL THEN 1 ELSE 0 END) AS has_chosen_ddd,
      ANY_VALUE(wnd.why_not_agg) AS why_not_agg
    FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_COMBINED_PRODUCTS_LOGIC_TABLE_ID }}` AS dcl
    LEFT JOIN why_not_agg wnd ON dcl.vmp_code = wnd.vmp_code
    GROUP BY dcl.vmp_code
  ),
  -- Among passing rows (why_ddd_not_chosen IS NULL), prefer exact match (strength_ratio = 1) over proportional
  passing AS (
    SELECT vmp_code, chosen_ddd_value, chosen_ddd_unit, strength_ratio
    FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_COMBINED_PRODUCTS_LOGIC_TABLE_ID }}`
    WHERE why_ddd_not_chosen IS NULL
  ),
  with_has_exact AS (
    SELECT *,
      MAX(CASE WHEN strength_ratio = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY vmp_code) AS has_exact_in_vmp
    FROM passing
  ),
  preferred AS (
    SELECT vmp_code, chosen_ddd_value, chosen_ddd_unit
    FROM with_has_exact
    WHERE (has_exact_in_vmp = 1 AND strength_ratio = 1) OR (has_exact_in_vmp = 0)
  ),
  -- Extract chosen_ddd_value/unit only when exactly one row per vmp_code
  preferred_agg AS (
    SELECT
      vmp_code,
      CASE WHEN COUNT(*) = 1 THEN ANY_VALUE(chosen_ddd_value) ELSE NULL END AS chosen_ddd_value,
      CASE WHEN COUNT(*) = 1 THEN ANY_VALUE(chosen_ddd_unit) ELSE NULL END AS chosen_ddd_unit,
      COUNT(*) AS chosen_row_count
    FROM preferred
    GROUP BY vmp_code
  ),
  combined_agg AS (
    SELECT
      fa.vmp_code,
      pa.chosen_ddd_value,
      pa.chosen_ddd_unit,
      COALESCE(pa.chosen_row_count, 0) AS chosen_row_count,
      fa.has_chosen_ddd,
      fa.why_not_agg
    FROM full_agg fa
    LEFT JOIN preferred_agg pa ON fa.vmp_code = pa.vmp_code
  ),
  -- chosen_ddd_unit comes from ddd_converted_basis_unit (populate_ddd_combined_products_logic), so unit = basis.
  updates AS (
    SELECT
      vmp_code,
      CASE WHEN has_chosen_ddd = 1 AND chosen_row_count = 1 THEN chosen_ddd_value ELSE NULL END AS selected_ddd_value,
      CASE WHEN has_chosen_ddd = 1 AND chosen_row_count = 1 THEN chosen_ddd_unit ELSE NULL END AS selected_ddd_unit,
      CASE WHEN has_chosen_ddd = 1 AND chosen_row_count = 1 THEN chosen_ddd_unit ELSE NULL END AS selected_ddd_basis_unit,
      CASE WHEN has_chosen_ddd = 1 AND chosen_row_count = 1 THEN TRUE ELSE FALSE END AS can_calculate_ddd,
      CASE
        WHEN chosen_row_count > 1 THEN 'Not calculated: Combination product - Multiple potential combined DDD values'
        WHEN has_chosen_ddd = 1 THEN 'Calculated using DDD for combined product'
        ELSE CONCAT('Not calculated: Combination product - ', COALESCE(why_not_agg, ''))
      END AS ddd_calculation_logic
    FROM combined_agg
  )
  SELECT * FROM updates
) AS source
ON target.vmp_code = source.vmp_code
WHEN MATCHED THEN
  UPDATE SET
    can_calculate_ddd = source.can_calculate_ddd,
    ddd_calculation_logic = source.ddd_calculation_logic,
    selected_ddd_value = source.selected_ddd_value,
    selected_ddd_unit = source.selected_ddd_unit,
    selected_ddd_basis_unit = source.selected_ddd_basis_unit
