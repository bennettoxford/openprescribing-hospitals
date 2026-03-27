CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_QUANTITY_TABLE_ID }}`
PARTITION BY year_month
CLUSTER BY vmp_code
AS
WITH 
lithium_vtms AS (
  SELECT 
    '776556004' AS vtm_code,
    'Lithium citrate' AS vtm_name,
    94.26/1000 AS grams_per_mmol
  UNION ALL
  SELECT 
    '776555000' AS vtm_code,
    'Lithium carbonate' AS vtm_name,
    37.04/1000 AS grams_per_mmol
),

normalized_data AS (
  SELECT
    processed.year_month,
    processed.ods_code,
    processed.vmp_code,
    processed.vmp_name,
    processed.normalised_uom_id as uom,
    processed.normalised_uom_name as uom_name,
    processed.normalised_quantity as quantity
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}` processed
),

-- Get DDD calculation logic and extract ingredient info when needed
data_with_ddd_logic AS (
  SELECT
    n.year_month,
    n.ods_code,
    n.vmp_code,
    n.vmp_name,
    n.uom,
    n.uom_name,
    n.quantity,
    ddd_logic.can_calculate_ddd,
    ddd_logic.ddd_calculation_logic,
    ddd_logic.override_comments,
    ddd_logic.refers_to_ingredient,
    ddd_logic.selected_ddd_value,
    ddd_logic.selected_ddd_unit,
    ddd_logic.expressed_as_ingredient_code,
    ddd_logic.expressed_as_strnt_nmrtr,
    ddd_logic.expressed_as_strnt_nmrtr_uom_name,
    lv.grams_per_mmol AS lithium_grams_per_mmol,
    -- Convert DDD to basis units
    CASE 
      WHEN ddd_logic.selected_ddd_value IS NOT NULL AND unit_conv.conversion_factor IS NOT NULL
      THEN ddd_logic.selected_ddd_value * unit_conv.conversion_factor
      ELSE ddd_logic.selected_ddd_value
    END AS selected_ddd_basis_value,
    ddd_logic.selected_ddd_basis_unit,
    -- Convert expressed_as strength numerator to basis units
    CASE
      WHEN ddd_logic.expressed_as_strnt_nmrtr IS NOT NULL 
        AND expressed_as_unit_conv.conversion_factor IS NOT NULL
      THEN ddd_logic.expressed_as_strnt_nmrtr * expressed_as_unit_conv.conversion_factor
      WHEN ddd_logic.expressed_as_strnt_nmrtr IS NOT NULL
      THEN ddd_logic.expressed_as_strnt_nmrtr
      ELSE NULL
    END AS expressed_as_strnt_nmrtr_basis_val,
    -- Get denominator info for expressed_as ingredient from DDD_CALCULATION_LOGIC_TABLE
    ddd_logic.expressed_as_strnt_dnmtr_basis_val,
    ddd_logic.expressed_as_strnt_dnmtr_basis_uom,
    -- Extract ingredient info when calculation uses ingredient quantity (for non-expressed-as cases)
    CASE 
      -- Strength override: use override from DDD_CALCULATION_LOGIC_TABLE (populated from vmp_strength_overrides)
      WHEN ddd_logic.ddd_calculation_logic LIKE 'Ingredient quantity%' 
        AND ddd_logic.expressed_as_ingredient_code IS NULL
        AND ddd_logic.override_strnt_nmrtr_uom IS NOT NULL
        AND ddd_logic.override_strnt_nmrtr_val IS NOT NULL
        AND override_unit_conv.conversion_factor IS NOT NULL THEN
        (SELECT AS STRUCT 
          ing.ingredient_code,
          n.quantity * ddd_logic.override_strnt_nmrtr_val * override_unit_conv.conversion_factor AS ingredient_quantity_basis,
          COALESCE(override_unit_conv.basis, ddd_logic.selected_ddd_basis_unit) AS ingredient_basis_unit
        FROM UNNEST(ddd_logic.ingredients_info) ing
        LIMIT 1)
      WHEN ddd_logic.ddd_calculation_logic LIKE 'Ingredient quantity%' 
        AND ddd_logic.expressed_as_ingredient_code IS NULL
        AND iq.ingredients IS NOT NULL THEN
        CASE 
          -- For "refers to" case: find the single matching ingredient
          WHEN ddd_logic.refers_to_ingredient IS NOT NULL THEN
            (SELECT AS STRUCT 
              ingredient_code,
              ingredient_quantity_basis,
              ingredient_basis_unit
            FROM UNNEST(iq.ingredients) ing
            WHERE UPPER(ing.ingredient_name) = UPPER(TRIM(ddd_logic.refers_to_ingredient))
            LIMIT 1)
          -- For single ingredient case (including lithium): use the first and only ingredient
          WHEN ddd_logic.ddd_calculation_logic LIKE 'Ingredient quantity%' THEN
            (SELECT AS STRUCT 
              ingredient_code,
              ingredient_quantity_basis,
              ingredient_basis_unit
            FROM UNNEST(iq.ingredients) LIMIT 1)
          ELSE NULL
        END
      ELSE NULL
    END AS ingredient_info
  FROM normalized_data n
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_CALCULATION_LOGIC_TABLE_ID }}` ddd_logic
    ON n.vmp_code = ddd_logic.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_TABLE_ID }}` vmp
    ON n.vmp_code = vmp.vmp_code
  LEFT JOIN lithium_vtms lv
    ON vmp.vtm_code = lv.vtm_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` unit_conv
    ON ddd_logic.selected_ddd_unit = unit_conv.unit
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` expressed_as_unit_conv
    ON ddd_logic.expressed_as_strnt_nmrtr_uom_name = expressed_as_unit_conv.unit
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ INGREDIENT_QUANTITY_TABLE_ID }}` iq
    ON n.vmp_code = iq.vmp_code 
    AND n.year_month = iq.year_month 
    AND n.ods_code = iq.ods_code
    AND ddd_logic.ddd_calculation_logic LIKE 'Ingredient quantity%'
    AND ddd_logic.expressed_as_ingredient_code IS NULL
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` override_unit_conv
    ON ddd_logic.override_strnt_nmrtr_uom = override_unit_conv.unit
),

ddd_calculations AS (
  SELECT
    *,
    CASE
      WHEN NOT can_calculate_ddd OR selected_ddd_basis_value IS NULL OR selected_ddd_basis_value <= 0 THEN
        NULL
      -- SCMD quantity / DDD: use normalised quantity directly
      WHEN ddd_calculation_logic = 'SCMD quantity / DDD' 
        AND uom_name = selected_ddd_basis_unit THEN
        quantity / selected_ddd_basis_value
      -- Combined product DDD: same formula as SCMD quantity / DDD
      WHEN ddd_calculation_logic = 'Calculated using DDD for combined product'
        AND uom_name = selected_ddd_basis_unit THEN
        quantity / selected_ddd_basis_value
      -- Ingredient quantity / DDD with "expressed as": calculate using expressed_as strength numerator
      -- Use denominator if available, as when calculating ingredient quantity
      WHEN ddd_calculation_logic LIKE 'Ingredient quantity%' 
        AND expressed_as_ingredient_code IS NOT NULL
        AND expressed_as_strnt_nmrtr_basis_val IS NOT NULL
        AND expressed_as_strnt_nmrtr_basis_val > 0
        AND selected_ddd_basis_unit IS NOT NULL THEN
        CASE

          WHEN expressed_as_strnt_dnmtr_basis_val IS NOT NULL
            AND expressed_as_strnt_dnmtr_basis_val > 0
            AND uom_name = expressed_as_strnt_dnmtr_basis_uom THEN
            ((quantity / expressed_as_strnt_dnmtr_basis_val) * expressed_as_strnt_nmrtr_basis_val) / selected_ddd_basis_value

          ELSE
            (quantity * expressed_as_strnt_nmrtr_basis_val) / selected_ddd_basis_value
        END
      -- Ingredient quantity (mmol lithium): convert grams to mmol, then divide by DDD
      WHEN ddd_calculation_logic LIKE 'Ingredient quantity (mmol lithium%'
        AND ingredient_info IS NOT NULL
        AND lithium_grams_per_mmol IS NOT NULL
        AND lithium_grams_per_mmol > 0
        AND ingredient_info.ingredient_basis_unit = 'gram' THEN
        ((ingredient_info.ingredient_quantity_basis) / lithium_grams_per_mmol) / selected_ddd_basis_value
      -- Ingredient quantity / DDD: use ingredient quantity (applies to both single ingredient and "refers to" cases)
      WHEN ddd_calculation_logic LIKE 'Ingredient quantity%' 
        AND ingredient_info IS NOT NULL
        AND ingredient_info.ingredient_basis_unit = selected_ddd_basis_unit THEN
        ingredient_info.ingredient_quantity_basis / selected_ddd_basis_value
      ELSE NULL
    END AS ddd_quantity
  FROM data_with_ddd_logic
)

SELECT
  vmp_code,
  year_month,
  ods_code,
  vmp_name,
  uom,
  uom_name,
  quantity,
  ddd_quantity,
  selected_ddd_value AS ddd_value,
  selected_ddd_unit AS ddd_unit,
  -- Include ingredient code when DDD calculation uses ingredient quantity
  COALESCE(expressed_as_ingredient_code, ingredient_info.ingredient_code) AS ingredient_code,
  CASE
    WHEN override_comments IS NOT NULL THEN CONCAT(ddd_calculation_logic, ' ("', override_comments, '")')
    ELSE ddd_calculation_logic
  END AS calculation_logic
FROM ddd_calculations
