CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_QUANTITY_TABLE_ID }}`
PARTITION BY year_month
CLUSTER BY vmp_code
AS
WITH normalized_data AS (
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
    ddd_logic.refers_to_ingredient,
    ddd_logic.selected_ddd_value,
    ddd_logic.selected_ddd_unit,
    -- Convert DDD to basis units
    CASE 
      WHEN ddd_logic.selected_ddd_value IS NOT NULL AND unit_conv.conversion_factor IS NOT NULL
      THEN ddd_logic.selected_ddd_value * unit_conv.conversion_factor
      ELSE ddd_logic.selected_ddd_value
    END AS selected_ddd_basis_value,
    ddd_logic.selected_ddd_basis_unit,
    -- Extract ingredient info when calculation uses ingredient quantity
    CASE 
      WHEN ddd_logic.ddd_calculation_logic LIKE 'Ingredient quantity%' AND iq.ingredients IS NOT NULL THEN
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
          -- For single ingredient case: use the first and only ingredient
          WHEN ddd_logic.ddd_calculation_logic = 'Ingredient quantity / DDD' THEN
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
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` unit_conv
    ON ddd_logic.selected_ddd_unit = unit_conv.unit
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ INGREDIENT_QUANTITY_TABLE_ID }}` iq
    ON n.vmp_code = iq.vmp_code 
    AND n.year_month = iq.year_month 
    AND n.ods_code = iq.ods_code
    AND ddd_logic.ddd_calculation_logic LIKE 'Ingredient quantity%'
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
  ingredient_info.ingredient_code,
  ddd_calculation_logic AS calculation_logic
FROM ddd_calculations
