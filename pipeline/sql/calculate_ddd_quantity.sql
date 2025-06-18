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
    processed.normalised_quantity as quantity,
    processed.indicative_cost as cost
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}` processed
),

ingredient_data AS (
  SELECT
    year_month,
    ods_code,
    vmp_code,
    -- For single ingredient products, get the basis quantity
    CASE 
      WHEN ARRAY_LENGTH(ingredients) = 1 THEN
        (SELECT ingredient_quantity_basis FROM UNNEST(ingredients) LIMIT 1)
      ELSE NULL
    END AS ingredient_basis_quantity,
    CASE 
      WHEN ARRAY_LENGTH(ingredients) = 1 THEN
        (SELECT ingredient_basis_unit FROM UNNEST(ingredients) LIMIT 1)
      ELSE NULL
    END AS ingredient_basis_unit
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ INGREDIENT_QUANTITY_TABLE_ID }}`
),

data_with_ddd AS (
  SELECT
    n.year_month,
    n.ods_code,
    n.vmp_code,
    n.vmp_name,
    n.uom,
    n.uom_name,
    n.quantity,
    i.ingredient_basis_quantity,
    i.ingredient_basis_unit,
    ddd_map.can_calculate_ddd,
    ddd_map.ddd_calculation_logic,
    ddd_map.selected_ddd_value,
    ddd_map.selected_ddd_unit,
    ddd_map.selected_ddd_basis_unit,
    ddd_map.selected_ddd_route_code
  FROM normalized_data n
  LEFT JOIN ingredient_data i
    ON n.vmp_code = i.vmp_code 
    AND n.year_month = i.year_month 
    AND n.ods_code = i.ods_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_DDD_MAPPING_TABLE_ID }}` ddd_map
    ON n.vmp_code = ddd_map.vmp_code
),

data_with_conversions AS (
  SELECT
    d.*,
    ing_unit.conversion_factor AS ingredient_conversion_factor,
    ing_unit.basis AS ingredient_basis,
    ddd_unit.conversion_factor AS ddd_conversion_factor,
    ddd_unit.basis AS ddd_basis
  FROM data_with_ddd d
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` ing_unit
    ON d.ingredient_basis_unit = ing_unit.unit
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` ddd_unit
    ON d.selected_ddd_unit = ddd_unit.unit
),

ddd_calculations AS (
  SELECT
    *,
    CASE
      -- When using SCMD unit directly
      WHEN can_calculate_ddd 
        AND ddd_calculation_logic = 'Calculated using SCMD unit'
        AND selected_ddd_value > 0 THEN
          quantity / (selected_ddd_value * ddd_conversion_factor)
          
      -- When using ingredient quantity
      WHEN can_calculate_ddd 
        AND ddd_calculation_logic = 'Calculated using ingredient quantity'
        AND selected_ddd_value > 0 
        AND ingredient_basis_quantity IS NOT NULL
        AND ingredient_basis = ddd_basis THEN
          ingredient_basis_quantity / (selected_ddd_value * ddd_conversion_factor)
          
      ELSE NULL
    END AS ddd_quantity,
    
    CASE
      WHEN NOT can_calculate_ddd THEN 
        ddd_calculation_logic
      WHEN selected_ddd_value = 0 THEN
        'DDD calculation not possible: DDD value is zero'
      WHEN ddd_calculation_logic = 'Calculated using SCMD unit' THEN
          'DDD calculation using SCMD quantity'
      WHEN ddd_calculation_logic = 'Calculated using ingredient quantity' 
        AND ingredient_basis_quantity IS NOT NULL
        AND ingredient_basis = ddd_basis THEN 'DDD calculation using ingredient quantity'
      ELSE
        'DDD calculation not possible: missing required quantities or incompatible units'
    END AS calculation_logic
  FROM data_with_conversions
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
  calculation_logic
FROM ddd_calculations
