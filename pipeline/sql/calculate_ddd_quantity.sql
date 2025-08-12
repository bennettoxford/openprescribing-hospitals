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

ingredient_data AS (
  SELECT
    iq.year_month,
    iq.ods_code,
    iq.vmp_code,
    ddd_map.can_calculate_ddd,
    ddd_map.ddd_calculation_logic,
    ARRAY_LENGTH(iq.ingredients) AS ingredient_count,
    
    ddd_map.has_expressed_as_data,
    ddd_map.expressed_as_strength,
    ddd_map.expressed_as_strength_unit,
    ddd_map.expressed_as_comment,
    
    -- Convert expressed_as strength per unit to basis units if available
    CASE 
      WHEN ddd_map.has_expressed_as_data = TRUE 
        AND ddd_map.expressed_as_strength IS NOT NULL THEN
        ddd_map.expressed_as_strength * COALESCE(unit_conv.conversion_factor, 1)
      ELSE NULL
    END AS expressed_as_basis_quantity_per_unit,
    
    CASE 
      WHEN ddd_map.has_expressed_as_data = TRUE 
        AND ddd_map.expressed_as_strength IS NOT NULL THEN
        COALESCE(unit_conv.basis, ddd_map.expressed_as_strength_unit)
      ELSE NULL
    END AS expressed_as_basis_unit,
    
    CASE 
      -- Single ingredient case (existing logic)
      WHEN ARRAY_LENGTH(iq.ingredients) = 1 THEN
        (SELECT ingredient_quantity_basis FROM UNNEST(iq.ingredients) LIMIT 1)
      
      -- Multi-ingredient "refers to" case
      WHEN ARRAY_LENGTH(iq.ingredients) > 1 
        AND ddd_map.can_calculate_ddd = TRUE
        AND ddd_map.ddd_calculation_logic LIKE 'Ingredient quantity / DDD (Refers to %'
      THEN
        -- Extract ingredient name from the calculation logic
        (SELECT ingredient_quantity_basis 
         FROM UNNEST(iq.ingredients) ing
         WHERE UPPER(ing.ingredient_name) = UPPER(
           TRIM(REGEXP_REPLACE(
             REGEXP_REPLACE(
               ddd_map.ddd_calculation_logic, 
               r'.*\(Refers to ', ''
             ), 
             r'\).*', ''
           ))
         )
         LIMIT 1)
      
      ELSE NULL
    END AS ingredient_basis_quantity,
    
    CASE 
      -- For VMPs with expressed_as data, use the DDD basis unit
      WHEN ddd_map.has_expressed_as_data = TRUE 
        AND ddd_map.expressed_as_strength IS NOT NULL THEN
        ddd_map.selected_ddd_basis_unit
      
      -- Single ingredient case
      WHEN ARRAY_LENGTH(iq.ingredients) = 1 THEN
        (SELECT ingredient_basis_unit FROM UNNEST(iq.ingredients) LIMIT 1)
      
      -- Multi-ingredient "refers to" case
      WHEN ARRAY_LENGTH(iq.ingredients) > 1 
        AND ddd_map.can_calculate_ddd = TRUE
        AND ddd_map.ddd_calculation_logic LIKE 'Ingredient quantity / DDD (Refers to %'
      THEN
        (SELECT ingredient_basis_unit 
         FROM UNNEST(iq.ingredients) ing
         WHERE UPPER(ing.ingredient_name) = UPPER(
           TRIM(REGEXP_REPLACE(
             REGEXP_REPLACE(
               ddd_map.ddd_calculation_logic, 
               r'.*\(Refers to ', ''
             ), 
             r'\).*', ''
           ))
         )
         LIMIT 1)
      
      ELSE NULL
    END AS ingredient_basis_unit,
    
    -- Get the ingredient code for single ingredient products or "refers to" cases
    CASE 
      -- Single ingredient case
      WHEN ARRAY_LENGTH(iq.ingredients) = 1 THEN
        (SELECT ingredient_code FROM UNNEST(iq.ingredients) LIMIT 1)
      
      -- Multi-ingredient "refers to" case
      WHEN ARRAY_LENGTH(iq.ingredients) > 1 
        AND ddd_map.can_calculate_ddd = TRUE
        AND ddd_map.ddd_calculation_logic LIKE 'Ingredient quantity / DDD (Refers to %'
      THEN
        (SELECT ingredient_code 
         FROM UNNEST(iq.ingredients) ing
         WHERE UPPER(ing.ingredient_name) = UPPER(
           TRIM(REGEXP_REPLACE(
             REGEXP_REPLACE(
               ddd_map.ddd_calculation_logic, 
               r'.*\(Refers to ', ''
             ), 
             r'\).*', ''
           ))
         )
         LIMIT 1)
      
      ELSE NULL
    END AS ingredient_code,
    
    -- Get DDD information from DDD mapping table
    ddd_map.selected_ddd_value,
    ddd_map.selected_ddd_unit,
    ddd_map.selected_ddd_basis_value,
    ddd_map.selected_ddd_basis_unit
    
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ INGREDIENT_QUANTITY_TABLE_ID }}` iq
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_DDD_MAPPING_TABLE_ID }}` ddd_map
    ON iq.vmp_code = ddd_map.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` unit_conv
    ON ddd_map.expressed_as_strength_unit = unit_conv.unit
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
    -- For expressed_as cases, calculate total expressed_as quantity
    CASE 
      WHEN i.has_expressed_as_data = TRUE 
        AND i.expressed_as_basis_quantity_per_unit IS NOT NULL THEN
        n.quantity * i.expressed_as_basis_quantity_per_unit
      ELSE i.ingredient_basis_quantity
    END AS ingredient_basis_quantity,
    i.ingredient_basis_unit,
    i.ingredient_code,
    i.ingredient_count,
    i.can_calculate_ddd,
    i.ddd_calculation_logic,
    i.selected_ddd_value,
    i.selected_ddd_unit,
    i.selected_ddd_basis_value,
    i.selected_ddd_basis_unit,
    i.has_expressed_as_data,
    i.expressed_as_strength,
    i.expressed_as_basis_quantity_per_unit,
    i.expressed_as_basis_unit,
    i.expressed_as_comment
  FROM normalized_data n
  LEFT JOIN ingredient_data i
    ON n.vmp_code = i.vmp_code 
    AND n.year_month = i.year_month 
    AND n.ods_code = i.ods_code
),

ddd_calculations AS (
  SELECT
    *,
    CASE
      -- When using SCMD quantity directly (basis units match)
      WHEN can_calculate_ddd 
        AND selected_ddd_basis_value > 0 
        AND uom_name = selected_ddd_basis_unit THEN
          quantity / selected_ddd_basis_value
          
      -- When using ingredient quantity
      WHEN can_calculate_ddd 
        AND selected_ddd_basis_value > 0 
        AND ingredient_basis_quantity IS NOT NULL
        AND ingredient_basis_unit = selected_ddd_basis_unit THEN
          ingredient_basis_quantity / selected_ddd_basis_value
          
      ELSE NULL
    END AS ddd_quantity,
    
    CASE
      WHEN NOT can_calculate_ddd THEN 
        ddd_calculation_logic
      WHEN selected_ddd_value = 0 OR selected_ddd_basis_value = 0 THEN
        'Not calculated: DDD value is zero'
      WHEN uom_name = selected_ddd_basis_unit THEN
        'SCMD quantity / DDD'
      WHEN ingredient_basis_quantity IS NOT NULL 
        AND ingredient_basis_unit = selected_ddd_basis_unit THEN 
        CASE 
          WHEN has_expressed_as_data = TRUE THEN 
            CONCAT('Expressed as quantity / DDD (', expressed_as_comment, ')')
          WHEN ingredient_count = 1 THEN 
            'Ingredient quantity / DDD'
          ELSE 
            ddd_calculation_logic
        END
      ELSE
        'Not calculated: Unit incompatibility'
    END AS calculation_logic
  FROM data_with_ddd
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
  CASE 
    WHEN calculation_logic LIKE '%Ingredient quantity / DDD%' 
      OR calculation_logic LIKE '%Expressed as quantity / DDD%' 
    THEN ingredient_code
    ELSE NULL
  END AS ingredient_code,
  calculation_logic
FROM ddd_calculations
