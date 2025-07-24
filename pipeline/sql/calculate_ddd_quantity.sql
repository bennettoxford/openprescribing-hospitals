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
    vmp.can_calculate_ddd,
    vmp.ddd_calculation_logic,
    ARRAY_LENGTH(iq.ingredients) AS ingredient_count,
    
    CASE 
      -- Single ingredient case (existing logic)
      WHEN ARRAY_LENGTH(iq.ingredients) = 1 THEN
        (SELECT ingredient_quantity_basis FROM UNNEST(iq.ingredients) LIMIT 1)
      
      -- Multi-ingredient "refers to" case
      WHEN ARRAY_LENGTH(iq.ingredients) > 1 
        AND vmp.can_calculate_ddd = TRUE
        AND vmp.ddd_calculation_logic LIKE 'Ingredient quantity / DDD (Refers to %'
      THEN
        -- Extract ingredient name from the calculation logic
        (SELECT ingredient_quantity_basis 
         FROM UNNEST(iq.ingredients) ing
         WHERE UPPER(ing.ingredient_name) = UPPER(
           TRIM(REGEXP_REPLACE(
             REGEXP_REPLACE(
               vmp.ddd_calculation_logic, 
               r'.*\(Refers to ', ''
             ), 
             r'\).*', ''
           ))
         )
         LIMIT 1)
      
      ELSE NULL
    END AS ingredient_basis_quantity,
    
    CASE 
      -- Single ingredient case
      WHEN ARRAY_LENGTH(iq.ingredients) = 1 THEN
        (SELECT ingredient_basis_unit FROM UNNEST(iq.ingredients) LIMIT 1)
      
      -- Multi-ingredient "refers to" case
      WHEN ARRAY_LENGTH(iq.ingredients) > 1 
        AND vmp.can_calculate_ddd = TRUE
        AND vmp.ddd_calculation_logic LIKE 'Ingredient quantity / DDD (Refers to %'
      THEN
        (SELECT ingredient_basis_unit 
         FROM UNNEST(iq.ingredients) ing
         WHERE UPPER(ing.ingredient_name) = UPPER(
           TRIM(REGEXP_REPLACE(
             REGEXP_REPLACE(
               vmp.ddd_calculation_logic, 
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
        AND vmp.can_calculate_ddd = TRUE
        AND vmp.ddd_calculation_logic LIKE 'Ingredient quantity / DDD (Refers to %'
      THEN
        (SELECT ingredient_code 
         FROM UNNEST(iq.ingredients) ing
         WHERE UPPER(ing.ingredient_name) = UPPER(
           TRIM(REGEXP_REPLACE(
             REGEXP_REPLACE(
               vmp.ddd_calculation_logic, 
               r'.*\(Refers to ', ''
             ), 
             r'\).*', ''
           ))
         )
         LIMIT 1)
      
      ELSE NULL
    END AS ingredient_code,
    
    -- Get DDD information from VMP table
    vmp.selected_ddd_value,
    vmp.selected_ddd_unit,
    vmp.selected_ddd_basis_value,
    vmp.selected_ddd_basis_unit
    
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ INGREDIENT_QUANTITY_TABLE_ID }}` iq
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_TABLE_ID }}` vmp
    ON iq.vmp_code = vmp.vmp_code
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
    i.ingredient_code,
    i.ingredient_count,
    i.can_calculate_ddd,
    i.ddd_calculation_logic,
    i.selected_ddd_value,
    i.selected_ddd_unit,
    i.selected_ddd_basis_value,
    i.selected_ddd_basis_unit
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
          
      -- When using ingredient quantity (works for both single and multi-ingredient "refers to" cases)
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
          WHEN ingredient_count = 1 THEN 'Ingredient quantity / DDD'
          ELSE ddd_calculation_logic
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
    WHEN calculation_logic LIKE '%Ingredient quantity / DDD%' THEN ingredient_code
    ELSE NULL
  END AS ingredient_code,
  calculation_logic
FROM ddd_calculations
