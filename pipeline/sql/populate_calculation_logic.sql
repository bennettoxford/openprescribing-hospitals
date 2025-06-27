CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ CALCULATION_LOGIC_TABLE_ID }}`
CLUSTER BY vmp_code, logic_type
AS

WITH
dose_logic AS (
  SELECT DISTINCT
    vmp_code,
    CAST(NULL AS STRING) AS ingredient_code,
    'dose' AS logic_type,
    calculation_logic AS logic
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DOSE_TABLE_ID }}`
  WHERE calculation_logic IS NOT NULL
),

ingredient_logic AS (
  SELECT DISTINCT
    vmp_code,
    ingredient.ingredient_code,
    'ingredient' AS logic_type,
    ingredient.calculation_logic AS logic
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ INGREDIENT_QUANTITY_TABLE_ID }}`,
  UNNEST(ingredients) AS ingredient
  WHERE ingredient.calculation_logic IS NOT NULL
    AND ingredient.ingredient_code IS NOT NULL
),

ddd_logic AS (
  SELECT DISTINCT
    vmp_code,
    ingredient_code,
    'ddd' AS logic_type,
    calculation_logic AS logic
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_QUANTITY_TABLE_ID }}`
  WHERE calculation_logic IS NOT NULL
),

all_logic AS (
  SELECT * FROM dose_logic
  UNION ALL
  SELECT * FROM ingredient_logic  
  UNION ALL
  SELECT * FROM ddd_logic
)

SELECT 
  vmp_code,
  ingredient_code,
  logic_type,
  logic
FROM all_logic
WHERE vmp_code IS NOT NULL
  AND logic IS NOT NULL
  AND TRIM(logic) != ''