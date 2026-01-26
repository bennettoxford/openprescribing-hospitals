CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_REFERS_TO_TABLE_ID }}`
AS

WITH ddd_comments_with_refers_to AS (
  SELECT DISTINCT
    comment AS ddd_comment,
    -- Extract ingredient name from "Refers to [ingredient]" pattern
    CASE 
      WHEN LOWER(comment) LIKE 'refers to %' THEN 
        TRIM(REGEXP_REPLACE(comment, r'(?i)^refers to[[:space:]]+', ''))
      ELSE NULL
    END AS refers_to_ingredient
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ WHO_DDD_TABLE_ID }}`
  WHERE comment IS NOT NULL 
    AND LOWER(comment) LIKE 'refers to %'
    AND LOWER(comment) != 'refers to sc injection' -- This is a different type of DDD comment handled elsewhere
),

-- Get all unique ingredients from dm+d data (full table)
dmd_ingredients AS (
  SELECT DISTINCT
    ingredient.ing_code AS dmd_ingredient_code,
    ingredient.ing_name AS dmd_ingredient_name
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_FULL_TABLE_ID }}`,
  UNNEST(ingredients) AS ingredient
  WHERE ingredient.ing_code IS NOT NULL
    AND ingredient.ing_name IS NOT NULL
),

-- Match ingredients to refers_to_ingredient
ingredient_matches AS (
  SELECT 
    dc.ddd_comment,
    dc.refers_to_ingredient,
    di.dmd_ingredient_code,
    di.dmd_ingredient_name
  FROM ddd_comments_with_refers_to dc
  INNER JOIN dmd_ingredients di 
    ON (
      STARTS_WITH(LOWER(TRIM(di.dmd_ingredient_name)), LOWER(TRIM(dc.refers_to_ingredient)))
      OR (
        LOWER(TRIM(dc.refers_to_ingredient)) = 'alendronic acid'
        AND LOWER(di.dmd_ingredient_name) LIKE '%alendronate%'
      )
      OR (
        LOWER(TRIM(dc.refers_to_ingredient)) = 'risedronic acid'
        AND LOWER(di.dmd_ingredient_name) LIKE '%risedronate%'
      )
    )
  WHERE dc.refers_to_ingredient IS NOT NULL
)

SELECT 
  ddd_comment,
  refers_to_ingredient,
  ARRAY_AGG(
    STRUCT(
      dmd_ingredient_code,
      dmd_ingredient_name
    )
    ORDER BY dmd_ingredient_name
  ) AS dmd_ingredients
FROM ingredient_matches
GROUP BY ddd_comment, refers_to_ingredient
ORDER BY ddd_comment, refers_to_ingredient