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
    processed.uom_id as original_uom_id,
    processed.normalised_uom_id as normalised_uom_id,
    processed.normalised_quantity as quantity,
    dmd.routes,
    CAST(dmd_supp.atc_code AS STRING) as atc_code,
    CAST(ddd.ddd AS FLOAT64) as ddd_value,
    CAST(ddd.ddd_unit AS STRING) as ddd_unit,

    ing.ingredients[SAFE_OFFSET(0)].ingredient_quantity as ingredient_quantity,
    CAST(ing.ingredients[SAFE_OFFSET(0)].ingredient_unit AS STRING) as ingredient_unit,
    ARRAY_LENGTH(ing.ingredients) as ingredient_count
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}` processed
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` dmd
    ON CAST(processed.vmp_code AS STRING) = CAST(dmd.vmp_code AS STRING)
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_SUPP_TABLE_ID }}` dmd_supp
    ON CAST(processed.vmp_code AS STRING) = CAST(dmd_supp.vmp_code AS STRING)
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ WHO_DDD_TABLE_ID }}` ddd
    ON CAST(dmd_supp.atc_code AS STRING) = CAST(ddd.atc_code AS STRING)
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ INGREDIENT_QUANTITY_TABLE_ID }}` ing
    ON CAST(processed.vmp_code AS STRING) = CAST(ing.vmp_code AS STRING)
    AND processed.year_month = ing.year_month
    AND CAST(processed.ods_code AS STRING) = CAST(ing.ods_code AS STRING)
),
ddd_unit_conversion AS (
  SELECT 
    n.*,

    CAST(ddd_uc.basis AS STRING) as ddd_basis,
    CAST(ddd_uc.conversion_factor AS FLOAT64) as ddd_conversion_factor,
    CAST(scmd_uc.basis AS STRING) as scmd_basis,

    CAST(ing_uc.basis AS STRING) as ingredient_basis,
    CAST(ing_uc.conversion_factor AS FLOAT64) as ingredient_conversion_factor,
    org.ods_name
  FROM normalized_data n
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` ddd_uc
    ON CAST(n.ddd_unit AS STRING) = CAST(ddd_uc.unit AS STRING)
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` scmd_uc
    ON CAST(n.normalised_uom_id AS STRING) = CAST(scmd_uc.unit AS STRING)
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` ing_uc
    ON CAST(n.ingredient_unit AS STRING) = CAST(ing_uc.unit AS STRING)
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ ORGANISATION_TABLE_ID }}` AS org
    ON CAST(n.ods_code AS STRING) = CAST(org.ods_code AS STRING)
),
ddd_calcs AS (
  SELECT 
    *,
    CASE
      WHEN ddd_value IS NULL THEN NULL
      WHEN ddd_unit IS NULL THEN NULL

      WHEN ddd_basis IS NOT NULL 
        AND scmd_basis IS NOT NULL 
        AND ddd_basis = scmd_basis
        AND ddd_conversion_factor IS NOT NULL
        AND quantity IS NOT NULL
        THEN quantity / (ddd_value * ddd_conversion_factor)

      WHEN ingredient_count = 1 
        AND ddd_basis IS NOT NULL
        AND ingredient_basis IS NOT NULL
        AND ddd_basis = ingredient_basis
        AND ingredient_quantity IS NOT NULL 
        AND ddd_conversion_factor IS NOT NULL
        THEN (quantity * ingredient_quantity) / (ddd_value * ddd_conversion_factor)
      ELSE NULL
    END AS ddd_quantity,
    CASE
      WHEN ddd_value IS NULL THEN 'Not calculated: No DDD value'
      WHEN ddd_unit IS NULL THEN 'Not calculated: No DDD unit'
      WHEN ddd_basis IS NULL THEN 'Not calculated: Cannot convert DDD unit'
      WHEN scmd_basis IS NULL THEN 'Not calculated: Cannot convert SCMD unit'
      WHEN ddd_basis != scmd_basis THEN 
        CONCAT('Not calculated: Incompatible units (', 
               COALESCE(scmd_basis, 'unknown'), 
               ' vs ', 
               COALESCE(ddd_basis, 'unknown'),
               ')')
      WHEN ddd_basis = scmd_basis 
        THEN 'SCMD quantity / (DDD value * conversion factor)'
      WHEN ingredient_count = 1 
        AND ddd_basis IS NOT NULL
        AND ingredient_basis IS NOT NULL
        AND ddd_basis = ingredient_basis
        AND ingredient_quantity IS NOT NULL 
        THEN 'Using ingredient quantity: (SCMD quantity * ingredient quantity) / (DDD value * conversion factor)'
      WHEN ingredient_count > 1 
        THEN 'Not calculated: Multiple ingredients'
      ELSE 'Not calculated: Units not convertible to same basis'
    END AS calculation_logic
  FROM ddd_unit_conversion
)
SELECT 
  vmp_code,
  year_month,
  ods_code,
  ods_name,
  vmp_name,
  quantity as converted_quantity,
  normalised_uom_id as quantity_basis,
  original_uom_id,
  ddd_quantity,
  ddd_value,
  ddd_unit,
  calculation_logic,
  scmd_basis,
  ddd_basis
FROM ddd_calcs
WHERE ddd_quantity IS NOT NULL
  AND calculation_logic IN (
    'SCMD quantity / (DDD value * conversion factor)',
    'Using ingredient quantity: (SCMD quantity * ingredient quantity) / (DDD value * conversion factor)'
  )
