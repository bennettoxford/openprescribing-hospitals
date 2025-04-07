CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ INGREDIENT_QUANTITY_TABLE_ID }}`
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
    processed.normalised_quantity as quantity,
    processed.indicative_cost as cost,
    dmd.vtm,
    dmd.vtm_name,
    dmd.df_ind,
    dmd.udfs as converted_udfs,
    dmd.udfs_uom as udfs_basis,
    dmd.unit_dose_uom,
    dmd.dform_form,
    dmd.vmp_name as dmd_vmp_name,
    dmd.ingredients
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}` processed
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` dmd
    ON processed.vmp_code = dmd.vmp_code
),
ingredient_calcs AS (
  SELECT 
    n.*,
    org.ods_name,
    ing.*,
    uc.basis as ingredient_basis_unit,
    uc.conversion_factor,
    CASE
        WHEN ing.ing_code IS NULL THEN NULL
        WHEN ing.strnt_dnmtr_uom_name IS NULL AND ing.strnt_nmrtr_uom_name IS NULL THEN NULL
        WHEN LOWER(ing.strnt_dnmtr_uom_name) = LOWER(n.uom) THEN
            (n.quantity / ing.strnt_dnmtr_val) * ing.strnt_nmrtr_val        
        WHEN n.df_ind = 'Discrete' THEN
            CASE
            WHEN LOWER(n.uom) = LOWER(ing.strnt_nmrtr_uom_name) THEN n.quantity
            WHEN ing.strnt_dnmtr_uom_name IS NULL THEN n.quantity * ing.strnt_nmrtr_val
            ELSE NULL
            END
        ELSE NULL
    END AS ingredient_quantity,
    CASE
        WHEN ing.ing_code IS NULL THEN 'Not calculated: No ingredient'
        WHEN ing.strnt_dnmtr_uom_name IS NULL AND ing.strnt_nmrtr_uom_name IS NULL THEN "Not calculated: No strength info"
        WHEN LOWER(ing.strnt_dnmtr_uom_name) = LOWER(n.uom) THEN
            '(SCMD quantity / strength denominator value) * strength numerator value'      
        WHEN n.df_ind = 'Discrete' THEN
            CASE
            WHEN LOWER(n.uom) = LOWER(ing.strnt_nmrtr_uom_name) THEN 'SCMD quantity'
            WHEN ing.strnt_dnmtr_uom_name IS NULL THEN "SCMD quantity * strength numerator value"
            ELSE "Not calculated: Discrete, numerator basis = quantity basis but has denominator"
            END
        ELSE "Not calculated: Strnt_dnmtr_basis != quantity_basis and not discrete"
    END AS calculation_logic
  FROM normalized_data n
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ ORGANISATION_TABLE_ID }}` AS org
    ON n.ods_code = org.ods_code
  LEFT JOIN UNNEST(ingredients) as ing
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` as uc
    ON LOWER(ing.strnt_nmrtr_uom_name) = LOWER(uc.unit)
)
SELECT 
  vmp_code,
  year_month,
  ods_code,
  ods_name,
  vmp_name,
  quantity as converted_quantity,
  uom as quantity_basis,
  ARRAY_AGG(
    STRUCT(
      ing_code as ingredient_code,
      ing_name as ingredient_name,
      ingredient_quantity,
      strnt_nmrtr_uom_name as ingredient_unit,
      CASE 
        WHEN ingredient_quantity IS NOT NULL AND conversion_factor IS NOT NULL 
        THEN ingredient_quantity * conversion_factor 
        ELSE NULL 
      END as ingredient_quantity_basis,
      ingredient_basis_unit,
      strnt_nmrtr_val as strength_numerator_value,
      strnt_nmrtr_uom_name as strength_numerator_unit,
      strnt_dnmtr_val as strength_denominator_value,
      strnt_dnmtr_uom_name as strength_denominator_unit,
      calculation_logic
    )
  ) as ingredients
FROM ingredient_calcs
GROUP BY 
  vmp_code,
  year_month,
  ods_code,
  ods_name,
  vmp_name,
  quantity,
  uom