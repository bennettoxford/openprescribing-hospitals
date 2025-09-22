CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_TABLE_ID }}`
CLUSTER BY vmp_code
AS

WITH
scmd_vmps AS (
  SELECT DISTINCT vmp_code 
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}`
),

vmp_base AS (
  SELECT
    dmd.vmp_code,
    dmd.vmp_name,
    dmd.vtm,
    dmd.vtm_name,
    dmd.df_ind,
    dmd.udfs,
    dmd.udfs_uom,
    dmd.unit_dose_uom,
    -- Convert UDFS to basis units
    dmd.udfs * COALESCE(udfs_conv.conversion_factor, 1.0) AS udfs_basis_quantity,
    COALESCE(udfs_conv.basis, dmd.udfs_uom) AS udfs_basis_uom,
    -- Get basis unit for unit dose
    COALESCE(unit_dose_conv.basis, dmd.unit_dose_uom) AS unit_dose_basis_uom
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` dmd
  JOIN scmd_vmps sv ON dmd.vmp_code = sv.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` udfs_conv
    ON dmd.udfs_uom = udfs_conv.unit
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` unit_dose_conv
    ON dmd.unit_dose_uom = unit_dose_conv.unit
),

vmp_bnf AS (
  SELECT
    supp.vmp_code,
    supp.bnf_code
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_SUPP_TABLE_ID }}` supp
  JOIN scmd_vmps sv ON supp.vmp_code = sv.vmp_code
  WHERE supp.bnf_code IS NOT NULL
),

vmp_ingredients AS (
  SELECT
    dmd.vmp_code,
    ARRAY_AGG(
      STRUCT(
        ing.ing_code AS ingredient_code,
        ing.ing_name AS ingredient_name,
        ing.strnt_nmrtr_val,
        ing.strnt_nmrtr_uom_name,
        -- Convert strength numerator to basis units
        ing.strnt_nmrtr_val * COALESCE(num_conv.conversion_factor, 1.0) AS strnt_nmrtr_basis_val,
        COALESCE(num_conv.basis, ing.strnt_nmrtr_uom_name) AS strnt_nmrtr_basis_uom,
        ing.strnt_dnmtr_val,
        ing.strnt_dnmtr_uom_name,
        -- Convert strength denominator to basis units
        ing.strnt_dnmtr_val * COALESCE(denom_conv.conversion_factor, 1.0) AS strnt_dnmtr_basis_val,
        COALESCE(denom_conv.basis, ing.strnt_dnmtr_uom_name) AS strnt_dnmtr_basis_uom,
        ing.basis_of_strength_type,
        ing.basis_of_strength_name
      )
    ) AS ingredients
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` dmd,
  UNNEST(ingredients) AS ing
  JOIN scmd_vmps sv ON dmd.vmp_code = sv.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` num_conv
    ON ing.strnt_nmrtr_uom_name = num_conv.unit
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` denom_conv
    ON ing.strnt_dnmtr_uom_name = denom_conv.unit
  GROUP BY dmd.vmp_code
),

vmp_routes AS (
  SELECT
    dmd.vmp_code,
    ARRAY_AGG(
      STRUCT(
        route.ontformroute_cd AS route_code,
        route.ontformroute_descr AS route_name
      )
    ) AS ont_form_routes
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` dmd,
  UNNEST(ontformroutes) AS route
  JOIN scmd_vmps sv ON dmd.vmp_code = sv.vmp_code
  GROUP BY dmd.vmp_code
),

vmp_atc_mappings AS (
  SELECT
    atc.vmp_code,
    ARRAY_AGG(
      STRUCT(
        atc.atc_code,
        who_atc.atc_name
      )
    ) AS atcs
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_SUPP_TABLE_ID }}` atc
  JOIN scmd_vmps sv ON atc.vmp_code = sv.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ WHO_ATC_TABLE_ID }}` who_atc
    ON atc.atc_code = who_atc.atc_code
  GROUP BY atc.vmp_code
),

vmp_amps AS (
  SELECT
    dmd.vmp_code,
    ARRAY_AGG(
      STRUCT(
        amp.amp_code,
        amp.amp_name,
        amp.avail_restrict
      )
    ) AS amps
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` dmd,
  UNNEST(amps) AS amp
  JOIN scmd_vmps sv ON dmd.vmp_code = sv.vmp_code
  GROUP BY dmd.vmp_code
),

vmp_special_status AS (
  SELECT
    dmd.vmp_code,
    CASE
      WHEN SUM(CASE WHEN amp.avail_restrict = 'Special' THEN 1 ELSE 0 END) > 0 
           AND SUM(CASE WHEN amp.avail_restrict != 'Special' OR amp.avail_restrict IS NULL THEN 1 ELSE 0 END) = 0 
      THEN TRUE
      ELSE FALSE
    END AS special
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` dmd,
  UNNEST(amps) AS amp
  JOIN scmd_vmps sv ON dmd.vmp_code = sv.vmp_code
  GROUP BY dmd.vmp_code
),

vmp_ddd_info AS (
  SELECT
    vmp_code,
    selected_ddd_value,
    selected_ddd_unit,
    -- Convert DDD to basis units
    selected_ddd_value * COALESCE(ddd_conv.conversion_factor, 1.0) AS selected_ddd_basis_value,
    COALESCE(ddd_conv.basis, selected_ddd_unit) AS selected_ddd_basis_unit,
    can_calculate_ddd,
    ddd_calculation_logic
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_DDD_MAPPING_TABLE_ID }}`
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` ddd_conv
    ON selected_ddd_unit = ddd_conv.unit
)

SELECT
  vb.vmp_code,
  vb.vmp_name,
  vb.vtm AS vtm_code,
  vb.vtm_name,
  vbnf.bnf_code,
  vb.df_ind,
  vb.udfs,
  vb.udfs_uom,
  vb.udfs_basis_quantity,
  vb.udfs_basis_uom,
  vb.unit_dose_uom,
  vb.unit_dose_basis_uom,
  COALESCE(vss.special, FALSE) AS special,
  COALESCE(vi.ingredients, []) AS ingredients,
  COALESCE(vr.ont_form_routes, []) AS ont_form_routes,
  COALESCE(va.atcs, []) AS atcs,
  COALESCE(vamp.amps, []) AS amps,
  vddd.selected_ddd_value,
  vddd.selected_ddd_unit,
  vddd.selected_ddd_basis_value,
  vddd.selected_ddd_basis_unit,
  vddd.can_calculate_ddd,
  vddd.ddd_calculation_logic
FROM vmp_base vb
LEFT JOIN vmp_bnf vbnf ON vb.vmp_code = vbnf.vmp_code
LEFT JOIN vmp_ingredients vi ON vb.vmp_code = vi.vmp_code
LEFT JOIN vmp_routes vr ON vb.vmp_code = vr.vmp_code
LEFT JOIN vmp_atc_mappings va ON vb.vmp_code = va.vmp_code
LEFT JOIN vmp_amps vamp ON vb.vmp_code = vamp.vmp_code
LEFT JOIN vmp_special_status vss ON vb.vmp_code = vss.vmp_code
LEFT JOIN vmp_ddd_info vddd ON vb.vmp_code = vddd.vmp_code 