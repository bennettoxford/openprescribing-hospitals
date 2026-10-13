CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_FULL_TABLE_ID }}`
CLUSTER BY vmp_code
AS

WITH route_data AS (
  SELECT
    ont.vmp_code,
    ARRAY_AGG(STRUCT(
      ont.form_code AS ontformroute_cd,
      route.description AS ontformroute_descr
    ) ORDER BY CAST(ont.form_code AS INT64)) AS ontformroutes
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_ONT_TABLE_ID }}` AS ont
  JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_ONT_FORM_ROUTE_TABLE_ID }}` AS route
    ON ont.form_code = route.ont_form_route_code
  GROUP BY ont.vmp_code
),
amp_data AS (
  SELECT
    amp.vmp_code,
    ARRAY_AGG(STRUCT(
      amp.amp_code AS amp_code,
      amp.description AS amp_name,
      avail.description AS avail_restrict
    ) ORDER BY CAST(amp.amp_code AS INT64)) AS amps
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_AMP_TABLE_ID }}` AS amp
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_AVAILABILITY_RESTRICTION_TABLE_ID }}` AS avail
    ON amp.avail_restrict_code = avail.avail_restrict_code
  GROUP BY amp.vmp_code
),
form_data AS (
  SELECT
    dform.vmp_code,
    IF(
      COUNT(DISTINCT dform.form_code) = 1,
      ANY_VALUE(form.description),
      ERROR(CONCAT('VMP ', dform.vmp_code, ' has more than one dose form'))
    ) AS dform_form
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_DFORM_TABLE_ID }}` AS dform
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_FORM_TABLE_ID }}` AS form
    ON dform.form_code = form.form_code
  GROUP BY dform.vmp_code
),
vmp_vpi_ing_data AS (
  SELECT DISTINCT
    vmp.vmp_code,
    vmp.vmp_name,
    df_ind.description AS df_ind,
    vmp.udfs,
    udfs_uom.description AS udfs_uom,
    unit_dose_uom.description AS unit_dose_uom,
    form_data.dform_form,
    vmp.vtm_code AS vtm,
    vtm.vtm_name,
    vpi.ing_code AS ing,
    vpi.strnt_nmrtr_val,
    vpi.strnt_dnmtr_val,
    nmrtr_uom.description AS strnt_nmrtr_uom_name,
    dnmtr_uom.description AS strnt_dnmtr_uom_name,
    ing.ing_name AS ing_nm,
    vpi.bs_subid,
    CAST(vpi.basis_strnt_code AS INT64) AS basis_strnt,
    bs.ing_name AS base_substance_name
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_VMP_TABLE_ID }}` AS vmp
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_DF_INDICATOR_TABLE_ID }}` AS df_ind
    ON vmp.df_ind_code = df_ind.df_ind_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_UNIT_OF_MEASURE_TABLE_ID }}` AS udfs_uom
    ON vmp.udfs_uom_code = udfs_uom.uom_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_UNIT_OF_MEASURE_TABLE_ID }}` AS unit_dose_uom
    ON vmp.unit_dose_uom_code = unit_dose_uom.uom_code
  LEFT JOIN form_data
    ON vmp.vmp_code = form_data.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_VTM_TABLE_ID }}` AS vtm
    ON vmp.vtm_code = vtm.vtm_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_VPI_TABLE_ID }}` AS vpi
    ON vmp.vmp_code = vpi.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_UNIT_OF_MEASURE_TABLE_ID }}` AS nmrtr_uom
    ON vpi.strnt_nmrtr_uom_code = nmrtr_uom.uom_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_UNIT_OF_MEASURE_TABLE_ID }}` AS dnmtr_uom
    ON vpi.strnt_dnmtr_uom_code = dnmtr_uom.uom_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_ING_TABLE_ID }}` AS ing
    ON vpi.ing_code = ing.ing_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_ING_TABLE_ID }}` AS bs
    ON vpi.bs_subid = bs.ing_code
)
SELECT
  src.vmp_code,
  src.vmp_name,
  src.vtm,
  src.vtm_name,
  src.df_ind,
  src.udfs,
  src.udfs_uom,
  src.unit_dose_uom,
  src.dform_form,
  ARRAY_AGG(
    IF(
      src.ing IS NOT NULL,
      STRUCT(
        src.ing AS ing_code,
        src.ing_nm AS ing_name,
        src.strnt_nmrtr_val AS strnt_nmrtr_val,
        src.strnt_nmrtr_uom_name AS strnt_nmrtr_uom_name,
        src.strnt_dnmtr_val AS strnt_dnmtr_val,
        src.strnt_dnmtr_uom_name AS strnt_dnmtr_uom_name,
        src.bs_subid AS basis_of_strength_code,
        src.base_substance_name AS basis_of_strength_name,
        src.basis_strnt AS basis_of_strength_type
      ),
      NULL
    ) IGNORE NULLS
    ORDER BY CAST(src.ing AS INT64)
  ) AS ingredients,
  route_data.ontformroutes,
  amp_data.amps
FROM vmp_vpi_ing_data AS src
LEFT JOIN route_data
  ON src.vmp_code = route_data.vmp_code
LEFT JOIN amp_data
  ON src.vmp_code = amp_data.vmp_code
GROUP BY
  src.vmp_code,
  src.vmp_name,
  src.vtm,
  src.vtm_name,
  src.df_ind,
  src.udfs,
  src.udfs_uom,
  src.unit_dose_uom,
  src.dform_form,
  route_data.ontformroutes,
  amp_data.amps
