CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_FULL_TABLE_ID }}`
CLUSTER BY vmp_code
AS

WITH route_data AS (
  SELECT
    vmp,
    ARRAY_AGG(STRUCT(
      CAST(ofr.cd AS STRING) AS ontformroute_cd,
      ofr.descr AS ontformroute_descr
    ) ORDER BY ofr.cd) AS ontformroutes
  FROM dmd.ont AS o
  JOIN dmd.ontformroute AS ofr ON o.form = ofr.cd
  GROUP BY vmp
),
amp_data AS (
  SELECT
    amp_full.vmp,
    ARRAY_AGG(STRUCT(
      CAST(amp_full.id AS STRING) AS amp_code,
      amp_full.descr AS amp_name,
      amp_full.avail_restrict AS avail_restrict
    ) ORDER BY amp_full.id) AS amps
  FROM dmd.amp_full
  GROUP BY amp_full.vmp
),
vmp_vpi_ing_data AS (
  SELECT DISTINCT
    vmp_full.id AS vmp_code,
    vmp_full.nm AS vmp_name,
    vmp_full.df_ind,
    vmp_full.udfs,
    vmp_full.udfs_uom,
    vmp_full.unit_dose_uom,
    vmp_full.dform_form,
    vmp_full.vtm,
    vtm.nm AS vtm_name,
    vpi.ing,
    vpi.strnt_nmrtr_val,
    vpi.strnt_dnmtr_val,
    nmrtr_uom.descr AS strnt_nmrtr_uom_name,
    dnmtr_uom.descr AS strnt_dnmtr_uom_name,
    ing.nm AS ing_nm,
    vpi.bs_subid,
    vpi.basis_strnt,
    bs.nm AS base_substance_name
  FROM
    dmd.vmp_full AS vmp_full
  LEFT JOIN
    dmd.vpi AS vpi
  ON
    vmp_full.id = vpi.vmp
  LEFT JOIN
    dmd.unitofmeasure AS nmrtr_uom
  ON
    vpi.strnt_nmrtr_uom = nmrtr_uom.cd
  LEFT JOIN
    dmd.unitofmeasure AS dnmtr_uom
  ON
    vpi.strnt_dnmtr_uom = dnmtr_uom.cd
  LEFT JOIN
    dmd.ing as ing
  ON
    vpi.ing = ing.id
  LEFT JOIN
    dmd.vtm as vtm
  ON
    vmp_full.vtm = vtm.id
  LEFT JOIN
    dmd.ing as bs
  ON
    vpi.bs_subid = bs.id
)
SELECT
  CAST(vmp_code AS STRING) AS vmp_code,
  vmp_name,
  CAST(vtm AS STRING) AS vtm,
  vtm_name,
  CAST(df_ind AS STRING) AS df_ind,
  CAST(udfs AS FLOAT64) AS udfs,
  udfs_uom,
  unit_dose_uom,
  dform_form,
  rd.ontformroutes,
  ad.amps,
  ARRAY_AGG(
    IF(ing IS NOT NULL,
      STRUCT(
        CAST(ing AS STRING) AS ing_code,
        ing_nm AS ing_name,
        CAST(strnt_nmrtr_val AS FLOAT64) AS strnt_nmrtr_val,
        strnt_nmrtr_uom_name,
        CAST(strnt_dnmtr_val AS FLOAT64) AS strnt_dnmtr_val,
        strnt_dnmtr_uom_name,
        CAST(bs_subid AS STRING) AS basis_of_strength_code,
        base_substance_name AS basis_of_strength_name,
        CAST(basis_strnt AS INT64) AS basis_of_strength_type
      ),
      NULL
    ) IGNORE NULLS
    ORDER BY ing
  ) AS ingredients
FROM vmp_vpi_ing_data
LEFT JOIN route_data rd ON CAST(vmp_code AS STRING) = CAST(rd.vmp AS STRING)
LEFT JOIN amp_data ad ON CAST(vmp_code AS STRING) = CAST(ad.vmp AS STRING)
GROUP BY
  vmp_code,
  vmp_name,
  vtm,
  vtm_name,
  df_ind,
  udfs,
  udfs_uom,
  unit_dose_uom,
  dform_form,
  rd.ontformroutes,
  ad.amps