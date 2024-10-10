CREATE OR REPLACE MATERIALIZED VIEW `ebmdatalab.scmd.dose` AS
WITH unit_conversions AS (
  SELECT 
    unit, 
    basis, 
    CAST(conversion_factor AS FLOAT64) AS conversion_factor 
  FROM `ebmdatalab.scmd.unit_conversion`
),
normalized_data AS (
  SELECT
    scmd_latest.year_month,
    scmd_latest.ods_code,
    COALESCE(vmp.id, scmd_latest.vmp_snomed_code) as vmp_code,
    scmd_latest.vmp_product_name as vmp_name,
    scmd_latest.unit_of_measure_name as uom,
    scmd_latest.total_quanity_in_vmp_unit as quantity,
    scmd_latest.indicative_cost
  FROM scmd.scmd_latest
  LEFT JOIN dmd.vmp
  ON scmd_latest.vmp_snomed_code = vmp.vpidprev
),
scmd_data AS (
  SELECT 
    n.year_month,
    ods_mapped.ods_code,
    ods_mapped.ods_name,
    n.quantity,
    n.uom,
    n.vmp_code,
    n.indicative_cost,
    vmp_full.vtm,
    vtm.nm AS vtm_name,
    vmp_full.df_ind,
    vmp_full.udfs,
    vmp_full.udfs_uom,
    vmp_full.unit_dose_uom,
    vmp_full.dform_form,
    vmp_full.nm AS vmp_name
  FROM 
    normalized_data n
  LEFT JOIN 
    dmd.vmp_full AS vmp_full
  ON 
    n.vmp_code = vmp_full.id
  LEFT JOIN
    dmd.vtm AS vtm
  ON
    vmp_full.vtm = vtm.id
  LEFT JOIN
    scmd.ods_mapped as ods_mapped
  ON
    n.ods_code = ods_mapped.ods_code
),
converted_data AS (
  SELECT 
    s.*,
    COALESCE(uc_udfs.conversion_factor, 1) AS udfs_conversion_factor,
    COALESCE(uc_udfs.basis, s.udfs_uom) AS udfs_basis,
    s.udfs * COALESCE(uc_udfs.conversion_factor, 1) AS converted_udfs
  FROM scmd_data s
  LEFT JOIN unit_conversions uc_udfs ON LOWER(s.udfs_uom) = LOWER(uc_udfs.unit)
),
calculated_doses AS (
  SELECT 
    *,
    CASE 
      WHEN df_ind = 'Discrete' THEN
        CASE
          WHEN uom = udfs_basis THEN quantity / converted_udfs
          WHEN uom = unit_dose_uom THEN quantity
          ELSE NULL
        END
      ELSE NULL
    END AS number_of_doses,
    CASE 
      WHEN df_ind = 'Discrete' AND (uom = udfs_basis OR uom = unit_dose_uom) THEN unit_dose_uom
      ELSE NULL
    END AS dose_unit,
    CASE 
      WHEN df_ind = 'Discrete' THEN
        CASE
          WHEN uom = udfs_basis THEN 'Quantity / udfs'
          WHEN uom = unit_dose_uom THEN 'Quantity'
          ELSE 'Not calculated: Discrete but SCMD quantity basis != udfs basis or unit dose form size'
        END
      ELSE 'Not calculated: not a discrete form'
    END AS logic
  FROM converted_data
)
SELECT 
  year_month,
  vmp_code,
  vmp_name,
  ods_code,
  ods_name,
  quantity AS SCMD_quantity,
  uom AS SCMD_quantity_basis,
  number_of_doses AS dose_quantity,
  dose_unit,
  df_ind,
  indicative_cost,
  logic
FROM calculated_doses