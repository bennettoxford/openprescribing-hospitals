CREATE OR REPLACE TABLE `ebmdatalab.scmd.scmd_dmd` AS
WITH unit_conversions AS (
  SELECT 
    unit, 
    basis, 
    CAST(conversion_factor AS FLOAT64) AS conversion_factor 
  FROM `ebmdatalab.scmd.unit_conversion`
),
vmp_vpi_ing_data AS (
  SELECT DISTINCT
    vmp_full.id AS vmp_code,
    vmp_full.vpidprev AS vmp_code_prev,
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
    ing.nm AS ing_nm
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
),
converted_data AS (
  SELECT 
    v.*,
    -- udfs conversion
    CASE 
      WHEN uc_udfs.unit IS NOT NULL THEN v.udfs * uc_udfs.conversion_factor
      ELSE v.udfs
    END AS converted_udfs,
    CASE 
      WHEN uc_udfs.unit IS NOT NULL THEN uc_udfs.basis
      ELSE v.udfs_uom
    END AS udfs_basis,
    
    -- strnt_nmrtr_val conversion
    CASE 
      WHEN uc_nmrtr.unit IS NOT NULL THEN v.strnt_nmrtr_val * uc_nmrtr.conversion_factor
      ELSE v.strnt_nmrtr_val
    END AS converted_strnt_nmrtr_val,
    CASE 
      WHEN uc_nmrtr.unit IS NOT NULL THEN uc_nmrtr.basis
      ELSE v.strnt_nmrtr_uom_name
    END AS strnt_nmrtr_basis,
    
    -- strnt_dnmtr_val conversion
    CASE 
      WHEN uc_dnmtr.unit IS NOT NULL THEN v.strnt_dnmtr_val * uc_dnmtr.conversion_factor
      ELSE v.strnt_dnmtr_val
    END AS converted_strnt_dnmtr_val,
    CASE 
      WHEN uc_dnmtr.unit IS NOT NULL THEN uc_dnmtr.basis
      ELSE v.strnt_dnmtr_uom_name
    END AS strnt_dnmtr_basis

  FROM vmp_vpi_ing_data v
  LEFT JOIN unit_conversions uc_udfs ON LOWER(v.udfs_uom) = LOWER(uc_udfs.unit)
  LEFT JOIN unit_conversions uc_nmrtr ON LOWER(v.strnt_nmrtr_uom_name) = LOWER(uc_nmrtr.unit)
  LEFT JOIN unit_conversions uc_dnmtr ON LOWER(v.strnt_dnmtr_uom_name) = LOWER(uc_dnmtr.unit)
),
ingredient_count_cte AS (
  SELECT 
    vmp_code,
    COUNT(DISTINCT ing) AS number_of_ingredients
  FROM vmp_vpi_ing_data
  GROUP BY vmp_code
)
SELECT 
  converted_data.vmp_code,
  converted_data.vmp_code_prev,
  converted_data.vmp_name,
  converted_data.vtm,
  converted_data.vtm_name,
  converted_data.df_ind,
  converted_data.converted_udfs,
  converted_data.udfs_basis,
  converted_data.unit_dose_uom,
  converted_data.dform_form,
  converted_data.ing,
  converted_data.ing_nm,
  ingredient_count_cte.number_of_ingredients,
  converted_data.converted_strnt_nmrtr_val,
  converted_data.strnt_nmrtr_basis,
  converted_data.converted_strnt_dnmtr_val,
  converted_data.strnt_dnmtr_basis
FROM converted_data
LEFT JOIN ingredient_count_cte
ON converted_data.vmp_code = ingredient_count_cte.vmp_code
ORDER BY vmp_code, ing