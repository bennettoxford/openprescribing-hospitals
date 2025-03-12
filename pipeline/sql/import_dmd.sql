MERGE INTO `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` T
USING (
  WITH route_data AS (
    SELECT 
      vmp,
      ARRAY_AGG(STRUCT(
        CAST(r.cd AS STRING) AS route_cd,
        r.descr AS route_descr
      ) ORDER BY r.cd) AS routes
    FROM dmd.droute AS d
    JOIN dmd.route AS r ON d.route = r.cd
    GROUP BY vmp
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
  )
  SELECT 
    CAST(vmp_code AS STRING) AS vmp_code,
    CAST(vmp_code_prev AS STRING) AS vmp_code_prev,
    vmp_name,
    CAST(vtm AS STRING) AS vtm,
    vtm_name,
    df_ind,
    udfs,
    udfs_uom,
    unit_dose_uom,
    dform_form,
    rd.routes,
    ARRAY_AGG(STRUCT(
      CAST(ing AS STRING) AS ing_code,
      ing_nm AS ing_name,
      CAST(strnt_nmrtr_val AS FLOAT64) AS strnt_nmrtr_val,
      strnt_nmrtr_uom_name,
      CAST(strnt_dnmtr_val AS FLOAT64) AS strnt_dnmtr_val,
      strnt_dnmtr_uom_name
    ) ORDER BY ing) AS ingredients
  FROM vmp_vpi_ing_data
  LEFT JOIN route_data rd ON CAST(vmp_code AS STRING) = CAST(rd.vmp AS STRING)
  GROUP BY
    vmp_code,
    vmp_code_prev,
    vmp_name,
    vtm,
    vtm_name,
    df_ind,
    udfs,
    udfs_uom,
    unit_dose_uom,
    dform_form,
    rd.routes
) S
ON T.vmp_code = S.vmp_code
WHEN MATCHED THEN
  UPDATE SET
    vmp_code_prev = S.vmp_code_prev,
    vmp_name = S.vmp_name,
    vtm = S.vtm,
    vtm_name = S.vtm_name,
    df_ind = S.df_ind,
    udfs = S.udfs,
    udfs_uom = S.udfs_uom,
    unit_dose_uom = S.unit_dose_uom,
    dform_form = S.dform_form,
    ingredients = S.ingredients,
    routes = S.routes
WHEN NOT MATCHED THEN
  INSERT (
    vmp_code, vmp_code_prev, vmp_name, vtm, vtm_name, df_ind, 
    udfs, udfs_uom, unit_dose_uom, dform_form, 
    ingredients, routes
  )
  VALUES (
    S.vmp_code, S.vmp_code_prev, S.vmp_name, S.vtm, S.vtm_name, S.df_ind,
    S.udfs, S.udfs_uom, S.unit_dose_uom, S.dform_form,
    S.ingredients, S.routes
  )