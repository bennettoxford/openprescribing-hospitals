CREATE OR REPLACE MATERIALIZED VIEW `ebmdatalab.scmd.ingredient_quantity` AS
WITH normalized_data AS (
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
    n.quantity as converted_quantity,
    n.uom as quantity_basis,
    n.vmp_code as vmp_code,
    scmd_dmd.vtm,
    scmd_dmd.vtm_name,
    scmd_dmd.df_ind,
    scmd_dmd.converted_udfs,
    scmd_dmd.udfs_basis,
    scmd_dmd.unit_dose_uom,
    scmd_dmd.dform_form,
    scmd_dmd.vmp_name,
    scmd_dmd.ing,
    scmd_dmd.ing_nm,
    scmd_dmd.number_of_ingredients,
    scmd_dmd.converted_strnt_nmrtr_val AS converted_strnt_nmrtr_val,
    scmd_dmd.converted_strnt_dnmtr_val AS converted_strnt_dnmtr_val,
    scmd_dmd.strnt_nmrtr_basis AS strnt_nmrtr_basis,
    scmd_dmd.strnt_dnmtr_basis AS strnt_dnmtr_basis
  FROM 
    normalized_data n
  LEFT JOIN 
    scmd.scmd_dmd AS scmd_dmd
  ON 
    n.vmp_code = scmd_dmd.vmp_code
  LEFT JOIN
    scmd.ods_mapped AS ods_mapped
  ON
    n.ods_code = ods_mapped.ods_code
),
ingredient_quantity_details AS (
  SELECT 
    cd.*,
    CASE
        WHEN cd.ing IS NULL THEN NULL
        WHEN cd.strnt_dnmtr_basis IS NULL AND cd.strnt_nmrtr_basis IS NULL THEN NULL
        WHEN cd.strnt_dnmtr_basis = cd.quantity_basis THEN
            (cd.converted_quantity / cd.converted_strnt_dnmtr_val) * cd.converted_strnt_nmrtr_val        
               
        WHEN cd.df_ind = 'Discrete' THEN
            CASE
            WHEN cd.quantity_basis = cd.strnt_nmrtr_basis THEN cd.converted_quantity
            WHEN cd.strnt_dnmtr_basis IS NULL THEN cd.converted_quantity * cd.converted_strnt_nmrtr_val
            ELSE NULL
            END
        ELSE NULL
    END AS ingredient_quantity
    FROM scmd_data cd
),
ingredient_quantity AS (
  SELECT 
    iq.*,
    CASE
      WHEN iq.ingredient_quantity IS NOT NULL THEN iq.strnt_nmrtr_basis
      ELSE NULL
    END AS ingredient_unit,
    CASE
        WHEN iq.ing IS NULL THEN 'Not calculated: No ingredient'
        WHEN iq.strnt_dnmtr_basis IS NULL AND iq.strnt_nmrtr_basis IS NULL THEN "Not calculated: No strength info"
        WHEN iq.strnt_dnmtr_basis = iq.quantity_basis THEN
            '(SCMD quantity / strength denominator value) * strength numerator value'      
        WHEN iq.df_ind = 'Discrete' THEN
            CASE
            WHEN iq.quantity_basis = iq.strnt_nmrtr_basis THEN 'SCMD quantity'
            WHEN iq.strnt_dnmtr_basis IS NULL THEN "SCMD quantity * strength numerator value"
            ELSE "Not calculated: Discrete, numerator basis = quantity basis but has denominator"
            END
        ELSE "Not calculated: Strnt_dnmtr_basis != quantity_basis and not discrete"
    END AS logic
  FROM ingredient_quantity_details iq
)
SELECT 
  iq.vmp_code,
  iq.year_month,
  iq.ods_code,
  iq.ods_name,
  iq.vmp_name,
  iq.ing AS ingredient_code,
  iq.ing_nm,
  iq.converted_quantity,
  iq.quantity_basis,
  iq.ingredient_quantity,
  iq.ingredient_unit,
  iq.number_of_ingredients,
  iq.converted_strnt_nmrtr_val,
  iq.converted_strnt_dnmtr_val,
  iq.strnt_nmrtr_basis,
  iq.strnt_dnmtr_basis,
  iq.logic
FROM ingredient_quantity iq