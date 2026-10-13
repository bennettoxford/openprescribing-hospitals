WITH {{ scope }}
SELECT
  v.vmp_code,
  ing.ingredient_code,
  ing.ingredient_name,
  ing.strnt_nmrtr_val,
  ing.strnt_nmrtr_uom_name,
  ing.strnt_dnmtr_val,
  ing.strnt_dnmtr_uom_name
FROM `ebmdatalab.scmd_pipeline.vmp_data` AS v
INNER JOIN products AS p
  ON p.vmp_code = v.vmp_code
CROSS JOIN UNNEST(v.ingredients) AS ing
ORDER BY v.vmp_code, ing.ingredient_name;
