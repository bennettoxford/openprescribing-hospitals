WITH {{ scope }}
SELECT
  l.vmp_code,
  ing.ingredient_code,
  ing.ingredient_calculation_logic AS logic
FROM `ebmdatalab.scmd_pipeline.ingredient_calculation_logic` AS l
INNER JOIN products AS p
  ON p.vmp_code = l.vmp_code
CROSS JOIN UNNEST(l.ingredients) AS ing
ORDER BY l.vmp_code, ing.ingredient_code;
