WITH {{ scope }}
SELECT
  q.vmp_code,
  q.year_month,
  q.ods_code,
  ing.ingredient_code,
  ing.ingredient_quantity_basis,
  ing.ingredient_basis_unit
FROM `ebmdatalab.scmd_pipeline.ingredient_quantity` AS q
CROSS JOIN bounds AS b
INNER JOIN products AS p
  ON p.vmp_code = q.vmp_code
INNER JOIN trusts AS t
  ON t.ods_code = q.ods_code
CROSS JOIN UNNEST(q.ingredients) AS ing
WHERE q.year_month BETWEEN b.start_month AND b.end_month
  AND ing.ingredient_quantity_basis IS NOT NULL
  AND ing.ingredient_basis_unit IS NOT NULL
ORDER BY q.vmp_code, ing.ingredient_code, q.ods_code, q.year_month;
