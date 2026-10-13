WITH {{ scope }}
SELECT
  q.vmp_code,
  q.year_month,
  q.ods_code,
  q.ddd_quantity,
  q.ddd_value,
  q.ddd_unit,
  q.ingredient_code
FROM `ebmdatalab.scmd_pipeline.ddd_quantity` AS q
CROSS JOIN bounds AS b
INNER JOIN products AS p
  ON p.vmp_code = q.vmp_code
INNER JOIN trusts AS t
  ON t.ods_code = q.ods_code
WHERE q.year_month BETWEEN b.start_month AND b.end_month
  AND q.ddd_quantity IS NOT NULL
ORDER BY q.vmp_code, q.ods_code, q.year_month;
