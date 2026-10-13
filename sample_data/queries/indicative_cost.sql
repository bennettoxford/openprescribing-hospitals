WITH {{ scope }}
SELECT
  s.year_month,
  s.vmp_code,
  s.ods_code,
  s.indicative_cost
FROM `ebmdatalab.scmd_pipeline.scmd_processed` AS s
CROSS JOIN bounds AS b
INNER JOIN products AS p
  ON p.vmp_code = s.vmp_code
INNER JOIN trusts AS t
  ON t.ods_code = s.ods_code
WHERE s.year_month BETWEEN b.start_month AND b.end_month
  AND s.indicative_cost IS NOT NULL
ORDER BY s.vmp_code, s.ods_code, s.year_month;
