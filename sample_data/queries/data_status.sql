WITH {{ bounds }}
SELECT
  s.year_month,
  s.file_type
FROM `ebmdatalab.scmd_pipeline.scmd_data_status` AS s
CROSS JOIN bounds AS b
WHERE s.year_month BETWEEN b.start_month AND b.end_month
ORDER BY s.year_month;
