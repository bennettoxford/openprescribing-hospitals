WITH {{ scope }}
SELECT
  d.year_month,
  d.vmp_code,
  d.ods_code,
  d.scmd_quantity,
  d.scmd_basis_unit_name,
  d.dose_quantity,
  d.dose_unit
FROM `ebmdatalab.scmd_pipeline.dose` AS d
CROSS JOIN bounds AS b
INNER JOIN products AS p
  ON p.vmp_code = d.vmp_code
INNER JOIN trusts AS t
  ON t.ods_code = d.ods_code
WHERE d.year_month BETWEEN b.start_month AND b.end_month
  AND (
    (d.dose_quantity IS NOT NULL AND d.dose_unit IS NOT NULL)
    OR (d.scmd_quantity IS NOT NULL AND d.scmd_basis_unit_name IS NOT NULL)
  )
ORDER BY d.vmp_code, d.ods_code, d.year_month;
