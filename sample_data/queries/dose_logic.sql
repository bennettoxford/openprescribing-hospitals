WITH {{ scope }}
SELECT
  l.vmp_code,
  l.dose_calculation_logic AS logic
FROM `ebmdatalab.scmd_pipeline.dose_calculation_logic` AS l
INNER JOIN products AS p
  ON p.vmp_code = l.vmp_code
ORDER BY l.vmp_code;
