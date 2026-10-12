WITH {{ scope }}
SELECT
  v.vmp_code,
  atc.atc_code,
  atc.atc_name
FROM `ebmdatalab.scmd_pipeline.vmp_data` AS v
INNER JOIN products AS p
  ON p.vmp_code = v.vmp_code
CROSS JOIN UNNEST(v.atcs) AS atc
ORDER BY v.vmp_code, atc.atc_code;
