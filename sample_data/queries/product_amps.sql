WITH {{ scope }}
SELECT
  v.vmp_code,
  amp.amp_code,
  amp.amp_name,
  amp.avail_restrict
FROM `ebmdatalab.scmd_pipeline.vmp_data` AS v
INNER JOIN products AS p
  ON p.vmp_code = v.vmp_code
CROSS JOIN UNNEST(v.amps) AS amp
ORDER BY v.vmp_code, amp.amp_name;
