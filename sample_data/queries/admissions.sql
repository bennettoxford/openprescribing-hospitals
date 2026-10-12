-- Finalised admissions replace provisional admissions for the same trust and month.
WITH {{ scope }},
finalised AS (
  SELECT
    a.ods_code,
    a.period,
    a.all_specialties_total AS count
  FROM `ebmdatalab.scmd_pipeline.trust_admissions_finalised` AS a
  CROSS JOIN bounds AS b
  INNER JOIN trusts AS t
    ON t.ods_code = a.ods_code
  WHERE a.period BETWEEN b.start_month AND b.end_month
),
provisional AS (
  SELECT
    a.ods_code,
    a.period,
    a.all_specialties_total AS count
  FROM `ebmdatalab.scmd_pipeline.trust_admissions_provisional` AS a
  CROSS JOIN bounds AS b
  INNER JOIN trusts AS t
    ON t.ods_code = a.ods_code
  WHERE a.period BETWEEN b.start_month AND b.end_month
)
SELECT ods_code, period, count
FROM finalised
UNION ALL
SELECT p.ods_code, p.period, p.count
FROM provisional AS p
LEFT JOIN finalised AS f
  ON f.ods_code = p.ods_code
  AND f.period = p.period
WHERE f.ods_code IS NULL
ORDER BY ods_code, period;
