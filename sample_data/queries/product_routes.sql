WITH {{ scope }}
SELECT
  v.vmp_code,
  route.route_code,
  route.route_name
FROM `ebmdatalab.scmd_pipeline.vmp_data` AS v
INNER JOIN products AS p
  ON p.vmp_code = v.vmp_code
CROSS JOIN UNNEST(v.ont_form_routes) AS route
ORDER BY v.vmp_code, route.route_name;
