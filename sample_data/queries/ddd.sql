WITH {{ scope }}
SELECT
  l.vmp_code,
  l.selected_ddd_value AS ddd,
  l.selected_ddd_unit AS unit_type,
  l.selected_ddd_route_code AS who_route
FROM `ebmdatalab.scmd_pipeline.ddd_calculation_logic` AS l
INNER JOIN products AS p
  ON p.vmp_code = l.vmp_code
WHERE l.selected_ddd_value IS NOT NULL
  AND l.selected_ddd_value > 0
  AND l.selected_ddd_unit IS NOT NULL
  AND l.selected_ddd_route_code IS NOT NULL
ORDER BY l.vmp_code;
