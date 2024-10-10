CREATE OR REPLACE TABLE `ebmdatalab.scmd.scmd_dmd_route` AS
WITH droute_data AS (
  SELECT 
    vmp,
    r.cd AS route_cd,
    r.descr AS route_descr
  FROM `ebmdatalab.dmd.droute` d
  JOIN `ebmdatalab.dmd.route` r ON d.route = r.cd
),
mapped_routes AS (
  SELECT
    d.*,
    COALESCE(arm.who_route, d.route_descr) AS mapped_route
  FROM droute_data d
  LEFT JOIN `ebmdatalab.scmd.adm_route_mapping` arm ON d.route_descr = arm.dmd_route
),
distinct_routes AS (
  SELECT DISTINCT
    s.vmp_code,
    m.route_cd,
    m.route_descr,
    m.mapped_route
  FROM `ebmdatalab.scmd.scmd_dmd` s
  LEFT JOIN mapped_routes m ON s.vmp_code = m.vmp
)
SELECT
  vmp_code,
  ARRAY_AGG(STRUCT(route_cd, route_descr, mapped_route)) AS dmd_routes,
  ARRAY_AGG(DISTINCT mapped_route) AS mapped_routes,
  COUNT(DISTINCT route_cd) AS route_count
FROM distinct_routes
GROUP BY vmp_code
ORDER BY vmp_code