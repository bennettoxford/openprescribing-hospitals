CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.vmp_ddd_mapping`
CLUSTER BY vmp_code, atc_code
AS
WITH vmp_base AS (
  SELECT DISTINCT
    CAST(d.vmp_code AS STRING) as vmp_code,
    d.vmp_name,
    CAST(ds.atc_code AS STRING) as atc_code,
    CAST(ds.ddd AS FLOAT64) as dmd_ddd,
    ds.ddd_uom as dmd_ddd_uom,
    uc.unit as dmd_ddd_uom_description,
    ARRAY_AGG(
      STRUCT(
        r.route_cd as dmd_route_code,
        r.route_descr as dmd_route_description
      ) IGNORE NULLS
    ) as dmd_routes
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` d
  INNER JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_SUPP_TABLE_ID }}` ds
    ON CAST(d.vmp_code AS STRING) = CAST(ds.vmp_code AS STRING)
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ UNITS_CONVERSION_TABLE_ID }}` uc
    ON ds.ddd_uom = uc.unit_id
  LEFT JOIN UNNEST(d.routes) as r
  WHERE ds.atc_code IS NOT NULL
  GROUP BY d.vmp_code, d.vmp_name, ds.atc_code, ds.ddd, ds.ddd_uom, uc.unit
),
atc_with_ddd AS (
  SELECT
    vb.*,
    wa.atc_name,
    ARRAY_AGG(
      STRUCT(
        CAST(wd.ddd AS FLOAT64) as ddd,
        wd.ddd_unit,
        wd.adm_code as adm_route_code,
        wd.comment as adm_route_comment
      ) IGNORE NULLS
    ) as ddd_values
  FROM vmp_base vb
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ WHO_ATC_TABLE_ID }}` wa
    ON CAST(vb.atc_code AS STRING) = CAST(wa.atc_code AS STRING)
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ WHO_DDD_TABLE_ID }}` wd
    ON CAST(vb.atc_code AS STRING) = CAST(wd.atc_code AS STRING)
  GROUP BY
    vb.vmp_code,
    vb.vmp_name,
    vb.atc_code,
    vb.dmd_ddd,
    vb.dmd_ddd_uom,
    vb.dmd_ddd_uom_description,
    wa.atc_name,
    vb.dmd_routes
)
SELECT
  vmp_code,
  vmp_name,
  atc_code,
  atc_name,
  dmd_ddd,
  dmd_ddd_uom,
  dmd_ddd_uom_description,
  dmd_routes,
  ddd_values
FROM atc_with_ddd 