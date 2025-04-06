CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_TABLE_ID }}`
CLUSTER BY vmp_code
AS

WITH
scmd_vmps AS (
  SELECT DISTINCT vmp_code 
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}`
),

vmp_base AS (
  SELECT
    dmd.vmp_code,
    dmd.vmp_name,
    dmd.vtm,
    dmd.vtm_name
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` dmd
  JOIN scmd_vmps sv ON dmd.vmp_code = sv.vmp_code
),

vmp_ingredients AS (
  SELECT
    dmd.vmp_code,
    ARRAY_AGG(
      STRUCT(
        ing.ing_code AS ingredient_code,
        ing.ing_name AS ingredient_name
      )
    ) AS ingredients
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` dmd,
  UNNEST(ingredients) AS ing
  JOIN scmd_vmps sv ON dmd.vmp_code = sv.vmp_code
  GROUP BY dmd.vmp_code
),

vmp_routes AS (
  SELECT
    dmd.vmp_code,
    ARRAY_AGG(
      STRUCT(
        route.ontformroute_cd AS route_code,
        route.ontformroute_descr AS route_name
      )
    ) AS ont_form_routes
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` dmd,
  UNNEST(ontformroutes) AS route
  JOIN scmd_vmps sv ON dmd.vmp_code = sv.vmp_code
  GROUP BY dmd.vmp_code
),

vmp_atc_mappings AS (
  SELECT
    atc.vmp_code,
    ARRAY_AGG(
      STRUCT(
        atc.atc_code,
        who_atc.atc_name
      )
    ) AS atcs
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_SUPP_TABLE_ID }}` atc
  JOIN scmd_vmps sv ON atc.vmp_code = sv.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ WHO_ATC_TABLE_ID }}` who_atc
    ON atc.atc_code = who_atc.atc_code
  GROUP BY atc.vmp_code
)

SELECT
  vb.vmp_code,
  vb.vmp_name,
  vb.vtm AS vtm_code,
  vb.vtm_name,
  COALESCE(vi.ingredients, []) AS ingredients,
  COALESCE(vr.ont_form_routes, []) AS ont_form_routes,
  COALESCE(va.atcs, []) AS atcs
FROM vmp_base vb
LEFT JOIN vmp_ingredients vi ON vb.vmp_code = vi.vmp_code
LEFT JOIN vmp_routes vr ON vb.vmp_code = vr.vmp_code
LEFT JOIN vmp_atc_mappings va ON vb.vmp_code = va.vmp_code 