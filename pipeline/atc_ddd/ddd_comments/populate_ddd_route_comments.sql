CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DDD_ROUTE_COMMENTS_TABLE_ID }}`
CLUSTER BY vmp_code
AS

-- Get VMPs that appear in SCMD data
WITH scmd_vmps AS (
  SELECT DISTINCT vmp_code
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}`
),
vmp_strength AS (
  SELECT
    vmp.vmp_code,
    COALESCE(
      (SELECT ing.strnt_nmrtr_val FROM UNNEST(vmp.ingredients) AS ing
       WHERE ing.basis_of_strength_type = 1 LIMIT 1),
      (SELECT ing.strnt_nmrtr_val FROM UNNEST(vmp.ingredients) AS ing LIMIT 1)
    ) AS strnt_nmrtr_val,
    COALESCE(
      (SELECT ing.strnt_dnmtr_val FROM UNNEST(vmp.ingredients) AS ing
       WHERE ing.basis_of_strength_type = 1 LIMIT 1),
      (SELECT ing.strnt_dnmtr_val FROM UNNEST(vmp.ingredients) AS ing LIMIT 1)
    ) AS strnt_dnmtr_val
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_TABLE_ID }}` vmp
  INNER JOIN scmd_vmps sv ON vmp.vmp_code = sv.vmp_code
),

-- Get ATC codes and routes for each VMP
vmp_with_atcs_and_routes AS (
  SELECT
    vmp.vmp_code,
    vmp.vmp_name,
    vmp.df_ind,
    vmp.vtm_code,
    vmp.vtm_name,
    dmd.dform_form,
    ARRAY_AGG(
      STRUCT(
        atc_struct.atc_code,
        who_atc.atc_name
      )
    ) AS atcs,
    ARRAY_AGG(
      STRUCT(
        route.route_code AS ontformroute_cd,
        route.route_name AS ontformroute_descr,
        route_map.who_route AS who_route_code
      )
    ) AS routes,
    ARRAY_AGG(DISTINCT route_map.who_route IGNORE NULLS) AS who_route_codes,
    ARRAY_AGG(
      STRUCT(
        atc_struct.atc_code AS atc_code,
        ddd.ddd,
        ddd.ddd_unit,
        ddd.adm_code AS ddd_route_code,
        ddd.comment AS ddd_comment
      )
    ) AS who_ddds
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_TABLE_ID }}` vmp
  JOIN scmd_vmps sv ON vmp.vmp_code = sv.vmp_code
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` dmd
    ON vmp.vmp_code = dmd.vmp_code
  LEFT JOIN UNNEST(vmp.atcs) AS atc_struct
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ WHO_ATC_TABLE_ID }}` who_atc
    ON atc_struct.atc_code = who_atc.atc_code
  LEFT JOIN UNNEST(vmp.ont_form_routes) AS route
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ ADM_ROUTE_MAPPING_TABLE_ID }}` route_map
    ON route.route_name = route_map.dmd_ontformroute
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ WHO_DDD_TABLE_ID }}` ddd
    ON atc_struct.atc_code = ddd.atc_code
  GROUP BY vmp.vmp_code, vmp.vmp_name, vmp.df_ind, vmp.vtm_code, vmp.vtm_name, dmd.dform_form
),

-- Analyse DDD options for each VMP
ddd_analysis AS (
  SELECT
    vmp_code,
    vmp_name,
    df_ind,
    vtm_code,
    vtm_name,
    dform_form,
    atcs,
    who_ddds,
    who_route_codes,
    routes,
    -- Check if any ATC codes
    (SELECT COUNT(1) FROM UNNEST(atcs) WHERE atc_code IS NOT NULL) > 0 AS has_atc_codes,
    -- Check if any WHO DDDs with non-NULL values
    (SELECT COUNT(1) FROM UNNEST(who_ddds) WHERE ddd IS NOT NULL) > 0 AS has_ddds,
    -- Check if any mapped WHO routes
    (SELECT COUNT(1) FROM UNNEST(routes) WHERE who_route_code IS NOT NULL) > 0 AS has_who_routes,
    -- Find DDDs that match the WHO route codes
    ARRAY(
      SELECT AS STRUCT ddd.*
      FROM UNNEST(who_ddds) AS ddd
      WHERE ddd.ddd IS NOT NULL
        AND ddd.ddd_route_code IN (SELECT code FROM UNNEST(who_route_codes) AS code)
    ) AS matching_route_ddds,
    -- Check if all matching DDDs have the same value
    (
      SELECT COUNT(DISTINCT ddd.ddd)
      FROM UNNEST(ARRAY(
        SELECT AS STRUCT ddd.*
        FROM UNNEST(who_ddds) AS ddd
        WHERE ddd.ddd IS NOT NULL
          AND ddd.ddd_route_code IN (SELECT code FROM UNNEST(who_route_codes) AS code)
      )) AS ddd
    ) = 1 AS all_matching_ddds_same,

    -- Check DDD comments for route specific comments AND corresponding routes
    (SELECT COUNT(1) FROM UNNEST(who_ddds) AS ddd
     WHERE REGEXP_CONTAINS(LOWER(COALESCE(ddd.ddd_comment, '')), r'\bgel\b')) > 0 AS has_gel_comment_ddd,
    (SELECT COUNT(1) FROM UNNEST(routes) AS route
     WHERE LOWER(COALESCE(route.ontformroute_descr, '')) LIKE 'gel%') > 0 AS has_gel_route,

    (SELECT COUNT(1) FROM UNNEST(who_ddds) AS ddd
     WHERE REGEXP_CONTAINS(LOWER(COALESCE(ddd.ddd_comment, '')), r'\bspray\b')) > 0 AS has_spray_comment_ddd,
    (SELECT COUNT(1) FROM UNNEST(routes) AS route
     WHERE LOWER(COALESCE(route.ontformroute_descr, '')) LIKE 'solutionspray%') > 0 AS has_spray_route,

    (SELECT COUNT(1) FROM UNNEST(who_ddds) AS ddd
     WHERE LOWER(COALESCE(ddd.ddd_comment, '')) LIKE '%vaginal ring%') > 0 AS has_vaginal_ring_comment_ddd,
    (SELECT COUNT(1) FROM UNNEST(routes) AS route
     WHERE LOWER(COALESCE(route.ontformroute_descr, '')) = 'ring.vaginal') > 0 AS has_vaginal_ring_route,

    (SELECT COUNT(1) FROM UNNEST(who_ddds) AS ddd
     WHERE REGEXP_CONTAINS(LOWER(COALESCE(ddd.ddd_comment, '')), r'\bpatch\b')) > 0 AS has_patch_comment_ddd,
    (SELECT COUNT(1) FROM UNNEST(routes) AS route
     WHERE LOWER(COALESCE(route.ontformroute_descr, '')) LIKE '%patch.transdermal%') > 0 AS has_patch_route,

    (SELECT COUNT(1) FROM UNNEST(who_ddds) AS ddd
     WHERE LOWER(COALESCE(ddd.ddd_comment, '')) = 'lipid formulations') > 0 AS has_lipid_formulations_comment_ddd,

    (SELECT COUNT(1) FROM UNNEST(who_ddds) AS ddd
     WHERE LOWER(COALESCE(ddd.ddd_comment, '')) = 'erythromycin ethylsuccinate tablets') > 0 AS has_erythromycin_tablets_comment_ddd,

    (SELECT COUNT(1) FROM UNNEST(who_ddds) AS ddd
     WHERE LOWER(COALESCE(ddd.ddd_comment, '')) = 'depot') > 0 AS has_depot_comment_ddd,
    LOWER(COALESCE(vtm_name, '')) LIKE '%decanoate%' AS vtm_name_contains_decanoate,
    (SELECT COUNT(1) FROM UNNEST(who_route_codes) AS code WHERE code = 'P') > 0 AS has_p_route,
    LOWER(COALESCE(dform_form, '')) LIKE '%prolonged-release%' AS has_prolonged_release_form
  FROM vmp_with_atcs_and_routes
),

-- Apply comment-based rules to select the appropriate DDD
ddd_selection AS (
  SELECT
    da.*,
    CASE
          -- If comment contains "gel" and product has ontformroute starting with "gel"
          WHEN has_gel_comment_ddd AND has_gel_route THEN (
            SELECT AS STRUCT ddd.atc_code, ddd.ddd, ddd.ddd_unit, ddd.ddd_comment
            FROM UNNEST(CASE WHEN ARRAY_LENGTH(matching_route_ddds) > 1 AND NOT all_matching_ddds_same THEN matching_route_ddds ELSE who_ddds END) AS ddd
            WHERE REGEXP_CONTAINS(LOWER(COALESCE(ddd.ddd_comment, '')), r'\bgel\b')
            LIMIT 1
          )

          -- If comment contains "spray" and product has ontformroute starting with "solutionspray"
          WHEN has_spray_comment_ddd AND has_spray_route THEN (
            SELECT AS STRUCT ddd.atc_code, ddd.ddd, ddd.ddd_unit, ddd.ddd_comment
            FROM UNNEST(CASE WHEN ARRAY_LENGTH(matching_route_ddds) > 1 AND NOT all_matching_ddds_same THEN matching_route_ddds ELSE who_ddds END) AS ddd
            WHERE REGEXP_CONTAINS(LOWER(COALESCE(ddd.ddd_comment, '')), r'\bspray\b')
            LIMIT 1
          )

          -- If comment contains "vaginal ring" and product has ontformroute = "ring.vaginal"
          WHEN has_vaginal_ring_comment_ddd AND has_vaginal_ring_route THEN (
            SELECT AS STRUCT ddd.atc_code, ddd.ddd, ddd.ddd_unit, ddd.ddd_comment
            FROM UNNEST(CASE WHEN ARRAY_LENGTH(matching_route_ddds) > 1 AND NOT all_matching_ddds_same THEN matching_route_ddds ELSE who_ddds END) AS ddd
            WHERE LOWER(COALESCE(ddd.ddd_comment, '')) LIKE '%vaginal ring%'
            LIMIT 1
          )

          -- If comment contains "patch" and product has ontformroute containing "patch.transdermal"
          WHEN has_patch_comment_ddd AND has_patch_route THEN (
            SELECT AS STRUCT ddd.atc_code, ddd.ddd, ddd.ddd_unit, ddd.ddd_comment
            FROM UNNEST(CASE WHEN ARRAY_LENGTH(matching_route_ddds) > 1 AND NOT all_matching_ddds_same THEN matching_route_ddds ELSE who_ddds END) AS ddd
            WHERE REGEXP_CONTAINS(LOWER(COALESCE(ddd.ddd_comment, '')), r'\bpatch\b')
            LIMIT 1
          )

          -- If comment is "lipid formulations" and VTM is in specific list, select that DDD
          WHEN has_lipid_formulations_comment_ddd AND vtm_code IN ('773381005', '21202811000001101', '773379008') THEN (
            SELECT AS STRUCT ddd.atc_code, ddd.ddd, ddd.ddd_unit, ddd.ddd_comment
            FROM UNNEST(CASE WHEN ARRAY_LENGTH(matching_route_ddds) > 1 AND NOT all_matching_ddds_same THEN matching_route_ddds ELSE who_ddds END) AS ddd
            WHERE LOWER(COALESCE(ddd.ddd_comment, '')) = 'lipid formulations'
            LIMIT 1
          )

          -- If comment is "erythromycin ethylsuccinate tablets" and VMP code matches, select that DDD
          WHEN has_erythromycin_tablets_comment_ddd AND vmp_code = '41949111000001108' THEN (
            SELECT AS STRUCT ddd.atc_code, ddd.ddd, ddd.ddd_unit, ddd.ddd_comment
            FROM UNNEST(CASE WHEN ARRAY_LENGTH(matching_route_ddds) > 1 AND NOT all_matching_ddds_same THEN matching_route_ddds ELSE who_ddds END) AS ddd
            WHERE LOWER(COALESCE(ddd.ddd_comment, '')) = 'erythromycin ethylsuccinate tablets'
            LIMIT 1
          )

          -- If comment is "depot" and VTM name contains "decanoate" and product has P route, select that DDD
          WHEN has_depot_comment_ddd AND vtm_name_contains_decanoate AND has_p_route THEN (
            SELECT AS STRUCT ddd.atc_code, ddd.ddd, ddd.ddd_unit, ddd.ddd_comment
            FROM UNNEST(CASE WHEN ARRAY_LENGTH(matching_route_ddds) > 1 AND NOT all_matching_ddds_same THEN matching_route_ddds ELSE who_ddds END) AS ddd
            WHERE LOWER(COALESCE(ddd.ddd_comment, '')) = 'depot'
            LIMIT 1
          )

          -- If comment is "depot" and product has P route and form contains "prolonged-release", select that DDD
          WHEN has_depot_comment_ddd AND has_p_route AND has_prolonged_release_form THEN (
            SELECT AS STRUCT ddd.atc_code, ddd.ddd, ddd.ddd_unit, ddd.ddd_comment
            FROM UNNEST(CASE WHEN ARRAY_LENGTH(matching_route_ddds) > 1 AND NOT all_matching_ddds_same THEN matching_route_ddds ELSE who_ddds END) AS ddd
            WHERE LOWER(COALESCE(ddd.ddd_comment, '')) = 'depot'
            LIMIT 1
          )

          -- Fallback: When multiple different DDD values exist for the same route, and one has a 
          -- route-specific comment that doesn't match the product's route, select the other DDD value 
          -- (the one without a comment) instead
          WHEN ARRAY_LENGTH(matching_route_ddds) > 1 AND NOT all_matching_ddds_same AND (
            SELECT COUNT(1)
            FROM UNNEST(matching_route_ddds) AS ddd
            WHERE COALESCE(ddd.ddd_comment, '') = ''
          ) = 1 THEN (
            SELECT AS STRUCT ddd.atc_code, ddd.ddd, ddd.ddd_unit, ddd.ddd_comment
            FROM UNNEST(matching_route_ddds) AS ddd
            WHERE COALESCE(ddd.ddd_comment, '') = ''
            LIMIT 1
          )

      -- No rule matches: exclude this VMP
      ELSE NULL
    END AS selected_ddd
  FROM ddd_analysis da
)

SELECT
  ds.vmp_code,
  ds.vmp_name,
  selected_ddd.atc_code,
  selected_ddd.ddd,
  selected_ddd.ddd_unit AS ddd_uom,
  selected_ddd.ddd_comment,
  vs.strnt_nmrtr_val AS strength_numerator,
  vs.strnt_dnmtr_val AS strength_denominator
FROM ddd_selection ds
LEFT JOIN vmp_strength vs ON ds.vmp_code = vs.vmp_code
WHERE has_atc_codes
  AND has_ddds
  AND has_who_routes
  AND selected_ddd IS NOT NULL