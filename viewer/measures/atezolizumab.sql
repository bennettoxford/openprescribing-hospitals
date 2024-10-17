WITH date_range AS (
    SELECT generate_series(
        (SELECT MIN(year_month) FROM viewer_ingredientquantity),
        (SELECT MAX(year_month) FROM viewer_ingredientquantity),
        '1 month'::interval
    )::date AS month
),
org_list AS (
    SELECT DISTINCT COALESCE(org_successor.ods_name, org.ods_name) AS organisation,
                    COALESCE(org_successor.region, org.region) AS region
    FROM viewer_organisation org
    LEFT JOIN viewer_organisation org_successor ON org.successor_id = org_successor.ods_code
),
all_org_months AS (
    SELECT ol.organisation, ol.region, to_char(dr.month, 'YYYY-MM') AS month
    FROM org_list ol
    CROSS JOIN date_range dr
),
measure_data AS (
    SELECT 
        COALESCE(org_successor.ods_name, org.ods_name) AS organisation,
        COALESCE(org_successor.region, org.region) AS region,
        to_char(ingredient_quantity.year_month, 'YYYY-MM') AS month,
        SUM(CASE WHEN vmp.code = '42264711000001102' THEN ingredient_quantity.quantity ELSE 0 END) AS numerator,
        SUM(CASE WHEN vtm.vtm = '774689009' THEN ingredient_quantity.quantity ELSE 0 END) AS denominator
    FROM 
        viewer_ingredientquantity ingredient_quantity
    JOIN viewer_vmp vmp ON ingredient_quantity.vmp_id = vmp.code
    JOIN viewer_vtm vtm ON vmp.vtm_id = vtm.vtm
    JOIN viewer_organisation org ON ingredient_quantity.organisation_id = org.ods_code
    LEFT JOIN viewer_organisation org_successor ON org.successor_id = org_successor.ods_code
    WHERE ingredient_quantity.year_month BETWEEN (SELECT MIN(year_month) FROM viewer_ingredientquantity) AND (SELECT MAX(year_month) FROM viewer_ingredientquantity)
        AND vtm.vtm = '774689009'
    GROUP BY
        COALESCE(org_successor.ods_name, org.ods_name), 
        COALESCE(org_successor.region, org.region),
        to_char(ingredient_quantity.year_month, 'YYYY-MM')
),
org_submission_check AS (
    SELECT
        organisation,
        month,
        CASE
            WHEN successor_ods_code IS NOT NULL THEN 
                BOOL_OR(CASE WHEN is_successor THEN NOT has_submitted ELSE FALSE END)
            ELSE
                BOOL_OR(NOT has_submitted)
        END AS has_missing_submission
    FROM (
        SELECT
            COALESCE(org_successor.ods_name, org.ods_name) AS organisation,
            to_char(osc.month, 'YYYY-MM') AS month,
            osc.has_submitted,
            org_successor.ods_code AS successor_ods_code,
            CASE WHEN org_successor.ods_code IS NOT NULL AND osc.organisation_id = org_successor.ods_code THEN TRUE ELSE FALSE END AS is_successor
        FROM
            viewer_orgsubmissioncache osc
        JOIN viewer_organisation org ON osc.organisation_id = org.ods_code
        LEFT JOIN viewer_organisation org_successor ON org.successor_id = org_successor.ods_code
        WHERE osc.month BETWEEN (SELECT MIN(year_month) FROM viewer_ingredientquantity) AND (SELECT MAX(year_month) FROM viewer_ingredientquantity)
    ) subquery
    GROUP BY organisation, month, successor_ods_code
)
SELECT 
    'atezolizumab' AS name,
    'Proportion of subcutaneous Atezolizumab of all Atezolizumab' AS description,
    'ratio' AS unit,
    jsonb_build_object(
        'measure_values', jsonb_agg(
            jsonb_build_object(
                'organisation', aom.organisation,
                'region', aom.region,
                'month', aom.month,
                'quantity', CASE
                    WHEN osc.has_missing_submission THEN NULL
                    WHEN COALESCE(md.denominator, 0) = 0 THEN 0
                    ELSE COALESCE(md.numerator, 0)::float / COALESCE(md.denominator, 1)::float
                END
            )
        )
    ) AS measure_values
FROM all_org_months aom
LEFT JOIN measure_data md ON aom.organisation = md.organisation AND aom.month = md.month
LEFT JOIN org_submission_check osc ON aom.organisation = osc.organisation AND aom.month = osc.month
GROUP BY name, description, unit
