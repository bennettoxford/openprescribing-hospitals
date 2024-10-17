WITH date_range AS (
    SELECT generate_series(
        (SELECT MIN(year_month) FROM viewer_dose),
        (SELECT MAX(year_month) FROM viewer_dose),
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
        to_char(dose.year_month, 'YYYY-MM') AS month,
        SUM(CASE WHEN dose.unit IN ('ampoule', 'vial') THEN dose.quantity ELSE 0 END) AS numerator,
        SUM(dose.quantity) AS denominator
    FROM 
        viewer_dose dose
    JOIN viewer_vmp vmp ON dose.vmp_id = vmp.code
    JOIN viewer_vtm vtm ON vmp.vtm_id = vtm.vtm
    JOIN viewer_organisation org ON dose.organisation_id = org.ods_code
    LEFT JOIN viewer_organisation org_successor ON org.successor_id = org_successor.ods_code
    WHERE vtm.vtm = '777274002'
    GROUP BY
        COALESCE(org_successor.ods_name, org.ods_name), 
        COALESCE(org_successor.region, org.region),
        to_char(dose.year_month, 'YYYY-MM')
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
    ) subquery
    GROUP BY organisation, month, successor_ods_code
),
vmp_lists AS (
    SELECT 
        array_agg(DISTINCT vmp.code) FILTER (WHERE dose.unit IN ('ampoule', 'vial')) AS numerator_vmps,
        array_agg(DISTINCT vmp.code) AS denominator_vmps
    FROM 
        viewer_dose dose
    JOIN viewer_vmp vmp ON dose.vmp_id = vmp.code
    JOIN viewer_vtm vtm ON vmp.vtm_id = vtm.vtm
    WHERE vtm.vtm = '777274002'
)
SELECT 
    'parenteral_potassium_chloride_proportion' AS name,
    'Proportion of parenteral potassium chloride in amps or vials' AS description,
    'ratio' AS unit,
    jsonb_build_object(
        'measure_values', jsonb_agg(
            jsonb_build_object(
                'organisation', aom.organisation,
                'region', aom.region,
                'month', aom.month,
                'numerator', CASE
                    WHEN osc.has_missing_submission THEN NULL
                    WHEN md.numerator IS NULL THEN 0
                    ELSE md.numerator
                END,
                'denominator', CASE
                    WHEN osc.has_missing_submission THEN NULL
                    WHEN md.denominator IS NULL THEN 0
                    ELSE md.denominator
                END
            )
        ),
        'numerator_vmps', (SELECT numerator_vmps FROM vmp_lists),
        'denominator_vmps', (SELECT denominator_vmps FROM vmp_lists)
    ) AS values
FROM all_org_months aom
LEFT JOIN measure_data md ON aom.organisation = md.organisation AND aom.month = md.month
LEFT JOIN org_submission_check osc ON aom.organisation = osc.organisation AND aom.month = osc.month
GROUP BY name, description, unit
