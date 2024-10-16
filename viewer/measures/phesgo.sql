WITH measure_data AS (
    SELECT 
        COALESCE(org_successor.ods_name, org.ods_name) AS organisation,
        COALESCE(org_successor.region, org.region) AS region,
        to_char(dose.year_month, 'YYYY-MM') AS month,
        SUM(CASE WHEN vmp.code IN ('39332111000001100', '39332211000001106') THEN dose.quantity ELSE 0 END) AS numerator,
        SUM(CASE WHEN vmp.code IN ('39332111000001100', '39332211000001106', '41772111000001108', '35903811000001108', '23204911000001105', '41329811000001106', '23985411000001104', '23985511000001100') THEN dose.quantity ELSE 0 END) AS denominator
    FROM 
        viewer_dose dose
    JOIN viewer_vmp vmp ON dose.vmp_id = vmp.code
    JOIN viewer_organisation org ON dose.organisation_id = org.ods_code
    LEFT JOIN viewer_organisation org_successor ON org.successor_id = org_successor.ods_code
    GROUP BY
        COALESCE(org_successor.ods_name, org.ods_name), 
        COALESCE(org_successor.region, org.region),
        to_char(dose.year_month, 'YYYY-MM')
),
org_submission_check AS (
    SELECT
        organisation,
        month,
        BOOL_OR(NOT has_submitted) AS has_missing_submission
    FROM (
        SELECT
            COALESCE(org_successor.ods_name, org.ods_name) AS organisation,
            to_char(osc.month, 'YYYY-MM') AS month,
            osc.has_submitted
        FROM
            viewer_orgsubmissioncache osc
        JOIN viewer_organisation org ON osc.organisation_id = org.ods_code
        LEFT JOIN viewer_organisation org_successor ON org.successor_id = org_successor.ods_code
    ) subquery
    GROUP BY organisation, month
)
SELECT 
    'phesgo' AS name,
    'Proportion of Phesgo of all Trastuzumab' AS description,
    'ratio' AS unit,
    jsonb_build_object(
        'measure_values', jsonb_agg(
            jsonb_build_object(
                'organisation', md.organisation,
                'region', md.region,
                'month', md.month,
                'quantity', CASE
                    WHEN md.numerator IS NULL OR md.denominator IS NULL OR md.denominator = 0 OR osc.has_missing_submission THEN NULL
                    ELSE md.numerator::float / md.denominator::float
                END
            )
        )
    ) AS measure_values
FROM measure_data md
LEFT JOIN org_submission_check osc ON md.organisation = osc.organisation AND md.month = osc.month
GROUP BY name, description, unit
