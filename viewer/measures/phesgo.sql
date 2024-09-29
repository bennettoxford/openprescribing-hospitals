WITH measure_data AS (
    SELECT 
        org.ods_name AS organisation,
        org.region AS region,
        to_char(dose.year_month, 'YYYY-MM') AS month,
        SUM(CASE WHEN vmp.code IN ('39332111000001100', '39332211000001106') THEN dose.quantity ELSE 0 END)::float / 
            NULLIF(SUM(CASE WHEN vmp.code IN ('39332111000001100', '39332211000001106', '41772111000001108', '35903811000001108', '23204911000001105', '41329811000001106', '23985411000001104', '23985511000001100') THEN dose.quantity ELSE 0 END), 0)::float AS quantity
    FROM 
        viewer_dose dose
    JOIN viewer_vmp vmp ON dose.vmp_id = vmp.code
    JOIN viewer_organisation org ON dose.organisation_id = org.ods_code
    GROUP BY 
        org.ods_name, 
        org.region,
        to_char(dose.year_month, 'YYYY-MM')
)
SELECT 
    'phesgo' AS name,
    'Proportion of Phesgo of all Trastuzumab' AS description,
    'ratio' AS unit,
    jsonb_build_object(
        'measure_values', jsonb_agg(
            jsonb_build_object(
                'organisation', organisation,
                'region', region,
                'month', month,
                'quantity', quantity
            )
        )
    ) AS measure_values
FROM measure_data
GROUP BY name, description, unit