WITH measure_data AS (
    SELECT 
        org.ods_name AS organisation,
        org.region AS region,
        strftime('%Y-%m', dose.year_month) AS month,
        SUM(CASE WHEN vmp.code IN ('39332111000001100', '39332211000001106') THEN dose.quantity ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN vmp.code IN ('39332111000001100', '39332211000001106', '41772111000001108', '35903811000001108', '23204911000001105', '41329811000001106', '23985411000001104', '23985511000001100') THEN dose.quantity ELSE 0 END), 0) AS quantity
    FROM 
        viewer_dose dose
    JOIN viewer_vmp vmp ON dose.vmp_id = vmp.code
    JOIN viewer_organisation org ON dose.organisation_id = org.ods_code
    GROUP BY 
        org.ods_name, 
        org.region,
        strftime('%Y-%m', dose.year_month)
)
SELECT 
    'phesgo' AS name,
    'Proportion of Phesgo of all Trastuzumab' AS description,
    'ratio' AS unit,
    json_group_array(
        json_object(
            'organisation', organisation,
            'region', region,
            'month', month,
            'quantity', quantity
        )
    ) AS measure_values
FROM measure_data
GROUP BY name, description, unit