WITH measure_data AS (
    SELECT 
        org.ods_name AS organization,
        org.region AS region,
        strftime('%Y-%m', dose.year_month) AS month,
        CAST(COUNT(*) FILTER (WHERE vmp.code = '7977911000001102') AS FLOAT) / 
            NULLIF(COUNT(*) FILTER (WHERE vtm.vtm = vmp.vtm_id), 0) AS quantity
    FROM 
        viewer_dose dose
    JOIN viewer_vmp vmp ON dose.vmp_id = vmp.code
    LEFT JOIN viewer_vtm vtm ON vmp.vtm_id = vtm.vtm
    JOIN viewer_organisation org ON dose.organisation_id = org.ods_code
    WHERE 
        vmp.code = '7977911000001102' 
        OR vmp.vtm_id = (
            SELECT vtm_id 
            FROM viewer_vmp 
            WHERE code = '7977911000001102'
        )
    GROUP BY 
        org.ods_name, 
        org.region,
        strftime('%Y-%m', dose.year_month)
)
SELECT 
    'demo_measure' AS name,
    'Ratio of doses for VMP 7977911000001102 to its parent VTM' AS description,
    'ratio' AS unit,
    json_group_array(
        json_object(
            'organization', organization,
            'region', region,
            'month', month,
            'quantity', quantity
        )
    ) AS measure_values
FROM measure_data
GROUP BY name, description, unit