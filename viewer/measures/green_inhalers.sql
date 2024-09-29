WITH measure_data AS (
    SELECT 
        org.ods_name AS organisation,
        org.region AS region,
        to_char(dose.year_month, 'YYYY-MM') AS month,
        SUM(CASE WHEN vmp.code = '41791911000001107' THEN dose.quantity ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN vmp.code IN ('41791911000001107', '41792011000001100') THEN dose.quantity ELSE 0 END), 0) AS quantity
    FROM 
        viewer_dose dose
    JOIN viewer_vmp vmp ON dose.vmp_id = vmp.code
    JOIN viewer_organisation org ON dose.organisation_id = org.ods_code
    WHERE 
        vmp.code IN ('41791911000001107', '41792011000001100')
    GROUP BY 
        org.ods_name, 
        org.region,
        to_char(dose.year_month, 'YYYY-MM')
)
SELECT 
    'methotrexate_proportion' AS name,
    'Proportion of methotrexate 10mg tablets to all methotrexate tablets' AS description,
    'ratio' AS unit,
    json_agg(
        json_build_object(
            'organisation', organisation,
            'region', region,
            'month', month,
            'quantity', quantity
        )
    ) AS measure_values
FROM measure_data
GROUP BY name, description, unit