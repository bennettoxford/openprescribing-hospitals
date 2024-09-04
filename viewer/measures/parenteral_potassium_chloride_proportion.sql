WITH measure_data AS (
    SELECT 
        org.ods_name AS organization,
        org.region AS region,
        strftime('%Y-%m', dose.year_month) AS month,
        SUM(CASE WHEN vmp.code IN ('22498311000001100', '22498911000001100', '4898111000001100', '22497711000001100', '36019911000001100', '26821911000001101') THEN dose.quantity ELSE 0 END) AS amp_vial_quantity,
        SUM(dose.quantity) AS total_quantity
    FROM 
        viewer_dose dose
    JOIN viewer_vmp vmp ON dose.vmp_id = vmp.code
    JOIN viewer_organisation org ON dose.organisation_id = org.ods_code
    WHERE 
        vmp.code IN ('22498311000001100', '22498911000001100', '4898111000001100', '22497711000001100', '26820511000001100', '36019911000001100', '36439011000001100', '26864011000001100', '36451711000001100', '31548311000001100', '31547211000001100', '41193811000001107', '26821911000001101')
    GROUP BY 
        org.ods_name, 
        org.region,
        strftime('%Y-%m', dose.year_month)
)
SELECT 
    'parenteral_potassium_chloride_proportion' AS name,
    'Proportion of parenteral potassium chloride in amps or vials' AS description,
    'ratio' AS unit,
    json_group_array(
        json_object(
            'organization', organization,
            'region', region,
            'month', month,
            'quantity', CAST(amp_vial_quantity AS FLOAT) / NULLIF(total_quantity, 0)
        )
    ) AS measure_values
FROM measure_data
GROUP BY name, description, unit