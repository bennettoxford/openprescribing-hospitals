WITH measure_data AS (
    SELECT 
        org.ods_name AS organisation,
        org.region AS region,
        strftime('%Y-%m', ingredient_quantity.year_month) AS month,
        COALESCE(
            CAST(SUM(CASE WHEN vtm.vtm IN ('774624002', '777455008') THEN ingredient_quantity.quantity ELSE 0 END) AS FLOAT) / 
            NULLIF(SUM(CASE WHEN vtm.vtm IN ('774624002', '777455008', '775732007', '13568411000001103') THEN ingredient_quantity.quantity ELSE 0 END), 0),
            0
        ) AS quantity
    FROM 
        viewer_ingredientquantity ingredient_quantity
    JOIN viewer_vmp vmp ON ingredient_quantity.vmp_id = vmp.code
    JOIN viewer_vtm vtm ON vmp.vtm_id = vtm.vtm
    JOIN viewer_organisation org ON ingredient_quantity.organisation_id = org.ods_code
    GROUP BY 
        org.ods_name, 
        org.region,
        strftime('%Y-%m', ingredient_quantity.year_month)
)
SELECT 
    'value_doacs' AS name,
    'Proportion of DOACs (apixaban and rivaroxaban) to all DOACs' AS description,
    'ratio' AS unit,
    json_group_array(
        json_object(
            'organisation', organisation,
            'region', region,
            'month', month,
            'quantity', ROUND(CASE WHEN quantity > 1 THEN 1 ELSE quantity END, 4)
        )
    ) AS measure_values
FROM measure_data
GROUP BY name, description, unit