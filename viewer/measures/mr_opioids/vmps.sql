WITH measure_vmps AS (
    SELECT DISTINCT
        vmp.id,
        CASE 
            WHEN 
                ofr.name LIKE '%modified-release%' -- only include modified-release in the numerator            
            THEN 'numerator'
            ELSE 'denominator'
        END as vmp_type
    FROM viewer_vmp vmp
    LEFT JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
    LEFT JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
)
SELECT DISTINCT
    vmp.id as vmp_id,
    mv.vmp_type
FROM viewer_vmp vmp
LEFT JOIN measure_vmps mv ON mv.id = vmp.id
LEFT JOIN viewer_vmp_atcs vmp_atcs ON vmp_atcs.vmp_id = vmp.id
LEFT JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
LEFT JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
LEFT JOIN viewer_vmpingredientstrength vis ON vis.vmp_id = vmp.id
LEFT JOIN viewer_ingredient ing ON ing.id = vis.ingredient_id
WHERE
    ofr.name LIKE '%.oral' -- only include oral products in the measure
    AND 
    vmp_atcs.atc_id LIKE 'N02%' -- only include products with ATC code N02 (opioids for analgesia)
    AND 
    (
        (
            LOWER(ing.name) LIKE 'morphine%'
            OR
            LOWER(ing.name) LIKE 'oxycodone%'
        ) -- only include products with morphine or oxycodone as an ingredient
    AND
        ( 
            CASE
                WHEN LOWER(vis.strnt_nmrtr_uom_name) IN ('mcg', 'mcgs', 'microgram', 'micrograms')  THEN vis.strnt_nmrtr_val / 1000   -- convert micrograms to mg
                WHEN LOWER(vis.strnt_nmrtr_uom_name) IN ('g', 'gs', 'gram', 'grams')                THEN vis.strnt_nmrtr_val * 1000   -- convert gram to mg
                WHEN LOWER(vis.strnt_nmrtr_uom_name) IN ('mg', 'mgs', 'milligram', 'milligrams')    THEN vis.strnt_nmrtr_val
                ELSE NULL
            END
            * CASE
                WHEN vis.strnt_dnmtr_val IS NOT NULL THEN 
                    5 / (
                        CASE
                            WHEN LOWER(vis.strnt_dnmtr_uom_name) IN ('l', 'ls', 'litre', 'litres') THEN vis.strnt_dnmtr_val * 1000
                            ELSE vis.strnt_dnmtr_val -- assume already in ml
                        END
                    ) -- for liquids give as mg per 5ml dose
                ELSE 1
            END
        ) <= 10 -- only include products 10mg or less per dose
    )