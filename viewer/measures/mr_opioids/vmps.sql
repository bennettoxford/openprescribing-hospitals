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
LEFT JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
LEFT JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
LEFT JOIN viewer_vmpingredientstrength vis ON CAST(vis.vmp AS STRING) = vmp.id
WHERE
    ofr.name LIKE '%.oral' -- only include oral products in the measure
    AND 
    (
        (
            vis.basis_of_strength_name LIKE 'Morphine%'
            OR
            vis.basis_of_strength_name LIKE 'Oxycodone%'
        ) -- only include products with morphine or oxycodone as an ingredient
    AND
        ( 
            CASE
                WHEN vis.strnt_nmrtr_uom_name = 'micrograms' THEN vis.strnt_nmrtr_val / 1000 -- convert micrograms to mg
                WHEN vis.strnt_nmrtr_uom_name = 'gram'      THEN vis.strnt_nmrtr_val * 1000 -- convert gram to mg
                ELSE vis.strnt_nmrtr_val
            END
            * CASE
                WHEN vis.strnt_dnmtr_val IS NOT NULL THEN 5 / vis.strnt_dnmtr_val -- for liquids give as mg per 5ml dose
                ELSE 1
            END
        ) <= 10 -- only include products 10mg or less per dose
    )