SELECT DISTINCT
    vmp.id AS vmp_id,
    CASE
        WHEN LOWER(TRIM(vmp.unit_dose_uom)) IN ('ampoule', 'vial')   -- makes lower case and trims any spaces around ampoule or vial
            THEN 'numerator'                                         -- unit dose is used to determine whether the VMP is a numerator or denominator, in this measure ampoules or vials become the numerator
        ELSE 'denominator'
    END AS vmp_type
FROM viewer_vmp vmp
WHERE vmp.vtm_id IS NOT NULL                                          -- vtm must not be blank. products with no vtm will not be returned
AND EXISTS (
    SELECT 1
    FROM viewer_vmpingredientstrength vis                           
    INNER JOIN viewer_ingredient ing ON ing.id = vis.ingredient_id    -- need to know what ingredient is and strength information (eg. how much potassium chloride in a ml)
    WHERE vis.vmp_id = vmp.id
      AND vis.strnt_dnmtr_val = 1
      AND vis.strnt_dnmtr_uom_name = 'ml'
      AND (
            (
                LOWER(ing.name) LIKE '%potassium chloride%'           -- we need denominator products to have a concentration greater than 40mmol/L. Here we need to make a threshold using the equivalent threshold in grams/L / mg/ml. The molecular weight of potassium chloride in 74.55g/mol (https://pubchem.ncbi.nlm.nih.gov/compound/Potassium-Chloride). 74.55*0.040 = 2.982g/L.
                AND (
                    (
                        vis.strnt_nmrtr_uom_name = 'gram'
                        AND vis.strnt_nmrtr_val >= 0.002982
                    )
                    OR (
                        vis.strnt_nmrtr_uom_name IN ('mg', 'milligram')
                        AND vis.strnt_nmrtr_val >= 2.982
                    )
                )
            )
            OR
            (
                LOWER(ing.name) LIKE '%potassium dihydrogen phosphate%'  --we need denominator products to have a concentration greater than 40mmol/L. Here we need to make a threshold using the equivalent threshold in grams/L / mg/ml. The molecular weight of potassium dihydrogen phsophate is 136.086g/mol (https://pubchem.ncbi.nlm.nih.gov/compound/24506). 136.086*0.040 = 5.44g/L.
                AND (
                    (
                        vis.strnt_nmrtr_uom_name = 'gram'
                        AND vis.strnt_nmrtr_val >= 0.005444
                    )
                    OR (
                        vis.strnt_nmrtr_uom_name IN ('mg', 'milligram')
                        AND vis.strnt_nmrtr_val >= 5.444
                    )
                )
            )
      )
)
AND LOWER(TRIM(vmp.unit_dose_uom)) IN (
    'vial',
    'bag',
    'bottle',
    'ampoule',
    'pre-filled syringe',
    'pre-filled disposable injection',
    'prefilled syringe'
);
