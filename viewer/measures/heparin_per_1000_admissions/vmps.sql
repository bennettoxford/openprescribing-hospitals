SELECT DISTINCT
    vmp.id AS vmp_id,
    'numerator' AS vmp_type
FROM viewer_vmp vmp
WHERE EXISTS (
    SELECT 1
    FROM viewer_vmpingredientstrength vis
    JOIN viewer_ingredient ing
        ON ing.id = vis.ingredient_id
    WHERE vis.vmp_id = vmp.id
      AND LOWER(ing.name) LIKE '%heparin sodium%'
      AND (
            (vis.strnt_nmrtr_val = 1000  AND vis.strnt_dnmtr_val = 1)
         OR (vis.strnt_nmrtr_val = 5000  AND vis.strnt_dnmtr_val = 1)
         OR (vis.strnt_nmrtr_val = 25000 AND vis.strnt_dnmtr_val = 5)
      )
)
AND LOWER(TRIM(vmp.unit_dose_uom)) NOT LIKE '%eye%';
