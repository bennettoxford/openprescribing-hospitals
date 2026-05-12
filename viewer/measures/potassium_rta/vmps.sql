-- Denominator:
-- KCl or potassium dihydrogen phosphate with strength > 0.2% w/v
-- (i.e. > 0.002 g per 1 ml when expressed as gram/ml,
-- or > 2 mg per 1 ml when expressed as mg/ml),
-- unit dose UOM in the injectable list.
-- Require a VTM (vtm_id IS NOT NULL) so VMPs with no VTM are excluded from the measure,
-- including e.g. VMP 5239011000001108 (Generic Addiphos solution for infusion 20ml vials).
SELECT DISTINCT
    vmp.id AS vmp_id,
    CASE
        WHEN LOWER(TRIM(vmp.unit_dose_uom)) IN ('ampoule', 'vial') THEN 'denominator'
        ELSE 'numerator'
    END AS vmp_type
FROM viewer_vmp vmp
WHERE vmp.vtm_id IS NOT NULL
AND EXISTS (
    SELECT 1
    FROM viewer_vmpingredientstrength vis
    INNER JOIN viewer_ingredient ing ON ing.id = vis.ingredient_id
    WHERE vis.vmp_id = vmp.id
      AND (
          LOWER(ing.name) LIKE '%potassium chloride%'
          OR LOWER(ing.name) LIKE '%potassium dihydrogen phosphate%'
      )
      AND vis.strnt_dnmtr_val = 1
      AND vis.strnt_dnmtr_uom_name = 'ml'
      AND (
          (
              vis.strnt_nmrtr_uom_name = 'gram'
              AND vis.strnt_nmrtr_val >= 0.002
          )
          OR (
              vis.strnt_nmrtr_uom_name IN ('mg', 'milligram')
              AND vis.strnt_nmrtr_val > 2
          )
      )
)
AND LOWER(TRIM(vmp.unit_dose_uom)) IN (
    'vial',
    'bag',
    'bottle',
    'ampoule',
    'pre-filled syringe',
    'prefilled syringe'
);
