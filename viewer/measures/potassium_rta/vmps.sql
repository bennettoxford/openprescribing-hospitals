WITH mmol_calculated AS ( -- This pre-calculates the mmol per litre for each VMP and calculates the number of ingredients in each VMP
  SELECT
    vmp.id AS vmp_id,
    vmp.name AS vmp_name,
    (
      SELECT COUNT(*)
      FROM viewer_vmpingredientstrength vis_count
      WHERE vis_count.vmp_id = vmp.id
    ) AS ing_count, -- Add a count of ingredients for each VMP
    ROUND( -- Calculate the mmol per litre for each VMP
      (
        CASE -- Convert the numerator to milligrams
          WHEN vis.strnt_nmrtr_uom_name IN ('gram', 'g')
            THEN vis.strnt_nmrtr_val * 1000
          WHEN vis.strnt_nmrtr_uom_name IN ('milligram', 'mg')
            THEN vis.strnt_nmrtr_val
          WHEN vis.strnt_nmrtr_uom_name IN ('microgram', 'mcg')
            THEN vis.strnt_nmrtr_val / 1000
        END
        /
        CASE -- Define the molecular weight from ingredient to convert strength from mg to mmol
          WHEN LOWER(ing.name) = 'potassium chloride'
            THEN 74.55 -- Molecular weight of potassium chloride is 74.55 g/mol - REF: https://pubchem.ncbi.nlm.nih.gov/compound/Potassium-Chloride
          WHEN LOWER(ing.name) = 'potassium dihydrogen phosphate'
            THEN 136.09 -- Molecular weight of potassium dihydrogen phosphate is 136.09 g/mol - REF: https://pubchem.ncbi.nlm.nih.gov/compound/24506
        END
      )
      /
      CASE -- Convert the denominator to litres
        WHEN vis.strnt_dnmtr_uom_name = 'ml'
          THEN vis.strnt_dnmtr_val / 1000
        WHEN vis.strnt_dnmtr_uom_name = 'litre'
          THEN vis.strnt_dnmtr_val
      END
    ) AS mmol_per_litre,
    vmp.udfs,
    vmp.udfs_uom,
    vmp.unit_dose_uom,
    vmp.special,
    ing.name AS ingredient_name,
    vis.strnt_nmrtr_val,
    vis.strnt_nmrtr_uom_name,
    vis.strnt_dnmtr_val,
    vis.strnt_dnmtr_uom_name

  FROM viewer_vmp AS vmp
  INNER JOIN viewer_vmpingredientstrength AS vis
    ON vis.vmp_id = vmp.id
  INNER JOIN viewer_ingredient AS ing
    ON ing.id = vis.ingredient_id
)

SELECT DISTINCT
  vmp_id,
  CASE
    WHEN udfs_uom = 'ml'
      AND udfs <= 20
      THEN 'numerator'
    ELSE 'denominator'
  END AS vmp_type
FROM mmol_calculated
WHERE LOWER(ingredient_name) IN ( -- -- Only include potassium chloride or potassium dihydrogen phosphate ingredient records
  'potassium chloride',
  'potassium dihydrogen phosphate'
)
AND ing_count <= 2 -- Only include VMPs with 2 or fewer ingredients, mainly to exclude Addiphos which has a different purpose
AND LOWER(TRIM(unit_dose_uom)) IN ( -- Restrict to relevant injectable unit-dose presentations; route cannot be relied on because some VMPs have no recorded route
  'vial',
  'bag',
  'bottle',
  'ampoule',
  'pre-filled syringe',
  'pre-filled disposable injection',
  'prefilled syringe'
)
AND mmol_per_litre > 80 -- Only include VMPs with mmol per litre greater than 80
AND (
    -- Include VMPs with no recorded form/route, or where at least one recorded route is intravenous - mainly aimed to exclude cardioplegia solutions which have intraarterial route
  NOT EXISTS (
    SELECT 1
    FROM viewer_vmp_ont_form_routes vofr
    WHERE vofr.vmp_id = mmol_calculated.vmp_id
  )
  OR EXISTS (
    SELECT 1
    FROM viewer_vmp_ont_form_routes vofr
    INNER JOIN viewer_ontformroute ofr
      ON ofr.id = vofr.ontformroute_id
    WHERE vofr.vmp_id = mmol_calculated.vmp_id
      AND ofr.name LIKE '%intravenous'
  )
);