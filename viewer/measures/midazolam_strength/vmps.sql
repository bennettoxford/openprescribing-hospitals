SELECT DISTINCT
    vmp.id AS vmp_id,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM viewer_vmpingredientstrength vis
            WHERE vis.vmp_id = vmp.id
              AND vis.strnt_nmrtr_uom_name = 'gram'
              AND vis.strnt_dnmtr_uom_name = 'ml'
              AND (
                    (
                        (vis.strnt_nmrtr_val * 1000) = 2 --all 2mg/2ml products as per NPSA / NHS Never event recommendations
                        AND vis.strnt_dnmtr_val = 2
                    )
                 OR (
                        (vis.strnt_nmrtr_val * 1000) = 5 --all 5mg/5ml products as per NPSA / NHS Never event recommendations
                        AND vis.strnt_dnmtr_val = 5
                    )
                  )
        )
        THEN 'denominator'
        ELSE 'numerator'
    END AS vmp_type
FROM viewer_vmp vmp
INNER JOIN viewer_vtm vtm
    ON vtm.id = vmp.vtm_id
INNER JOIN viewer_vmp_ont_form_routes vofr
    ON vofr.vmp_id = vmp.id
INNER JOIN viewer_ontformroute ofr
    ON ofr.id = vofr.ontformroute_id
WHERE
    vtm.vtm = '776785007' -- Midazolam
    AND ofr.name IN (                             --to get a denominator of all midazolam products
        'solutioninjection.intravenous',
        'solutioninfusion.intravenous',
        'solutioninjection.intramuscular'
    );
