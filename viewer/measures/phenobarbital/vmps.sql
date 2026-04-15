SELECT DISTINCT
    vmp.id as vmp_id,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM viewer_vmpingredientstrength vis
            WHERE vis.vmp_id = vmp.id
              AND vis.strnt_nmrtr_val = 0.01
              AND vis.strnt_nmrtr_uom_name = 'gram'
              AND vis.strnt_dnmtr_val = 1
              AND vis.strnt_dnmtr_uom_name = 'ml'
        ) THEN 'denominator'
        ELSE 'numerator'
    END as vmp_type
FROM viewer_vmp vmp
INNER JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
INNER JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
INNER JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
WHERE 
    vtm.vtm IN (
        '777167003', -- phenobarbital
        '13912111000001101' --phenobarbital sodium
    )
    AND (
            ofr.name IN ('suspension.oral', 'solution.oral')
        )