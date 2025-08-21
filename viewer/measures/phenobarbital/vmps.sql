SELECT DISTINCT
    vmp.id as vmp_id,
    CASE 
        WHEN vmp.code = '42133811000001100' -- Phenobarbital 15mg/5ml elixir
        THEN 'numerator'
        ELSE 'denominator'
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