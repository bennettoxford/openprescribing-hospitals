SELECT DISTINCT
    vmp.id as vmp_id,
    CASE 
        WHEN ofr.name = 'solutioninjection.subcutaneous' THEN 'numerator' -- only subcutaneous formulations in numerator
        WHEN ofr.name = 'solutioninfusion.intravenous' THEN 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
INNER JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
INNER JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
INNER JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
WHERE vtm.vtm = '776934005' -- Nivolumab VTM