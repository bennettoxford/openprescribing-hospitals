SELECT DISTINCT
    vmp.id AS vmp_id,
    CASE 
        WHEN vmp.unit_dose_uom = 'ampoule'
        THEN 'numerator'
        ELSE 'denominator'
    END AS vmp_type
FROM viewer_vmp vmp
INNER JOIN viewer_vtm vtm 
    ON vtm.id = vmp.vtm_id
INNER JOIN viewer_vmp_ont_form_routes vofr 
    ON vofr.vmp_id = vmp.id
INNER JOIN viewer_ontformroute ofr 
    ON ofr.id = vofr.ontformroute_id
WHERE vtm.vtm = '775778004' -- Ephedrine VTM
AND ofr.name = 'solutioninjection.intravenous'