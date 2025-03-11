SELECT DISTINCT
    vmp.id as vmp_id,
    CASE 
        WHEN (
            vtm.vtm = '22209111000001100' -- VTM for perindopril arginine
        ) THEN 'numerator'
        ELSE 'denominator'
    END as vmp_type 
FROM viewer_vmp vmp
    INNER JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
    INNER JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
    INNER JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
WHERE 
    (
        vtm.vtm = '22209111000001100' -- VTM for perindopril arginine
        OR
        vtm.vtm = '22209011000001101' -- VTM for perindopril erbumine
    )
    AND 
    ofr.name = 'tablet.oral' -- oral tablet