WITH measure_vmps AS (
    SELECT DISTINCT
        vmp.id,
        CASE 
            WHEN (
                '22209111000001100' IN (vtm.vtm, vtm.vtmidprev) -- VTM for perindopril arginine
            ) THEN 'numerator'
            ELSE 'denominator'
        END as vmp_type
    FROM viewer_vmp vmp
    JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
    JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
    JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
)
SELECT 
    vmp.id as vmp_id,
    mv.vmp_type
FROM viewer_vmp vmp
JOIN measure_vmps mv ON mv.id = vmp.id
JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
WHERE 
    (
        '22209111000001100' IN (vtm.vtm, vtm.vtmidprev) -- VTM for perindopril arginine
        OR
        '22209011000001101' IN (vtm.vtm, vtm.vtmidprev) -- VTM for perindopril erbumine
    )
    AND 
    ofr.name = 'tablet.oral' -- oral tablet
