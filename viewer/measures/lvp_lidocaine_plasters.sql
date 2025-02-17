WITH measure_vmps AS (
    SELECT DISTINCT
        vmp.id,
        CASE 
            WHEN (
                vtm.vtm = '776528009' -- VTM for lidocaine
                AND
                ofr.name = 'medicatedplaster.cutaneous' -- only medicated plaster in numerator
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
    vtm.vtm = '776528009' -- VTM for lidocaine
    AND 
    ofr.name = 'medicatedplaster.cutaneous' -- only medicated plaster in numerator
