WITH measure_vmps AS (
    SELECT DISTINCT
        vmp.id,
        CASE 
            WHEN (
                vtm.vtm = '773245005' -- VTM for doxazosin
                AND
                ofr.name = 'tabletmodified-release.oral' -- only oral MR tablet formulations in numerator
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
    vtm.vtm = '773245005' -- VTM for doxazosin
    AND 
    ofr.name IN ('tabletmodified-release.oral', 'tablet.oral') -- oral tablet/MR tablet formulations in denominator
