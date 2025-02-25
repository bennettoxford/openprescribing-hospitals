WITH measure_vmps AS (
    SELECT DISTINCT
        vmp.id,
        CASE 
            WHEN '18037311000001104' IN (vtm.vtm, vtm.vtmidprev) THEN 'numerator' -- VTM for co-proxamol
            ELSE 'denominator'
        END as vmp_type
    FROM viewer_vmp vmp
    JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
)
SELECT 
    vmp.id as vmp_id,
    mv.vmp_type
FROM viewer_vmp vmp
JOIN measure_vmps mv ON mv.id = vmp.id
JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
WHERE '18037311000001104' IN (vtm.vtm, vtm.vtmidprev) -- VTM for co-proxamol