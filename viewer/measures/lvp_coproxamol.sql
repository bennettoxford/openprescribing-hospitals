WITH measure_vmps AS (
    SELECT DISTINCT
        vmp.id,
        CASE 
            WHEN vtm.vtm IN ('18037311000001104') THEN 'numerator'  -- VTM for co-proxamol
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
WHERE vtm.vtm IN ('18037311000001104') -- VTM for co-proxamol