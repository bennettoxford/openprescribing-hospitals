WITH measure_vmps AS (
    SELECT DISTINCT
        vmp.id,
        CASE 
            WHEN vmp.code = '42264711000001102' THEN 'numerator'
            ELSE 'denominator'
        END as vmp_type
    FROM viewer_vmp vmp
)
SELECT 
    vmp.id as vmp_id,
    mv.vmp_type
FROM viewer_vmp vmp
JOIN measure_vmps mv ON mv.id = vmp.id
JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
WHERE vtm.vtm = '774689009'