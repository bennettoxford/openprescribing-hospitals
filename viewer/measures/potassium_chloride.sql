WITH measure_vmps AS (
    SELECT DISTINCT
        vmp.id,
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM viewer_dose d 
                WHERE d.vmp_id = vmp.id 
                AND d.data[1][3] IN ('ampoule', 'vial')
            ) THEN 'numerator'
            ELSE 'denominator'
        END as vmp_type
    FROM viewer_vmp vmp
    JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
    WHERE LOWER(vtm.name) LIKE '%potassium chloride%'
    AND LOWER(vmp.name) NOT LIKE '%ringers%'
)
SELECT 
    vmp.id as vmp_id,
    mv.vmp_type
FROM viewer_vmp vmp
JOIN measure_vmps mv ON mv.id = vmp.id
JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
WHERE LOWER(vtm.name) LIKE '%potassium chloride%'
    AND LOWER(vmp.name) NOT LIKE '%ringers%'