WITH measure_vmps AS (
    SELECT DISTINCT
        vmp.id,
        CASE 
            WHEN NOT EXISTS (
                SELECT 1 
                FROM viewer_dose d 
                WHERE d.vmp_id = vmp.id 
                AND LOWER(d.unit) IN ('ampoule', 'vial')
            ) THEN 1
            ELSE 0
        END as numerator
    FROM viewer_vmp vmp
    JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
    WHERE LOWER(vtm.name) LIKE '%potassium chloride%'
      AND LOWER(vmp.name) NOT LIKE '%ringers%'
)
SELECT 
    mv.id as vmp_id,
    1 as denominator,
    mv.numerator
FROM measure_vmps mv