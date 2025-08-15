SELECT DISTINCT
    vmp.id as vmp_id,
    CASE 
        WHEN  vmp.code NOT IN (
            '42202211000001101', -- Omeprazole 1mg/ml oral suspension sugar free
            '38072111000001103', -- Omeprazole 10mg/5ml oral suspension sugar free
            '38072211000001109' -- Omeprazole 20mg/5ml oral suspension sugar free
        )
        THEN 'numerator'
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
INNER JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
INNER JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
INNER JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
WHERE 
    vtm.vtm = '776991000' -- omeprazole VTM
    AND (
            ofr.name IN ('suspension.oral', 'solution.oral')
            AND NOT EXISTS (
                SELECT 1 
                FROM viewer_vmp_ont_form_routes vofr2
                INNER JOIN viewer_ontformroute ofr2 ON ofr2.id = vofr2.ontformroute_id
                WHERE vofr2.vmp_id = vmp.id 
                AND ofr2.name = 'tablet.oral'
            )
        )