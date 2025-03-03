WITH measure_vmps AS (
    SELECT DISTINCT
        vmp.id,
        CASE 
            WHEN vtm.vtm IN (
                '775732007', -- VTM code for edoxaban
                '13568411000001103'  -- VTM code for dabigatran
            )
            OR (
                vtm.vtm IN (
                '774624002', -- VTM code for apixaban
                '777455008' -- VTM code for rivaroxaban
                )
                AND
                ofr.name != 'tablet.oral' -- only NONE tablet formulations in numerator as these are higher cost
            )
             THEN 'numerator'
            ELSE 'denominator'
        END as vmp_type
    FROM viewer_vmp vmp
    LEFT JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
    LEFT JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
    LEFT JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
    WHERE vmp.id != '34819111000001102' -- exclude Rivaroxaban 15mg tablets and Rivaroxaban 20mg tablets from the measure as no DDD
)
SELECT DISTINCT
    vmp.id as vmp_id,
    mv.vmp_type
FROM viewer_vmp vmp
LEFT JOIN measure_vmps mv ON mv.id = vmp.id
LEFT JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
LEFT JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
LEFT JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
WHERE vtm.vtm IN (
    '774624002', -- VTM code for apixaban
    '777455008', -- VTM code for rivaroxaban
    '775732007', -- VTM code for edoxaban
    '13568411000001103'  -- VTM code for dabigatran
)
AND vmp.id != '34819111000001102' -- exclude Rivaroxaban 15mg tablets and Rivaroxaban 20mg tablets from the measure as no DDD