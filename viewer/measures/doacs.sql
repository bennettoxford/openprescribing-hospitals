WITH measure_vmps AS (
    SELECT DISTINCT
        vmp.id,
        CASE 
            WHEN vtm.vtm IN (
                '775732007', -- VTM code for edoxaban
                '13568411000001104'  -- 13568411000001103 is the VTM code for dabigatran - this has been manually changed to account for data upload issue - needs to be fixed before launching
            )
            OR (
                vtm.vtm IN (
                '774624002', -- VTM code for apixaban
                '777455008', -- VTM code for rivaroxaban
                )
                AND
                ofr.name = 'tablet.oral' -- only oral MR tablet formulations in numerator
            )
             THEN 'numerator'
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
WHERE vtm.vtm IN (
    '774624002', -- VTM code for apixaban
    '777455008', -- VTM code for rivaroxaban
    '775732007', -- VTM code for edoxaban
    '13568411000001104'  -- 13568411000001103 is the VTM code for dabigatran - this has been manually changed to account for data upload issue - needs to be fixed before launching
)