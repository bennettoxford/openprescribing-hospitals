WITH measure_vmps AS (
    SELECT DISTINCT
        vmp.id,
        CASE 
            WHEN 
                vtm.vtm = '773372004' -- VTM code for morphine
                AND ofr.name LIKE 'solution.oral'
            THEN 'numerator'
            ELSE 'denominator'
        END as vmp_type
    FROM viewer_vmp vmp
)
SELECT DISTINCT
    vmp.id as vmp_id,
    mv.vmp_type
FROM viewer_vmp vmp
LEFT JOIN measure_vmps mv ON mv.id = vmp.id
LEFT JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
LEFT JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
LEFT JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
WHERE
    ofr.name IN (
        'solution.oral',
        'tablet.oral',
        'capsule.oral',
        'tabletdispersible.oral'
    )
    AND vtm.vtm = '773372004' -- VTM code for morphine