WITH measure_vmps AS (
    SELECT DISTINCT
        vmp.id,
        vmp.name,
        CASE 
            WHEN 
                vmp.code IN (
                    '36127511000001108', -- VMP code for Morphine 5mg modified-release tablets
                    '36126411000001107', -- VMP code for Morphine 10mg modified-release capsules
                    '36126511000001106', -- VMP code for Morphine 10mg modified-release tablets
                    '36131011000001107', -- VMP code for Oxycodone 5mg modified-release tablets
                    '36129511000001101' -- VMP code for Oxycodone 10mg modified-release tablets

                )
            THEN 'numerator'
            ELSE 'denominator'
        END as vmp_type
    FROM scmd_dmd_views.viewer_vmp vmp
    LEFT JOIN scmd_dmd_views.viewer_vtm vtm ON vtm.id = vmp.vtm_id
    LEFT JOIN scmd_dmd_views.viewer_vmp_atcs vmp_atcs ON vmp_atcs.vmp_id = vmp.id
    LEFT JOIN scmd_dmd_views.viewer_atc atc ON atc.id = vmp_atcs.atc_id
    LEFT JOIN scmd_dmd_views.viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
    LEFT JOIN scmd_dmd_views.viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id

)
SELECT DISTINCT
    vmp.id as vmp_id,
    mv.name,
    mv.vmp_type
FROM scmd_dmd_views.viewer_vmp vmp
LEFT JOIN measure_vmps mv ON mv.id = vmp.id
LEFT JOIN scmd_dmd_views.viewer_vtm vtm ON vtm.id = vmp.vtm_id
LEFT JOIN scmd_dmd_views.viewer_vmp_atcs vmp_atcs ON vmp_atcs.vmp_id = vmp.id
LEFT JOIN scmd_dmd_views.viewer_atc atc ON atc.id = vmp_atcs.atc_id
LEFT JOIN scmd_dmd_views.viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
LEFT JOIN scmd_dmd_views.viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
WHERE
    atc.code LIKE 'N02A%'
    AND
    ofr.name LIKE '%.oral'
    AND
    vtm.id IN (
        '773372004', -- VTM code for morphine
        '777027001' -- VTM code for oxycodone
    )