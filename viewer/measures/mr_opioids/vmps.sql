WITH measure_vmps AS (
    SELECT DISTINCT
        vmp.id,
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
    ofr.name LIKE '%.oral'
    AND
    vtm.id IN (
        '773372004', -- VTM code for morphine
        '777027001' -- VTM code for oxycodone
    )