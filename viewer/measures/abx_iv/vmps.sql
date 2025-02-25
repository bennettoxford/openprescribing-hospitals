WITH measure_vmps AS (
    SELECT DISTINCT
        vmp.id,
        CASE 
            WHEN ofr.name LIKE '%.intravenous' 
                AND (
                    atc.code LIKE 'J01%'
                    OR 
                    vtm.vtm IN (
                        '776774005', -- VTM for metronidazole to account for recording outside J01
                        '776885003', -- VTM for neomycin to account for recording outside J01
                        '777775000', -- VTM for tinidazole to account for recording outside J01
                        '774587000' -- VTM for co-amoxiclav to account for product without dmd -> ATC link
                    ) 
                )
            THEN 'numerator'
            ELSE 'denominator'
        END as vmp_type
    FROM viewer_vmp vmp
    LEFT JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
    LEFT JOIN viewer_vmp_atcs vmp_atcs ON vmp_atcs.vmp_id = vmp.id
    LEFT JOIN viewer_atc atc ON atc.id = vmp_atcs.atc_id
    LEFT JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
    LEFT JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id

)
SELECT DISTINCT
    vmp.id as vmp_id,
    mv.vmp_type
FROM viewer_vmp vmp
LEFT JOIN measure_vmps mv ON mv.id = vmp.id
LEFT JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
LEFT JOIN viewer_vmp_atcs vmp_atcs ON vmp_atcs.vmp_id = vmp.id
LEFT JOIN viewer_atc atc ON atc.id = vmp_atcs.atc_id
LEFT JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
LEFT JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
WHERE (ofr.name LIKE '%.intravenous' OR ofr.name LIKE '%.oral')
AND (
    atc.code LIKE 'J01%'
    OR 
    vtm.vtm IN (
        '776774005', -- VTM for metronidazole to account for recording outside J01
        '776885003', -- VTM for neomycin to account for recording outside J01
        '777775000', -- VTM for tinidazole to account for recording outside J01
        '774587000' -- VTM for co-amoxiclav to account for product without dmd -> ATC link
    ) 
)