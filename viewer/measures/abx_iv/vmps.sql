WITH measure_vmps AS (
    SELECT DISTINCT
        vmp.id,
        CASE 
            WHEN ofr.name LIKE '.intravenous' THEN 'numerator'
            ELSE 'denominator'
        END as vmp_type
    FROM viewer_vmp vmp
LEFT JOIN viewer_vmp_atcs vmp_atcs ON vmp_atcs.vmp_id = vmp.id
LEFT JOIN viewer_atc atc ON atc.id = vmp_atcs.atc_id
LEFT JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
LEFT JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
WHERE atc.code LIKE 'J01%'
)
SELECT 
    vmp.id as vmp_id,
    mv.vmp_type
FROM viewer_vmp vmp
LEFT JOIN measure_vmps mv ON mv.id = vmp.id
LEFT JOIN viewer_vmp_atcs vmp_atcs ON vmp_atcs.vmp_id = vmp.id
LEFT JOIN viewer_atc atc ON atc.id = vmp_atcs.atc_id
LEFT JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
LEFT JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
WHERE atc.code LIKE 'J01%'
AND (
    ofr.name LIKE '.intravenous'
    OR
    ofr.name LIKE '.oral'
)