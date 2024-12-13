WITH measure_vmps AS (
    SELECT DISTINCT
        vmp.id,
        CASE 
            WHEN ofr.name = 'pressurizedinhalation.inhalation' THEN 'numerator'
            ELSE 'denominator'
        END as vmp_type
    FROM viewer_vmp vmp
    JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
    JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
    WHERE ofr.name IN (
        'pressurizedinhalation.inhalation',
        'powderinhalation.inhalation',
        'inhalationsolution.inhalation'
    )
)
SELECT 
    vmp.id as vmp_id,
    mv.vmp_type
FROM viewer_vmp vmp
JOIN measure_vmps mv ON mv.id = vmp.id
JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
WHERE ofr.name IN (
    'pressurizedinhalation.inhalation',
    'powderinhalation.inhalation',
    'inhalationsolution.inhalation'
)
