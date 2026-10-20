WITH measure_vmps AS (
    SELECT
        vmp.id,
        MAX(
            CASE
                WHEN atc.code LIKE 'J01M%'
                     AND (
                         ofr.name LIKE '%.intravenous'
                         OR ofr.name LIKE '%.oral'
                     )
                THEN 1
                ELSE 0
            END
        ) AS is_quinolone
    FROM viewer_vmp vmp
    LEFT JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
    LEFT JOIN viewer_vmp_atcs vmp_atcs ON vmp_atcs.vmp_id = vmp.id
    LEFT JOIN viewer_atc atc ON atc.id = vmp_atcs.atc_id
    LEFT JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
    LEFT JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
    GROUP BY vmp.id
)
SELECT DISTINCT
    vmp.id AS vmp_id,
    CASE
        WHEN mv.is_quinolone = 1 THEN 'numerator'
        ELSE 'denominator'
    END AS vmp_type
FROM viewer_vmp vmp
LEFT JOIN measure_vmps mv ON mv.id = vmp.id
LEFT JOIN viewer_vmp_atcs vmp_atcs ON vmp_atcs.vmp_id = vmp.id
LEFT JOIN viewer_atc atc ON atc.id = vmp_atcs.atc_id
LEFT JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
LEFT JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
WHERE (ofr.name LIKE '%.intravenous' OR ofr.name LIKE '%.oral')
AND atc.code LIKE 'J01%';
