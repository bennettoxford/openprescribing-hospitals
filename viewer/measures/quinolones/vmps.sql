WITH measure_vmps AS (
    SELECT DISTINCT
        vmp.id,
        CASE
            WHEN atc.code LIKE 'J01M%' --quinolones 
                 AND (
                     ofr.name LIKE '%.intravenous' --we only want systemic preparations be that oral or parenteral
                     OR ofr.name LIKE '%.oral'
                 )
            THEN 1
            ELSE 0
        END AS numerator,
        CASE
            WHEN atc.code LIKE 'J01%' -- atc code for all abx
            THEN 1
            ELSE 0
        END AS denominator
    FROM viewer_vmp vmp
    LEFT JOIN viewer_vtm vtm
        ON vtm.id = vmp.vtm_id
    LEFT JOIN viewer_vmp_atcs vmp_atcs
        ON vmp_atcs.vmp_id = vmp.id
    LEFT JOIN viewer_atc atc
        ON atc.id = vmp_atcs.atc_id
    LEFT JOIN viewer_vmp_ont_form_routes vofr
        ON vofr.vmp_id = vmp.id
    LEFT JOIN viewer_ontformroute ofr
        ON ofr.id = vofr.ontformroute_id
)
