WITH potassium_vmps AS (
    -- 1. All potassium chloride products this should include potassium chloride diluted in NaCL, glucose
    SELECT DISTINCT vmp.id, vmp.name
    FROM viewer_vmp vmp
    INNER JOIN viewer_vtm vtm 
        ON vtm.id = vmp.vtm_id
    WHERE LOWER(vtm.name) LIKE '%potassium chloride%'
),

injectable_vmps AS (
    -- 2. Restrict to injectables. Many Potassium products are oral
    SELECT DISTINCT kv.id, kv.name
    FROM potassium_vmps kv
    INNER JOIN viewer_vmp_ont_form_routes vofr 
        ON vofr.vmp_id = kv.id
    INNER JOIN viewer_ontformroute ofr 
        ON ofr.id = vofr.ontformroute_id
    WHERE ofr.name = 'solutioninjection.intravenous'
),

measure_vmps AS (
    -- 4. Define numerator (not amp/vial). The lower the proportion of amps and vials the better
    SELECT DISTINCT
        sv.id,
        CASE 
            WHEN NOT EXISTS (
                SELECT 1 
                FROM viewer_dose d 
                WHERE d.vmp_id = sv.id
                  AND LOWER(d.unit) IN ('ampoule', 'vial')
            ) THEN 1
            ELSE 0
        END AS numerator
    FROM strong_vmps sv
)

SELECT 
    mv.id AS vmp_id,
    1 AS denominator,
    mv.numerator
FROM measure_vmps mv;
