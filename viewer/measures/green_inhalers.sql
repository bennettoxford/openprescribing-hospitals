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
AND vmp.code NOT IN (
    '32687711000001109',  -- Loxapine 9.1mg/dose inhalation powder
    '22555311000001104',  -- Colistimethate 1,662,500unit inhalation powder capsules
    '15426411000001107',  -- Mannitol 40mg inhalation powder capsules
    '20514511000001105',  -- Mannitol 40mg inhalation powder capsules with device
    '20515111000001102',  -- Mannitol 40mg inhalation powder capsules with two devices
    '39488111000001102',  -- Pentetic acid 20.8mg kit for radiopharmaceutical preparation
    '19544811000001101',  -- Tobramycin 28mg inhalation powder capsules
    '36151011000001106'   -- Zanamivir 5mg inhalation powder blisters with device
)
