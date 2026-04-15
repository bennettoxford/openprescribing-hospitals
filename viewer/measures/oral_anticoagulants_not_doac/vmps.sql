SELECT DISTINCT
    vmp.id as vmp_id,
    CASE
        WHEN vtm.vtm IN (
            '777947006', -- warfarin
            '777163004', -- acenocoumarol
            '774409003'  -- phenindione
        ) THEN 'numerator'
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
INNER JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
INNER JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
INNER JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
WHERE vtm.vtm IN (
    '777455008', -- rivaroxaban
    '13568411000001103', -- dabigatran
    '775732007', -- edoxaban
    '774624002', -- apixaban
    '777947006', -- warfarin
    '777163004', -- acenocoumarol
    '774409003' -- phenindione
)
AND ofr.name LIKE '%.oral'
AND vmp.id != '34819111000001102' -- exclude rivaroxaban initiation pack as no DDD
