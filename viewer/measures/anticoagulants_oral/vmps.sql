SELECT DISTINCT
    vmp.id as vmp_id,
    CASE 
        WHEN vtm.vtm IN (
            '777947006', -- warfarin
            '777163004', -- phenindione
            '774409003'-- acenocoumarol
        ) 
        THEN 'numerator'
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
INNER JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
WHERE 
    vtm.vtm IN (
        '777947006', -- warfarin
        '777163004', -- phenindione
        '774409003', -- acenocoumarol
        '774624002', -- apixaban
        '777455008', -- rivaroxaban
        '775732007', -- edoxaban
        '13568411000001103'  -- dabigatran
    )
