SELECT DISTINCT
    vmp.id as vmp_id,
    'numerator' as vmp_type -- this measure has no denominator
FROM viewer_vmp vmp
LEFT JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
WHERE vtm.vtm = '776342000' -- VTM for insulin detemir
