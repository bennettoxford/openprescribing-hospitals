SELECT DISTINCT
    vmp.id as vmp_id,
    'numerator' as vmp_type -- this measure has no denominator
FROM viewer_vmp vmp
LEFT JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
WHERE vtm.vtm = '18037311000001104' -- VTM for co-proxamol
