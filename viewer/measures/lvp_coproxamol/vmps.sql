SELECT DISTINCT
    vmp.id as vmp_id,
    CASE 
        WHEN '18037311000001104' IN (vtm.vtm, vtm.vtmidprev) THEN 'numerator' -- VTM for co-proxamol
    END as vmp_type -- this measure has no denominator
FROM viewer_vmp vmp
LEFT JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
WHERE '18037311000001104' IN (vtm.vtm, vtm.vtmidprev) -- VTM for co-proxamol
