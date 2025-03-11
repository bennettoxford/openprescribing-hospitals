SELECT DISTINCT
    vmp.id,
    CASE 
        WHEN vtm.vtm = '1306706004' THEN 'numerator' -- VTM code for Pertuzumab + Trastuzumab
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
INNER JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
WHERE 
    vtm.vtm = '1306706004' -- VTM code for Pertuzumab + Trastuzumab
    OR
    vtm.vtm = '777148002' -- VTM code for Pertuzumab
