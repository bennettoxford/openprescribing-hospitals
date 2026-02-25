SELECT DISTINCT
    vmp.id as vmp_id,
    CASE 
        WHEN vtm.vtm != '775745006' -- eltrombopag
        THEN 'numerator'
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
INNER JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
WHERE 
    vtm.vtm IN (
        '775745006', -- eltrombopag
        '777463009', -- rompiplostim
        '774706009'  -- avatrombopag
    )
