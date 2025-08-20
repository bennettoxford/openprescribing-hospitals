SELECT DISTINCT
    vmp.id as vmp_id,
    CASE 
        WHEN vtm.vtm != '775447002' -- dapagliflozin
        THEN 'numerator'
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
INNER JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
WHERE 
    vtm.vtm IN (
        '775447002', -- dapagliflozin
        '775752008', -- empagliflozin
        '775025005', -- canagliflozin
        '775821002' -- ertugliflozin
    )
