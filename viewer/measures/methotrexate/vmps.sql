SELECT DISTINCT
    vmp.id as vmp_id,
    CASE
        WHEN '41791911000001107' IN (vtm.code, vtm.vtmidprev) THEN 'numerator' -- VMP code for methotrexate 10mg tablets
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
WHERE
    '41791911000001107' IN (vtm.code, vtm.vtmidprev) -- VMP code for methotrexate 10mg tablets
    OR
    '41792011000001100' IN (vtm.code, vtm.vtmidprev) -- VMP code for methotrexate 2.5mg tablets
