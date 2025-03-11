SELECT DISTINCT
    vmp.id as vmp_id,
    CASE
        WHEN vmp.code = '41791911000001107' THEN 'numerator' -- VMP code for methotrexate 10mg tablets
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
WHERE
    vmp.code = '41791911000001107' -- VMP code for methotrexate 10mg tablets
    OR
    vmp.code = '41792011000001100' -- VMP code for methotrexate 2.5mg tablets
