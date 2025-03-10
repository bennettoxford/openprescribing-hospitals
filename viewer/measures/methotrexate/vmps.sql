SELECT DISTINCT
    vmp.code
    CASE
        WHEN vmp.code = '41791911000001107' THEN 'numerator'
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
WHERE vmp.code IN ('41791911000001107', '41792011000001100')