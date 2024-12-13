WITH measure_vmps AS (
    SELECT DISTINCT
        vmp.id,
        CASE 
            WHEN vmp.code IN ('39332111000001100', '39332211000001106') THEN 'numerator'
            ELSE 'denominator'
        END as vmp_type
    FROM viewer_vmp vmp
)
SELECT 
    vmp.id as vmp_id,
    mv.vmp_type
FROM viewer_vmp vmp
JOIN measure_vmps mv ON mv.id = vmp.id
WHERE vmp.code IN ('39332111000001100', '39332211000001106', '41772111000001108', '35903811000001108', '23204911000001105', '41329811000001106', '23985411000001104', '23985511000001100')