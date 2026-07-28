SELECT DISTINCT
    vmp.id as vmp_id,
    CASE
        WHEN vmp.code = '36125511000001104' THEN 'numerator' -- VMP code for Midazolam 5mg/5ml solution for injection ampoules
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
WHERE
    vmp.code = '36125511000001104' -- VMP code for Midazolam 5mg/5ml solution for injection ampoules
    OR
    vmp.code = '22710211000001106' -- VMP code for Midazolam 5mg/5ml solution for injection pre-filled syringes
