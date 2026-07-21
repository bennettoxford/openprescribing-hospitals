SELECT DISTINCT
    vmp.id as vmp_id,
    CASE
        WHEN vmp.code = '4508111000001108' THEN 'numerator' -- VMP code for Midazolam 50mg/50ml solution for infusion vials
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
WHERE
    vmp.code = '4508111000001108' -- VMP code for Midazolam 50mg/50ml solution for infusion vials
    OR
    vmp.code = '22710611000001108' -- VMP code for Midazolam 50mg/50ml solution for infusion pre-filled syringes
