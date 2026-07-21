SELECT DISTINCT
    vmp.id as vmp_id,
    CASE
        WHEN vmp.code = '7977911000001102' THEN 'numerator' -- VMP code for Midazolam 100mg/50ml solution for infusion vials
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
WHERE
    vmp.code = '7977911000001102' -- VMP code for Midazolam 100mg/50ml solution for infusion vials
    OR
    vmp.code = '38082511000001102' -- VMP code for Midazolam 100mg/50ml solution for infusion pre-filled syringes
