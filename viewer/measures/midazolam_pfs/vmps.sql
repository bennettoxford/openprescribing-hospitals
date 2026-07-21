SELECT DISTINCT
    vmp.id as vmp_id,
    CASE
        WHEN vmp.code = '36072211000001102' THEN 'numerator' -- VMP code for Ephedrine 30mg/1ml solution for injection ampoules
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
WHERE
    vmp.code = '7977911000001102' -- VMP code for Midazolam 100mg/50ml solution for infusion vials
    OR
    vmp.code = '38082511000001102' -- VMP code for Midazolam 100mg/50ml solution for infusion pre-filled syringes
