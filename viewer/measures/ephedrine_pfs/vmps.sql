SELECT DISTINCT
    vmp.id as vmp_id,
    CASE
        WHEN vmp.code = '36072211000001102' THEN 'numerator' -- VMP code for Ephedrine 30mg/1ml solution for injection ampoules
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
WHERE
    vmp.code = '36072211000001102' -- VMP code for Ephedrine 30mg/1ml solution for injection ampoules
    OR
    vmp.code = '14606011000001103' -- VMP code for Ephedrine 30mg/10ml solution for injection pre-filled syringes
