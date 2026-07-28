SELECT DISTINCT
    vmp.id as vmp_id,
    CASE
        WHEN vmp.code = '3776011000001101' THEN 'numerator' -- VMP code for VMP code for Lidocaine 100mg/10ml (1%) solution for injection ampoules
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
WHERE
    vmp.code = '3776011000001101' -- VMP code for Lidocaine 100mg/10ml (1%) solution for injection ampoules
    OR
    vmp.code = '36039011000001102' -- VMP code for Lidocaine 100mg/10ml (1%) solution for injection pre-filled syringes
