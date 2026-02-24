SELECT DISTINCT
    vmp.id as vmp_id,
    CASE
        WHEN vmp.code = '3601911000001107' THEN 'numerator' -- VMP code for Lidocaine 100mg/5ml (2%) solution for injection ampoules
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
WHERE
    vmp.code = '36039111000001101' -- VMP code for Lidocaine 100mg/5ml (2%) solution for injection pre-filled syringes
    OR
    vmp.code = '3601911000001107' -- VMP code for Lidocaine 100mg/5ml (2%) solution for injection ampoules
