SELECT DISTINCT
    vmp.id as vmp_id,
    CASE
        WHEN vmp.code = '36030311000001108' THEN 'numerator' -- VMP code for Natalizumab 300mg/15ml solution for infusion vials
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
WHERE
    vmp.code = '39819411000001106' -- VMP code for Natalizumab 150mg/1ml solution for injection pre-filled syringes
    OR
    vmp.code = '36030311000001108' -- VMP code for Natalizumab 300mg/15ml solution for infusion vials
