SELECT DISTINCT
    vmp.id as vmp_id,
    CASE
        WHEN vmp.code = '4977611000001104' THEN 'numerator' -- VMP code for Adrenaline (base) 100micrograms/1ml (1 in 10,000) dilute solution for injection ampoules
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
WHERE
    vmp.code = '4977611000001104' -- VMP code for Adrenaline (base) 100micrograms/1ml (1 in 10,000) dilute solution for injection ampoules
    OR
    vmp.code = '20122411000001103' -- VMP code for Adrenaline (base) 1mg/10ml (1 in 10,000) dilute solution for injection pre-filled syringes
