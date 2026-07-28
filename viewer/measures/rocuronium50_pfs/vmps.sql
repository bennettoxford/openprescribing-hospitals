SELECT DISTINCT
    vmp.id as vmp_id,
    CASE
        WHEN vmp.code IN (
            '35935111000001105', -- VMP codes for Rocuronium bromide 50mg/5ml solution for injection vials
            '33570811000001108' -- VMP codes for Rocuronium bromide 50mg/5ml solution for injection ampoules
        )
        THEN 'numerator' 
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
WHERE vmp.code IN (
    '35935111000001105', -- VMP code for Rocuronium bromide 50mg/5ml solution for injection vials
    '33570811000001108', -- VMP code for Rocuronium bromide 50mg/5ml solution for injection ampoules
    '44947811000001102' -- VMP code for Rocuronium bromide 50mg/5ml solution for injection pre-filled syringes
)