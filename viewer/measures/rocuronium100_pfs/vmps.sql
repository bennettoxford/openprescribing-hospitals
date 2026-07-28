SELECT DISTINCT
    vmp.id as vmp_id,
    CASE
        WHEN vmp.code IN (
            '35935011000001109', -- VMP codes for Rocuronium bromide 100mg/10ml solution for injection vials
            '37455011000001105' -- VMP codes for Rocuronium bromide 100mg/10ml solution for injection ampoules
        )
        THEN 'numerator' 
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
WHERE vmp.code IN (
    '35935011000001109', -- VMP code for Rocuronium bromide 100mg/10ml solution for injection vials
    '37455011000001105', -- VMP code for Rocuronium bromide 100mg/10ml solution for injection ampoules
    '44099611000001109' -- VMP code for Rocuronium bromide 100mg/10ml solution for injection pre-filled syringes
)