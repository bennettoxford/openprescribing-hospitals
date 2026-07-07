SELECT DISTINCT
    vmp.id as vmp_id,
    vmp.name,
    vmp.bnf_code,
    'numerator' as vmp_type -- this measure has no denominator
FROM viewer_vmp vmp
WHERE 
    LOWER(vmp.name) NOT LIKE '% shampoo%'AND -- exclude all shampoos
    LOWER(vmp.name) NOT LIKE '%wash cap%' AND -- exclude wash caps
    LOWER(vmp.name) NOT LIKE '%wash mitts%' AND -- exclude wash mitts
    LOWER(vmp.name) NOT LIKE '%antimicrobial%' AND  -- exclude formulations with antimicrobial in name
    LOWER(vmp.name) NOT LIKE '%feminine%' AND -- exclude formulations with feminine in name
    LOWER(vmp.name) NOT LIKE '%mouthwash%' AND -- exclude formulations with mouthwash in name
    LOWER(vmp.name) NOT LIKE '%facewash%' AND -- exclude formulations with facewash in name
    LOWER(vmp.name) NOT LIKE '%eye %' AND -- exclude formulations with eye in name
    LOWER(vmp.name) NOT LIKE '%coal tar%' AND -- exclude formulations with coal in name
    LOWER(vmp.name) NOT LIKE 'tar%' AND -- exclude formulations with tar in name
    LOWER(vmp.name) NOT LIKE '%benzoyl peroxide%' AND -- exclude formulations with benzoyl peroxide in name
    LOWER(vmp.name) NOT LIKE '%methoxsalen%' -- exclude formulations with methoxsalen in name
    AND (
        LOWER(vmp.name) LIKE '%bath%' OR -- include formulations with bath in name
        LOWER(vmp.name) LIKE '%wash%' OR -- include formulations with wash in name
        LOWER(vmp.name) LIKE '%shower%' -- include formulations with shower in name
    )