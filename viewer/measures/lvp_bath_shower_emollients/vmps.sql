SELECT DISTINCT
    vmp.id as vmp_id,
    'numerator' as vmp_type -- this measure has no denominator
FROM viewer_vmp vmp
WHERE
    (
        LOWER(vmp.name) LIKE '%bath%' OR -- include formulations with bath in name
        LOWER(vmp.name) LIKE '%wash%' OR -- include formulations with wash in name
        LOWER(vmp.name) LIKE '%shower%' -- include formulations with shower in name
    )
    AND LOWER(vmp.name) NOT LIKE '%shampoo%'            -- exclude shampoos
    AND LOWER(vmp.name) NOT LIKE '%antimicrobial%'      -- exclude antimicrobial cleansing preparations
    AND LOWER(vmp.name) NOT LIKE '%facewash%'           -- exclude facial/medicated cleansing preparations
    AND LOWER(vmp.name) NOT LIKE '%hand wash%'          -- exclude hand washing products
    AND LOWER(vmp.name) NOT LIKE '%mouthwash%'          -- exclude oral preparations
    AND LOWER(vmp.name) NOT LIKE '%eye %'               -- exclude ophthalmic preparations
    AND LOWER(vmp.name) NOT LIKE '%feminine%'           -- exclude feminine hygiene preparations

    AND LOWER(vmp.name) NOT LIKE '%methoxsalen%'        -- exclude medicated phototherapy preparations
    AND LOWER(vmp.name) NOT LIKE '%benzoyl peroxide%'   -- exclude medicated acne preparations
    AND LOWER(vmp.name) NOT LIKE '%coal tar%'           -- exclude medicated coal tar preparations
    AND LOWER(vmp.name) NOT LIKE 'tar%'                 -- exclude medicated tar preparations

    AND LOWER(vmp.name) NOT LIKE '%wash cap%'           -- exclude wash caps
    AND LOWER(vmp.name) NOT LIKE '%wash mitts%'         -- exclude wash mitts
    AND LOWER(vmp.name) NOT LIKE '%washcloth%'          -- exclude patient cleansing wipes