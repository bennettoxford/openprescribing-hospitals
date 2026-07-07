SELECT DISTINCT
    vmp.id as vmp_id,
    'numerator' as vmp_type -- this measure has no denominator
FROM viewer_vmp vmp
WHERE 
    LOWER(vmp.name) NOT LIKE '% shampoo%' -- exclude all shampoos
    AND
        (
            vmp.bnf_code LIKE '1302011%' AND -- include all Emollient Bath & Shower Preparation
            vmp.bnf_code NOT LIKE '130201100%AM' -- exclude Aqueous Cream (brand and generics)
        )
    OR
        (
            (
                vmp.bnf_code LIKE '130201000%' OR -- include Other emollient preparations (with forms listed below)
                vmp.bnf_code LIKE '1902055%' OR -- include Toiletries (with forms listed below)
                vmp.bnf_code LIKE '2122%' -- include Emollients (with forms listed below)
            )
            AND
            (
                LOWER(vmp.name) LIKE '%bath%' OR -- include formulations with bath in name
                LOWER(vmp.name) LIKE '%wash%' OR -- include formulations with wash in name
                LOWER(vmp.name) LIKE '%shower%' -- include formulations with shower in name
            )
            AND
            (
                LOWER(vmp.name) NOT LIKE '%wash cap%' AND -- exclude wash caps
                LOWER(vmp.name) NOT LIKE '%wash mitts%' AND -- exclude wash mitts
                LOWER(vmp.name) NOT LIKE '%antimicrobial%' AND  -- exclude formulations with antimicrobial in name
                LOWER(vmp.name) NOT LIKE '%feminine%' -- exclude formulations with feminine in name
            )
        )