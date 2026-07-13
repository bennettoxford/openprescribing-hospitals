SELECT DISTINCT
    vmp.id as vmp_id,
    'numerator' as vmp_type -- this measure has no denominator
FROM viewer_vmp vmp
WHERE 
    vmp.bnf_code LIKE '10030200%' -- Rubefacients, topical NSAIDS, capsaicin and poultice
    AND 
    vmp.bnf_code NOT LIKE '10030200AD%' -- Arnica montana
    AND 
    vmp.bnf_code NOT LIKE '10030200AA%' -- Capsaicin
    AND 
    vmp.bnf_code NOT LIKE '10030200AF%' -- Diclofenac diethyl
    AND 
    vmp.bnf_code NOT LIKE '10030200U0%' -- Diclofenac sodium
    AND 
    vmp.bnf_code NOT LIKE '10030200M0%' -- Felbinac
    AND 
    vmp.bnf_code NOT LIKE '10030200W0%' -- Heparinoid
    AND 
    vmp.bnf_code NOT LIKE '10030200P0%' -- Ibuprofen
    AND 
    vmp.bnf_code NOT LIKE '10030200L0%' -- Kaolin Heavy
    AND 
    vmp.bnf_code NOT LIKE '1003020010%' -- Ketoprofen
    AND 
    vmp.bnf_code NOT LIKE '10030200R0%' -- Piroxicam
