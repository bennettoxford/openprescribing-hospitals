SELECT DISTINCT
    vmp.id as vmp_id,
    'numerator' as vmp_type -- this measure has no denominator
FROM viewer_vmp vmp
WHERE 
    vmp.bnf_code IN ('1003020', '10030200') -- Rubefacients, topical NSAIDS, capsaicin and poultice
    AND 
    vmp.vtm_id NOT IN 
        (
            '9874511000001102', -- Arnica montana
            '775036003', -- Capsaicin
            '32889211000001103', -- Diclofenac diethyl
            '36409011000001100', -- Diclofenac sodium
            '775922005', -- Felbinac
            '776180006', -- Heparinoid
            '776287003', -- Ibuprofen
            '776288008', -- Ibuprofen + Levomenthol
            '776444003', -- Kaolin Heavy
            '776450008', -- Ketoprofen
            '777223005', -- Piroxicam
            '20448811000001100' -- Amitriptyline + Ketamine
        )
