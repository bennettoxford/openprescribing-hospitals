SELECT DISTINCT
    vmp.id as vmp_id,
    'numerator' as vmp_type -- this measure has no denominator
FROM viewer_vmp vmp
WHERE 
    vmp.bnf_code IN ('1003020', '10030200') -- Rubefacients, topical NSAIDS, capsaicin and poultice
    AND 
    vmp.vtm NOT LIKE '9874511000001102' -- Arnica montana
    AND 
    vmp.vtm NOT LIKE '775036003' -- Capsaicin
    AND 
    vmp.vtm NOT LIKE '32889211000001103' -- Diclofenac diethyl
    AND 
    vmp.vtm NOT LIKE '36409011000001100' -- Diclofenac sodium
    AND 
    vmp.vtm NOT LIKE '775922005' -- Felbinac
    AND 
    vmp.vtm NOT LIKE '776180006' -- Heparinoid
    AND 
    vmp.vtm NOT LIKE '776287003' -- Ibuprofen
    AND 
    vmp.vtm NOT LIKE '776288008' -- Ibuprofen + Levomenthol
    AND 
    vmp.vtm NOT LIKE '776444003' -- Kaolin Heavy
    AND 
    vmp.vtm NOT LIKE '776450008' -- Ketoprofen
    AND 
    vmp.vtm NOT LIKE '777223005' -- Piroxicam
    AND
    vmp.vtm NOT LIKE '20448811000001100' -- Amitriptyline + Ketamine
