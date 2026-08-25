SELECT DISTINCT
    vmp.id as vmp_id,
    'numerator' as vmp_type -- this measure has no denominator
FROM viewer_vmp vmp
INNER JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
WHERE 
    vmp.bnf_code = '1003020' -- BNF Section for Rubefacients, topical NSAIDs, capsaicin and poultice
    AND 
    vtm.vtm NOT IN 
        (
            '775036003',         -- Capsaicin - excluded from guidance

            '32889211000001103', -- Diclofenac diethyl - topical NSAID - excluded from guidance
            '36409011000001100', -- Diclofenac sodium - topical NSAID - excluded from guidance
            '775922005',         -- Felbinac - topical NSAID - excluded from guidance
            '776287003',         -- Ibuprofen - topical NSAID - excluded from guidance
            '776288008',         -- Ibuprofen + Levomenthol - topical NSAID - excluded from guidance
            '776450008',         -- Ketoprofen - topical NSAID - excluded from guidance
            '777223005',         -- Piroxicam - topical NSAID - excluded from guidance
            
            '776180006',         -- Heparinoid - outside scope
            '9874511000001102',  -- Arnica montana - outside scope
            '776444003',         -- Kaolin Heavy - outside scope
            '20448811000001100', -- Amitriptyline + Ketamine - outside scope
            
            '775611008'          -- Dimethyl sulfoxide - specialist/unlicensed use, including chemotherapy extravasation
        )
