SELECT DISTINCT
    vmp.id,
    CASE 
        WHEN antibiotic.aware_2024 IN ('Watch', 'Reserve') THEN 'numerator'
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
INNER JOIN viewer_awarevmpmapping mapping ON vmp.id = mapping.vmp_id
INNER JOIN viewer_awareantibiotic antibiotic ON mapping.aware_antibiotic_id = antibiotic.id
WHERE 
    antibiotic.aware_2024 IN ('Access', 'Watch', 'Reserve')
