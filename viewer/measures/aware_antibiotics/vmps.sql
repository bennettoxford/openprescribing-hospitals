SELECT DISTINCT
    vmp.id,
    CASE 
        WHEN antibiotic.aware_2024 IN ('Watch', 'Reserve') THEN 'numerator'
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
INNER JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
INNER JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
INNER JOIN viewer_awarevmpmapping mapping ON vmp.id = mapping.vmp_id
INNER JOIN viewer_awareantibiotic antibiotic ON mapping.aware_antibiotic_id = antibiotic.id
WHERE 
    antibiotic.aware_2024 IN ('Access', 'Watch', 'Reserve')
    -- remove product which are ONLY for the below routes as the DDDs are not applicable to these routes
    AND ofr.name NOT LIKE "%.intraarticular"
    AND ofr.name NOT LIKE "%.intracameral"
    AND ofr.name NOT LIKE "%.intracerebroventricular"
    AND ofr.name NOT LIKE "%.intraperitoneal"
    AND ofr.name NOT LIKE "%.intrapleural"
    AND ofr.name NOT LIKE "%.intrathecal"
    AND ofr.name NOT LIKE "%.intravitreal"
