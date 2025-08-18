SELECT DISTINCT
    vmp.id as vmp_id,
    'numerator' as vmp_type -- this measure has no denominator
FROM viewer_vmp vmp
WHERE vmp.code = '116281000001109' -- VMP for Lidocaine 700mg medicated plasters
