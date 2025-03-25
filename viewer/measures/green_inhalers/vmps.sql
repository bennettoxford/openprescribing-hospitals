SELECT DISTINCT
    vmp.id as vmp_id,
    CASE 
        WHEN  ofr.name = 'pressurizedinhalation.inhalation' -- only pMDI formulations in numerator
        THEN 'numerator'
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
INNER JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
INNER JOIN viewer_vmp_atcs vmp_atcs ON vmp_atcs.vmp_id = vmp.id
INNER JOIN viewer_atc atc ON atc.id = vmp_atcs.atc_id
INNER JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
INNER JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
WHERE 
    ofr.name IN (
        'pressurizedinhalation.inhalation',
        'powderinhalation.inhalation',
        'inhalationsolution.inhalation'
    ) -- all standard inhaler formulations in denominator
    AND (
        atc.code LIKE 'R03%' -- only "DRUGS FOR OBSTRUCTIVE AIRWAY DISEASES" ATC codes
        OR -- below is a temporary solution where these 2 VMPs are missing ATC codes in dm+d lookup
        vmp.code = '13801911000001104' -- include Sodium cromoglicate 5mg/dose inhaler CFC free VMP
        OR 
        vmp.code = '39134511000001107' -- include Generic Enerzair Breezhaler 114micrograms/dose / 46micrograms/dose / 136micrograms/dose inhalation powder capsules with device VMP
    )
    AND
    vtm.vtm != '777483005' -- exclude salbutamol VTM