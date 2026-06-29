SELECT DISTINCT
    vmp.id as vmp_id,
    CASE 
        WHEN  
            vmp.id NOT IN (
                26148711000001101, -- Beclometasone 100micrograms-dose Formoterol 6micrograms-dose dry powder inhaler
                12911011000001100, -- Beclometasone 100micrograms-dose  Formoterol 6micrograms-dose inhaler CFC free
                39133611000001108, -- Budesonide 100micrograms-dose Formoterol 3micrograms-dose inhaler CFC free
                35912011000001109, -- Budesonide 100micrograms-dose Formoterol 6micrograms-dose dry powder inhaler
                38896811000001103, -- Budesonide 200micrograms-dose Formoterol 6micrograms-dose dry powder inhaler
                32960711000001105 -- Budesonide 200micrograms-dose Formoterol 6micrograms-dose inhaler CFC free
            ) -- exclude VMPs which have products licensed for AIR or MART thearpy
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
        atc.code LIKE 'R03AK%' -- only "Adrenergics in combination with corticosteroids or other drugs, excl. anticholinergics" ATC codes - excludes triple inhalers as these are under ATC R03AL
        OR
        atc.code LIKE 'R03BA%' -- only "OTHER DRUGS FOR OBSTRUCTIVE AIRWAY DISEASES, INHALANTS - R03BA Glucocorticoids" ATC codes
    )