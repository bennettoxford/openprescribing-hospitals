SELECT DISTINCT
    vmp.id as vmp_id,
    CASE 
        WHEN
            vtm.vtm IN (
                '36408911000001109', -- diclofenac potassium
                '36409011000001100', -- diclofenac sodium
                '36409111000001104' -- diclofenac + misoprostol
            )
            AND
            ofr.name LIKE '%.oral'
        THEN 'numerator'
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
INNER JOIN viewer_vtm vtm ON vtm.id = vmp.vtm_id
INNER JOIN viewer_vmp_ont_form_routes vofr ON vofr.vmp_id = vmp.id
INNER JOIN viewer_ontformroute ofr ON ofr.id = vofr.ontformroute_id
WHERE 
    vtm.vtm IN (
        '36408911000001109', -- diclofenac potassium
        '36409011000001100', -- diclofenac sodium
        '36409111000001104', -- diclofenac + misoprostol
        '776287003', -- ibuprofen
        '776871008', -- naproxen
        '775123001', -- celecoxib
        '775901004', -- etoricoxib
        '776666003', -- meloxicam
        '776329000', -- indometacin
        '776450008', -- ketoprofen
        '775513003', -- dexketoprofen
        '776013000', -- flurbiprofen
        '777764004', -- tiaprofenic acid
        '774407001', -- aceclofenac
        '775895002', -- etodolac
        '776654009', -- mefenamic acid
        '776850003', -- nabumetone
        '777223005', -- piroxicam
        '777652003', -- sulindac
        '777707009', -- tenoxicam
        '777800001' -- tolfenamic acid
    )
    AND (
        ofr.name LIKE '%.oral'
    )