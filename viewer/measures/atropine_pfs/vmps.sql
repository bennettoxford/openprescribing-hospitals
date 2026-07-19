SELECT DISTINCT
    vmp.id as vmp_id,
    CASE
        WHEN vmp.code = '35905711000001104' THEN 'numerator' -- VMP code for Atropine 600micrograms/1ml solution for injection ampoules
        ELSE 'denominator'
    END as vmp_type
FROM viewer_vmp vmp
WHERE
    vmp.code = '35905711000001104' -- VMP code for Atropine 600micrograms/1ml solution for injection ampoules
    OR
    vmp.code = '35905311000001103' -- VMP code for Atropine 3mg/10ml solution for injection pre-filled syringes
