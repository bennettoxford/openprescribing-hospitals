-- The sample keeps a wider product list, and it also keeps every oral and
-- intravenous paracetamol pack with quantity in the sample months.
-- That paracetamol set is the population of viewer/measures/paracetamol_iv/vmps.sql.
-- The measure file assigns numerator and denominator when load_sample_data runs.
-- The sample has:
--   one common pack for each of these 10 medicines:
--     Paracetamol 500mg tablets
--     Paracetamol 1g/100ml solution for infusion
--     Omeprazole 20mg gastro-resistant capsules
--     Amoxicillin 500mg capsules
--     Atorvastatin 20mg tablets
--     Amlodipine 5mg tablets
--     Metformin 500mg tablets
--     Ramipril 5mg capsules
--     Salbutamol 100micrograms/dose inhaler
--     Co-amoxiclav 500mg/125mg tablets
--   extra packs for more measures:
--     one intravenous amoxicillin or co-amoxiclav pack
--     one dosulepin pack
--     one emollient bath, wash, or shower pack
--     doxazosin plain tablets and modified-release tablets
--     adrenaline ampoule 35894911000001108
--     adrenaline pre-filled syringe 20122411000001103
--     one avacopan pack
-- window_dose is every pack with quantity in the sample months.
-- Each rule below keeps the highest quantity pack in its group.
window_dose AS (
  SELECT
    d.vmp_code,
    ANY_VALUE(v.vmp_name) AS vmp_name,
    ANY_VALUE(CAST(v.vtm_code AS STRING)) AS vtm_code,
    SUM(d.scmd_quantity) AS total_qty
  FROM `ebmdatalab.scmd_pipeline.dose` AS d
  INNER JOIN `ebmdatalab.scmd_pipeline.vmp_data` AS v
    ON v.vmp_code = d.vmp_code
  CROSS JOIN bounds AS b
  WHERE d.year_month BETWEEN b.start_month AND b.end_month
    AND d.scmd_quantity > 0
    AND d.dose_quantity IS NOT NULL
  GROUP BY d.vmp_code
),
core_products AS (
  SELECT vmp_code
  FROM window_dose AS w
  INNER JOIN UNNEST([
    'Paracetamol 500mg tablets',
    'Paracetamol 1g/100ml solution for infusion',
    'Omeprazole 20mg gastro-resistant capsules',
    'Amoxicillin 500mg capsules',
    'Atorvastatin 20mg tablets',
    'Amlodipine 5mg tablets',
    'Metformin 500mg tablets',
    'Ramipril 5mg capsules',
    'Salbutamol 100microgram',
    'Co-amoxiclav 500mg/125mg tablets'
  ]) AS name_prefix
    ON STARTS_WITH(w.vmp_name, name_prefix)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY name_prefix
    ORDER BY w.total_qty DESC
  ) = 1
),
extra_products AS (
  -- One intravenous amoxicillin or co-amoxiclav pack.
  SELECT vmp_code
  FROM window_dose AS w
  INNER JOIN (
    SELECT DISTINCT intravenous.vmp_code
    FROM `ebmdatalab.scmd_pipeline.vmp_data` AS intravenous
    CROSS JOIN UNNEST(intravenous.ont_form_routes) AS route
    WHERE route.route_name LIKE '%.intravenous'
  ) AS intravenous_products
    ON intravenous_products.vmp_code = w.vmp_code
  LEFT JOIN core_products AS already_chosen
    ON already_chosen.vmp_code = w.vmp_code
  WHERE (
      STARTS_WITH(w.vmp_name, 'Amoxicillin')
      OR STARTS_WITH(w.vmp_name, 'Co-amoxiclav')
    )
    AND already_chosen.vmp_code IS NULL
  QUALIFY ROW_NUMBER() OVER (ORDER BY w.total_qty DESC) = 1

  UNION DISTINCT

  -- One dosulepin pack (VTM 775693007) and one avacopan pack (VTM 1264466009).
  SELECT vmp_code
  FROM window_dose AS w
  LEFT JOIN core_products AS already_chosen
    ON already_chosen.vmp_code = w.vmp_code
  WHERE w.vtm_code IN ('775693007', '1264466009')
    AND already_chosen.vmp_code IS NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY w.vtm_code
    ORDER BY w.total_qty DESC
  ) = 1

  UNION DISTINCT

  -- One emollient bath, wash, or shower pack.
  -- The name rules are the same as viewer/measures/lvp_bath_shower_emollients/vmps.sql.
  SELECT vmp_code
  FROM window_dose AS w
  LEFT JOIN core_products AS already_chosen
    ON already_chosen.vmp_code = w.vmp_code
  WHERE (
      LOWER(w.vmp_name) LIKE '%bath%'
      OR LOWER(w.vmp_name) LIKE '%wash%'
      OR LOWER(w.vmp_name) LIKE '%shower%'
    )
    AND LOWER(w.vmp_name) NOT LIKE '%shampoo%'
    AND LOWER(w.vmp_name) NOT LIKE '%antimicrobial%'
    AND LOWER(w.vmp_name) NOT LIKE '%facewash%'
    AND LOWER(w.vmp_name) NOT LIKE '%hand wash%'
    AND LOWER(w.vmp_name) NOT LIKE '%mouthwash%'
    AND LOWER(w.vmp_name) NOT LIKE '%eye %'
    AND LOWER(w.vmp_name) NOT LIKE '%feminine%'
    AND LOWER(w.vmp_name) NOT LIKE '%methoxsalen%'
    AND LOWER(w.vmp_name) NOT LIKE '%benzoyl peroxide%'
    AND LOWER(w.vmp_name) NOT LIKE '%coal tar%'
    AND LOWER(w.vmp_name) NOT LIKE 'tar%'
    AND LOWER(w.vmp_name) NOT LIKE '%wash cap%'
    AND LOWER(w.vmp_name) NOT LIKE '%wash mitts%'
    AND LOWER(w.vmp_name) NOT LIKE '%washcloth%'
    AND already_chosen.vmp_code IS NULL
  QUALIFY ROW_NUMBER() OVER (ORDER BY w.total_qty DESC) = 1

  UNION DISTINCT

  -- Doxazosin plain tablets and modified-release tablets. VTM 773245005.
  SELECT vmp_code
  FROM window_dose AS w
  INNER JOIN `ebmdatalab.scmd_pipeline.vmp_data` AS v
    ON v.vmp_code = w.vmp_code
  CROSS JOIN UNNEST(v.ont_form_routes) AS route
  LEFT JOIN core_products AS already_chosen
    ON already_chosen.vmp_code = w.vmp_code
  WHERE w.vtm_code = '773245005'
    AND route.route_name IN ('tablet.oral', 'tabletmodified-release.oral')
    AND already_chosen.vmp_code IS NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY route.route_name
    ORDER BY w.total_qty DESC
  ) = 1

  UNION DISTINCT

  -- Adrenaline ampoule and pre-filled syringe from the preview measure.
  SELECT v.vmp_code
  FROM `ebmdatalab.scmd_pipeline.vmp_data` AS v
  WHERE CAST(v.vmp_code AS STRING) IN (
    '35894911000001108',
    '20122411000001103'
  )
),
paracetamol_measure_products AS (
  -- Same population as viewer/measures/paracetamol_iv/vmps.sql:
  -- VTM 777067000 and an oral or intravenous route.
  SELECT DISTINCT w.vmp_code
  FROM window_dose AS w
  INNER JOIN `ebmdatalab.scmd_pipeline.vmp_data` AS v
    ON v.vmp_code = w.vmp_code
  CROSS JOIN UNNEST(v.ont_form_routes) AS route
  WHERE w.vtm_code = '777067000'
    AND (
      route.route_name LIKE '%.intravenous'
      OR route.route_name LIKE '%.oral'
    )
),
products AS (
  SELECT vmp_code FROM core_products
  UNION DISTINCT
  SELECT vmp_code FROM extra_products
  UNION DISTINCT
  SELECT vmp_code FROM paracetamol_measure_products
)
