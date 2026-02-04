CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_EXPRESSED_AS_TABLE_ID }}`
AS

WITH
-- ============================================================================
-- STEP 1: Get VMP ingredients data
-- ============================================================================
vmp_ingredients AS (
  SELECT
    dmd.vmp_code AS vmp_id,
    ingredient.ing_code AS ingredient_code,
    ingredient.ing_name AS ingredient_name,
    ingredient.strnt_nmrtr_val,
    ingredient.strnt_nmrtr_uom_name AS strnt_nmrtr_uom_name
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` dmd,
  UNNEST(dmd.ingredients) AS ingredient
),

-- ============================================================================
-- STEP 2: Identify VMPs with "expressed as" DDD comments
-- ============================================================================
expressed_as_mapped AS (
  SELECT
    dmd.vmp_code,
    dmd.vmp_name AS vmp_nm,
    ddd.comment AS ddd_comment,
    ddd.comment AS who_ddd_comment,
    dmd.vtm AS vtm_id,
    dmd.vtm_name AS vtm_nm,
    ingredient.ing_code AS ing_id,
    ingredient.ing_code AS matched_ingredient_code,
    ingredient.ing_name AS ing_nm,
    ingredient.ing_name AS matched_ingredient_name,
    ingredient.strnt_nmrtr_val,
    ingredient.strnt_nmrtr_uom_name AS strnt_nmrtr_uom_name,

    -- Calculate expressed_as_strnt_nmrtr based on specific rules
    CASE
      -- Folate derivatives (expressed as folinic/levofolinic acid) - use actual strength
      WHEN dmd.vtm = '776024003' AND ddd.comment = 'Expressed as folinic acid' THEN ingredient.strnt_nmrtr_val -- VTM for folinic acid
      WHEN dmd.vtm = '782480006' AND ddd.comment = 'Expressed as levofolinic acid' THEN ingredient.strnt_nmrtr_val -- VTM for levofolinic acid

      -- Lanthanum - use actual strength
      WHEN dmd.vtm = '785371003' AND ddd.comment = 'Expressed as lanthanum' THEN ingredient.strnt_nmrtr_val -- VTM for lanthanum carbonate

      -- Benzathine benzylpenicillin (expressed as benzylpenicillin) - specific conversion factors
      WHEN dmd.vmp_code IN ('10150911000001107') AND ddd.comment = 'Expressed as benzylpenicillin' THEN 360 -- Benzathine benzylpenicillin 600,000 units vial
      WHEN dmd.vmp_code IN ('10153111000001102', '42683111000001104') AND ddd.comment = 'Expressed as benzylpenicillin' THEN 720 -- Benzathine benzylpenicillin 1.2 million units vial
      WHEN dmd.vmp_code IN ('10153211000001108', '44168511000001109') AND ddd.comment = 'Expressed as benzylpenicillin' THEN 1440 -- Benzathine benzylpenicillin 2.4 million units vial

      -- Respiratory inhalers (delivered dose) - specific delivered dose values
      WHEN dmd.vmp_code = '38893611000001108' AND ddd.comment = 'Expressed as aclidinium, delivered dose' THEN 322 -- Aclidinium
      WHEN dmd.vmp_code = '21496211000001102' AND ddd.comment = 'Expressed as glycopyrronium, delivered dose' THEN 44 -- Glycopyrronium
      WHEN dmd.vmp_code IN ('33596311000001107', '9478911000001107', '9479011000001103') AND ddd.comment = 'Expressed as tiotropium, delivered dose' THEN 10 -- Tiotropium powder for inhalation capsules
      WHEN dmd.vmp_code = '27890611000001109' AND ddd.comment = 'Expressed as umeclidinium, delivered dose' THEN 55 -- Umeclidinium

      -- Iron supplements (expressed as Fe2+ elemental iron) - specific elemental iron values
      WHEN dmd.vtm = '776398000' AND ddd.comment = 'Fe' THEN ingredient.strnt_nmrtr_val -- VTM for iron sucrose
      WHEN dmd.vtm = '776395002' AND ddd.comment = 'Fe' THEN ingredient.strnt_nmrtr_val -- VTM for iron dextran
      WHEN dmd.vmp_code = '41984711000001104' AND ddd.comment = 'Fe2+' THEN 69 -- Ferrous fumarate 210mg tablets
      WHEN dmd.vmp_code = '41984811000001107' AND ddd.comment = 'Fe2+' THEN 100 -- Ferrous fumarate 305mg capsules
      WHEN dmd.vmp_code = '41985011000001102' AND ddd.comment = 'Fe2+' THEN 106 -- Ferrous fumarate 322mg tablets
      WHEN dmd.vmp_code = '38898111000001109' AND ddd.comment = 'Fe2+' THEN 9 -- Ferrous fumarate 140mg/5ml oral solution sugar free
      WHEN dmd.vmp_code = '39108711000001103' AND ddd.comment = 'Fe2+' THEN 9 -- Ferrous fumarate 140mg/5ml oral solution
      WHEN dmd.vmp_code = '7820811000001101' AND ddd.comment = 'Fe2+' THEN 5 -- Generic Spatone Original 20ml sachets
      WHEN dmd.vmp_code = '12313711000001105' AND ddd.comment = 'Fe2+' THEN 5.6 -- Ferrous sulfate 140mg/5ml oral solution
      WHEN dmd.vmp_code = '8513511000001107' AND ddd.comment = 'Fe2+' THEN 25 -- Ferrous sulfate 625mg/5ml oral solution
      WHEN dmd.vmp_code = '8513311000001101' AND ddd.comment = 'Fe2+' THEN 2.4 -- Ferrous sulfate 60mg/5ml oral solution
      WHEN dmd.vmp_code = '41985211000001107' AND ddd.comment = 'Fe2+' THEN 65 -- Ferrous sulfate 200mg tablets
      WHEN dmd.vmp_code = '41985111000001101' AND ddd.comment = 'Fe2+' THEN 35 -- Ferrous gluconate 300mg tablets
      WHEN dmd.vmp_code = '12661411000001106' AND ddd.comment = 'Fe2+' THEN 25 -- Ferrous sulfate 125mg/ml oral drops sugar free
      WHEN dmd.vmp_code = '39107411000001105' AND ddd.comment = 'Fe2+' THEN 105 -- Ferrous sulfate 325mg modified-release tablets

      -- Anticoagulants (anti-Xa) - conversion to units
      WHEN dmd.vtm = '775768002' AND ddd.comment = 'anti Xa' THEN ingredient.strnt_nmrtr_val * 100 -- Enoxaparin (mg to units)
      WHEN dmd.vtm = '775440000' AND ddd.comment = 'anti Xa' THEN ingredient.strnt_nmrtr_val -- Dalteparin (already in units)
      WHEN dmd.vtm = '775441001' AND ddd.comment = 'anti Xa' THEN ingredient.strnt_nmrtr_val -- Danaparoid (already in units)
      WHEN dmd.vtm = '777430002' AND ddd.comment = 'anti Xa' THEN ingredient.strnt_nmrtr_val -- Reviparin (already in units)
      WHEN dmd.vtm = '777776004' AND ddd.comment = 'anti Xa' THEN ingredient.strnt_nmrtr_val -- Tinzaparin (already in units)
    END AS expressed_as_strnt_nmrtr,

    -- Determine expressed_as_strnt_uom_name
    CASE
      -- Folate derivatives
      WHEN dmd.vtm = '776024003' AND ddd.comment = 'Expressed as folinic acid' THEN ingredient.strnt_nmrtr_uom_name -- VTM for folinic acid
      WHEN dmd.vtm = '782480006' AND ddd.comment = 'Expressed as levofolinic acid' THEN ingredient.strnt_nmrtr_uom_name -- VTM for levofolinic acid

      -- Minerals and metals
      WHEN dmd.vtm = '785371003' AND ddd.comment = 'Expressed as lanthanum' THEN ingredient.strnt_nmrtr_uom_name -- VTM for lanthanum carbonate

      -- Benzathine benzylpenicillin 600,000 units vial, 1.2 million units vial, 2.4 million units vial
      WHEN dmd.vmp_code IN ('10150911000001107', '10153111000001102', '42683111000001104', '10153211000001108', '44168511000001109') AND ddd.comment = 'Expressed as benzylpenicillin' THEN 'mg'

      -- Respiratory inhalers
      WHEN dmd.vmp_code = '38893611000001108' AND ddd.comment = 'Expressed as aclidinium, delivered dose' THEN 'microgram' -- Aclidinium
      WHEN dmd.vmp_code = '21496211000001102' AND ddd.comment = 'Expressed as glycopyrronium, delivered dose' THEN 'microgram' -- Glycopyrronium
      WHEN dmd.vmp_code IN ('33596311000001107', '9478911000001107', '9479011000001103') AND ddd.comment = 'Expressed as tiotropium, delivered dose' THEN 'microgram' -- Tiotropium powder for inhalation capsules
      WHEN dmd.vmp_code = '27890611000001109' AND ddd.comment = 'Expressed as umeclidinium, delivered dose' THEN 'microgram' -- Umeclidinium

      -- Iron supplements (Fe2+)
      WHEN dmd.vtm = '776398000' AND ddd.comment = 'Fe' THEN ingredient.strnt_nmrtr_uom_name -- Iron sucrose
      WHEN dmd.vtm = '776395002' AND ddd.comment = 'Fe' THEN ingredient.strnt_nmrtr_uom_name -- Iron dextran
      WHEN dmd.vmp_code IN ('41984711000001104', '41984811000001107', '41985011000001102', '38898111000001109', '39108711000001103', '7820811000001101', '12313711000001105', '8513511000001107', '8513311000001101', '41985211000001107', '41985111000001101', '12661411000001106', '39107411000001105') AND ddd.comment = 'Fe2+' THEN 'mg'

      -- Anticoagulants
      WHEN dmd.vtm = '775768002' AND ddd.comment = 'anti Xa' THEN 'unit' -- Enoxaparin (converted from mg to units)
      WHEN dmd.vtm = '775440000' AND ddd.comment = 'anti Xa' THEN ingredient.strnt_nmrtr_uom_name -- Dalteparin
      WHEN dmd.vtm = '775441001' AND ddd.comment = 'anti Xa' THEN ingredient.strnt_nmrtr_uom_name -- Danaparoid
      WHEN dmd.vtm = '777430002' AND ddd.comment = 'anti Xa' THEN ingredient.strnt_nmrtr_uom_name -- Reviparin
      WHEN dmd.vtm = '777776004' AND ddd.comment = 'anti Xa' THEN ingredient.strnt_nmrtr_uom_name -- Tinzaparin
    END AS expressed_as_strnt_uom_name

  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}` dmd
  INNER JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_TABLE_ID }}` vmp_table
    ON dmd.vmp_code = vmp_table.vmp_code
  INNER JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_SUPP_TABLE_ID }}` dmd_supp
    ON dmd.vmp_code = dmd_supp.vmp_code
  LEFT JOIN UNNEST(dmd.ontformroutes) AS route
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ ADM_ROUTE_MAPPING_TABLE_ID }}` routelookup
    ON route.ontformroute_descr = routelookup.dmd_ontformroute
  LEFT JOIN UNNEST(dmd.ingredients) AS ingredient
  INNER JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ WHO_DDD_TABLE_ID }}` ddd
    ON dmd_supp.atc_code = ddd.atc_code
    AND routelookup.who_route = ddd.adm_code
  WHERE dmd_supp.atc_code IS NOT NULL
    AND ddd.comment IS NOT NULL
    AND TRIM(ddd.comment) != ''
    AND ddd.comment NOT LIKE 'Refers to%'
    AND LOWER(TRIM(ddd.comment)) IN (
      'expressed as folinic acid', 'expressed as lanthanum',
      'expressed as levofolinic acid', 'expressed as benzylpenicillin',
      'expressed as aclidinium, delivered dose', 'expressed as glycopyrronium, delivered dose',
      'expressed as tiotropium, delivered dose', 'expressed as umeclidinium, delivered dose',
      'anti xa', 'fe', 'fe2+'
    )
),

-- ============================================================================
-- STEP 3: Get denominator information from DMD ingredients
-- ============================================================================
expressed_as_with_denominator AS (
  SELECT
    eam.*,
    vmp_ing.strnt_dnmtr_val AS expressed_as_strnt_dnmtr,
    vmp_ing.strnt_dnmtr_uom_name AS expressed_as_strnt_dnmtr_uom_name,
    vmp_ing.strnt_dnmtr_basis_val AS expressed_as_strnt_dnmtr_basis_val,
    vmp_ing.strnt_dnmtr_basis_uom AS expressed_as_strnt_dnmtr_basis_uom
  FROM expressed_as_mapped eam
  LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ VMP_TABLE_ID }}` vmp
    ON eam.vmp_code = vmp.vmp_code
  LEFT JOIN UNNEST(vmp.ingredients) AS vmp_ing
    ON eam.matched_ingredient_code = vmp_ing.ingredient_code
  WHERE eam.expressed_as_strnt_nmrtr IS NOT NULL
)

-- ============================================================================
-- STEP 4: Format final output with UOM codes
-- ============================================================================
SELECT DISTINCT
  vmp_code AS vmp_id,
  vmp_nm AS vmp_name,
  ddd_comment,
  expressed_as_strnt_nmrtr,
  nmrtr_uom_lookup.uom_code AS expressed_as_strnt_nmrtr_uom,
  expressed_as_strnt_uom_name AS expressed_as_strnt_nmrtr_uom_name,
  expressed_as_strnt_dnmtr,
  dnmtr_uom_lookup.uom_code AS expressed_as_strnt_dnmtr_uom,
  expressed_as_strnt_dnmtr_uom_name,
  expressed_as_strnt_dnmtr_basis_val,
  expressed_as_strnt_dnmtr_basis_uom,
  matched_ingredient_code AS ingredient_code,
  matched_ingredient_name AS ingredient_name
FROM expressed_as_with_denominator
LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_UOM_TABLE_ID }}` nmrtr_uom_lookup
  ON expressed_as_strnt_uom_name = nmrtr_uom_lookup.description
LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_UOM_TABLE_ID }}` dnmtr_uom_lookup
  ON expressed_as_strnt_dnmtr_uom_name = dnmtr_uom_lookup.description
ORDER BY who_ddd_comment, vmp_nm