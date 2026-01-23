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
      WHEN ingredient.ing_code = '126223008' AND ddd.comment = 'Expressed as folinic acid' THEN ingredient.strnt_nmrtr_val
      WHEN ingredient.ing_code = '4812211000001103' AND ddd.comment = 'Expressed as folinic acid' THEN ingredient.strnt_nmrtr_val
      WHEN ingredient.ing_code = '4640211000001100' AND ddd.comment = 'Expressed as levofolinic acid' THEN ingredient.strnt_nmrtr_val
      WHEN ingredient.ing_code = '36002211000001107' AND ddd.comment = 'Expressed as levofolinic acid' THEN ingredient.strnt_nmrtr_val

      -- Minerals and metals - use actual strength
      WHEN ingredient.ing_code = '414571007' AND ddd.comment = 'Expressed as lanthanum' THEN ingredient.strnt_nmrtr_val
      WHEN ingredient.ing_code = '129497004' AND ddd.comment = 'Fe' THEN ingredient.strnt_nmrtr_val -- Iron sucrose
      WHEN ingredient.ing_code = '387118003' AND ddd.comment = 'Fe' THEN ingredient.strnt_nmrtr_val -- Iron dextran

      -- Benzathine benzylpenicillin (expressed as benzylpenicillin) - specific conversion factors
      WHEN dmd.vmp_code IN ('10150911000001107') AND ddd.comment = 'Expressed as benzylpenicillin' THEN 360 -- Benzathine benzylpenicillin
      WHEN dmd.vmp_code IN ('10153111000001102', '42683111000001104') AND ddd.comment = 'Expressed as benzylpenicillin' THEN 720 -- Benzathine benzylpenicillin
      WHEN dmd.vmp_code IN ('10153211000001108', '44168511000001109') AND ddd.comment = 'Expressed as benzylpenicillin' THEN 1440 -- Benzathine benzylpenicillin

      -- Respiratory inhalers (delivered dose) - specific delivered dose values
      WHEN dmd.vmp_code = '38893611000001108' AND ddd.comment = 'Expressed as aclidinium, delivered dose' THEN 322 -- Aclidinium
      WHEN dmd.vmp_code = '21496211000001102' AND ddd.comment = 'Expressed as glycopyrronium, delivered dose' THEN 44 -- Glycopyrronium
      WHEN dmd.vmp_code IN ('33596311000001107', '9478911000001107', '9479011000001103') AND ddd.comment = 'Expressed as tiotropium, delivered dose' THEN 10 -- Tiotropium
      WHEN dmd.vmp_code = '27890611000001109' AND ddd.comment = 'Expressed as umeclidinium, delivered dose' THEN 55 -- Umeclidinium

      -- Iron supplements (expressed as Fe2+ elemental iron) - specific elemental iron values
      WHEN dmd.vmp_code = '41984711000001104' AND ddd.comment = 'Fe2+' THEN 69 -- Ferrous fumarate 210mg tablets
      WHEN dmd.vmp_code = '41984811000001107' AND ddd.comment = 'Fe2+' THEN 100 -- Ferrous fumarate 305mg capsules
      WHEN dmd.vmp_code = '41985011000001102' AND ddd.comment = 'Fe2+' THEN 106 -- Ferrous fumarate 322mg tablets
      WHEN dmd.vmp_code = '38898111000001109' AND ddd.comment = 'Fe2+' THEN 9 -- Ferrous fumarate 140mg/5ml oral solution sugar free
      WHEN dmd.vmp_code = '39108711000001103' AND ddd.comment = 'Fe2+' THEN 9 -- Ferrous fumarate 140mg/5ml oral solution

      -- Anticoagulants (anti-Xa) - conversion to units
      WHEN ingredient.ing_code = '108983001' AND ddd.comment = 'anti Xa' THEN ingredient.strnt_nmrtr_val * 100 -- Enoxaparin (mg to units)
      WHEN ingredient.ing_code = '108987000' AND ddd.comment = 'anti Xa' THEN ingredient.strnt_nmrtr_val -- Dalteparin (already in units)
      WHEN ingredient.ing_code = '386954009' AND ddd.comment = 'anti Xa' THEN ingredient.strnt_nmrtr_val -- Danaparoid (already in units)
      WHEN ingredient.ing_code = '395864005' AND ddd.comment = 'anti Xa' THEN ingredient.strnt_nmrtr_val -- Reviparin (already in units)
      WHEN ingredient.ing_code = '395908006' AND ddd.comment = 'anti Xa' THEN ingredient.strnt_nmrtr_val -- Tinzaparin (already in units)
    END AS expressed_as_strnt_nmrtr,

    -- Determine expressed_as_strnt_uom_name
    CASE
      -- Folate derivatives
      WHEN ingredient.ing_code = '126223008' AND ddd.comment = 'Expressed as folinic acid' THEN ingredient.strnt_nmrtr_uom_name
      WHEN ingredient.ing_code = '4812211000001103' AND ddd.comment = 'Expressed as folinic acid' THEN ingredient.strnt_nmrtr_uom_name
      WHEN ingredient.ing_code = '4640211000001100' AND ddd.comment = 'Expressed as levofolinic acid' THEN ingredient.strnt_nmrtr_uom_name
      WHEN ingredient.ing_code = '36002211000001107' AND ddd.comment = 'Expressed as levofolinic acid' THEN ingredient.strnt_nmrtr_uom_name

      -- Minerals and metals
      WHEN ingredient.ing_code = '414571007' AND ddd.comment = 'Expressed as lanthanum' THEN ingredient.strnt_nmrtr_uom_name
      WHEN ingredient.ing_code = '129497004' AND ddd.comment = 'Fe' THEN ingredient.strnt_nmrtr_uom_name -- Iron sucrose
      WHEN ingredient.ing_code = '387118003' AND ddd.comment = 'Fe' THEN ingredient.strnt_nmrtr_uom_name -- Iron dextran

      -- Benzathine benzylpenicillin
      WHEN dmd.vmp_code IN ('10150911000001107', '10153111000001102', '42683111000001104', '10153211000001108', '44168511000001109') AND ddd.comment = 'Expressed as benzylpenicillin' THEN 'mg'

      -- Respiratory inhalers
      WHEN dmd.vmp_code = '38893611000001108' AND ddd.comment = 'Expressed as aclidinium, delivered dose' THEN ingredient.strnt_nmrtr_uom_name -- Aclidinium
      WHEN dmd.vmp_code = '21496211000001102' AND ddd.comment = 'Expressed as glycopyrronium, delivered dose' THEN ingredient.strnt_nmrtr_uom_name -- Glycopyrronium
      WHEN dmd.vmp_code IN ('33596311000001107', '9478911000001107', '9479011000001103') AND ddd.comment = 'Expressed as tiotropium, delivered dose' THEN ingredient.strnt_nmrtr_uom_name -- Tiotropium
      WHEN dmd.vmp_code = '27890611000001109' AND ddd.comment = 'Expressed as umeclidinium, delivered dose' THEN ingredient.strnt_nmrtr_uom_name -- Umeclidinium

      -- Iron supplements (Fe2+)
      WHEN dmd.vmp_code IN ('41984711000001104', '41984811000001107', '41985011000001102', '38898111000001109', '39108711000001103') AND ddd.comment = 'Fe2+' THEN ingredient.strnt_nmrtr_uom_name

      -- Anticoagulants
      WHEN ingredient.ing_code = '108983001' AND ddd.comment = 'anti Xa' THEN 'unit' -- Enoxaparin (converted from mg to units)
      WHEN ingredient.ing_code = '108987000' AND ddd.comment = 'anti Xa' THEN ingredient.strnt_nmrtr_uom_name -- Dalteparin
      WHEN ingredient.ing_code = '386954009' AND ddd.comment = 'anti Xa' THEN ingredient.strnt_nmrtr_uom_name -- Danaparoid
      WHEN ingredient.ing_code = '395864005' AND ddd.comment = 'anti Xa' THEN ingredient.strnt_nmrtr_uom_name -- Reviparin
      WHEN ingredient.ing_code = '395908006' AND ddd.comment = 'anti Xa' THEN ingredient.strnt_nmrtr_uom_name -- Tinzaparin
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
)

-- ============================================================================
-- STEP 3: Format final output with UOM codes
-- ============================================================================
SELECT DISTINCT
  vmp_code AS vmp_id,
  vmp_nm AS vmp_name,
  ddd_comment,
  expressed_as_strnt_nmrtr,
  uom_lookup.uom_code AS expressed_as_strnt_nmrtr_uom,
  expressed_as_strnt_uom_name AS expressed_as_strnt_nmrtr_uom_name,
  matched_ingredient_code AS ingredient_code,
  matched_ingredient_name AS ingredient_name
FROM expressed_as_mapped
LEFT JOIN `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_UOM_TABLE_ID }}` uom_lookup
  ON expressed_as_strnt_uom_name = uom_lookup.description
WHERE expressed_as_strnt_nmrtr IS NOT NULL
ORDER BY who_ddd_comment, vmp_nm