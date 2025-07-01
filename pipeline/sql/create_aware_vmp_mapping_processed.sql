CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ AWARE_VMP_MAPPING_PROCESSED_TABLE_ID }}`
AS

WITH 
matched_vtms AS ( -- This query does the fuzzy matching of Antibiotic name to VTM name
  SELECT  
    aware.Antibiotic,
    aware.atc_route,
    aware.aware_2019,
    aware.aware_2024,
    vtm.nm,
    vtm.id,
    CASE
      WHEN aware.Antibiotic = vtm.nm THEN 'DIRECT'
      ELSE 'ASSUMED'
    END AS match_type
  FROM `ebmdatalab.chris.aware_list_extended` aware
  LEFT JOIN `ebmdatalab.dmd.vtm` vtm 
    ON vtm.nm LIKE CONCAT('%', aware.Antibiotic, '%')
  WHERE vtm.invalid IS NOT TRUE
),

duplicate_vtms AS ( -- This produces a list of duplicate rows where a direct match already exists
  SELECT *
  FROM matched_vtms
  WHERE nm IN (
    SELECT nm
    FROM matched_vtms
    WHERE atc_route IS NULL
    GROUP BY nm
    HAVING COUNT(*) > 1
  )
  AND match_type = 'ASSUMED'
),

vmp_with_route AS (
  SELECT DISTINCT
    vmp.id AS id,
    vmp.vtm AS vtm,
    vmp.nm AS nm,
    COALESCE(sroute.route, routelookup.who_route) AS vmp_atc_route
  FROM `ebmdatalab.dmd.vmp` vmp
  LEFT JOIN dmd.ont ont ON vmp.id = ont.vmp
  LEFT JOIN dmd.ontformroute ofr ON ont.form = ofr.cd
  LEFT JOIN chris.vmp_single_route_identifier sroute ON vmp.id = sroute.vmp_id
  LEFT JOIN `scmd_dmd_views.dmd_to_atc_route` routelookup ON ofr.descr = routelookup.dmd_ofr
  WHERE ofr.descr NOT LIKE '%.auricular'
  AND ofr.descr NOT LIKE '%.cutaneous'
  AND ofr.descr NOT LIKE '%.ophthalmic'
  AND ofr.descr NOT LIKE '%.oromucosal'
  AND ofr.descr NOT LIKE '%.intralesional'
  AND ofr.descr NOT LIKE '%.nasal'
  AND ofr.descr NOT LIKE '%.gingival'
  AND ofr.descr != 'gel.vaginal'
  AND ofr.descr != 'cream.vaginal'
),

base_aware_mapping AS (
  SELECT DISTINCT
    matched_vtms.Antibiotic,
    matched_vtms.nm as vtm_nm,
    matched_vtms.id as vtm_id,
    vmp.nm as vmp_nm,
    vmp.id as vmp_id,
    matched_vtms.aware_2019,
    matched_vtms.aware_2024
  FROM matched_vtms
  LEFT JOIN vmp_with_route vmp
    ON matched_vtms.id = vmp.vtm
    AND (
      matched_vtms.atc_route IS NULL
      OR matched_vtms.atc_route = vmp.vmp_atc_route
    )
  WHERE NOT EXISTS ( -- removes the duplicates identified above
    SELECT 1
    FROM duplicate_vtms
    WHERE matched_vtms.Antibiotic = duplicate_vtms.Antibiotic AND matched_vtms.nm = duplicate_vtms.nm
  ) AND vmp.id IS NOT NULL
),

-- Historical mapping logic
history_mappings AS (
  SELECT 
    previous_id,
    current_id,
    entity_type,
    start_date,
    end_date
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_HISTORY_TABLE_ID }}`
  WHERE entity_type IN ('VTM', 'VMP')
  AND previous_id != current_id
),

-- Some codes have multiple entries mapping to the same code for different time periods. We deduplicate by selecting the most recent
deduplicated_history AS (
  SELECT 
    previous_id,
    current_id,
    entity_type,
    ROW_NUMBER() OVER (
      PARTITION BY previous_id, entity_type 
      ORDER BY 
        CASE WHEN end_date IS NULL THEN 1 ELSE 0 END DESC, -- Prefer active mappings (no end date)
        end_date DESC,  -- Then prefer latest end date
        start_date DESC -- Finally prefer latest start date
    ) as rn
  FROM history_mappings
),

vtm_mappings AS (
  SELECT previous_id, current_id
  FROM deduplicated_history
  WHERE entity_type = 'VTM' AND rn = 1
),

vmp_mappings AS (
  SELECT previous_id, current_id
  FROM deduplicated_history
  WHERE entity_type = 'VMP' AND rn = 1
),

-- Get distinct VMP codes from processed SCMD
scmd_vmps AS (
  SELECT DISTINCT vmp_code
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}`
),

-- Get current VTM and VMP names from DMD
current_vtm_names AS (
  SELECT 
    CAST(id AS STRING) as vtm_id,
    nm as vtm_name
  FROM `ebmdatalab.dmd.vtm`
  WHERE invalid IS NOT TRUE
),

current_vmp_names AS (
  SELECT 
    CAST(id AS STRING) as vmp_id,
    nm as vmp_name
  FROM `ebmdatalab.dmd.vmp`
  WHERE invalid IS NOT TRUE
),

-- Apply historical mappings to the base AWaRe mapping
mapped_aware_data AS (
  SELECT
    aware.Antibiotic,
    aware.vtm_nm as original_vtm_nm,
    aware.vtm_id as original_vtm_id,
    aware.vmp_nm as original_vmp_nm,
    aware.vmp_id as original_vmp_id,
    aware.aware_2019,
    aware.aware_2024,
    
    -- Map VTM ID to current ID if mapping exists
    CAST(COALESCE(vtm_map.current_id, CAST(aware.vtm_id AS STRING)) AS STRING) as current_vtm_id,
    
    -- Map VMP ID to current ID if mapping exists  
    CAST(COALESCE(vmp_map.current_id, CAST(aware.vmp_id AS STRING)) AS STRING) as current_vmp_id
    
  FROM base_aware_mapping aware
  LEFT JOIN vtm_mappings vtm_map
    ON CAST(aware.vtm_id AS STRING) = vtm_map.previous_id
  LEFT JOIN vmp_mappings vmp_map
    ON CAST(aware.vmp_id AS STRING) = vmp_map.previous_id
),

-- Join with current names and restrict to SCMD VMPs
final_mapped_data AS (
  SELECT
    mapped.Antibiotic,
    
    -- Use current VTM name if available, otherwise keep original
    COALESCE(current_vtm.vtm_name, mapped.original_vtm_nm) as vtm_nm,
    CAST(mapped.current_vtm_id AS INTEGER) as vtm_id,
    
    -- Use current VMP name if available, otherwise keep original  
    COALESCE(current_vmp.vmp_name, mapped.original_vmp_nm) as vmp_nm,
    CAST(mapped.current_vmp_id AS INTEGER) as vmp_id,
    
    mapped.aware_2019,
    mapped.aware_2024,
    
    -- Add flags to indicate if historical mapping was applied
    CASE 
      WHEN mapped.current_vtm_id != CAST(mapped.original_vtm_id AS STRING) THEN TRUE 
      ELSE FALSE 
    END as vtm_id_updated,
    
    CASE 
      WHEN mapped.current_vmp_id != CAST(mapped.original_vmp_id AS STRING) THEN TRUE 
      ELSE FALSE 
    END as vmp_id_updated
    
  FROM mapped_aware_data mapped
  LEFT JOIN current_vtm_names current_vtm
    ON mapped.current_vtm_id = current_vtm.vtm_id
  LEFT JOIN current_vmp_names current_vmp
    ON mapped.current_vmp_id = current_vmp.vmp_id
 
  INNER JOIN scmd_vmps scmd
    ON mapped.current_vmp_id = scmd.vmp_code
)

SELECT DISTINCT
  Antibiotic,
  vtm_nm,
  vtm_id,
  vmp_nm,
  vmp_id,
  aware_2019,
  aware_2024,
  vtm_id_updated,
  vmp_id_updated
FROM final_mapped_data
ORDER BY Antibiotic, vmp_nm;