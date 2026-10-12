WITH {{ scope }},
eric AS (
  SELECT trust_code, trust_type
  FROM `ebmdatalab.scmd_pipeline.eric_trust_data`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY trust_code
    ORDER BY data_year DESC
  ) = 1
)
SELECT
  o.ods_code,
  o.ods_name,
  o.region,
  o.region_code,
  o.icb,
  o.icb_code,
  eric.trust_type,
  ca.cancer_alliance_code,
  ca.cancer_alliance_name,
  (sg.ods_code IS NOT NULL) AS in_shelford_group,
  sc.successor_code AS successors,
  ARRAY_TO_STRING(o.ultimate_successors, '|') AS ultimate_successors
FROM trusts AS t
INNER JOIN `ebmdatalab.scmd_pipeline.ods` AS o
  ON o.ods_code = t.ods_code
LEFT JOIN successor_choice AS sc
  ON sc.ods_code = o.ods_code
LEFT JOIN eric
  ON eric.trust_code = o.ods_code
LEFT JOIN `ebmdatalab.scmd_pipeline.cancer_alliance_categorisations` AS ca
  ON ca.ods_code = o.ods_code
LEFT JOIN `ebmdatalab.scmd_pipeline.shelford_group_trusts` AS sg
  ON sg.ods_code = o.ods_code
ORDER BY o.ods_name;
