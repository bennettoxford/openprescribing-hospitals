WITH {{ scope }}
SELECT
  v.vmp_code,
  v.vmp_name,
  v.vtm_code,
  v.vtm_name,
  v.bnf_code,
  v.df_ind,
  v.udfs,
  v.udfs_uom,
  v.unit_dose_uom,
  v.special,
  v.scmd_uom_name,
  ARRAY_TO_STRING(v.who_route_codes_for_ddd, '|') AS who_route_codes
FROM `ebmdatalab.scmd_pipeline.vmp_data` AS v
INNER JOIN products AS p
  ON p.vmp_code = v.vmp_code
ORDER BY v.vmp_name;
