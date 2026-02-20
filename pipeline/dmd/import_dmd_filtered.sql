CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_TABLE_ID }}`
CLUSTER BY vmp_code
AS

SELECT
  df.vmp_code,
  df.vmp_name,
  df.vtm,
  df.vtm_name,
  df.df_ind,
  df.udfs,
  df.udfs_uom,
  df.unit_dose_uom,
  df.dform_form,
  df.ingredients,
  df.ontformroutes,
  df.amps
FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_FULL_TABLE_ID }}` df
INNER JOIN (
  SELECT DISTINCT vmp_code
  FROM `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ SCMD_PROCESSED_TABLE_ID }}`
) scmd ON df.vmp_code = scmd.vmp_code
