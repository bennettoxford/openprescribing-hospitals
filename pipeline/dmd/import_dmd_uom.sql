CREATE OR REPLACE TABLE `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_UOM_TABLE_ID }}`
CLUSTER BY uom_code
AS
SELECT
  CAST(cd AS STRING) AS uom_code,
  CAST(cdprev AS STRING) AS uom_code_prev,
  CAST(cddt AS DATE) AS change_date,
  descr AS description
FROM dmd.unitofmeasure
WHERE CAST(cd AS STRING) NOT IN (
  SELECT CAST(cdprev AS STRING)
  FROM dmd.unitofmeasure
  WHERE cdprev IS NOT NULL
);