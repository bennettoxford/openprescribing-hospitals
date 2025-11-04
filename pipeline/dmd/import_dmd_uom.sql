MERGE INTO `{{ PROJECT_ID }}.{{ DATASET_ID }}.{{ DMD_UOM_TABLE_ID }}` T
USING (
  SELECT 
    CAST(cd AS STRING) as uom_code,
    CAST(cdprev AS STRING) as uom_code_prev,
    CAST(cddt AS DATE) as change_date,
    descr as description
  FROM dmd.unitofmeasure
) S
ON T.uom_code = S.uom_code
WHEN MATCHED THEN
  UPDATE SET
    uom_code_prev = S.uom_code_prev,
    change_date = S.change_date,
    description = S.description
WHEN NOT MATCHED THEN
  INSERT (
    uom_code,
    uom_code_prev,
    change_date,
    description
  )
  VALUES (
    S.uom_code,
    S.uom_code_prev,
    S.change_date,
    S.description
  );