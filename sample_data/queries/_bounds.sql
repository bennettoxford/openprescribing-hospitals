-- The sample covers the latest 24 months in the dose table.
bounds AS (
  SELECT
    MAX(year_month) AS end_month,
    DATE_SUB(MAX(year_month), INTERVAL 23 MONTH) AS start_month
  FROM `ebmdatalab.scmd_pipeline.dose`
)
