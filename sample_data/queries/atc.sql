WITH {{ scope }},
product_atc AS (
  SELECT DISTINCT atc.atc_code
  FROM `ebmdatalab.scmd_pipeline.vmp_data` AS v
  INNER JOIN products AS p
    ON p.vmp_code = v.vmp_code
  CROSS JOIN UNNEST(v.atcs) AS atc
),
atc_codes AS (
  SELECT DISTINCT ancestor_code
  FROM product_atc
  CROSS JOIN UNNEST([
    SUBSTR(atc_code, 1, 1),
    SUBSTR(atc_code, 1, 3),
    SUBSTR(atc_code, 1, 4),
    SUBSTR(atc_code, 1, 5),
    atc_code
  ]) AS ancestor_code
)
SELECT
  a.atc_code,
  a.atc_name,
  a.anatomical_main_group,
  a.therapeutic_subgroup,
  a.pharmacological_subgroup,
  a.chemical_subgroup,
  a.chemical_substance
FROM `ebmdatalab.scmd_pipeline.who_atc` AS a
INNER JOIN atc_codes AS c
  ON c.ancestor_code = a.atc_code
ORDER BY a.atc_code;
