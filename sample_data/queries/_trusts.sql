-- The sample has:
--   50 current trusts with the highest quantity for the original 10 products
--   one older trust that merged into one of those 50
--   up to five more current trusts for each extra product that has no quantity in the 50
-- A current trust has no ultimate successor.
-- The 50 trusts stay based on the original 10 products.
current_trusts AS (
  SELECT ods_code
  FROM (
    SELECT d.ods_code, SUM(d.scmd_quantity) AS total_qty
    FROM `ebmdatalab.scmd_pipeline.dose` AS d
    CROSS JOIN bounds AS b
    INNER JOIN core_products AS p
      ON p.vmp_code = d.vmp_code
    INNER JOIN `ebmdatalab.scmd_pipeline.ods` AS o
      ON o.ods_code = d.ods_code
    WHERE d.year_month BETWEEN b.start_month AND b.end_month
      AND d.scmd_quantity > 0
      AND IFNULL(ARRAY_LENGTH(o.ultimate_successors), 0) = 0
    GROUP BY d.ods_code
    ORDER BY total_qty DESC
    LIMIT 50
  )
),
predecessor AS (
  -- One older trust whose ultimate successor is one of the 50 current trusts.
  SELECT ods_code
  FROM (
    SELECT d.ods_code, SUM(d.scmd_quantity) AS total_qty
    FROM `ebmdatalab.scmd_pipeline.dose` AS d
    CROSS JOIN bounds AS b
    INNER JOIN core_products AS p
      ON p.vmp_code = d.vmp_code
    INNER JOIN `ebmdatalab.scmd_pipeline.ods` AS o
      ON o.ods_code = d.ods_code
    INNER JOIN (
      SELECT DISTINCT predecessor.ods_code
      FROM `ebmdatalab.scmd_pipeline.ods` AS predecessor
      CROSS JOIN UNNEST(predecessor.ultimate_successors) AS successor_code
      INNER JOIN current_trusts AS c
        ON c.ods_code = successor_code
    ) AS merged
      ON merged.ods_code = d.ods_code
    WHERE d.year_month BETWEEN b.start_month AND b.end_month
      AND d.scmd_quantity > 0
    GROUP BY d.ods_code
    ORDER BY total_qty DESC
    LIMIT 1
  )
),
sparse_products AS (
  -- Extra products with no quantity at the 50 current trusts.
  SELECT e.vmp_code
  FROM extra_products AS e
  LEFT JOIN (
    SELECT DISTINCT d.vmp_code
    FROM `ebmdatalab.scmd_pipeline.dose` AS d
    CROSS JOIN bounds AS b
    INNER JOIN current_trusts AS c
      ON c.ods_code = d.ods_code
    WHERE d.year_month BETWEEN b.start_month AND b.end_month
      AND d.scmd_quantity > 0
  ) AS issued
    ON issued.vmp_code = e.vmp_code
  WHERE issued.vmp_code IS NULL
),
sparse_trusts AS (
  -- Up to five current trusts for each sparse extra product.
  SELECT ods_code
  FROM (
    SELECT
      ranked.ods_code,
      ROW_NUMBER() OVER (
        PARTITION BY ranked.vmp_code
        ORDER BY ranked.total_qty DESC
      ) AS trust_rank
    FROM (
      SELECT d.vmp_code, d.ods_code, SUM(d.scmd_quantity) AS total_qty
      FROM `ebmdatalab.scmd_pipeline.dose` AS d
      CROSS JOIN bounds AS b
      INNER JOIN sparse_products AS s
        ON s.vmp_code = d.vmp_code
      INNER JOIN `ebmdatalab.scmd_pipeline.ods` AS o
        ON o.ods_code = d.ods_code
      LEFT JOIN current_trusts AS already_current
        ON already_current.ods_code = d.ods_code
      WHERE d.year_month BETWEEN b.start_month AND b.end_month
        AND d.scmd_quantity > 0
        AND IFNULL(ARRAY_LENGTH(o.ultimate_successors), 0) = 0
        AND already_current.ods_code IS NULL
      GROUP BY d.vmp_code, d.ods_code
    ) AS ranked
  )
  WHERE trust_rank <= 5
),
trusts AS (
  SELECT ods_code FROM current_trusts
  UNION DISTINCT
  SELECT ods_code FROM predecessor
  UNION DISTINCT
  SELECT ods_code FROM sparse_trusts
),
successor_choice AS (
  SELECT candidate.ods_code, candidate.successor_code
  FROM (
    SELECT
      o.ods_code,
      code AS successor_code,
      off AS sort_key
    FROM `ebmdatalab.scmd_pipeline.ods` AS o
    CROSS JOIN UNNEST(IFNULL(o.successors, [])) AS code WITH OFFSET AS off
    UNION ALL
    SELECT
      o.ods_code,
      code,
      off + 1000
    FROM `ebmdatalab.scmd_pipeline.ods` AS o
    CROSS JOIN UNNEST(IFNULL(o.ultimate_successors, [])) AS code WITH OFFSET AS off
  ) AS candidate
  INNER JOIN current_trusts AS c
    ON c.ods_code = candidate.successor_code
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY candidate.ods_code
    ORDER BY candidate.sort_key
  ) = 1
)
