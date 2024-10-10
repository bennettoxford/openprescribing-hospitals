CREATE OR REPLACE MATERIALIZED VIEW ebmdatalab.scmd.scmd_latest_norm AS
SELECT
    scmd_latest.year_month,
    scmd_latest.ods_code,
    coalesce(vmp.id, scmd_latest.vmp_snomed_code) as vmp_code,
    scmd_latest.vmp_product_name as vmp_name,
    scmd_latest.unit_of_measure_name as uom,
    scmd_latest.total_quanity_in_vmp_unit as quantity,
    scmd_latest.indicative_cost
    FROM scmd.scmd_latest
    LEFT JOIN dmd.vmp
    ON scmd_latest.vmp_snomed_code = vmp.vpidprev