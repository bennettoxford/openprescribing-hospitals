from dataclasses import dataclass
from typing import List
from google.cloud import bigquery

from pipeline.utils.config import (
    PROJECT_ID,
    DATASET_ID,
    ORGANISATION_TABLE_ID,
    SCMD_RAW_TABLE_ID,
    SCMD_PROCESSED_TABLE_ID,
    SCMD_DATA_STATUS_TABLE_ID,
    UNITS_CONVERSION_TABLE_ID,
    ORG_AE_STATUS_TABLE_ID,
    DMD_TABLE_ID,
    DMD_SUPP_TABLE_ID,
    WHO_ATC_TABLE_ID,
    WHO_DDD_TABLE_ID,
    ADM_ROUTE_MAPPING_TABLE_ID,
    DOSE_TABLE_ID,
    INGREDIENT_QUANTITY_TABLE_ID,
    DDD_QUANTITY_TABLE_ID,
    WHO_ROUTES_OF_ADMINISTRATION_TABLE_ID,
    VMP_DDD_MAPPING_TABLE_ID,
    VMP_TABLE_ID,
    VMP_UNIT_STANDARDISATION_TABLE_ID,
    VTM_INGREDIENTS_TABLE_ID,
    DMD_HISTORY_TABLE_ID,
    DMD_UOM_TABLE_ID,
)


@dataclass
class TableSpec:
    project_id: str
    dataset_id: str
    table_id: str
    schema: List[bigquery.SchemaField]
    description: str = ""
    partition_field: str = None
    cluster_fields: List[str] = None

    @property
    def full_table_id(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"


ORGANISATION_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=ORGANISATION_TABLE_ID,
    description="Details for all NHS Trusts (identified using the following role codes: RO197, RO24) obtained using the ORD API",
    schema=[
        bigquery.SchemaField(
            "ods_code", "STRING", mode="REQUIRED", description="ODS code of the organisation"
        ),
        bigquery.SchemaField(
            "ods_name", "STRING", mode="REQUIRED", description="Name of the organisation"
        ),
        bigquery.SchemaField(
            "successors",
            "STRING",
            mode="REPEATED",
            description="ODS codes for all successor organisations",
        ),
        bigquery.SchemaField(
            "ultimate_successors",
            "STRING",
            mode="REPEATED",
            description="ODS code for the ultimate successor organisation",
        ),
        bigquery.SchemaField(
            "predecessors",
            "STRING",
            mode="REPEATED",
            description="ODS codes for all predecessor organisations",
        ),
        bigquery.SchemaField(
            "legal_closed_date",
            "DATE",
            mode="NULLABLE",
            description="Legal closed date of the organisation",
        ),
        bigquery.SchemaField(
            "operational_closed_date",
            "DATE",
            mode="NULLABLE",
            description="Operational closed date of the organisation",
        ),
        bigquery.SchemaField(
            "legal_open_date", 
            "DATE",
            mode="NULLABLE",
            description="Legal open date of the organisation"
        ),
        bigquery.SchemaField(
            "operational_open_date",
            "DATE",
            mode="NULLABLE",
            description="Operational open date of the organisation",
        ),
        bigquery.SchemaField(
            "postcode", "STRING", mode="NULLABLE", description="Postcode of the organisation"
        ),
        bigquery.SchemaField(
            "region",
            "STRING",
            mode="NULLABLE",
            description="Region of the organisation (through the ICB)",
        ),
        bigquery.SchemaField("icb", "STRING", mode="NULLABLE", description="ICB of the organisation"),
    ],
)

SCMD_RAW_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=SCMD_RAW_TABLE_ID,
    description="Raw SCMD data",
    schema=[
        bigquery.SchemaField(
            "year_month", "DATE", mode="REQUIRED", description="Year and month of the data"
        ),
        bigquery.SchemaField("ods_code", "STRING", mode="REQUIRED", description="ODS code"),
        bigquery.SchemaField(
            "vmp_snomed_code",
            "STRING",
            mode="REQUIRED",
            description="SNOMED code indicating VMP from dm+d",
        ),
        bigquery.SchemaField(
            "vmp_product_name", "STRING", mode="REQUIRED", description="Product name from dm+d"
        ),
        bigquery.SchemaField(
            "unit_of_measure_identifier",
            "STRING",
            mode="REQUIRED",
            description="Identifier for the unit of measure from dm+d",
        ),
        bigquery.SchemaField(
            "unit_of_measure_name",
            "STRING",
            mode="REQUIRED",
            description="Name of the unit of measure from dm+d",
        ),
        bigquery.SchemaField(
            "total_quantity_in_vmp_unit",
            "FLOAT",
            mode="REQUIRED",
            description="Total quantity in the unit of measure",
        ),
        bigquery.SchemaField("indicative_cost", "FLOAT", mode="NULLABLE", description="Indicative cost"),
    ],
    partition_field="year_month",
    cluster_fields=["vmp_snomed_code"],
)

SCMD_PROCESSED_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=SCMD_PROCESSED_TABLE_ID,
    description="SCMD data with the following transformations applied: 1) VMP codes are mapped to their latest version, 2) Quantity is converted to the quantity in basis units, e.g. mg converted to g",
    schema=[
        bigquery.SchemaField(
            "year_month", "DATE", mode="REQUIRED", description="Year and month of the data"
        ),
        bigquery.SchemaField("ods_code", "STRING", mode="REQUIRED", description="ODS code"),
        bigquery.SchemaField(
            "vmp_code", "STRING", mode="REQUIRED", description="SNOMED code for the VMP from dm+d"
        ),
        bigquery.SchemaField(
            "vmp_name", "STRING", mode="REQUIRED", description="Product name from dm+d"
        ),
        bigquery.SchemaField(
            "uom_id",
            "STRING",
            mode="REQUIRED",
            description="Identifier for the unit of measure from dm+d",
        ),
        bigquery.SchemaField(
            "uom_name",
            "STRING",
            mode="REQUIRED",
            description="Name of the unit of measure from dm+d",
        ),
        bigquery.SchemaField(
            "normalised_uom_id",
            "STRING",
            mode="REQUIRED",
            description="Identifier for the normalised unit of measure",
        ),
        bigquery.SchemaField(
            "normalised_uom_name",
            "STRING",
            mode="REQUIRED",
            description="Name of the normalised unit of measure",
        ),
        bigquery.SchemaField(
            "quantity",
            "FLOAT",
            mode="REQUIRED",
            description="Total quantity in the unit of measure",
        ),
        bigquery.SchemaField(
            "normalised_quantity",
            "FLOAT",
            mode="REQUIRED",
            description="Total quantity in basis units",
        ),
        bigquery.SchemaField("indicative_cost", "FLOAT", mode="NULLABLE", description="Indicative cost"),
    ],
    partition_field="year_month",
    cluster_fields=["vmp_code"],
)

SCMD_DATA_STATUS_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=SCMD_DATA_STATUS_TABLE_ID,
    description="Status (provisional, finalised) for each month of SCMD data",
    schema=[
        bigquery.SchemaField(
            "year_month", "DATE", mode="REQUIRED", description="Year and month of the data"
        ),
        bigquery.SchemaField(
            "file_type", "STRING", mode="REQUIRED", description="File type (provisional, wip, finalised)"
        ),
    ],
    partition_field="year_month",
)

UNITS_CONVERSION_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=UNITS_CONVERSION_TABLE_ID,
    description="Units conversion table. Generated manually taking all unique units from dm+d and WHO DDDs",
    schema=[
        bigquery.SchemaField("unit", "STRING", mode="REQUIRED", description="Unit"),
        bigquery.SchemaField("basis", "STRING", mode="REQUIRED", description="Basis"),
        bigquery.SchemaField(
            "conversion_factor", "FLOAT", mode="REQUIRED", description="Conversion factor"
        ),
        bigquery.SchemaField("unit_id", "STRING", mode="NULLABLE", description="Unit ID"),
        bigquery.SchemaField("basis_id", "STRING", mode="NULLABLE", description="Basis ID"),
    ],
)

ORG_AE_STATUS_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=ORG_AE_STATUS_TABLE_ID,
    description="Indicator for whether an organisation has had A&E attendances Type 1 by month",
    schema=[
        bigquery.SchemaField(
            "ods_code", "STRING", mode="REQUIRED", description="ODS code of the organisation"
        ),
        bigquery.SchemaField(
            "period", "DATE", mode="REQUIRED", description="Start date of the month, YYYY-MM-DD"
        ),
        bigquery.SchemaField(
            "has_ae",
            "BOOLEAN",
            mode="REQUIRED",
            description="Binary indicator for each organisation if they have over 0 A&E attendances Type 1",
        ),
    ],
    partition_field="period",
)

DMD_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=DMD_TABLE_ID,
    description="Dictionary of Medicines and Devices (dm+d) data",
    schema=[
        bigquery.SchemaField(
            "vmp_code", "STRING", mode="REQUIRED", description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField(
            "vmp_code_prev", "STRING", description="Previous VMP code"
        ),
        bigquery.SchemaField("vmp_name", "STRING", description="VMP name"),
        bigquery.SchemaField(
            "vtm", "STRING", mode="NULLABLE", description="Virtual Therapeutic Moiety (VTM) code"
        ),
        bigquery.SchemaField("vtm_name", "STRING", mode="NULLABLE", description="VTM name"),
        bigquery.SchemaField("df_ind", "STRING", mode="REQUIRED", description="Dose form indicator"),
        bigquery.SchemaField("udfs", "FLOAT", mode="NULLABLE", description="Unit dose form size"),
        bigquery.SchemaField(
            "udfs_uom", "STRING", mode="NULLABLE", description="Unit dose form size unit of measure"
        ),
        bigquery.SchemaField(
            "unit_dose_uom", "STRING", mode="NULLABLE", description="Unit dose unit of measure"
        ),
        bigquery.SchemaField("dform_form", "STRING", mode="NULLABLE", description="Dose form"),
        bigquery.SchemaField(
            "ingredients",
            "RECORD",
            mode="REPEATED",
            description="Ingredients information",
            fields=[
                bigquery.SchemaField(
                    "ing_code", "STRING", mode="REQUIRED", description="Ingredient code"
                ),
                bigquery.SchemaField(
                    "ing_name", "STRING", mode="REQUIRED", description="Ingredient name"
                ),
                bigquery.SchemaField(
                    "strnt_nmrtr_val", "FLOAT", mode="NULLABLE", description="Strength numerator value"
                ),
                bigquery.SchemaField(
                    "strnt_nmrtr_uom_name",
                    "STRING",
                    mode="NULLABLE",
                    description="Strength numerator unit of measure",
                ),
                bigquery.SchemaField(
                    "strnt_dnmtr_val", "FLOAT", mode="NULLABLE", description="Strength denominator value"
                ),
                bigquery.SchemaField(
                    "strnt_dnmtr_uom_name",
                    "STRING",
                    mode="NULLABLE",
                    description="Strength denominator unit of measure",
                ),
            ],
        ),
        bigquery.SchemaField(
            "ontformroutes",
            "RECORD",
            mode="REPEATED",
            description="Routes of administration",
            fields=[
                bigquery.SchemaField(
                    "ontformroute_cd", "STRING", mode="REQUIRED", description="Route code"
                ),
                bigquery.SchemaField(
                    "ontformroute_descr", "STRING", mode="REQUIRED", description="Route description"
                ),
            ],
        ),
    ],
    cluster_fields=["vmp_code"],
)

DMD_SUPP_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=DMD_SUPP_TABLE_ID,
    description="dm+d supplementary data containing BNF codes, ATC codes and DDDs for VMPs",
    schema=[
        bigquery.SchemaField(
            "vmp_code", "STRING", mode="REQUIRED", description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField("bnf_code", "STRING", mode="NULLABLE", description="BNF code"),
        bigquery.SchemaField("atc_code", "STRING", mode="NULLABLE", description="ATC code"),
        bigquery.SchemaField("ddd", "FLOAT", mode="NULLABLE", description="DDD"),
        bigquery.SchemaField("ddd_uom", "STRING", mode="NULLABLE", description="DDD unit"),
    ],
)

WHO_ATC_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=WHO_ATC_TABLE_ID,
    description="ATC information from the WHO, including hierarchical level information",
    schema=[
        bigquery.SchemaField("atc_code", "STRING", mode="REQUIRED", description="ATC code"),
        bigquery.SchemaField("atc_name", "STRING", mode="REQUIRED", description="ATC name"),
        bigquery.SchemaField("comment", "STRING", mode="NULLABLE", description="ATC level"),
        bigquery.SchemaField(
            "anatomical_main_group", 
            "STRING", 
            mode="REQUIRED",
            description="1st level: Anatomical main group (e.g., 'A' for Alimentary tract and metabolism)"
        ),
        bigquery.SchemaField(
            "therapeutic_subgroup", 
            "STRING", 
            mode="NULLABLE",
            description="2nd level: Therapeutic subgroup (e.g., 'A10' for Drugs used in diabetes)"
        ),
        bigquery.SchemaField(
            "pharmacological_subgroup", 
            "STRING", 
            mode="NULLABLE",
            description="3rd level: Pharmacological subgroup (e.g., 'A10B' for Blood glucose lowering drugs, excl. insulins)"
        ),
        bigquery.SchemaField(
            "chemical_subgroup", 
            "STRING", 
            mode="NULLABLE",
            description="4th level: Chemical subgroup (e.g., 'A10BA' for Biguanides)"
        ),
        bigquery.SchemaField(
            "chemical_substance", 
            "STRING", 
            mode="NULLABLE",
            description="5th level: Chemical substance (e.g., 'A10BA02' for Metformin)"
        ),
        bigquery.SchemaField(
            "level", 
            "INTEGER", 
            mode="REQUIRED",
            description="The hierarchical level of this ATC code (1-5)"
        ),
    ],
)

WHO_DDD_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=WHO_DDD_TABLE_ID,
    description="DDD information for ATC codes from the WHO",
    schema=[
        bigquery.SchemaField("atc_code", "STRING", mode="REQUIRED", description="ATC code"),
        bigquery.SchemaField("ddd", "FLOAT", mode="REQUIRED", description="DDD"),
        bigquery.SchemaField("ddd_unit", "STRING", mode="NULLABLE", description="DDD unit"),
        bigquery.SchemaField(
            "adm_code", "STRING", mode="NULLABLE", description="Route of administration code"
        ),
        bigquery.SchemaField("comment", "STRING", mode="NULLABLE", description="Comment"),
    ],
)

WHO_ROUTES_OF_ADMINISTRATION_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=WHO_ROUTES_OF_ADMINISTRATION_TABLE_ID,
    description="Routes of administration information from the WHO. Manually generated.",
    schema=[
        bigquery.SchemaField("who_route_code", "STRING", mode="NULLABLE", description="WHO route code"),
        bigquery.SchemaField(
            "who_route_description", "STRING", mode="NULLABLE", description="WHO route description"
        ),
    ],
)

ADM_ROUTE_MAPPING_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=ADM_ROUTE_MAPPING_TABLE_ID,
    description="Mapping between dm+d ontformroute and WHO routes of administration. Manually generated.",
    schema=[
        bigquery.SchemaField("dmd_ontformroute", "STRING", mode="REQUIRED", description="dm+d route"),
        bigquery.SchemaField("who_route", "STRING", mode="NULLABLE", description="WHO route"),
    ],
)

DOSE_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=DOSE_TABLE_ID,
    description="SCMD quantity converted to doses, using basis unit conversions for calculations",
    schema=[
        bigquery.SchemaField(
            "year_month", "DATE", mode="REQUIRED", description="Year and month of the data"
        ),
        bigquery.SchemaField(
            "vmp_code", "STRING", mode="REQUIRED", description="SNOMED code for the VMP from dm+d"
        ),
        bigquery.SchemaField(
            "vmp_name", "STRING", mode="REQUIRED", description="Product name from dm+d"
        ),
        bigquery.SchemaField("ods_code", "STRING", mode="REQUIRED", description="ODS code"),
        bigquery.SchemaField("ods_name", "STRING", mode="REQUIRED", description="Organisation name"),
        bigquery.SchemaField("scmd_quantity", "FLOAT", mode="REQUIRED", description="Original SCMD quantity"),
        bigquery.SchemaField("scmd_quantity_unit_name", "STRING", mode="REQUIRED", description="Original SCMD quantity unit"),
        bigquery.SchemaField(
            "scmd_basis_unit",
            "STRING",
            mode="REQUIRED",
            description="Basis unit for the SCMD quantity",
        ),
        bigquery.SchemaField(
            "scmd_basis_unit_name",
            "STRING",
            mode="REQUIRED",
            description="Unit of measure name for SCMD quantity",
        ),
        bigquery.SchemaField(
            "scmd_quantity_in_basis_units",
            "FLOAT",
            mode="REQUIRED",
            description="SCMD quantity converted to basis units",
        ),
        bigquery.SchemaField("udfs", "FLOAT", mode="REQUIRED", description="Unit dose form size"),
        bigquery.SchemaField("udfs_uom", "STRING", mode="REQUIRED", description="Unit dose form size unit of measure"),
        bigquery.SchemaField(
            "udfs_basis_quantity", "FLOAT", mode="REQUIRED", description="Unit dose form size converted to basis units"
        ),
        bigquery.SchemaField(
            "udfs_basis_uom", "STRING", mode="REQUIRED", description="Basis unit for the unit dose form size"
        ),
        bigquery.SchemaField(
            "unit_dose_uom", "STRING", mode="REQUIRED", description="Unit dose unit of measure"
        ),
        bigquery.SchemaField(
            "unit_dose_basis_uom", "STRING", mode="REQUIRED", description="Basis unit for the unit dose"
        ),
        bigquery.SchemaField(
            "dose_quantity", "FLOAT", mode="REQUIRED", description="Calculated number of doses"
        ),
        bigquery.SchemaField(
            "dose_unit", "STRING", mode="REQUIRED", description="Unit of measure for the dose"
        ),
        bigquery.SchemaField("df_ind", "STRING", mode="REQUIRED", description="Dose form indicator"),
        bigquery.SchemaField(
            "logic", "STRING", mode="REQUIRED", description="Logic used for dose calculation including unit conversion details"
        ),
    ],
    partition_field="year_month",
    cluster_fields=["vmp_code"],
)

INGREDIENT_QUANTITY_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=INGREDIENT_QUANTITY_TABLE_ID,
    description="Ingredient level quantities calculated from the SCMD data",
    schema=[
        bigquery.SchemaField(
            "vmp_code", "STRING", mode="REQUIRED", description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField(
            "year_month", "DATE", mode="REQUIRED", description="Year and month of the data"
        ),
        bigquery.SchemaField("ods_code", "STRING", mode="REQUIRED", description="ODS code"),
        bigquery.SchemaField("ods_name", "STRING", mode="REQUIRED", description="Organisation name"),
        bigquery.SchemaField("vmp_name", "STRING", mode="REQUIRED", description="VMP name from dm+d"),
        bigquery.SchemaField(
            "converted_quantity", "FLOAT", mode="REQUIRED", description="Converted quantity from SCMD"
        ),
        bigquery.SchemaField(
            "quantity_basis", "STRING", mode="REQUIRED", description="Unit of measure ID for the quantity"
        ),
        bigquery.SchemaField(
            "quantity_basis_name", "STRING", mode="REQUIRED", description="Unit of measure name for the quantity"
        ),
        bigquery.SchemaField(
            "ingredients",
            "RECORD",
            mode="REPEATED",
            description="Ingredient quantities",
            fields=[
                bigquery.SchemaField(
                    "ingredient_code", "STRING", mode="REQUIRED", description="Ingredient code"
                ),
                bigquery.SchemaField(
                    "ingredient_name", "STRING", mode="REQUIRED", description="Ingredient name"
                ),
                bigquery.SchemaField(
                    "ingredient_quantity",
                    "FLOAT",
                    mode="REQUIRED",
                    description="Calculated ingredient quantity",
                ),
                bigquery.SchemaField(
                    "ingredient_unit",
                    "STRING",
                    mode="REQUIRED",
                    description="Unit of measure for the ingredient",
                ),
                bigquery.SchemaField(
                    "ingredient_quantity_basis",
                    "FLOAT",
                    mode="REQUIRED",
                    description="Calculated ingredient quantity in basis units",
                ),
                bigquery.SchemaField(
                    "ingredient_basis_unit",
                    "STRING",
                    mode="REQUIRED",
                    description="Basis unit for the ingredient",
                ),
                bigquery.SchemaField(
                    "strength_numerator_value",
                    "FLOAT",
                    mode="REQUIRED",
                    description="Strength numerator value",
                ),
                bigquery.SchemaField(
                    "strength_numerator_unit",
                    "STRING",
                    mode="REQUIRED",
                    description="Strength numerator unit",
                ),
                bigquery.SchemaField(
                    "strength_denominator_value",
                    "FLOAT",
                    mode="NULLABLE",
                    description="Strength denominator value",
                ),
                bigquery.SchemaField(
                    "strength_denominator_unit",
                    "STRING",
                    mode="NULLABLE",
                    description="Strength denominator unit",
                ),
                bigquery.SchemaField(
                    "quantity_to_denominator_conversion_factor",
                    "FLOAT",
                    mode="NULLABLE",
                    description="Conversion factor from quantity units to denominator units",
                ),
                bigquery.SchemaField(
                    "denominator_basis_unit",
                    "STRING",
                    mode="NULLABLE",
                    description="Basis unit for the denominator",
                ),
                bigquery.SchemaField(
                    "calculation_logic",
                    "STRING",
                    mode="REQUIRED",
                    description="Logic used for ingredient calculation",
                ),
            ],
        ),
    ],
    partition_field="year_month",
    cluster_fields=["vmp_code"],
)

DDD_QUANTITY_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=DDD_QUANTITY_TABLE_ID,
    description="Calculated DDD quantities from SCMD data with simplified structure",
    schema=[
        bigquery.SchemaField(
            "vmp_code", "STRING", mode="REQUIRED", description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField(
            "year_month", "DATE", mode="REQUIRED", description="Year and month of the data"
        ),
        bigquery.SchemaField("ods_code", "STRING", mode="REQUIRED", description="ODS code"),
        bigquery.SchemaField("vmp_name", "STRING", mode="REQUIRED", description="VMP name"),
        bigquery.SchemaField("uom", "STRING", mode="REQUIRED", description="Unit of measure identifier"),
        bigquery.SchemaField("uom_name", "STRING", mode="REQUIRED", description="Unit of measure name"),
        bigquery.SchemaField(
            "quantity", "FLOAT", mode="REQUIRED", description="Total quantity in the unit of measure"
        ),
        bigquery.SchemaField(
            "ddd_quantity", "FLOAT", mode="REQUIRED", description="Calculated number of DDDs"
        ),
        bigquery.SchemaField("ddd_value", "FLOAT", mode="REQUIRED", description="DDD value"),
        bigquery.SchemaField("ddd_unit", "STRING", mode="REQUIRED", description="DDD unit"),
        bigquery.SchemaField(
            "calculation_explanation",
            "STRING",
            mode="REQUIRED",
            description="Explanation of the DDD calculation",
        ),
    ],
    partition_field="year_month",
    cluster_fields=["vmp_code"],
)

VMP_DDD_MAPPING_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=VMP_DDD_MAPPING_TABLE_ID,
    description="Simple mapping between VMPs and their corresponding DDD values. Contains a single DDD value and unit per VMP (set to NULL if multiple conflicting values exist). Also includes UOM information from SCMD data.",
    schema=[
        bigquery.SchemaField(
            "vmp_code", "STRING", mode="REQUIRED", description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField("vmp_name", "STRING", mode="REQUIRED", description="VMP name"),
        bigquery.SchemaField(
            "uoms",
            "RECORD",
            mode="REPEATED",
            description="Units of measure from SCMD data",
            fields=[
                bigquery.SchemaField(
                    "uom_id", "STRING", mode="REQUIRED", description="Unit of measure identifier"
                ),
                bigquery.SchemaField(
                    "uom_name", "STRING", mode="REQUIRED", description="Unit of measure name"
                ),
                bigquery.SchemaField(
                    "basis_id", "STRING", mode="REQUIRED", description="Basis unit identifier"
                ),
                bigquery.SchemaField(
                    "basis_name", "STRING", mode="REQUIRED", description="Basis unit name"
                ),
            ],
        ),
        bigquery.SchemaField(
            "atcs",
            "RECORD",
            mode="REPEATED",
            description="ATC codes",
            fields=[
                bigquery.SchemaField("atc_code", "STRING", mode="REQUIRED", description="ATC code"),
                bigquery.SchemaField("atc_name", "STRING", mode="REQUIRED", description="ATC name"),
            ],
        ),
        bigquery.SchemaField(
            "ontformroutes",
            "RECORD",
            mode="REPEATED",
            description="Routes of administration",
            fields=[
                bigquery.SchemaField(
                    "ontformroute_cd", "STRING", mode="REQUIRED", description="Route code"
                ),
                bigquery.SchemaField(
                    "ontformroute_descr", "STRING", mode="REQUIRED", description="Route description"
                ),
                bigquery.SchemaField(
                    "who_route_code",
                    "STRING",
                    mode="REQUIRED",
                    description="WHO route code",
                ),
            ],
        ),
        bigquery.SchemaField(
            "who_ddds",
            "RECORD",
            mode="REPEATED",
            description="DDD values from the WHO",
            fields=[
                bigquery.SchemaField("ddd", "FLOAT", mode="REQUIRED", description="DDD value"),
                bigquery.SchemaField("ddd_unit", "STRING", mode="REQUIRED", description="DDD unit"),
                bigquery.SchemaField(
                    "ddd_route_code",
                    "STRING",
                    mode="REQUIRED",
                    description="Route of administration code",
                ),
                bigquery.SchemaField(
                    "ddd_comment",
                    "STRING",
                    mode="NULLABLE",
                    description="Comment",
                ),
            ],
        ),
        bigquery.SchemaField(
            "ingredients_info",
            "RECORD",
            mode="REPEATED",
            description="Ingredient information",
            fields=[
                bigquery.SchemaField(
                    "ingredient_code", "STRING", mode="REQUIRED", description="Ingredient code"
                ),
                bigquery.SchemaField(
                    "ingredient_name", "STRING", mode="REQUIRED", description="Ingredient name"
                ),
                bigquery.SchemaField(
                    "ingredient_unit", "STRING", mode="REQUIRED", description="Ingredient unit"
                ),
                bigquery.SchemaField(
                    "ingredient_basis_unit",
                    "STRING",
                    mode="REQUIRED",
                    description="Basis unit for the ingredient",
                ),
            ],
        ),
        bigquery.SchemaField(
            "can_calculate_ddd",
            "BOOLEAN",
            mode="REQUIRED",
            description="Flag indicating whether DDD can be calculated for this VMP",
        ),
        bigquery.SchemaField(
            "ddd_calculation_logic",
            "STRING",
            mode="REQUIRED",
            description="Explanation of the DDD calculation logic or issue (for both successful and failed calculations)",
        ),
        bigquery.SchemaField(
            "selected_ddd_value",
            "FLOAT",
            mode="REQUIRED",
            description="The DDD value that was selected for calculations",
        ),
        bigquery.SchemaField(
            "selected_ddd_unit",
            "STRING",
            mode="REQUIRED",
            description="The unit of the selected DDD value",
        ),
        bigquery.SchemaField(
            "selected_ddd_basis_unit",
            "STRING",
            mode="REQUIRED",
            description="The basis unit for the selected DDD value",
        ),
        bigquery.SchemaField(
            "selected_ddd_route_code",
            "STRING",
            mode="REQUIRED",
            description="The route code of the selected DDD",
        ),
    ],
    cluster_fields=["vmp_code"],
)

VMP_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=VMP_TABLE_ID,
    description="A table containing VMP (Virtual Medicinal Product) data including related ingredients, routes of administration, and ATC mappings for VMPs found in SCMD data",
    schema=[
        bigquery.SchemaField(
            "vmp_code", "STRING", mode="REQUIRED", description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField("vmp_name", "STRING", mode="REQUIRED", description="VMP name"),
        bigquery.SchemaField("vtm_code", "STRING", mode="NULLABLE", description="Virtual Therapeutic Moiety (VTM) code"),
        bigquery.SchemaField("vtm_name", "STRING", mode="NULLABLE", description="VTM name"),
        bigquery.SchemaField(
            "ingredients",
            "RECORD",
            mode="REPEATED",
            description="Ingredients information",
            fields=[
                bigquery.SchemaField(
                    "ingredient_code", "STRING", mode="REQUIRED", description="Ingredient code"
                ),
                bigquery.SchemaField(
                    "ingredient_name", "STRING", mode="REQUIRED", description="Ingredient name"
                ),
            ],
        ),
        bigquery.SchemaField(
            "ont_form_routes",
            "RECORD",
            mode="REPEATED",
            description="Routes of administration",
            fields=[
                bigquery.SchemaField(
                    "route_code", "STRING", mode="REQUIRED", description="Route code"
                ),
                bigquery.SchemaField(
                    "route_name", "STRING", mode="REQUIRED", description="Route name"
                ),
            ],
        ),
        bigquery.SchemaField(
            "atcs",
            "RECORD",
            mode="REPEATED",
            description="ATC codes and names",
            fields=[
                bigquery.SchemaField("atc_code", "STRING", mode="REQUIRED", description="ATC code"),
                bigquery.SchemaField("atc_name", "STRING", mode="REQUIRED", description="ATC name"),
            ],
        ),
    ],
    cluster_fields=["vmp_code"],
)

VMP_UNIT_STANDARDISATION_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=VMP_UNIT_STANDARDISATION_TABLE_ID,
    description="Mapping table for VMPs that appear with multiple units in SCMD data, specifying the chosen standard unit and conversion details",
    schema=[
        bigquery.SchemaField(
            "vmp_code", 
            "STRING", 
            mode="REQUIRED",
            description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField(
            "vmp_name", 
            "STRING", 
            mode="REQUIRED",
            description="VMP name"
        ),
        bigquery.SchemaField(
            "scmd_units",
            "RECORD",
            mode="REPEATED",
            description="Units found in SCMD data for this VMP",
            fields=[
                bigquery.SchemaField(
                    "unit_id", 
                    "STRING", 
                    mode="REQUIRED",
                    description="Unit identifier"
                ),
                bigquery.SchemaField(
                    "unit_name", 
                    "STRING", 
                    mode="REQUIRED",
                    description="Unit name"
                ),
            ],
        ),
        bigquery.SchemaField(
            "chosen_unit_id", 
            "STRING", 
            mode="REQUIRED",
            description="The chosen standard unit identifier"
        ),
        bigquery.SchemaField(
            "chosen_unit_name", 
            "STRING", 
            mode="REQUIRED",
            description="The chosen standard unit name"
        ),
        bigquery.SchemaField(
            "conversion_logic", 
            "STRING", 
            mode="REQUIRED",
            description="Explanation of how to convert from other units to the chosen unit"
        ),
        bigquery.SchemaField(
            "conversion_factor",
            "FLOAT",
            mode="REQUIRED",
            description="Numerical factor to convert from the original unit to the chosen unit"
        ),
    ],
    cluster_fields=["vmp_code"],
)

VTM_INGREDIENTS_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=VTM_INGREDIENTS_TABLE_ID,
    description="Mapping between Virtual Therapeutic Moieties (VTM) and their ingredients from dm+d supplementary data",
    schema=[
        bigquery.SchemaField(
            "vtm_id", 
            "STRING", 
            mode="REQUIRED",
            description="Virtual Therapeutic Moiety (VTM) identifier"
        ),
        bigquery.SchemaField(
            "ingredient_id", 
            "STRING", 
            mode="REQUIRED",
            description="Ingredient identifier"
        ),
    ],
)

DMD_HISTORY_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=DMD_HISTORY_TABLE_ID,
    description="Historical mapping of dm+d identifiers including VTMs, VMPs, ingredients, suppliers, forms, routes, and units of measure",
    schema=[
        bigquery.SchemaField(
            "current_id", 
            "STRING", 
            mode="REQUIRED",
            description="Current identifier"
        ),
        bigquery.SchemaField(
            "previous_id", 
            "STRING", 
            mode="REQUIRED",
            description="Previous identifier"
        ),
        bigquery.SchemaField(
            "start_date", 
            "DATE", 
            mode="REQUIRED",
            description="Start date of the mapping"
        ),
        bigquery.SchemaField(
            "end_date", 
            "DATE", 
            description="End date of the mapping (if applicable)",
            mode="NULLABLE"
        ),
        bigquery.SchemaField(
            "entity_type",
            "STRING",
            mode="REQUIRED",
            description="Type of entity (VTM, VMP, ING, SUPP, FORM, ROUTE, UOM)"
        ),
    ],
)

DMD_UOM_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=DMD_UOM_TABLE_ID,
    description="Unit of measure reference data from dm+d containing current and previous codes and descriptions",
    schema=[
        bigquery.SchemaField(
            "uom_code", 
            "STRING", 
            mode="REQUIRED",
            description="Unit of measure code"
        ),
        bigquery.SchemaField(
            "uom_code_prev", 
            "STRING", 
            mode="NULLABLE",
            description="Previous unit of measure code"
        ),
        bigquery.SchemaField(
            "change_date", 
            "DATE", 
            mode="NULLABLE",
            description="Date of code change"
        ),
        bigquery.SchemaField(
            "description", 
            "STRING", 
            mode="NULLABLE",
            description="Description of the unit of measure"
        ),
    ],
    cluster_fields=["uom_code"],
)
