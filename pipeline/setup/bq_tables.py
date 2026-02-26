from dataclasses import dataclass
from typing import List
from google.cloud import bigquery

from pipeline.setup.config import (
    PROJECT_ID,
    DATASET_ID,
    ORGANISATION_TABLE_ID,
    SCMD_RAW_PROVISIONAL_TABLE_ID,
    SCMD_RAW_FINALISED_TABLE_ID,
    SCMD_PROCESSED_TABLE_ID,
    SCMD_DATA_STATUS_TABLE_ID,
    UNITS_CONVERSION_TABLE_ID,
    ORG_AE_STATUS_TABLE_ID,
    DMD_TABLE_ID,
    DMD_FULL_TABLE_ID,
    DMD_SUPP_TABLE_ID,
    VMP_ATC_MANUAL_TABLE_ID,
    WHO_ATC_TABLE_ID,
    WHO_DDD_TABLE_ID,
    ADM_ROUTE_MAPPING_TABLE_ID,
    DOSE_TABLE_ID,
    INGREDIENT_QUANTITY_TABLE_ID,
    DDD_QUANTITY_TABLE_ID,
    WHO_ROUTES_OF_ADMINISTRATION_TABLE_ID,
    VMP_TABLE_ID,
    VMP_UNIT_STANDARDISATION_TABLE_ID,
    VTM_INGREDIENTS_TABLE_ID,
    DMD_HISTORY_TABLE_ID,
    DMD_UOM_TABLE_ID,
    WHO_DDD_ALTERATIONS_TABLE_ID,
    WHO_ATC_ALTERATIONS_TABLE_ID,
    AWARE_VMP_MAPPING_PROCESSED_TABLE_ID,
    DDD_REFERS_TO_TABLE_ID,
    ERIC_TRUST_DATA_TABLE_ID,
    DOSE_CALCULATION_LOGIC_TABLE_ID,
    INGREDIENT_CALCULATION_LOGIC_TABLE_ID,
    DDD_CALCULATION_LOGIC_TABLE_ID,
    VMP_EXPRESSED_AS_TABLE_ID,
    DDD_ROUTE_COMMENTS_TABLE_ID,
    VMP_STRENGTH_OVERRIDES_TABLE_ID,
    WHO_DDD_COMBINED_PRODUCTS_TABLE_ID,
    DDD_COMBINED_PRODUCTS_LOGIC_TABLE_ID,
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
            "region_code",
            "STRING",
            mode="NULLABLE",
            description="Region code of the organisation",
        ),
        bigquery.SchemaField(
            "region",
            "STRING",
            mode="NULLABLE",
            description="Region of the organisation (through the ICB)",
        ),
        bigquery.SchemaField(
            "icb_code",
            "STRING",
            mode="NULLABLE",
            description="ICB code of the organisation",
        ),
        bigquery.SchemaField("icb", "STRING", mode="NULLABLE", description="ICB of the organisation"),
    ],
)

SCMD_RAW_PROVISIONAL_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=SCMD_RAW_PROVISIONAL_TABLE_ID,
    description="Raw SCMD provisional data",
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

SCMD_RAW_FINALISED_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=SCMD_RAW_FINALISED_TABLE_ID,
    description="Raw SCMD finalised data",
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
                bigquery.SchemaField(
                    "basis_of_strength_code",
                    "STRING",
                    mode="NULLABLE",
                    description="SNOMED code for the basis of strength substance",
                ),
                bigquery.SchemaField(
                    "basis_of_strength_name",
                    "STRING",
                    mode="NULLABLE",
                    description="Name of the basis of strength substance",
                ),
                bigquery.SchemaField(
                    "basis_of_strength_type",
                    "INTEGER",
                    mode="NULLABLE",
                    description="Type of basis of strength (1=Ingredient Substance, 2=Base Substance)",
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
        bigquery.SchemaField(
            "amps",
            "RECORD",
            mode="REPEATED",
            description="Actual Medicinal Products (AMPs) associated with this VMP",
            fields=[
                bigquery.SchemaField(
                    "amp_code", "STRING", mode="REQUIRED", description="AMP code"
                ),
                bigquery.SchemaField(
                    "amp_name", "STRING", mode="REQUIRED", description="AMP name"
                ),
                bigquery.SchemaField(
                    "avail_restrict", "STRING", mode="NULLABLE", description="Availability restriction description"
                ),
            ],
        ),
    ],
    cluster_fields=["vmp_code"],
)

DMD_FULL_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=DMD_FULL_TABLE_ID,
    description="Complete Dictionary of Medicines and Devices (dm+d) data - all VMPs, not filtered by SCMD",
    schema=[
        bigquery.SchemaField(
            "vmp_code", "STRING", mode="REQUIRED", description="Virtual Medicinal Product (VMP) code"
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
                bigquery.SchemaField(
                    "basis_of_strength_code",
                    "STRING",
                    mode="NULLABLE",
                    description="SNOMED code for the basis of strength substance",
                ),
                bigquery.SchemaField(
                    "basis_of_strength_name",
                    "STRING",
                    mode="NULLABLE",
                    description="Name of the basis of strength substance",
                ),
                bigquery.SchemaField(
                    "basis_of_strength_type",
                    "INTEGER",
                    mode="NULLABLE",
                    description="Type of basis of strength (1=Ingredient Substance, 2=Base Substance)",
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
        bigquery.SchemaField(
            "amps",
            "RECORD",
            mode="REPEATED",
            description="Actual Medicinal Products (AMPs) associated with this VMP",
            fields=[
                bigquery.SchemaField(
                    "amp_code", "STRING", mode="REQUIRED", description="AMP code"
                ),
                bigquery.SchemaField(
                    "amp_name", "STRING", mode="REQUIRED", description="AMP name"
                ),
                bigquery.SchemaField(
                    "avail_restrict", "STRING", mode="NULLABLE", description="Availability restriction description"
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

VMP_ATC_MANUAL_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=VMP_ATC_MANUAL_TABLE_ID,
    description="Manual VMP to ATC code mappings for products without dm+d ATC",
    schema=[
        bigquery.SchemaField(
            "vmp_code", "STRING", mode="REQUIRED", description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField(
            "vmp_name", "STRING", mode="NULLABLE", description="VMP name for identification"
        ),
        bigquery.SchemaField("atc_code", "STRING", mode="REQUIRED", description="ATC code"),
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
        bigquery.SchemaField(
            "dose_quantity", "FLOAT", mode="NULLABLE", description="Calculated number of doses (NULL when calculation not possible)"
        ),
        bigquery.SchemaField(
            "dose_unit", "STRING", mode="NULLABLE", description="Unit of measure for the dose (NULL when calculation not possible)"
        ),
        bigquery.SchemaField(
            "calculation_logic", "STRING", mode="REQUIRED", description="Logic used for dose calculation including unit conversion details"
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
            "calculation_logic",
            "STRING",
            mode="REQUIRED",
            description="Logic used for DDD calculation",
        ),
        bigquery.SchemaField(
            "ingredient_code",
            "STRING", 
            mode="NULLABLE",
            description="Ingredient code used for DDD calculation (only populated when calculation uses ingredient quantity)"
        ),
    ],
    partition_field="year_month",
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
        bigquery.SchemaField("bnf_code", "STRING", mode="NULLABLE", description="BNF code"),
        bigquery.SchemaField("df_ind", "STRING", mode="NULLABLE", description="Dose form indicator"),
        bigquery.SchemaField("udfs", "FLOAT", mode="NULLABLE", description="Unit dose form size"),
        bigquery.SchemaField("udfs_uom", "STRING", mode="NULLABLE", description="Unit dose form size unit of measure"),
        bigquery.SchemaField("udfs_basis_quantity", "FLOAT", mode="NULLABLE", description="Unit dose form size converted to basis units"),
        bigquery.SchemaField("udfs_basis_uom", "STRING", mode="NULLABLE", description="Basis unit for the unit dose form size"),
        bigquery.SchemaField("unit_dose_uom", "STRING", mode="NULLABLE", description="Unit dose unit of measure"),
        bigquery.SchemaField("unit_dose_basis_uom", "STRING", mode="NULLABLE", description="Basis unit for the unit dose"),
        bigquery.SchemaField("special", "BOOLEAN", mode="REQUIRED", description="Whether this VMP is a special (unlicensed medicine)"),
        bigquery.SchemaField("scmd_uom_id", "STRING", mode="NULLABLE", description="SCMD unit of measure identifier"),
        bigquery.SchemaField("scmd_uom_name", "STRING", mode="NULLABLE", description="SCMD unit of measure name"),
        bigquery.SchemaField("scmd_basis_uom_id", "STRING", mode="NULLABLE", description="SCMD basis unit identifier"),
        bigquery.SchemaField("scmd_basis_uom_name", "STRING", mode="NULLABLE", description="SCMD basis unit name"),
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
                bigquery.SchemaField(
                    "strnt_nmrtr_val", "FLOAT", mode="NULLABLE", description="Strength numerator value"
                ),
                bigquery.SchemaField(
                    "strnt_nmrtr_uom_name", "STRING", mode="NULLABLE", description="Strength numerator unit of measure"
                ),
                bigquery.SchemaField(
                    "strnt_nmrtr_basis_val", "FLOAT", mode="NULLABLE", description="Strength numerator value converted to basis units"
                ),
                bigquery.SchemaField(
                    "strnt_nmrtr_basis_uom", "STRING", mode="NULLABLE", description="Basis unit for strength numerator"
                ),
                bigquery.SchemaField(
                    "strnt_dnmtr_val", "FLOAT", mode="NULLABLE", description="Strength denominator value"
                ),
                bigquery.SchemaField(
                    "strnt_dnmtr_uom_name", "STRING", mode="NULLABLE", description="Strength denominator unit of measure"
                ),
                bigquery.SchemaField(
                    "strnt_dnmtr_basis_val", "FLOAT", mode="NULLABLE", description="Strength denominator value converted to basis units"
                ),
                bigquery.SchemaField(
                    "strnt_dnmtr_basis_uom", "STRING", mode="NULLABLE", description="Basis unit for strength denominator"
                ),
                bigquery.SchemaField(
                    "basis_of_strength_type", "INTEGER", mode="NULLABLE", description="Type of basis of strength (1=Ingredient Substance, 2=Base Substance)"
                ),
                bigquery.SchemaField(
                    "basis_of_strength_name", "STRING", mode="NULLABLE", description="Name of the basis of strength substance"
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
        bigquery.SchemaField(
            "amps",
            "RECORD",
            mode="REPEATED",
            description="Actual Medicinal Products (AMPs) associated with this VMP",
            fields=[
                bigquery.SchemaField(
                    "amp_code", "STRING", mode="REQUIRED", description="AMP code"
                ),
                bigquery.SchemaField(
                    "amp_name", "STRING", mode="REQUIRED", description="AMP name"
                ),
                bigquery.SchemaField(
                    "avail_restrict", "STRING", mode="NULLABLE", description="Availability restriction description"
                ),
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

WHO_DDD_ALTERATIONS_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=WHO_DDD_ALTERATIONS_TABLE_ID,
    description="Historical alterations to DDD values from WHO, tracking changes over time. Source: https://atcddd.fhi.no/atc_ddd_alterations__cumulative/ddd_alterations/",
    schema=[
        bigquery.SchemaField(
            "substance", 
            "STRING", 
            mode="REQUIRED",
            description="Name of the substance"
        ),
        bigquery.SchemaField(
            "previous_ddd", 
            "FLOAT", 
            mode="NULLABLE",
            description="Previous DDD value"
        ),
        bigquery.SchemaField(
            "previous_ddd_unit", 
            "STRING", 
            mode="NULLABLE",
            description="Unit of the previous DDD"
        ),
        bigquery.SchemaField(
            "previous_route", 
            "STRING", 
            mode="NULLABLE",
            description="Previous route of administration"
        ),
        bigquery.SchemaField(
            "new_ddd", 
            "FLOAT", 
            mode="NULLABLE",
            description="New DDD value"
        ),
        bigquery.SchemaField(
            "new_ddd_unit", 
            "STRING", 
            mode="NULLABLE",
            description="Unit of the new DDD"
        ),
        bigquery.SchemaField(
            "new_route", 
            "STRING", 
            mode="NULLABLE",
            description="New route of administration"
        ),
        bigquery.SchemaField(
            "atc_code", 
            "STRING",
            mode="REQUIRED",
            description="Present ATC code for the substance"
        ),
        bigquery.SchemaField(
            "year_changed", 
            "INTEGER", 
            mode="REQUIRED",
            description="Year when the alteration was implemented"
        ),
        bigquery.SchemaField(
            "comment", 
            "STRING", 
            mode="NULLABLE",
            description="Automatically generated comment describing the type of change"
        ),
        bigquery.SchemaField(
            "alterations_comment", 
            "STRING", 
            mode="NULLABLE",
            description="Manually added notes or comments from the Excel file"
        ),
    ],
    cluster_fields=["atc_code", "year_changed"]
)

WHO_ATC_ALTERATIONS_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=WHO_ATC_ALTERATIONS_TABLE_ID,
    description="Historical alterations to ATC codes from WHO, tracking changes over time. Source: https://atcddd.fhi.no/atc_ddd_alterations__cumulative/atc_alterations/",
    schema=[
        bigquery.SchemaField(
            "substance", 
            "STRING", 
            mode="REQUIRED",
            description="Name of the substance"
        ),
        bigquery.SchemaField(
            "previous_atc_code", 
            "STRING", 
            mode="NULLABLE",
            description="Previous ATC code"
        ),
        bigquery.SchemaField(
            "new_atc_code", 
            "STRING", 
            mode="REQUIRED",
            description="New ATC code"
        ),
        bigquery.SchemaField(
            "year_changed", 
            "INTEGER", 
            mode="REQUIRED",
            description="Year when the alteration was implemented"
        ),
        bigquery.SchemaField(
            "comment", 
            "STRING", 
            mode="NULLABLE",
            description="Automatically generated comment describing the type of change"
        ),
        bigquery.SchemaField(
            "alterations_comment", 
            "STRING", 
            mode="NULLABLE",
            description="Manually added notes or comments from the Excel file"
        ),
    ],
    cluster_fields=["previous_atc_code", "new_atc_code", "year_changed"]
)

AWARE_VMP_MAPPING_PROCESSED_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=AWARE_VMP_MAPPING_PROCESSED_TABLE_ID,
    description="AWaRe VMP mapping with historical dm+d code mapping applied and restricted to VMPs present in processed SCMD data",
    schema=[
        bigquery.SchemaField(
            "Antibiotic", 
            "STRING", 
            mode="NULLABLE",
            description="Name of the antibiotic from AWaRe classification"
        ),
        bigquery.SchemaField(
            "vtm_nm", 
            "STRING", 
            mode="NULLABLE",
            description="Virtual Therapeutic Moiety (VTM) name from dm+d (updated to current name if historical mapping applied)"
        ),
        bigquery.SchemaField(
            "vtm_id", 
            "INTEGER", 
            mode="NULLABLE",
            description="Virtual Therapeutic Moiety (VTM) ID from dm+d (updated to current ID if historical mapping applied)"
        ),
        bigquery.SchemaField(
            "vmp_nm", 
            "STRING", 
            mode="NULLABLE",
            description="Virtual Medicinal Product (VMP) name from dm+d (updated to current name if historical mapping applied)"
        ),
        bigquery.SchemaField(
            "vmp_id", 
            "INTEGER", 
            mode="NULLABLE",
            description="Virtual Medicinal Product (VMP) ID from dm+d (updated to current ID if historical mapping applied)"
        ),
        bigquery.SchemaField(
            "aware_2019", 
            "STRING", 
            mode="NULLABLE",
            description="2019 AWaRe category (Access, Watch, Reserve, Other)"
        ),
        bigquery.SchemaField(
            "aware_2024", 
            "STRING", 
            mode="NULLABLE",
            description="2024 AWaRe category (Access, Watch, Reserve, Other)"
        ),
        bigquery.SchemaField(
            "vtm_id_updated", 
            "BOOLEAN", 
            mode="NULLABLE",
            description="Flag indicating if the VTM ID was updated through historical mapping"
        ),
        bigquery.SchemaField(
            "vmp_id_updated", 
            "BOOLEAN", 
            mode="NULLABLE",
            description="Flag indicating if the VMP ID was updated through historical mapping"
        ),
    ],
)

DDD_REFERS_TO_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=DDD_REFERS_TO_TABLE_ID,
    description="For VMPs where there are multiple linked DDDs for the same route, identifies the ingredient quantity that should be used.",
    schema=[
        bigquery.SchemaField(
            "ddd_comment", 
            "STRING",
            mode="REQUIRED",
            description="DDD comment"
        ),
        bigquery.SchemaField(
            "refers_to_ingredient", 
            "STRING",
            mode="REQUIRED",
            description="The ingredient extracted from the DDD comment"
        ),
        bigquery.SchemaField(
            "dmd_ingredients",
            "RECORD",
            mode="REPEATED",
            description="Matching dm+d ingredients",
            fields=[
                bigquery.SchemaField(
                    "dmd_ingredient_code", 
                    "STRING",
                    mode="REQUIRED",
                    description="The dm+d ingredient code"
                ),
                bigquery.SchemaField(
                    "dmd_ingredient_name", 
                    "STRING",
                    mode="REQUIRED",
                    description="The dm+d ingredient name"
                ),
            ],
        ),
    ],
)

ERIC_TRUST_DATA_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=ERIC_TRUST_DATA_TABLE_ID,
    description="Estates Returns Information Collection (ERIC) data for NHS Trusts containing estates, facilities, and infrastructure information",
    schema=[
        bigquery.SchemaField(
            "trust_code", "STRING", mode="REQUIRED", description="Trust ODS code"
        ),
        bigquery.SchemaField(
            "trust_name", "STRING", mode="REQUIRED", description="Name of the NHS Trust"
        ),
        bigquery.SchemaField(
            "trust_type", "STRING", mode="NULLABLE", description="Type of trust (e.g., ACUTE - TEACHING, COMMUNITY)"
        ),
        bigquery.SchemaField(
            "data_year", "STRING", mode="REQUIRED", description="Year of the ERIC data (e.g., 2023_24)"
        ),
    ],
    cluster_fields=["trust_code"],
)


DOSE_CALCULATION_LOGIC_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=DOSE_CALCULATION_LOGIC_TABLE_ID,
    description="Table to check the dose calculation logic for each VMP, showing which calculation methods are available",
    schema=[
        bigquery.SchemaField(
            "vmp_code", "STRING", mode="REQUIRED", description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField(
            "vmp_name", "STRING", mode="REQUIRED", description="VMP name"
        ),
        bigquery.SchemaField(
            "df_ind", "STRING", mode="NULLABLE", description="Dose form indicator"
        ),
        bigquery.SchemaField(
            "can_calculate_dose", "BOOLEAN", mode="REQUIRED", description="Whether dose calculation is possible for this VMP"
        ),
        bigquery.SchemaField(
            "dose_calculation_logic", "STRING", mode="REQUIRED", description="Explanation of the dose calculation logic or reason why calculation is not possible"
        ),
        bigquery.SchemaField(
            "udfs_basis_quantity", "FLOAT", mode="NULLABLE", description="Unit dose form size converted to basis units (only populated when calculation is possible)"
        ),
        bigquery.SchemaField(
            "udfs_basis_uom", "STRING", mode="NULLABLE", description="Basis unit for the unit dose form size (only populated when calculation is possible)"
        ),
        bigquery.SchemaField(
            "unit_dose_uom", "STRING", mode="NULLABLE", description="Unit dose unit of measure (only populated when calculation is possible)"
        ),
        bigquery.SchemaField(
            "unit_dose_basis_uom", "STRING", mode="NULLABLE", description="Basis unit for the unit dose (only populated when calculation is possible)"
        ),
    ],
    cluster_fields=["vmp_code"],
)

INGREDIENT_CALCULATION_LOGIC_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=INGREDIENT_CALCULATION_LOGIC_TABLE_ID,
    description="Table to check the ingredient calculation logic for each VMP, showing which calculation methods are available for each ingredient",
    schema=[
        bigquery.SchemaField(
            "vmp_code", "STRING", mode="REQUIRED", description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField(
            "vmp_name", "STRING", mode="REQUIRED", description="VMP name"
        ),
        bigquery.SchemaField(
            "ingredients",
            "RECORD",
            mode="REPEATED",
            description="Ingredient calculation information",
            fields=[
                bigquery.SchemaField(
                    "ingredient_code", "STRING", mode="REQUIRED", description="Ingredient code"
                ),
                bigquery.SchemaField(
                    "ingredient_name", "STRING", mode="REQUIRED", description="Ingredient name"
                ),
                bigquery.SchemaField(
                    "can_calculate_ingredient", "BOOLEAN", mode="REQUIRED", description="Whether ingredient calculation is possible for this ingredient"
                ),
                bigquery.SchemaField(
                    "ingredient_calculation_logic", "STRING", mode="REQUIRED", description="Explanation of the ingredient calculation logic or reason why calculation is not possible"
                ),
                bigquery.SchemaField(
                    "strength_numerator_value", "FLOAT", mode="NULLABLE", description="Strength numerator value (only populated when calculation is possible)"
                ),
                bigquery.SchemaField(
                    "strength_numerator_unit", "STRING", mode="NULLABLE", description="Strength numerator unit (only populated when calculation is possible)"
                ),
                bigquery.SchemaField(
                    "numerator_basis_value", "FLOAT", mode="NULLABLE", description="Strength numerator value converted to basis units (only populated when calculation is possible)"
                ),
                bigquery.SchemaField(
                    "numerator_basis_unit", "STRING", mode="NULLABLE", description="Basis unit for strength numerator (only populated when calculation is possible)"
                ),
                bigquery.SchemaField(
                    "strength_denominator_value", "FLOAT", mode="NULLABLE", description="Strength denominator value (only populated when calculation is possible and denominator exists)"
                ),
                bigquery.SchemaField(
                    "strength_denominator_unit", "STRING", mode="NULLABLE", description="Strength denominator unit (only populated when calculation is possible and denominator exists)"
                ),
                bigquery.SchemaField(
                    "denominator_basis_value", "FLOAT", mode="NULLABLE", description="Strength denominator value converted to basis units (only populated when calculation is possible and denominator exists)"
                ),
                bigquery.SchemaField(
                    "denominator_basis_unit", "STRING", mode="NULLABLE", description="Basis unit for strength denominator (only populated when calculation is possible and denominator exists)"
                ),
            ],
        ),
    ],
    cluster_fields=["vmp_code"],
)

DDD_CALCULATION_LOGIC_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=DDD_CALCULATION_LOGIC_TABLE_ID,
    description="Table to check the DDD calculation logic for each VMP, showing which calculation methods are available and the selected DDD information",
    schema=[
        bigquery.SchemaField(
            "vmp_code", "STRING", mode="REQUIRED", description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField(
            "vmp_name", "STRING", mode="REQUIRED", description="VMP name"
        ),
        bigquery.SchemaField(
            "can_calculate_ddd", "BOOLEAN", mode="REQUIRED", description="Whether DDD calculation is possible for this VMP"
        ),
        bigquery.SchemaField(
            "ddd_calculation_logic", "STRING", mode="REQUIRED", description="Explanation of the DDD calculation logic or reason why calculation is not possible"
        ),
        bigquery.SchemaField(
            "selected_ddd_value", "FLOAT", mode="NULLABLE", description="The DDD value that was selected for calculations (only populated when calculation is possible)"
        ),
        bigquery.SchemaField(
            "selected_ddd_unit", "STRING", mode="NULLABLE", description="The unit of the selected DDD value (only populated when calculation is possible)"
        ),
        bigquery.SchemaField(
            "selected_ddd_basis_unit", "STRING", mode="NULLABLE", description="The basis unit for the selected DDD value (only populated when calculation is possible)"
        ),
        bigquery.SchemaField(
            "selected_ddd_route_code", "STRING", mode="NULLABLE", description="The route code of the selected DDD (only populated when calculation is possible)"
        ),
        bigquery.SchemaField(
            "scmd_uom_id", "STRING", mode="NULLABLE", description="Unit of measure identifier from SCMD data"
        ),
        bigquery.SchemaField(
            "scmd_uom_name", "STRING", mode="NULLABLE", description="Unit of measure name from SCMD data"
        ),
        bigquery.SchemaField(
            "scmd_basis_uom_id", "STRING", mode="NULLABLE", description="Normalised basis unit identifier from SCMD data"
        ),
        bigquery.SchemaField(
            "scmd_basis_uom_name", "STRING", mode="NULLABLE", description="Normalised basis unit name from SCMD data"
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
            "routes",
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
                    "who_route_code", "STRING", mode="REQUIRED", description="WHO route code"
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
                    "ddd_route_code", "STRING", mode="REQUIRED", description="Route of administration code"
                ),
                bigquery.SchemaField(
                    "ddd_comment", "STRING", mode="NULLABLE", description="Comment"
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
                    "ingredient_basis_unit", "STRING", mode="REQUIRED", description="Basis unit for the ingredient"
                ),
            ],
        ),
        bigquery.SchemaField(
            "selected_ddd_comment", "STRING", mode="NULLABLE", description="Comment from the selected DDD (only populated for VMPs with DDD comments)"
        ),
        bigquery.SchemaField(
            "refers_to_ingredient", "STRING", mode="NULLABLE", description="The ingredient that the DDD refers to (only populated when selected_ddd_comment contains 'refers to')"
        ),    
        bigquery.SchemaField(
            "expressed_as_strnt_nmrtr", "FLOAT", mode="NULLABLE", description="Expressed as strength numerator value (only populated for VMPs with expressed as DDD comments)"
        ),
        bigquery.SchemaField(
            "expressed_as_strnt_nmrtr_uom", "STRING", mode="NULLABLE", description="Expressed as strength numerator unit of measure identifier (only populated for VMPs with expressed as DDD comments)"
        ),
        bigquery.SchemaField(
            "expressed_as_strnt_nmrtr_uom_name", "STRING", mode="NULLABLE", description="Expressed as strength numerator unit of measure name (only populated for VMPs with expressed as DDD comments)"
        ),
        bigquery.SchemaField(
            "expressed_as_strnt_dnmtr", "FLOAT", mode="NULLABLE", description="Expressed as strength denominator value (only populated for VMPs with expressed as DDD comments when denominator exists)"
        ),
        bigquery.SchemaField(
            "expressed_as_strnt_dnmtr_uom", "STRING", mode="NULLABLE", description="Expressed as strength denominator unit of measure identifier (only populated for VMPs with expressed as DDD comments when denominator exists)"
        ),
        bigquery.SchemaField(
            "expressed_as_strnt_dnmtr_uom_name", "STRING", mode="NULLABLE", description="Expressed as strength denominator unit of measure name (only populated for VMPs with expressed as DDD comments when denominator exists)"
        ),
        bigquery.SchemaField(
            "expressed_as_strnt_dnmtr_basis_val", "FLOAT", mode="NULLABLE", description="Expressed as strength denominator value converted to basis units (only populated for VMPs with expressed as DDD comments when denominator exists)"
        ),
        bigquery.SchemaField(
            "expressed_as_strnt_dnmtr_basis_uom", "STRING", mode="NULLABLE", description="Expressed as strength denominator basis unit (only populated for VMPs with expressed as DDD comments when denominator exists)"
        ),
        bigquery.SchemaField(
            "expressed_as_ingredient_code", "STRING", mode="NULLABLE", description="Expressed as ingredient code (only populated for VMPs with expressed as DDD comments)"
        ),
        bigquery.SchemaField(
            "expressed_as_ingredient_name", "STRING", mode="NULLABLE", description="Expressed as ingredient name (only populated for VMPs with expressed as DDD comments)"
        ),
        bigquery.SchemaField(
            "override_strnt_nmrtr_val", "FLOAT", mode="NULLABLE", description="Override strength numerator (from vmp_strength_overrides when dm+d strength is wrong)"
        ),
        bigquery.SchemaField(
            "override_strnt_nmrtr_uom", "STRING", mode="NULLABLE", description="Override strength numerator unit"
        ),
        bigquery.SchemaField(
            "override_strnt_dnmtr_val", "FLOAT", mode="NULLABLE", description="Override strength denominator"
        ),
        bigquery.SchemaField(
            "override_strnt_dnmtr_uom", "STRING", mode="NULLABLE", description="Override strength denominator unit"
        ),
        bigquery.SchemaField(
            "override_comments", "STRING", mode="NULLABLE", description="Reason for strength override (from vmp_strength_overrides)"
        ),
    ],
    cluster_fields=["vmp_code"],
)


VMP_EXPRESSED_AS_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=VMP_EXPRESSED_AS_TABLE_ID,
    description="For VMPs with a DDD comment indicating that the DDD is expressed as strength of something other than the active ingredient, this table contains the details of the expressed as strength",
    schema=[
        bigquery.SchemaField(
            "vmp_id", 
            "STRING",
            mode="REQUIRED",
            description="VMP ID"
        ),
        bigquery.SchemaField(
            "vmp_name", 
            "STRING",
            mode="REQUIRED",
            description="VMP name"
        ),
        bigquery.SchemaField(
            "ddd_comment", 
            "STRING",
            mode="REQUIRED",
            description="DDD comment, which will indicate what the DDD is expressed as. E.g. Expressed as levofolinic acid."
        ),
        bigquery.SchemaField(
            "expressed_as_strnt_nmrtr", 
                        "FLOAT",
                        mode="REQUIRED",
                        description="Expressed as strength numerator. This is a manually specified value, identified from external sources for product characteristics"
        ),
        bigquery.SchemaField(
            "expressed_as_strnt_nmrtr_uom", 
            "STRING",
            mode="REQUIRED",
            description="Expressed as strength numerator unit of measure"
        ),
        bigquery.SchemaField(
            "expressed_as_strnt_nmrtr_uom_name", 
            "STRING",
            mode="REQUIRED",
            description="Expressed as strength numerator unit of measure name"
        ),
        bigquery.SchemaField(
            "expressed_as_strnt_dnmtr", 
            "FLOAT",
            mode="NULLABLE",
            description="Expressed as strength denominator value from dm+d ingredient (only populated when denominator exists)"
        ),
        bigquery.SchemaField(
            "expressed_as_strnt_dnmtr_uom", 
            "STRING",
            mode="NULLABLE",
            description="Expressed as strength denominator unit of measure identifier (only populated when denominator exists)"
        ),
        bigquery.SchemaField(
            "expressed_as_strnt_dnmtr_uom_name", 
            "STRING",
            mode="NULLABLE",
            description="Expressed as strength denominator unit of measure name (only populated when denominator exists)"
        ),
        bigquery.SchemaField(
            "expressed_as_strnt_dnmtr_basis_val", 
            "FLOAT",
            mode="NULLABLE",
            description="Expressed as strength denominator value converted to basis units (only populated when denominator exists)"
        ),
        bigquery.SchemaField(
            "expressed_as_strnt_dnmtr_basis_uom", 
            "STRING",
            mode="NULLABLE",
            description="Basis unit for expressed as strength denominator (only populated when denominator exists)"
        ),
        bigquery.SchemaField(
            "ingredient_code", 
            "STRING",
            mode="REQUIRED",
            description="Ingredient code for the dm+d ingredient that the expressed as strength refers to"
        ),
        bigquery.SchemaField(
            "ingredient_name",
            "STRING",
            mode="REQUIRED",
            description="Name of the dm+d ingredient that the expressed as strength refers to"
        ),
    ],
)

WHO_DDD_COMBINED_PRODUCTS_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=WHO_DDD_COMBINED_PRODUCTS_TABLE_ID,
    description="DDD information for combined products from the WHO. Source: https://atcddd.fhi.no/ddd/list_of_ddds_combined_products/",
    schema=[
        bigquery.SchemaField(
            "atc_code", 
            "STRING",
            mode="REQUIRED",
            description="ATC code for the combined product"
        ),
        bigquery.SchemaField(
            "brand_name", 
            "STRING",
            mode="NULLABLE",
            description="Brand name of the product"
        ),
        bigquery.SchemaField(
            "dosage_form", 
            "STRING",
            mode="NULLABLE",
            description="Dosage form of the product given in the WHO combined product list (e.g., tab, caps, inhal powd)"
        ),
        bigquery.SchemaField(
            "form", 
            "STRING",
            mode="NULLABLE",
            description="Mapped dm+d standard form from dosage form (e.g., tablet, capsule, solutioninjection)"
        ),
        bigquery.SchemaField(
            "route", 
            "STRING",
            mode="NULLABLE",
            description="Mapped dm+d standard route from dosage form (e.g., oral, inhalation, nasal)"
        ),
        bigquery.SchemaField(
            "active_ingredients", 
            "RECORD",
            mode="REPEATED",
            description="Active ingredients per unit dose (UD) from the WHO combined product list",
            fields=[
                bigquery.SchemaField(
                    "ingredient", "STRING", mode="REQUIRED", description="Ingredient name"
                ),
                bigquery.SchemaField(
                    "numerator_quantity", "FLOAT", mode="NULLABLE", description="Numerator quantity of the ingredient"
                ),
                bigquery.SchemaField(
                    "numerator_unit", "STRING", mode="NULLABLE", description="Unit of measure for the numerator quantity"
                ),
                bigquery.SchemaField(
                    "denominator_quantity", "FLOAT", mode="NULLABLE", description="Denominator quantity (e.g., 25 from '178.5 mg/ 25 ml')"
                ),
                bigquery.SchemaField(
                    "denominator_unit", "STRING", mode="NULLABLE", description="Unit of measure for the denominator quantity"
                ),
            ],
        ),
        bigquery.SchemaField(
            "ddd_comb", 
            "STRING",
            mode="NULLABLE",
            description="DDD for the combined product expressed as unit doses (e.g., '2 UD (=2 tab)')"
        ),
        bigquery.SchemaField(
            "ddd_ud_value", 
            "FLOAT",
            mode="NULLABLE",
            description="Numeric value of the DDD in unit doses extracted from ddd_comb"
        ),
        bigquery.SchemaField(
            "ddd_converted_value", 
            "FLOAT",
            mode="NULLABLE",
            description="Numeric value extracted from inside brackets (e.g., 2 from '(=2 tab)')"
        ),
        bigquery.SchemaField(
            "ddd_converted_unit", 
            "STRING",
            mode="NULLABLE",
            description="Unit converted to dm+d standard format (e.g., 'tablet' from 'tab', 'capsule' from 'caps')"
        ),
    ],
    cluster_fields=["atc_code"]
)

DDD_COMBINED_PRODUCTS_LOGIC_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=DDD_COMBINED_PRODUCTS_LOGIC_TABLE_ID,
    description="VMPs where standard DDD cannot be calculated, matched to WHO combined product DDDs with an indication of whether a combined DDD could not be chosen",
    schema=[
        bigquery.SchemaField("vmp_code", "STRING", mode="REQUIRED", description="Virtual Medicinal Product (VMP) code"),
        bigquery.SchemaField("vmp_name", "STRING", mode="REQUIRED", description="VMP name"),
        bigquery.SchemaField("atc_code", "STRING", mode="REQUIRED", description="ATC code"),
        bigquery.SchemaField("atc_name", "STRING", mode="REQUIRED", description="ATC name"),
        bigquery.SchemaField("form", "STRING", mode="NULLABLE", description="Mapped dm+d form from WHO combined product"),
        bigquery.SchemaField("route", "STRING", mode="NULLABLE", description="Mapped dm+d route from WHO combined product"),
        bigquery.SchemaField(
            "active_ingredients",
            "RECORD",
            mode="REPEATED",
            description="Active ingredients per unit dose from WHO combined product list",
            fields=[
                bigquery.SchemaField("ingredient", "STRING", mode="REQUIRED", description="Ingredient name"),
                bigquery.SchemaField("numerator_quantity", "FLOAT", mode="NULLABLE", description="Numerator quantity"),
                bigquery.SchemaField("numerator_unit", "STRING", mode="NULLABLE", description="Numerator unit"),
                bigquery.SchemaField("denominator_quantity", "FLOAT", mode="NULLABLE", description="Denominator quantity"),
                bigquery.SchemaField("denominator_unit", "STRING", mode="NULLABLE", description="Denominator unit"),
            ],
        ),
        bigquery.SchemaField("ddd_ud_value", "FLOAT", mode="NULLABLE", description="DDD in unit doses"),
        bigquery.SchemaField("ddd_converted_value", "FLOAT", mode="NULLABLE", description="DDD value from combined product (e.g. from brackets)"),
        bigquery.SchemaField("ddd_converted_unit", "STRING", mode="NULLABLE", description="Unit of ddd_converted_value"),
        bigquery.SchemaField("ud_unit_conversion", "FLOAT", mode="NULLABLE", description="Conversion factor: ddd_converted_value / ddd_ud_value"),
        bigquery.SchemaField(
            "active_ingredients_per_unit",
            "RECORD",
            mode="REPEATED",
            description="Active ingredients with quantities divided by ud_unit_conversion (per converted unit)",
            fields=[
                bigquery.SchemaField("ingredient", "STRING", mode="REQUIRED", description="Ingredient name"),
                bigquery.SchemaField("numerator_quantity", "FLOAT", mode="NULLABLE", description="Numerator quantity per converted unit"),
                bigquery.SchemaField("numerator_unit", "STRING", mode="NULLABLE", description="Numerator unit"),
                bigquery.SchemaField("denominator_quantity", "FLOAT", mode="NULLABLE", description="Denominator quantity per converted unit"),
                bigquery.SchemaField("denominator_unit", "STRING", mode="NULLABLE", description="Denominator unit"),
            ],
        ),
        bigquery.SchemaField(
            "ingredients",
            "RECORD",
            mode="REPEATED",
            description="VMP ingredients with strengths and units",
            fields=[
                bigquery.SchemaField("ingredient_code", "STRING", mode="REQUIRED", description="Ingredient code"),
                bigquery.SchemaField("ingredient_name", "STRING", mode="REQUIRED", description="Ingredient name"),
                bigquery.SchemaField("strnt_nmrtr_val", "FLOAT", mode="NULLABLE", description="Strength numerator value"),
                bigquery.SchemaField("strnt_nmrtr_uom_name", "STRING", mode="NULLABLE", description="Strength numerator unit"),
                bigquery.SchemaField("strnt_nmrtr_basis_val", "FLOAT", mode="NULLABLE", description="Strength numerator in basis units"),
                bigquery.SchemaField("strnt_nmrtr_basis_uom", "STRING", mode="NULLABLE", description="Basis unit for numerator"),
                bigquery.SchemaField("strnt_dnmtr_val", "FLOAT", mode="NULLABLE", description="Strength denominator value"),
                bigquery.SchemaField("strnt_dnmtr_uom_name", "STRING", mode="NULLABLE", description="Strength denominator unit"),
                bigquery.SchemaField("strnt_dnmtr_basis_val", "FLOAT", mode="NULLABLE", description="Strength denominator in basis units"),
                bigquery.SchemaField("strnt_dnmtr_basis_uom", "STRING", mode="NULLABLE", description="Basis unit for denominator"),
                bigquery.SchemaField("basis_of_strength_type", "INTEGER", mode="NULLABLE", description="Basis of strength type"),
                bigquery.SchemaField("basis_of_strength_name", "STRING", mode="NULLABLE", description="Basis of strength name"),
            ],
        ),
        bigquery.SchemaField(
            "ont_form_routes",
            "RECORD",
            mode="REPEATED",
            description="VMP routes of administration (form.route)",
            fields=[
                bigquery.SchemaField("route_code", "STRING", mode="REQUIRED", description="Route code"),
                bigquery.SchemaField("route_name", "STRING", mode="REQUIRED", description="Route name (form.route)"),
            ],
        ),
        bigquery.SchemaField("scmd_uom_id", "STRING", mode="NULLABLE", description="SCMD unit of measure identifier"),
        bigquery.SchemaField("scmd_uom_name", "STRING", mode="NULLABLE", description="SCMD unit of measure name"),
        bigquery.SchemaField("scmd_basis_uom_id", "STRING", mode="NULLABLE", description="SCMD basis unit identifier"),
        bigquery.SchemaField("scmd_basis_uom_name", "STRING", mode="NULLABLE", description="SCMD basis unit name"),
        bigquery.SchemaField("ddd_converted_basis_unit", "STRING", mode="NULLABLE", description="Basis unit for ddd_converted_unit"),
        bigquery.SchemaField("scmd_basis_unit", "STRING", mode="NULLABLE", description="Basis unit for SCMD unit"),
        bigquery.SchemaField("strength_ratio", "FLOAT", mode="NULLABLE", description="VMP strength / WHO strength; 1.0 = exact match, other = proportional match"),
        bigquery.SchemaField("why_ddd_not_chosen", "STRING", mode="NULLABLE", description="Reasons why a combined DDD could not be chosen (NULL when chosen)"),
        bigquery.SchemaField("chosen_ddd_value", "FLOAT", mode="NULLABLE", description="Chosen DDD value in basis units when all checks pass"),
        bigquery.SchemaField("chosen_ddd_unit", "STRING", mode="NULLABLE", description="Chosen DDD unit (basis) when all checks pass"),
    ],
    cluster_fields=["vmp_code"],
)



DDD_ROUTE_COMMENTS_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=DDD_ROUTE_COMMENTS_TABLE_ID,
    description="VMP DDD information including code, name, DDD value, unit, and comment",
    schema=[
        bigquery.SchemaField(
            "vmp_code", "STRING", mode="REQUIRED", description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField(
            "vmp_name", "STRING", mode="REQUIRED", description="VMP name"
        ),
        bigquery.SchemaField(
            "atc_code", "STRING", mode="NULLABLE", description="ATC code"
        ),
        bigquery.SchemaField(
            "ddd", "FLOAT", mode="NULLABLE", description="DDD value"
        ),
        bigquery.SchemaField(
            "ddd_uom", "STRING", mode="NULLABLE", description="DDD unit of measure"
        ),
        bigquery.SchemaField(
            "ddd_comment", "STRING", mode="NULLABLE", description="DDD comment"
        ),
        bigquery.SchemaField(
            "strength_numerator", "FLOAT", mode="NULLABLE", description="Strength numerator value from VMP ingredient"
        ),
        bigquery.SchemaField(
            "strength_denominator", "FLOAT", mode="NULLABLE", description="Strength denominator value from VMP ingredient"
        ),
    ],
    cluster_fields=["vmp_code"],
)

VMP_STRENGTH_OVERRIDES_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=VMP_STRENGTH_OVERRIDES_TABLE_ID,
    description="VMPs where dm+d strength is incorrect; override values used for DDD calculation",
    schema=[
        bigquery.SchemaField(
            "vmp_code", "STRING", mode="REQUIRED", description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField(
            "strnt_nmrtr_val", "FLOAT", mode="NULLABLE", description="Override strength numerator value"
        ),
        bigquery.SchemaField(
            "strnt_dnmtr_val", "FLOAT", mode="NULLABLE", description="Override strength denominator value"
        ),
        bigquery.SchemaField(
            "strnt_nmrtr_uom", "STRING", mode="NULLABLE", description="Strength numerator unit (e.g. microgram)"
        ),
        bigquery.SchemaField(
            "strnt_dnmtr_uom", "STRING", mode="NULLABLE", description="Strength denominator unit (e.g. hour)"
        ),
        bigquery.SchemaField(
            "comments", "STRING", mode="NULLABLE", description="Reason for override (e.g. why dm+d strength is wrong)"
        ),
    ],
    cluster_fields=["vmp_code"],
)