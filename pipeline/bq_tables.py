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
            "ods_code", "STRING", description="ODS code of the organisation"
        ),
        bigquery.SchemaField(
            "ods_name", "STRING", description="Name of the organisation"
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
            description="Legal closed date of the organisation",
        ),
        bigquery.SchemaField(
            "operational_closed_date",
            "DATE",
            description="Operational closed date of the organisation",
        ),
        bigquery.SchemaField(
            "legal_open_date", "DATE", description="Legal open date of the organisation"
        ),
        bigquery.SchemaField(
            "operational_open_date",
            "DATE",
            description="Operational open date of the organisation",
        ),
        bigquery.SchemaField(
            "postcode", "STRING", description="Postcode of the organisation"
        ),
        bigquery.SchemaField(
            "region",
            "STRING",
            description="Region of the organisation (through the ICB)",
        ),
        bigquery.SchemaField("icb", "STRING", description="ICB of the organisation"),
    ],
)

SCMD_RAW_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=SCMD_RAW_TABLE_ID,
    description="Raw SCMD data",
    schema=[
        bigquery.SchemaField(
            "year_month", "DATE", description="Year and month of the data"
        ),
        bigquery.SchemaField("ods_code", "STRING", description="ODS code"),
        bigquery.SchemaField(
            "vmp_snomed_code",
            "STRING",
            description="SNOMED code indicating VMP from dm+d",
        ),
        bigquery.SchemaField(
            "vmp_product_name", "STRING", description="Product name from dm+d"
        ),
        bigquery.SchemaField(
            "unit_of_measure_identifier",
            "STRING",
            description="Identifier for the unit of measure from dm+d",
        ),
        bigquery.SchemaField(
            "unit_of_measure_name",
            "STRING",
            description="Name of the unit of measure from dm+d",
        ),
        bigquery.SchemaField(
            "total_quantity_in_vmp_unit",
            "FLOAT",
            description="Total quantity in the unit of measure",
        ),
        bigquery.SchemaField("indicative_cost", "FLOAT", description="Indicative cost"),
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
            "year_month", "DATE", description="Year and month of the data"
        ),
        bigquery.SchemaField("ods_code", "STRING", description="ODS code"),
        bigquery.SchemaField(
            "vmp_code", "STRING", description="SNOMED code for the VMP from dm+d"
        ),
        bigquery.SchemaField(
            "vmp_name", "STRING", description="Product name from dm+d"
        ),
        bigquery.SchemaField(
            "uom_id",
            "STRING",
            description="Identifier for the unit of measure from dm+d",
        ),
        bigquery.SchemaField(
            "uom_name",
            "STRING",
            description="Name of the unit of measure from dm+d",
        ),
        bigquery.SchemaField(
            "normalised_uom_id",
            "STRING",
            description="Identifier for the normalised unit of measure",
        ),
        bigquery.SchemaField(
            "normalised_uom_name",
            "STRING",
            description="Name of the normalised unit of measure",
        ),
        bigquery.SchemaField(
            "quantity",
            "FLOAT",
            description="Total quantity in the unit of measure",
        ),
        bigquery.SchemaField(
            "normalised_quantity",
            "FLOAT",
            description="Total quantity in basis units",
        ),
        bigquery.SchemaField("indicative_cost", "FLOAT", description="Indicative cost"),
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
            "year_month", "DATE", description="Year and month of the data"
        ),
        bigquery.SchemaField(
            "file_type", "STRING", description="File type (provisional, wip, finalised)"
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
        bigquery.SchemaField("unit", "STRING", description="Unit"),
        bigquery.SchemaField("basis", "STRING", description="Basis"),
        bigquery.SchemaField(
            "conversion_factor", "FLOAT", description="Conversion factor"
        ),
        bigquery.SchemaField("unit_id", "STRING", description="Unit ID"),
        bigquery.SchemaField("basis_id", "STRING", description="Basis ID"),
    ],
)

ORG_AE_STATUS_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=ORG_AE_STATUS_TABLE_ID,
    description="Indicator for whether an organisation has had A&E attendances Type 1 by month",
    schema=[
        bigquery.SchemaField(
            "ods_code", "STRING", description="ODS code of the organisation"
        ),
        bigquery.SchemaField(
            "period", "DATE", description="Start date of the month, YYYY-MM-DD"
        ),
        bigquery.SchemaField(
            "has_ae",
            "BOOLEAN",
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
            "vmp_code", "STRING", description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField(
            "vmp_code_prev", "STRING", description="Previous VMP code"
        ),
        bigquery.SchemaField("vmp_name", "STRING", description="VMP name"),
        bigquery.SchemaField(
            "vtm", "STRING", description="Virtual Therapeutic Moiety (VTM) code"
        ),
        bigquery.SchemaField("vtm_name", "STRING", description="VTM name"),
        bigquery.SchemaField("df_ind", "STRING", description="Dose form indicator"),
        bigquery.SchemaField("udfs", "FLOAT", description="Unit dose form size"),
        bigquery.SchemaField(
            "udfs_uom", "STRING", description="Unit dose form size unit of measure"
        ),
        bigquery.SchemaField(
            "unit_dose_uom", "STRING", description="Unit dose unit of measure"
        ),
        bigquery.SchemaField("dform_form", "STRING", description="Dose form"),
        bigquery.SchemaField(
            "ingredients",
            "RECORD",
            mode="REPEATED",
            description="Ingredients information",
            fields=[
                bigquery.SchemaField(
                    "ing_code", "STRING", description="Ingredient code"
                ),
                bigquery.SchemaField(
                    "ing_name", "STRING", description="Ingredient name"
                ),
                bigquery.SchemaField(
                    "strnt_nmrtr_val", "FLOAT", description="Strength numerator value"
                ),
                bigquery.SchemaField(
                    "strnt_nmrtr_uom_name",
                    "STRING",
                    description="Strength numerator unit of measure",
                ),
                bigquery.SchemaField(
                    "strnt_dnmtr_val", "FLOAT", description="Strength denominator value"
                ),
                bigquery.SchemaField(
                    "strnt_dnmtr_uom_name",
                    "STRING",
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
                    "ontformroute_cd", "STRING", description="Route code"
                ),
                bigquery.SchemaField(
                    "ontformroute_descr", "STRING", description="Route description"
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
            "vmp_code", "STRING", description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField("bnf_code", "STRING", description="BNF code"),
        bigquery.SchemaField("atc_code", "STRING", description="ATC code"),
        bigquery.SchemaField("ddd", "FLOAT", description="DDD"),
        bigquery.SchemaField("ddd_uom", "STRING", description="DDD unit"),
    ],
)

WHO_ATC_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=WHO_ATC_TABLE_ID,
    description="ATC information from the WHO",
    schema=[
        bigquery.SchemaField("atc_code", "STRING", description="ATC code"),
        bigquery.SchemaField("atc_name", "STRING", description="ATC name"),
        bigquery.SchemaField("comment", "STRING", description="ATC level"),
    ],
)

WHO_DDD_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=WHO_DDD_TABLE_ID,
    description="DDD information for ATC codes from the WHO",
    schema=[
        bigquery.SchemaField("atc_code", "STRING", description="ATC code"),
        bigquery.SchemaField("ddd", "FLOAT", description="DDD"),
        bigquery.SchemaField("ddd_unit", "STRING", description="DDD unit"),
        bigquery.SchemaField(
            "adm_code", "STRING", description="Route of administration code"
        ),
        bigquery.SchemaField("comment", "STRING", description="Comment"),
    ],
)

WHO_ROUTES_OF_ADMINISTRATION_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=WHO_ROUTES_OF_ADMINISTRATION_TABLE_ID,
    description="Routes of administration information from the WHO. Manually generated.",
    schema=[
        bigquery.SchemaField("who_route_code", "STRING", description="WHO route code"),
        bigquery.SchemaField(
            "who_route_description", "STRING", description="WHO route description"
        ),
    ],
)

ADM_ROUTE_MAPPING_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=ADM_ROUTE_MAPPING_TABLE_ID,
    description="Mapping between dm+d ontformroute and WHO routes of administration. Manually generated.",
    schema=[
        bigquery.SchemaField("dmd_ontformroute", "STRING", description="dm+d route"),
        bigquery.SchemaField("who_route", "STRING", description="WHO route"),
    ],
)

DOSE_TABLE_SPEC = TableSpec(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=DOSE_TABLE_ID,
    description="SCMD quantity converted to doses, using basis unit conversions for calculations",
    schema=[
        bigquery.SchemaField(
            "year_month", "DATE", description="Year and month of the data"
        ),
        bigquery.SchemaField(
            "vmp_code", "STRING", description="SNOMED code for the VMP from dm+d"
        ),
        bigquery.SchemaField(
            "vmp_name", "STRING", description="Product name from dm+d"
        ),
        bigquery.SchemaField("ods_code", "STRING", description="ODS code"),
        bigquery.SchemaField("ods_name", "STRING", description="Organisation name"),
        bigquery.SchemaField("scmd_quantity", "FLOAT", description="Original SCMD quantity"),
        bigquery.SchemaField("scmd_quantity_unit_name", "STRING", description="Original SCMD quantity unit"),
        bigquery.SchemaField(
            "scmd_basis_unit",
            "STRING",
            description="Basis unit for the SCMD quantity",
        ),
        bigquery.SchemaField(
            "scmd_basis_unit_name",
            "STRING",
            description="Unit of measure name for SCMD quantity",
        ),
        bigquery.SchemaField(
            "scmd_quantity_in_basis_units",
            "FLOAT",
            description="SCMD quantity converted to basis units",
        ),
        bigquery.SchemaField("udfs", "FLOAT", description="Unit dose form size"),
        bigquery.SchemaField("udfs_uom", "STRING", description="Unit dose form size unit of measure"),
        bigquery.SchemaField(
            "udfs_basis_quantity", "FLOAT", description="Unit dose form size converted to basis units"
        ),
        bigquery.SchemaField(
            "udfs_basis_uom", "STRING", description="Basis unit for the unit dose form size"
        ),
        bigquery.SchemaField(
            "unit_dose_uom", "STRING", description="Unit dose unit of measure"
        ),
        bigquery.SchemaField(
            "unit_dose_basis_uom", "STRING", description="Basis unit for the unit dose"
        ),
        bigquery.SchemaField(
            "dose_quantity", "FLOAT", description="Calculated number of doses"
        ),
        bigquery.SchemaField(
            "dose_unit", "STRING", description="Unit of measure for the dose"
        ),
        bigquery.SchemaField("df_ind", "STRING", description="Dose form indicator"),
        bigquery.SchemaField(
            "logic", "STRING", description="Logic used for dose calculation including unit conversion details"
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
            "vmp_code", "STRING", description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField(
            "year_month", "DATE", description="Year and month of the data"
        ),
        bigquery.SchemaField("ods_code", "STRING", description="ODS code"),
        bigquery.SchemaField("ods_name", "STRING", description="Organisation name"),
        bigquery.SchemaField("vmp_name", "STRING", description="VMP name from dm+d"),
        bigquery.SchemaField(
            "converted_quantity", "FLOAT", description="Converted quantity from SCMD"
        ),
        bigquery.SchemaField(
            "quantity_basis", "STRING", description="Unit of measure ID for the quantity"
        ),
        bigquery.SchemaField(
            "quantity_basis_name", "STRING", description="Unit of measure name for the quantity"
        ),
        bigquery.SchemaField(
            "ingredients",
            "RECORD",
            mode="REPEATED",
            description="Ingredient quantities",
            fields=[
                bigquery.SchemaField(
                    "ingredient_code", "STRING", description="Ingredient code"
                ),
                bigquery.SchemaField(
                    "ingredient_name", "STRING", description="Ingredient name"
                ),
                bigquery.SchemaField(
                    "ingredient_quantity",
                    "FLOAT",
                    description="Calculated ingredient quantity",
                ),
                bigquery.SchemaField(
                    "ingredient_unit",
                    "STRING",
                    description="Unit of measure for the ingredient",
                ),
                bigquery.SchemaField(
                    "ingredient_quantity_basis",
                    "FLOAT",
                    description="Calculated ingredient quantity in basis units",
                ),
                bigquery.SchemaField(
                    "ingredient_basis_unit",
                    "STRING",
                    description="Basis unit for the ingredient",
                ),
                bigquery.SchemaField(
                    "strength_numerator_value",
                    "FLOAT",
                    description="Strength numerator value",
                ),
                bigquery.SchemaField(
                    "strength_numerator_unit",
                    "STRING",
                    description="Strength numerator unit",
                ),
                bigquery.SchemaField(
                    "strength_denominator_value",
                    "FLOAT",
                    description="Strength denominator value",
                ),
                bigquery.SchemaField(
                    "strength_denominator_unit",
                    "STRING",
                    description="Strength denominator unit",
                ),
                bigquery.SchemaField(
                    "quantity_to_denominator_conversion_factor",
                    "FLOAT",
                    description="Conversion factor from quantity units to denominator units",
                ),
                bigquery.SchemaField(
                    "denominator_basis_unit",
                    "STRING",
                    description="Basis unit for the denominator",
                ),
                bigquery.SchemaField(
                    "calculation_logic",
                    "STRING",
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
            "vmp_code", "STRING", description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField(
            "year_month", "DATE", description="Year and month of the data"
        ),
        bigquery.SchemaField("ods_code", "STRING", description="ODS code"),
        bigquery.SchemaField("vmp_name", "STRING", description="VMP name"),
        bigquery.SchemaField("uom", "STRING", description="Unit of measure identifier"),
        bigquery.SchemaField("uom_name", "STRING", description="Unit of measure name"),
        bigquery.SchemaField(
            "quantity", "FLOAT", description="Total quantity in the unit of measure"
        ),
        bigquery.SchemaField(
            "ddd_quantity", "FLOAT", description="Calculated number of DDDs"
        ),
        bigquery.SchemaField("ddd_value", "FLOAT", description="DDD value"),
        bigquery.SchemaField("ddd_unit", "STRING", description="DDD unit"),
        bigquery.SchemaField(
            "calculation_explanation",
            "STRING",
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
            "vmp_code", "STRING", description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField("vmp_name", "STRING", description="VMP name"),
        bigquery.SchemaField(
            "uoms",
            "RECORD",
            mode="REPEATED",
            description="Units of measure from SCMD data",
            fields=[
                bigquery.SchemaField(
                    "uom_id", "STRING", description="Unit of measure identifier"
                ),
                bigquery.SchemaField(
                    "uom_name", "STRING", description="Unit of measure name"
                ),
                bigquery.SchemaField(
                    "basis_id", "STRING", description="Basis unit identifier"
                ),
                bigquery.SchemaField(
                    "basis_name", "STRING", description="Basis unit name"
                ),
            ],
        ),
        bigquery.SchemaField(
            "atcs",
            "RECORD",
            mode="REPEATED",
            description="ATC codes",
            fields=[
                bigquery.SchemaField("atc_code", "STRING", description="ATC code"),
                bigquery.SchemaField("atc_name", "STRING", description="ATC name"),
            ],
        ),
        bigquery.SchemaField(
            "ontformroutes",
            "RECORD",
            mode="REPEATED",
            description="Routes of administration",
            fields=[
                bigquery.SchemaField(
                    "ontformroute_cd", "STRING", description="Route code"
                ),
                bigquery.SchemaField(
                    "ontformroute_descr", "STRING", description="Route description"
                ),
                bigquery.SchemaField(
                    "who_route_code",
                    "STRING",
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
                bigquery.SchemaField("ddd", "FLOAT", description="DDD value"),
                bigquery.SchemaField("ddd_unit", "STRING", description="DDD unit"),
                bigquery.SchemaField(
                    "ddd_route_code",
                    "STRING",
                    description="Route of administration code",
                ),
                bigquery.SchemaField(
                    "ddd_comment",
                    "STRING",
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
                    "ingredient_code", "STRING", description="Ingredient code"
                ),
                bigquery.SchemaField(
                    "ingredient_name", "STRING", description="Ingredient name"
                ),
                bigquery.SchemaField(
                    "ingredient_unit", "STRING", description="Ingredient unit"
                ),
                bigquery.SchemaField(
                    "ingredient_basis_unit",
                    "STRING",
                    description="Basis unit for the ingredient",
                ),
            ],
        ),
        bigquery.SchemaField(
            "can_calculate_ddd",
            "BOOLEAN",
            description="Flag indicating whether DDD can be calculated for this VMP",
        ),
        bigquery.SchemaField(
            "ddd_calculation_logic",
            "STRING",
            description="Explanation of the DDD calculation logic or issue (for both successful and failed calculations)",
        ),
        bigquery.SchemaField(
            "selected_ddd_value",
            "FLOAT",
            description="The DDD value that was selected for calculations",
        ),
        bigquery.SchemaField(
            "selected_ddd_unit",
            "STRING",
            description="The unit of the selected DDD value",
        ),
        bigquery.SchemaField(
            "selected_ddd_basis_unit",
            "STRING",
            description="The basis unit for the selected DDD value",
        ),
        bigquery.SchemaField(
            "selected_ddd_route_code",
            "STRING",
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
            "vmp_code", "STRING", description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField("vmp_name", "STRING", description="VMP name"),
        bigquery.SchemaField("vtm_code", "STRING", description="Virtual Therapeutic Moiety (VTM) code"),
        bigquery.SchemaField("vtm_name", "STRING", description="VTM name"),
        bigquery.SchemaField(
            "ingredients",
            "RECORD",
            mode="REPEATED",
            description="Ingredients information",
            fields=[
                bigquery.SchemaField(
                    "ingredient_code", "STRING", description="Ingredient code"
                ),
                bigquery.SchemaField(
                    "ingredient_name", "STRING", description="Ingredient name"
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
                    "route_code", "STRING", description="Route code"
                ),
                bigquery.SchemaField(
                    "route_name", "STRING", description="Route name"
                ),
            ],
        ),
        bigquery.SchemaField(
            "atcs",
            "RECORD",
            mode="REPEATED",
            description="ATC codes and names",
            fields=[
                bigquery.SchemaField("atc_code", "STRING", description="ATC code"),
                bigquery.SchemaField("atc_name", "STRING", description="ATC name"),
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
            description="Virtual Medicinal Product (VMP) code"
        ),
        bigquery.SchemaField(
            "vmp_name", 
            "STRING", 
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
                    description="Unit identifier"
                ),
                bigquery.SchemaField(
                    "unit_name", 
                    "STRING", 
                    description="Unit name"
                ),
            ],
        ),
        bigquery.SchemaField(
            "chosen_unit_id", 
            "STRING", 
            description="The chosen standard unit identifier"
        ),
        bigquery.SchemaField(
            "chosen_unit_name", 
            "STRING", 
            description="The chosen standard unit name"
        ),
        bigquery.SchemaField(
            "conversion_logic", 
            "STRING", 
            description="Explanation of how to convert from other units to the chosen unit"
        ),
        bigquery.SchemaField(
            "conversion_factor",
            "FLOAT",
            description="Numerical factor to convert from the original unit to the chosen unit"
        ),
    ],
    cluster_fields=["vmp_code"],
)
