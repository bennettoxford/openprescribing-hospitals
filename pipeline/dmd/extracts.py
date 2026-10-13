from dataclasses import dataclass
from typing import Mapping

from google.cloud import bigquery

from pipeline.setup.table_spec import TableSpec


@dataclass(frozen=True)
class Column:
    xml_tag: str
    name: str
    field_type: str
    mode: str
    description: str


@dataclass(frozen=True)
class Extract:
    key: str
    table_id: str
    description: str
    cluster_field: str | None
    xml_prefix: str | None
    record_tag: str | None
    columns: tuple[Column, ...]
    lookup_parent: str | None = None


def _column(
    xml_tag: str,
    name: str,
    field_type: str,
    mode: str,
    description: str,
) -> Column:
    return Column(xml_tag, name, field_type, mode, description)


XML_EXTRACTS: tuple[Extract, ...] = (
    Extract(
        key="vmp",
        table_id="dmd_vmp",
        description="Raw dm+d virtual medicinal products from TRUD item 24",
        cluster_field="vmp_code",
        xml_prefix="f_vmp2_",
        record_tag="VMP",
        columns=(
            _column("VPID", "vmp_code", "STRING", "REQUIRED", "VMP identifier (VPID)"),
            _column("VTMID", "vtm_code", "STRING", "NULLABLE", "VTM identifier (VTMID)"),
            _column("NM", "vmp_name", "STRING", "REQUIRED", "VMP name (NM)"),
            _column("DF_INDCD", "df_ind_code", "STRING", "REQUIRED", "Dose form indicator code (DF_INDCD)"),
            _column("UDFS", "udfs", "FLOAT", "NULLABLE", "Unit dose form size (UDFS)"),
            _column(
                "UDFS_UOMCD",
                "udfs_uom_code",
                "STRING",
                "NULLABLE",
                "Unit dose form size unit of measure code (UDFS_UOMCD)",
            ),
            _column(
                "UNIT_DOSE_UOMCD",
                "unit_dose_uom_code",
                "STRING",
                "NULLABLE",
                "Unit dose unit of measure code (UNIT_DOSE_UOMCD)",
            ),
            _column(
                "INVALID",
                "invalid",
                "BOOL",
                "REQUIRED",
                "True when dm+d marks the VMP invalid (INVALID = 1)",
            ),
        ),
    ),
    Extract(
        key="vpi",
        table_id="dmd_vpi",
        description="Raw dm+d virtual product ingredients from TRUD item 24",
        cluster_field="vmp_code",
        xml_prefix="f_vmp2_",
        record_tag="VPI",
        columns=(
            _column("VPID", "vmp_code", "STRING", "REQUIRED", "VMP identifier (VPID)"),
            _column("ISID", "ing_code", "STRING", "REQUIRED", "Ingredient identifier (ISID)"),
            _column(
                "BASIS_STRNTCD",
                "basis_strnt_code",
                "STRING",
                "NULLABLE",
                "Basis of strength code (BASIS_STRNTCD)",
            ),
            _column(
                "BS_SUBID",
                "bs_subid",
                "STRING",
                "NULLABLE",
                "Basis of strength substance identifier (BS_SUBID)",
            ),
            _column(
                "STRNT_NMRTR_VAL",
                "strnt_nmrtr_val",
                "FLOAT",
                "NULLABLE",
                "Strength numerator value",
            ),
            _column(
                "STRNT_NMRTR_UOMCD",
                "strnt_nmrtr_uom_code",
                "STRING",
                "NULLABLE",
                "Strength numerator unit of measure code (STRNT_NMRTR_UOMCD)",
            ),
            _column(
                "STRNT_DNMTR_VAL",
                "strnt_dnmtr_val",
                "FLOAT",
                "NULLABLE",
                "Strength denominator value",
            ),
            _column(
                "STRNT_DNMTR_UOMCD",
                "strnt_dnmtr_uom_code",
                "STRING",
                "NULLABLE",
                "Strength denominator unit of measure code (STRNT_DNMTR_UOMCD)",
            ),
        ),
    ),
    Extract(
        key="ont",
        table_id="dmd_ont",
        description="Raw dm+d ontology form and route links from TRUD item 24",
        cluster_field="vmp_code",
        xml_prefix="f_vmp2_",
        record_tag="ONT",
        columns=(
            _column("VPID", "vmp_code", "STRING", "REQUIRED", "VMP identifier (VPID)"),
            _column("FORMCD", "form_code", "STRING", "REQUIRED", "Ontology form and route code (FORMCD)"),
        ),
    ),
    Extract(
        key="dform",
        table_id="dmd_dform",
        description="Raw dm+d dose form links from TRUD item 24",
        cluster_field="vmp_code",
        xml_prefix="f_vmp2_",
        record_tag="DFORM",
        columns=(
            _column("VPID", "vmp_code", "STRING", "REQUIRED", "VMP identifier (VPID)"),
            _column("FORMCD", "form_code", "STRING", "REQUIRED", "Dose form code (FORMCD)"),
        ),
    ),
    Extract(
        key="amp",
        table_id="dmd_amp",
        description="Raw dm+d actual medicinal products from TRUD item 24",
        cluster_field="vmp_code",
        xml_prefix="f_amp2_",
        record_tag="AMP",
        columns=(
            _column("APID", "amp_code", "STRING", "REQUIRED", "AMP identifier (APID)"),
            _column("VPID", "vmp_code", "STRING", "REQUIRED", "VMP identifier (VPID)"),
            _column("DESC", "description", "STRING", "REQUIRED", "AMP description (DESC)"),
            _column(
                "AVAIL_RESTRICTCD",
                "avail_restrict_code",
                "STRING",
                "REQUIRED",
                "Availability restriction code (AVAIL_RESTRICTCD)",
            ),
        ),
    ),
    Extract(
        key="ing",
        table_id="dmd_ing",
        description="Raw dm+d ingredients from TRUD item 24",
        cluster_field="ing_code",
        xml_prefix="f_ingredient2_",
        record_tag="ING",
        columns=(
            _column("ISID", "ing_code", "STRING", "REQUIRED", "Ingredient identifier (ISID)"),
            _column("NM", "ing_name", "STRING", "REQUIRED", "Ingredient name (NM)"),
        ),
    ),
    Extract(
        key="vtm",
        table_id="dmd_vtm",
        description="Raw dm+d virtual therapeutic moieties from TRUD item 24",
        cluster_field="vtm_code",
        xml_prefix="f_vtm2_",
        record_tag="VTM",
        columns=(
            _column("VTMID", "vtm_code", "STRING", "REQUIRED", "VTM identifier (VTMID)"),
            _column("NM", "vtm_name", "STRING", "REQUIRED", "VTM name (NM)"),
            _column(
                "INVALID",
                "invalid",
                "BOOL",
                "REQUIRED",
                "True when dm+d marks the VTM invalid (INVALID = 1)",
            ),
        ),
    ),
    Extract(
        key="unit_of_measure",
        table_id="dmd_unit_of_measure",
        description="Raw dm+d unit of measure lookup from TRUD item 24",
        cluster_field="uom_code",
        xml_prefix="f_lookup2_",
        record_tag="INFO",
        lookup_parent="UNIT_OF_MEASURE",
        columns=(
            _column("CD", "uom_code", "STRING", "REQUIRED", "Unit of measure code (CD)"),
            _column("CDPREV", "uom_code_prev", "STRING", "NULLABLE", "Previous unit of measure code (CDPREV)"),
            _column("CDDT", "change_date", "DATE", "NULLABLE", "Date the code became valid (CDDT)"),
            _column("DESC", "description", "STRING", "REQUIRED", "Unit of measure description (DESC)"),
        ),
    ),
    Extract(
        key="form",
        table_id="dmd_form",
        description="Raw dm+d dose form lookup from TRUD item 24",
        cluster_field="form_code",
        xml_prefix="f_lookup2_",
        record_tag="INFO",
        lookup_parent="FORM",
        columns=(
            _column("CD", "form_code", "STRING", "REQUIRED", "Form code (CD)"),
            _column("CDPREV", "form_code_prev", "STRING", "NULLABLE", "Previous form code (CDPREV)"),
            _column("CDDT", "change_date", "DATE", "NULLABLE", "Date the code became valid (CDDT)"),
            _column("DESC", "description", "STRING", "REQUIRED", "Form description (DESC)"),
        ),
    ),
    Extract(
        key="ont_form_route",
        table_id="dmd_ont_form_route",
        description="Raw dm+d ontology form and route lookup from TRUD item 24",
        cluster_field="ont_form_route_code",
        xml_prefix="f_lookup2_",
        record_tag="INFO",
        lookup_parent="ONT_FORM_ROUTE",
        columns=(
            _column("CD", "ont_form_route_code", "STRING", "REQUIRED", "Ontology form and route code (CD)"),
            _column(
                "DESC",
                "description",
                "STRING",
                "REQUIRED",
                "Ontology form and route description (DESC)",
            ),
        ),
    ),
    Extract(
        key="availability_restriction",
        table_id="dmd_availability_restriction",
        description="Raw dm+d availability restriction lookup from TRUD item 24",
        cluster_field="avail_restrict_code",
        xml_prefix="f_lookup2_",
        record_tag="INFO",
        lookup_parent="AVAILABILITY_RESTRICTION",
        columns=(
            _column("CD", "avail_restrict_code", "STRING", "REQUIRED", "Availability restriction code (CD)"),
            _column(
                "DESC",
                "description",
                "STRING",
                "REQUIRED",
                "Availability restriction description (DESC)",
            ),
        ),
    ),
    Extract(
        key="df_indicator",
        table_id="dmd_df_indicator",
        description="Raw dm+d dose form indicator lookup from TRUD item 24",
        cluster_field="df_ind_code",
        xml_prefix="f_lookup2_",
        record_tag="INFO",
        lookup_parent="DF_INDICATOR",
        columns=(
            _column("CD", "df_ind_code", "STRING", "REQUIRED", "Dose form indicator code (CD)"),
            _column("DESC", "description", "STRING", "REQUIRED", "Dose form indicator description (DESC)"),
        ),
    ),
)

RELEASE_EXTRACT = Extract(
    key="release",
    table_id="dmd_release",
    description="TRUD item 24 release last loaded into the raw dm+d tables",
    cluster_field=None,
    xml_prefix=None,
    record_tag=None,
    columns=(
        _column("", "archive_file_name", "STRING", "REQUIRED", "TRUD archive file name"),
        _column("", "release_date", "DATE", "REQUIRED", "TRUD release date"),
        _column("", "sha256", "STRING", "REQUIRED", "SHA-256 checksum of the archive"),
        _column("", "imported_at", "TIMESTAMP", "REQUIRED", "Time the archive was loaded"),
    ),
)

ALL_EXTRACTS: tuple[Extract, ...] = (*XML_EXTRACTS, RELEASE_EXTRACT)

TABLE_IDS: dict[str, str] = {
    f"DMD_{extract.key.upper()}_TABLE_ID": extract.table_id for extract in ALL_EXTRACTS
}


def map_cell(column: Column, values: Mapping[str, str]):
    raw = values.get(column.xml_tag)
    if column.field_type == "FLOAT":
        if raw is None:
            return None
        return float(raw)
    if column.field_type == "BOOL":
        return raw == "1"
    if raw is None and column.mode == "REQUIRED":
        raise KeyError(column.xml_tag)
    return raw


def map_row(extract: Extract, values: Mapping[str, str]) -> dict:
    return {column.name: map_cell(column, values) for column in extract.columns}


def extracts_by_prefix() -> dict[str, tuple[Extract, ...]]:
    grouped: dict[str, list[Extract]] = {}
    for extract in XML_EXTRACTS:
        grouped.setdefault(extract.xml_prefix, []).append(extract)
    return {prefix: tuple(items) for prefix, items in grouped.items()}


def matching_extract(
    extracts: tuple[Extract, ...],
    parent: str | None,
    tag: str | None,
) -> Extract | None:
    found = None
    for extract in extracts:
        if extract.record_tag != tag:
            continue
        if extract.lookup_parent is not None and extract.lookup_parent != parent:
            continue
        if found is not None:
            raise ValueError(f"More than one extract matches parent {parent} and tag {tag}")
        found = extract
    return found


def table_spec(extract: Extract, project_id: str, dataset_id: str) -> TableSpec:
    return TableSpec(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=extract.table_id,
        description=extract.description,
        schema=[
            bigquery.SchemaField(
                column.name,
                column.field_type,
                mode=column.mode,
                description=column.description,
            )
            for column in extract.columns
        ],
        cluster_fields=[extract.cluster_field] if extract.cluster_field else None,
    )


def raw_specs(project_id: str, dataset_id: str) -> dict[str, TableSpec]:
    return {
        extract.key: table_spec(extract, project_id, dataset_id)
        for extract in ALL_EXTRACTS
    }
