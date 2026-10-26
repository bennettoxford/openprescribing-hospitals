from dataclasses import dataclass
from typing import List

from google.cloud import bigquery


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
