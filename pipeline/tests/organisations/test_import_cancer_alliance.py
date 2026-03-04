import pandas as pd
from unittest.mock import MagicMock, patch

from pipeline.organisations.import_cancer_alliance import (
    enrich_organisations_with_cancer_alliance,
    fetch_ods_ca_mapping_from_gcs,
)


class TestFetchOdsCaMappingFromGcs:
    @patch("pipeline.organisations.import_cancer_alliance.storage.Client")
    @patch("pipeline.organisations.import_cancer_alliance.get_bigquery_client")
    def test_fetch_ods_ca_mapping_parses_csv(self, mock_bq_client, mock_storage_client):
        csv_content = b"ods_code,ods_name,cancer_alliance_name,cancer_alliance_code,notes\n"
        csv_content += b"R0B,South Tyneside Trust,Northern Cancer Alliance,N69,\n"
        csv_content += b"RNN,North Cumbria Trust,Lancashire Alliance,N76,\n"

        mock_blob = MagicMock()
        mock_blob.download_as_bytes.return_value = csv_content
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage_client.return_value.bucket.return_value = mock_bucket
        mock_bq_client.return_value.project = "test"
        mock_bq_client.return_value._credentials = MagicMock()

        result = fetch_ods_ca_mapping_from_gcs()

        assert result["R0B"] == {
            "cancer_alliance_code": "N69",
            "cancer_alliance_name": "Northern Cancer Alliance",
            "notes": None,
        }
        assert result["RNN"] == {
            "cancer_alliance_code": "N76",
            "cancer_alliance_name": "Lancashire Alliance",
            "notes": None,
        }
        assert len(result) == 2

    @patch("pipeline.organisations.import_cancer_alliance.storage.Client")
    @patch("pipeline.organisations.import_cancer_alliance.get_bigquery_client")
    def test_fetch_skips_rows_with_missing_ca(self, mock_bq_client, mock_storage_client):
        csv_content = b"ods_code,ods_name,cancer_alliance_name,cancer_alliance_code,notes\n"
        csv_content += b"R0B,Trust A,Northern Alliance,N69,\n"
        csv_content += b"X99,Trust B,,\n"

        mock_blob = MagicMock()
        mock_blob.download_as_bytes.return_value = csv_content
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage_client.return_value.bucket.return_value = mock_bucket
        mock_bq_client.return_value.project = "test"
        mock_bq_client.return_value._credentials = MagicMock()

        result = fetch_ods_ca_mapping_from_gcs()

        assert result["R0B"] == {
            "cancer_alliance_code": "N69",
            "cancer_alliance_name": "Northern Alliance",
            "notes": None,
        }
        assert "X99" not in result
        assert len(result) == 1

    @patch("pipeline.organisations.import_cancer_alliance.storage.Client")
    @patch("pipeline.organisations.import_cancer_alliance.get_bigquery_client")
    def test_fetch_includes_notes_column(self, mock_bq_client, mock_storage_client):
        csv_content = b"ods_code,ods_name,cancer_alliance_name,cancer_alliance_code,notes\n"
        csv_content += b"RCD,Harrogate Trust,West Yorkshire Alliance,N63,Also partners with Humber\n"

        mock_blob = MagicMock()
        mock_blob.download_as_bytes.return_value = csv_content
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage_client.return_value.bucket.return_value = mock_bucket
        mock_bq_client.return_value.project = "test"
        mock_bq_client.return_value._credentials = MagicMock()

        result = fetch_ods_ca_mapping_from_gcs()

        assert result["RCD"] == {
            "cancer_alliance_code": "N63",
            "cancer_alliance_name": "West Yorkshire Alliance",
            "notes": "Also partners with Humber",
        }


class TestEnrichOrganisationsWithCancerAlliance:
    def test_enrich_adds_cancer_alliance_columns(self):
        df = pd.DataFrame([
            {"ods_code": "R0B", "ods_name": "South Tyneside Trust"},
            {"ods_code": "X99", "ods_name": "Unknown Trust"},
        ])
        ods_ca_mapping = {
            "R0B": {
                "cancer_alliance_code": "N69",
                "cancer_alliance_name": "Northern Cancer Alliance",
                "notes": "Some note",
            },
        }

        result = enrich_organisations_with_cancer_alliance(df, ods_ca_mapping)

        assert "cancer_alliance_code" in result.columns
        assert "cancer_alliance" in result.columns
        assert "notes" in result.columns
        assert result.iloc[0]["cancer_alliance_code"] == "N69"
        assert result.iloc[0]["cancer_alliance"] == "Northern Cancer Alliance"
        assert result.iloc[0]["notes"] == "Some note"
        assert result.iloc[1]["cancer_alliance_code"] is None
        assert result.iloc[1]["cancer_alliance"] is None
        assert result.iloc[1]["notes"] is None

    def test_enrich_uses_mapping_for_all_matched_orgs(self):
        df = pd.DataFrame([
            {"ods_code": "R0B", "ods_name": "Trust A"},
            {"ods_code": "RNN", "ods_name": "Trust B"},
        ])
        ods_ca_mapping = {
            "R0B": {
                "cancer_alliance_code": "N69",
                "cancer_alliance_name": "Northern Cancer Alliance",
                "notes": None,
            },
            "RNN": {
                "cancer_alliance_code": "N76",
                "cancer_alliance_name": "Lancashire Alliance",
                "notes": "Partners with Humber",
            },
        }

        result = enrich_organisations_with_cancer_alliance(df, ods_ca_mapping)

        assert result.iloc[0]["cancer_alliance_code"] == "N69"
        assert result.iloc[0]["notes"] is None
        assert result.iloc[1]["cancer_alliance_code"] == "N76"
        assert result.iloc[1]["notes"] == "Partners with Humber"
