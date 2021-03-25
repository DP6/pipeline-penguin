from pipeline_penguin.connector import Connector
from pipeline_penguin.connector_sql import ConnectorSQL
from pipeline_penguin.connector_sql_bigquery import ConnectorSQLBigQuery

from google.oauth2.service_account import Credentials
from os import path
import pandas as pd

import pytest


@pytest.fixture
def mock_isfile(monkeypatch):
    def mock_result(path):
        return path == "true_file.json"

    monkeypatch.setattr(path, "isfile", mock_result)


class TestConnectorSQLBigQuery:
    def test_instance_class_type(self, mock_isfile):
        assert isinstance(
            ConnectorSQLBigQuery(credentials_path="true_file.json", max_rows=50),
            ConnectorSQLBigQuery,
        )

    def test_instance_superclass_type(self, mock_isfile):
        assert isinstance(
            ConnectorSQLBigQuery(credentials_path="true_file.json", max_rows=50),
            ConnectorSQL,
        )

    def test_connector_type(self, mock_isfile):
        conn = ConnectorSQLBigQuery(credentials_path="true_file.json", max_rows=50)
        assert conn.type == "SQL"

    def test_connector_source(self, mock_isfile):
        conn = ConnectorSQLBigQuery(credentials_path="true_file.json", max_rows=50)
        assert conn.source == "BigQuery"

    def test_invalid_filename(self, monkeypatch, mock_isfile):
        with pytest.raises(FileNotFoundError):
            conn = ConnectorSQLBigQuery(credentials_path="fake_file.json", max_rows=50)

    def test_connection_run_returns_dataframe(self, monkeypatch, mock_isfile):
        def mock_from_service_account_file(credentials_path):
            return

        def mock_pandas_read_gbq(query, credentials, max_results):
            return pd.DataFrame([i for i in range(50)])

        monkeypatch.setattr(
            Credentials, "from_service_account_file", mock_from_service_account_file
        )
        monkeypatch.setattr(pd, "read_gbq", mock_pandas_read_gbq)

        conn = ConnectorSQLBigQuery(credentials_path="true_file.json", max_rows=50)

        result = conn.run("SELECT * FROM `project.dataset.table`", 50)

        assert isinstance(result, pd.DataFrame)
        assert result.size == 50
