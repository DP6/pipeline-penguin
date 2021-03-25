from pipeline_penguin.connector import Connector
from pipeline_penguin.connector_sql import ConnectorSQL
from pipeline_penguin.connector_sql_bigquery import ConnectorSQLBigQuery

from google.oauth2.service_account import Credentials
from os import path
import pandas as pd

import pytest


@pytest.fixture
def mock_from_service_account_file(monkeypatch):
    def mock_function(credentials_path):
        return

    monkeypatch.setattr(Credentials, "from_service_account_file", mock_function)


@pytest.fixture
def mock_pandas_read_gbq(monkeypatch):
    def mock_function(query, credentials, max_results):
        return pd.DataFrame([i for i in range(max_results)])

    monkeypatch.setattr(pd, "read_gbq", mock_function)


@pytest.fixture
def mock_isfile(monkeypatch):
    def mock_function(path):
        return path == "true_file.json"

    monkeypatch.setattr(path, "isfile", mock_function)


class TestConnectorSQLBigQuery:
    def test_instance_class_type(self, mock_isfile):
        assert isinstance(
            ConnectorSQLBigQuery(credentials_path="true_file.json"),
            ConnectorSQLBigQuery,
        )

    def test_instance_superclass_type(self, mock_isfile):
        assert isinstance(
            ConnectorSQLBigQuery(credentials_path="true_file.json"),
            ConnectorSQL,
        )

    def test_connector_type(self, mock_isfile):
        conn = ConnectorSQLBigQuery(credentials_path="true_file.json")
        assert conn.type == "SQL"

    def test_connector_source(self, mock_isfile):
        conn = ConnectorSQLBigQuery(credentials_path="true_file.json")
        assert conn.source == "BigQuery"

    def test_invalid_filename(self, monkeypatch, mock_isfile):
        with pytest.raises(FileNotFoundError):
            conn = ConnectorSQLBigQuery(credentials_path="fake_file.json")


class TestConnectorSQLBigQueryRunExecution:
    def test_returns_dataframe(
        self, mock_isfile, mock_from_service_account_file, mock_pandas_read_gbq
    ):
        conn = ConnectorSQLBigQuery(credentials_path="true_file.json")
        result = conn.run("SELECT * FROM `project.dataset.table`")
        assert isinstance(result, pd.DataFrame)

    def test_default_dataframe_size(
        self, mock_isfile, mock_from_service_account_file, mock_pandas_read_gbq
    ):
        conn = ConnectorSQLBigQuery(credentials_path="true_file.json")
        result = conn.run("SELECT * FROM `project.dataset.table`")
        assert result.size == 1000

    def test_override_default_dataframe_size(
        self, mock_isfile, mock_from_service_account_file, mock_pandas_read_gbq
    ):
        conn = ConnectorSQLBigQuery(credentials_path="true_file.json", max_rows=50)
        result = conn.run("SELECT * FROM `project.dataset.table`")
        assert result.size == 50

    def test_override_dataframe_size(
        self, mock_isfile, mock_from_service_account_file, mock_pandas_read_gbq
    ):
        conn = ConnectorSQLBigQuery(credentials_path="true_file.json")
        result = conn.run("SELECT * FROM `project.dataset.table`", max_rows=50)
        assert result.size == 50
