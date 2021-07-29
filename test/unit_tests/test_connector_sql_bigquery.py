import pytest

from google.oauth2.service_account import Credentials
from os import path
import pandas as pd

from pipeline_penguin.core.connector import ConnectorSQL
from pipeline_penguin.connector.sql.bigquery import ConnectorSQLBigQuery


@pytest.fixture
def mock_from_service_account_file(monkeypatch):
    monkeypatch.setattr(Credentials, "from_service_account_file", lambda path: None)


@pytest.fixture
def mock_pandas_read_gbq(monkeypatch):
    def mock_function(query, credentials, max_results, project_id):
        return pd.DataFrame([i for i in range(max_results)])

    monkeypatch.setattr(pd, "read_gbq", mock_function)


@pytest.fixture
def mock_isfile(monkeypatch):
    def mock_function(path):
        return path == "true_file.json"

    monkeypatch.setattr(path, "isfile", mock_function)


class TestConnectorSQLBigQuery:
    def test_instance_class_type(self, mock_isfile, mock_from_service_account_file):
        assert isinstance(
            ConnectorSQLBigQuery(credentials_path="true_file.json"),
            ConnectorSQLBigQuery,
        )

    def test_instance_superclass_type(
        self, mock_isfile, mock_from_service_account_file
    ):
        assert isinstance(
            ConnectorSQLBigQuery(credentials_path="true_file.json"),
            ConnectorSQL,
        )

    def test_connector_type(self, mock_isfile, mock_from_service_account_file):
        conn = ConnectorSQLBigQuery(credentials_path="true_file.json")
        assert conn.type == "SQL"

    def test_connector_source(self, mock_isfile, mock_from_service_account_file):
        conn = ConnectorSQLBigQuery(credentials_path="true_file.json")
        assert conn.source == "BigQuery"

    def test_invalid_filename(self, monkeypatch, mock_isfile):
        with pytest.raises(FileNotFoundError):
            conn = ConnectorSQLBigQuery(credentials_path="fake_file.json")


class TestConnectorSQLBigQueryRunExecution:
    def test_returns_dataframe(
        self,
        mock_isfile,
        mock_from_service_account_file,
        mock_pandas_read_gbq,
    ):
        conn = ConnectorSQLBigQuery(credentials_path="true_file.json")
        result = conn.run("SELECT * FROM `project.dataset.table`")
        assert isinstance(result, pd.DataFrame)

    def test_default_dataframe_size(
        self,
        mock_isfile,
        mock_from_service_account_file,
        mock_pandas_read_gbq,
    ):
        conn = ConnectorSQLBigQuery(credentials_path="true_file.json")
        result = conn.run("SELECT * FROM `project.dataset.table`")
        assert result.size == 1000

    def test_override_default_dataframe_size(
        self,
        mock_isfile,
        mock_from_service_account_file,
        mock_pandas_read_gbq,
    ):
        conn = ConnectorSQLBigQuery(credentials_path="true_file.json", max_results=50)
        result = conn.run("SELECT * FROM `project.dataset.table`")
        assert result.size == 50

    def test_override_dataframe_size(
        self,
        mock_isfile,
        mock_from_service_account_file,
        mock_pandas_read_gbq,
    ):
        conn = ConnectorSQLBigQuery(credentials_path="true_file.json")
        result = conn.run("SELECT * FROM `project.dataset.table`", max_results=50)
        assert result.size == 50
