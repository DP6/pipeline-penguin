import pytest

from google.oauth2.service_account import Credentials
from os import path

from pipeline_penguin.core.connector import Connector
from pipeline_penguin.connector import ConnectorManager
from pipeline_penguin.connector.sql import ConnectorSQLBigQuery
from pipeline_penguin.exceptions import (
    ConnectorManagerMissingCorrectArgs,
    WrongTypeReference,
)


@pytest.fixture
def mock_from_service_account_file(monkeypatch):
    def mock_function(credentials_path):
        return

    monkeypatch.setattr(Credentials, "from_service_account_file", mock_function)


@pytest.fixture
def mock_isfile(monkeypatch):
    def mock_function(path):
        return path == "true_file.json"

    monkeypatch.setattr(path, "isfile", mock_function)


class TestDefineDefault:
    def test_if_set_connector(self, mock_isfile):
        connector_manager = ConnectorManager()
        connector = ConnectorSQLBigQuery(credentials_path="true_file.json")

        assert connector_manager.define_default(connector) is None


class TestGetDefault:
    def test_if_get_correct_connector(self, mock_isfile):
        connector_manager = ConnectorManager()
        connector = ConnectorSQLBigQuery(credentials_path="true_file.json")

        connector_manager.define_default(connector)
        second_connector = connector_manager.get_default(ConnectorSQLBigQuery)

        assert connector == second_connector

    def test_if_returns_none_if_connector_was_not_found(self, mock_isfile):
        connector_manager = ConnectorManager()

        connector = connector_manager.get_default(ConnectorSQLBigQuery)

        assert connector is None


class TestRemoveDefault:
    def test_if_remove_correct_connector(self, mock_isfile):
        connector_manager = ConnectorManager()
        connector = ConnectorSQLBigQuery(credentials_path="true_file.json")

        connector_manager.define_default(connector)
        second_connector = connector_manager.remove_default(ConnectorSQLBigQuery)

        assert connector == second_connector

    def test_if_connector_was_removed(self, mock_isfile):
        connector_manager = ConnectorManager()
        connector = ConnectorSQLBigQuery(credentials_path="true_file.json")

        connector_manager.define_default(connector)
        connector_manager.remove_default(ConnectorSQLBigQuery)

        assert connector_manager.get_default(ConnectorSQLBigQuery) is None

    def test_if_returns_none_if_connector_was_not_found(self, mock_isfile):
        connector_manager = ConnectorManager()

        assert connector_manager.remove_default(ConnectorSQLBigQuery) is None
