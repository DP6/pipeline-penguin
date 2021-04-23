from pipeline_penguin.core.data_node import DataNode, NodeType
from pipeline_penguin.data_node import DataNodeBigQuery
from pipeline_penguin.data_premise.sql.check_null import DataPremiseSQLCheckNull
from pipeline_penguin.core.data_premise.data_premise import DataPremise
import pytest
from os import path
from pipeline_penguin.exceptions import WrongTypeReference
from pipeline_penguin.connector.sql.bigquery import ConnectorSQLBigQuery
from pipeline_penguin.connector.connector_manager import ConnectorManager
import unittest.mock


@pytest.fixture()
def _mock_premise():
    def mock_premise():
        class MockPremise(DataPremise):
            type = "MOCK"

            def __init__(self, name, data_node, column):
                super().__init__(name, data_node)

        return MockPremise

    yield mock_premise


@pytest.fixture
def _mock_isfile(monkeypatch):
    def mock_function(path):
        return path == "true_file.json"

    monkeypatch.setattr(path, "isfile", mock_function)


@pytest.fixture
def _mock_connector_manager(monkeypatch):
    def mock_function(self, premise_type, source):
        return "ConnectorSQLBigQuery"

    monkeypatch.setattr(ConnectorManager, "get_default", mock_function)


class TestDataNodeBigQuery:
    def test_instance_superclass_type(self):
        assert isinstance(
            DataNodeBigQuery(
                "name_test",
                "project_test",
                "dataset_test",
                "table_test",
                "account_test",
            ),
            DataNode,
        )

    def test_node_type_reference(self):
        data_node = DataNodeBigQuery(
            "name_test",
            "project_test",
            "dataset_test",
            "table_test",
            "account_test",
        )
        assert data_node.source == NodeType.BIG_QUERY

    def test_insert_premise(self):
        data_node = DataNodeBigQuery(
            "name_test",
            "project_test",
            "dataset_test",
            "table_test",
            "account_test",
        )

        data_node.insert_premise(
            "check_nulls", DataPremiseSQLCheckNull, column="test_column"
        )

    def test_raises_error_when_inserting_unsupported_premise(self, _mock_premise):
        data_node = DataNodeBigQuery(
            "name_test",
            "project_test",
            "dataset_test",
            "table_test",
            "account_test",
        )

        with pytest.raises(WrongTypeReference):
            data_node.insert_premise(
                "check_nulls", _mock_premise(), column="test_column"
            )

    def test_get_internal_connector(self, _mock_isfile):
        data_node = DataNodeBigQuery(
            "name_test",
            "project_test",
            "dataset_test",
            "table_test",
            "account_test",
        )

        mock_connector = ConnectorSQLBigQuery("true_file.json")

        data_node.connectors["SQLBigQuery"] = mock_connector
        assert data_node.get_connector("SQL") == mock_connector

    def test_get_connector_from_manager(self, _mock_isfile, _mock_connector_manager):
        data_node = DataNodeBigQuery(
            "name_test",
            "project_test",
            "dataset_test",
            "table_test",
            "account_test",
        )
        assert data_node.get_connector("SQL") == "ConnectorSQLBigQuery"

    def test_raises_error_when_connector_is_not_found(self):
        data_node = DataNodeBigQuery(
            "name_test",
            "project_test",
            "dataset_test",
            "table_test",
            "account_test",
        )
        with pytest.raises(WrongTypeReference):
            data_node.get_connector("SQL")
