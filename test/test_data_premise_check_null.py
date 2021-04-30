import pytest

from unittest.mock import MagicMock
import pandas as pd
from pipeline_penguin.data_premise.sql import DataPremiseSQLCheckNull
from pipeline_penguin.core.data_node.data_node import DataNode
from pipeline_penguin.core.connector.connector import Connector
from pipeline_penguin.data_node.sql.bigquery import DataNodeBigQuery


@pytest.fixture
def _mock_data_node_with_passed_validation(monkeypatch):
    def mock_function(self, type):
        connector_mock = Connector()
        connector_mock.run = MagicMock(return_value=pd.DataFrame())
        return connector_mock

    monkeypatch.setattr(DataNodeBigQuery, "get_connector", mock_function)


@pytest.fixture
def _mock_data_node_with_failed_validation(monkeypatch):
    def mock_function(self, type):
        connector_mock = Connector()
        connector_mock.run = MagicMock(return_value=pd.DataFrame([1, 2, 3]))
        return connector_mock

    monkeypatch.setattr(DataNodeBigQuery, "get_connector", mock_function)


class TestDataPremiseSQLCheckNull:
    def test_instance_type(self, _mock_data_node_with_passed_validation):
        data_node = DataNodeBigQuery(
            "mock_node",
            "project_test",
            "dataset_test",
            "table_test",
            "service_account.json",
        )
        data_premise = DataPremiseSQLCheckNull("test_name", data_node, "test_column")
        assert isinstance(data_premise, DataPremiseSQLCheckNull)

    def test_passing_validate(self, _mock_data_node_with_passed_validation):
        data_node = DataNodeBigQuery(
            "mock_node",
            "project_test",
            "dataset_test",
            "table_test",
            "service_account.json",
        )
        data_premise = DataPremiseSQLCheckNull("test_name", data_node, "test_column")
        output = data_premise.validate()
        assert output.pass_validation is True

    def test_failing_validate(self, _mock_data_node_with_failed_validation):
        data_node = DataNodeBigQuery(
            "mock_node",
            "project_test",
            "dataset_test",
            "table_test",
            "service_account.json",
        )
        data_premise = DataPremiseSQLCheckNull("test_name", data_node, "test_column")
        output = data_premise.validate()
        assert output.pass_validation is False
