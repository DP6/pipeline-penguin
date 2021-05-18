from pipeline_penguin.exceptions import WrongTypeReference
import pytest

from unittest.mock import MagicMock
import pandas as pd
from pipeline_penguin.data_premise.sql import (
    DataPremiseSQLCheckLogicalComparisonWithValue,
)


@pytest.fixture
def _mock_data_node_with_passed_validation(monkeypatch):
    def mock_data_node():
        class MockDataNode:
            project_id = "project_test"
            dataset_id = "dataset_test"
            table_id = "table_test"

            def get_connector(self, *args, **kwargs):
                connector_mock = MagicMock()
                connector_mock.run = MagicMock(
                    return_value=pd.DataFrame([], columns=["result"])
                )
                return connector_mock

        return MockDataNode()

    yield mock_data_node


@pytest.fixture
def _mock_data_node_with_failed_validation(monkeypatch):
    def mock_data_node():
        class MockDataNode:
            project_id = "project_test"
            dataset_id = "dataset_test"
            table_id = "table_test"

            def get_connector(self, *args, **kwargs):
                connector_mock = MagicMock()
                connector_mock.run = MagicMock(
                    return_value=pd.DataFrame([20, 10, 50], columns=["result"])
                )
                return connector_mock

        return MockDataNode()

    yield mock_data_node


class TestDataPremiseSQLCheckIsNull:
    def test_instance_type(self, _mock_data_node_with_passed_validation):
        data_node = _mock_data_node_with_passed_validation()
        data_premise = DataPremiseSQLCheckLogicalComparisonWithValue(
            "test_name", data_node, "test_column", "<", 100
        )
        assert isinstance(data_premise, DataPremiseSQLCheckLogicalComparisonWithValue)

    def test_passing_validate(self, _mock_data_node_with_passed_validation):
        data_node = _mock_data_node_with_passed_validation()
        data_premise = DataPremiseSQLCheckLogicalComparisonWithValue(
            "test_name", data_node, "test_column", "<", 100
        )
        output = data_premise.validate()
        assert output.pass_validation == True
        assert output.failed_count == 0

    def test_failing_validate(self, _mock_data_node_with_failed_validation):
        data_node = _mock_data_node_with_failed_validation()
        data_premise = DataPremiseSQLCheckLogicalComparisonWithValue(
            "test_name", data_node, "test_column", "<", 100
        )
        output = data_premise.validate()
        assert output.pass_validation == False
        assert output.failed_count == 3

    def test_return_query_args(self, _mock_data_node_with_passed_validation):
        data_node = _mock_data_node_with_passed_validation()
        data_premise = DataPremiseSQLCheckLogicalComparisonWithValue(
            "test_name", data_node, "test_column", "<", 100
        )
        args = data_premise.query_args()

        assert args == {
            "project": "project_test",
            "dataset": "dataset_test",
            "table": "table_test",
            "column": "test_column",
            "operator": "<",
            "value": 100,
        }

    def test_raise_error_when_given_wrong_operator(
        self, _mock_data_node_with_passed_validation
    ):

        data_node = _mock_data_node_with_passed_validation()

        with pytest.raises(WrongTypeReference):
            data_premise = DataPremiseSQLCheckLogicalComparisonWithValue(
                "test_name", data_node, "test_column", "G", 100
            )
