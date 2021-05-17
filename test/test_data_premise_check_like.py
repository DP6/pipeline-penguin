import pytest

from unittest.mock import MagicMock
import pandas as pd
from pipeline_penguin.data_premise.sql import DataPremiseSQLCheckLikePattern


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
                    return_value=pd.DataFrame(["abc", "def", "ghi"], columns=["result"])
                )
                return connector_mock

        return MockDataNode()

    yield mock_data_node


class TestDataPremiseSQLCheckIsNull:
    def test_instance_type(self, _mock_data_node_with_passed_validation):
        data_node = _mock_data_node_with_passed_validation()
        data_premise = DataPremiseSQLCheckLikePattern(
            "test_name", data_node, "test_column", "test_%"
        )
        assert isinstance(data_premise, DataPremiseSQLCheckLikePattern)

    def test_passing_validate(self, _mock_data_node_with_passed_validation):
        data_node = _mock_data_node_with_passed_validation()
        data_premise = DataPremiseSQLCheckLikePattern(
            "test_name", data_node, "test_column", "test_%"
        )
        output = data_premise.validate()
        assert output.pass_validation == True
        assert output.failed_count == 0

    def test_failing_validate(self, _mock_data_node_with_failed_validation):
        data_node = _mock_data_node_with_failed_validation()
        data_premise = DataPremiseSQLCheckLikePattern(
            "test_name", data_node, "test_column", "test_%"
        )
        output = data_premise.validate()
        assert output.pass_validation == False
        assert output.failed_count == 3

    def test_return_query_args(self, _mock_data_node_with_passed_validation):
        data_node = _mock_data_node_with_passed_validation()
        data_premise = DataPremiseSQLCheckLikePattern(
            "test_name", data_node, "test_column", "test_%"
        )
        args = data_premise.query_args()

        assert args == {
            "project": "project_test",
            "dataset": "dataset_test",
            "table": "table_test",
            "column": "test_column",
            "pattern": "test_%",
        }
