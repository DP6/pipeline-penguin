import pytest

from pipeline_penguin.data_node_bigquery import DataNodeBigQuery
from pipeline_penguin.exceptions import NodeManagerMissingArgs
from pipeline_penguin.node_manager import NodeManager


@pytest.fixture()
def bigquery_args():
    yield {
        "name": "bq_test",
        "project_id": "teste",
        "dataset_id": "dataset_test",
        "table_id": "table",
        "service_account_json": "service_account.json",
    }


@pytest.fixture()
def bigquery_missing_args():
    yield {
        "name": "bq_test",
        "table_id": "table",
        "service_account_json": "service_account.json",
    }


class TestNodeManager:

    def test_if_creates_bigquery_node_with_node_manager_with_right_args(self, bigquery_args):
        node_manager = NodeManager()
        data_node = node_manager.create_bigquery_node(name="Pipeline X - Table Y", args=bigquery_args)

        assert isinstance(data_node, DataNodeBigQuery)

        bigquery_args.update({"source": "BigQuery"})
        assert dict(data_node.__dict__.items()) == bigquery_args

    def test_if_raises_exception_with_node_manager_is_called_without_right_args(self, bigquery_missing_args):
        node_manager = NodeManager()

        with pytest.raises(NodeManagerMissingArgs) as b:
            node_manager.create_bigquery_node(name="Pipeline X - Table Y", args=bigquery_missing_args)

        expected_message = "__init__() missing 2 required positional arguments: 'project_id' and 'dataset_id'"
        assert str(b.value) == expected_message

