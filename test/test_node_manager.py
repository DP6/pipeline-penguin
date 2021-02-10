import pytest

from pipeline_penguin import NodeType
from pipeline_penguin.data_node_bigquery import DataNodeBigQuery
from pipeline_penguin.exceptions import NodeManagerMissingCorrectArgs, NodeTypeNotFound
from pipeline_penguin.node_manager import NodeManager


@pytest.fixture()
def bigquery_args():
    yield {
        "project_id": "teste",
        "dataset_id": "dataset_test",
        "table_id": "table",
        "service_account_json": "service_account.json",
    }


@pytest.fixture()
def bigquery_missing_args():
    yield {
        "table_id": "table",
        "service_account_json": "service_account.json",
    }


class TestCreateNode:
    def test_if_creates_bigquery_node_with_node_manager_with_right_args(
        self, bigquery_args
    ):
        node_manager = NodeManager()
        data_node = node_manager.create_node(
            name="Pipeline X - Table Y",
            node_type=NodeType.BIG_QUERY,
            args=bigquery_args,
        )

        assert isinstance(data_node, DataNodeBigQuery)

        bigquery_args.update({"source": "BigQuery"})
        assert dict(data_node.__dict__.items()) == bigquery_args

    def test_if_raises_exception_with_node_manager_is_called_without_right_args(
        self, bigquery_missing_args
    ):
        node_manager = NodeManager()

        with pytest.raises(NodeManagerMissingCorrectArgs) as b:
            node_manager.create_node(
                name="Pipeline X - Table Y",
                node_type=NodeType.BIG_QUERY,
                args=bigquery_missing_args,
            )

        expected_message = "__init__() missing 2 required positional arguments: 'project_id' and 'dataset_id'"
        assert str(b.value) == expected_message

    def test_if_raises_exception_with_node_type_was_not_correct(self, bigquery_args):
        node_manager = NodeManager()

        with pytest.raises(NodeTypeNotFound) as b:
            node_manager.create_node(
                name="Pipeline X - Table Y",
                node_type="wrong_type",
                args=bigquery_args,
            )

        expected_message = "'DataNode should be of type NodeType'"
        assert str(b.value) == expected_message

    def test_if_raises_attribute_error_when_try_to_use_nodes_dict_directly(
        self, bigquery_args
    ):
        node_manager = NodeManager()

        bigquery_args.update({"name": "Direct Node"})
        node = DataNodeBigQuery(**bigquery_args)

        with pytest.raises(AttributeError):
            node_manager.nodes.update({"Direct Node": DataNodeBigQuery})

    def test_if_node_manager_is_instantiated_only_one_time(self):
        node_manager_instance_1 = NodeManager()
        node_manager_instance_2 = NodeManager()

        assert node_manager_instance_1 == node_manager_instance_2


class TestGetNode:
    def test_if_get_correct_node(self, bigquery_args):
        node_manager = NodeManager()
        node_manager.create_node(
            name="Pipeline X - Table Y",
            node_type=NodeType.BIG_QUERY,
            args=bigquery_args,
        )
        second_data_node = node_manager.create_node(
            name="Pipeline Z - Table K",
            node_type=NodeType.BIG_QUERY,
            args=bigquery_args,
        )

        node = node_manager.get_node(name="Pipeline Z - Table K")

        assert node == second_data_node

    def test_if_returns_none_if_data_node_was_not_found(self, bigquery_args):
        node_manager = NodeManager()
        node_manager.create_node(
            name="Pipeline X - Table Y",
            node_type=NodeType.BIG_QUERY,
            args=bigquery_args,
        )
        node_manager.create_node(
            name="Pipeline Z - Table K",
            node_type=NodeType.BIG_QUERY,
            args=bigquery_args,
        )

        node = node_manager.get_node(name="Pipeline Random")

        assert node is None


class TestListNodes:
    def test_if_list_all_nodes_created(self, bigquery_args):
        node_manager = NodeManager()
        node_manager.create_node(
            name="Pipeline X - Table Y",
            node_type=NodeType.BIG_QUERY,
            args=bigquery_args,
        )
        node_manager.create_node(
            name="Pipeline Z - Table K",
            node_type=NodeType.BIG_QUERY,
            args=bigquery_args,
        )

        nodes = node_manager.list_nodes()

        expected = ["Pipeline X - Table Y", "Pipeline Z - Table K"]

        assert nodes == expected


class TestRemoveNode:
    def test_if_remove_node_if_name_was_found(self, bigquery_args):
        node_manager = NodeManager()
        node_manager.create_node(
            name="Pipeline X - Table Y",
            node_type=NodeType.BIG_QUERY,
            args=bigquery_args,
        )
        node_manager.create_node(
            name="Pipeline Z - Table K",
            node_type=NodeType.BIG_QUERY,
            args=bigquery_args,
        )

        nodes = node_manager.list_nodes()
        expected_before_remove = ["Pipeline X - Table Y", "Pipeline Z - Table K"]

        assert nodes == expected_before_remove

        node_manager.remove_node(name="Pipeline Z - Table K")

        nodes = node_manager.list_nodes()
        expected_after_remove = ["Pipeline X - Table Y"]

        assert nodes == expected_after_remove

    def test_if_remove_node_action_let_nodes_in_the_same_way_if_name_was_not_found(
        self, bigquery_args
    ):
        node_manager = NodeManager()
        node_manager.create_node(
            name="Pipeline X - Table Y",
            node_type=NodeType.BIG_QUERY,
            args=bigquery_args,
        )
        node_manager.create_node(
            name="Pipeline Z - Table K",
            node_type=NodeType.BIG_QUERY,
            args=bigquery_args,
        )

        nodes = node_manager.list_nodes()
        expected_before_remove = ["Pipeline X - Table Y", "Pipeline Z - Table K"]

        assert nodes == expected_before_remove

        node_manager.remove_node(name="Pipeline Random")

        nodes = node_manager.list_nodes()
        assert nodes == expected_before_remove


class TestCopyNode:
    def test_if_copy_node_create_a_new_node(self, bigquery_args):
        node_manager = NodeManager()
        node_manager.create_node(
            name="Pipeline X - Table Y",
            node_type=NodeType.BIG_QUERY,
            args=bigquery_args,
        )
        node_manager.create_node(
            name="Pipeline Z - Table K",
            node_type=NodeType.BIG_QUERY,
            args=bigquery_args,
        )

        nodes = node_manager.list_nodes()
        expected_before_copy = ["Pipeline X - Table Y", "Pipeline Z - Table K"]

        assert nodes == expected_before_copy

        new_node = node_manager.copy_node(
            node="Pipeline Z - Table K", name="Pipeline New - Table New"
        )

        nodes = node_manager.list_nodes()
        expected_after_copy = [
            "Pipeline X - Table Y",
            "Pipeline Z - Table K",
            "Pipeline New - Table New",
        ]

        assert nodes == expected_after_copy

    def test_if_new_copy_node_is_a_different_data_node_object_with_the_same_attributes(
        self, bigquery_args
    ):
        node_manager = NodeManager()
        node_manager.create_node(
            name="Pipeline X - Table Y",
            node_type=NodeType.BIG_QUERY,
            args=bigquery_args,
        )
        node = node_manager.create_node(
            name="Pipeline Z - Table K",
            node_type=NodeType.BIG_QUERY,
            args=bigquery_args,
        )

        new_node = node_manager.copy_node(
            node="Pipeline Z - Table K", name="Pipeline New - Table New"
        )

        assert node != new_node
        assert dict(node.__dict__.items()) == dict(new_node.__dict__.items())
